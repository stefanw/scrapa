import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime
import json
import math
import signal
import sys
import traceback
from urllib.parse import urljoin, urlsplit, urlencode, parse_qsl, urlunsplit
import uuid

import aiohttp
from lxml import html

from .logger import make_logger, add_websocket_handler
from .exceptions import HttpConnectionError, HttpError
from .utils import (args_kwargs_iterator, add_func_to_iterator, get_cache_id,
                    json_dumps, json_loads)
from .config import ScrapaConfig, DefaultValue

if not hasattr(asyncio, 'ensure_future'):
    asyncio.ensure_future = asyncio.async


class Scraper(object):
    def __init__(self, **kwargs):
        self.config_kwargs = kwargs

    def init_configuration(self, config):
        proper_config = {}
        for key in (k for k in dir(ScrapaConfig) if not k.startswith('_')):
            val = config.get(key.lower(),
                    self.config_kwargs.get(key.lower(),
                        getattr(self, key, getattr(ScrapaConfig, key))
                    )
            )
            if isinstance(val, DefaultValue):
                val = val.get(self)
            proper_config[key] = val

        self.config = type('CustomScrapaConfig', (ScrapaConfig,), proper_config)

        self.stopping = False
        self.consumer_count = 0
        self.tasks_running = 0
        self.timeout_count = 0
        self.storage = None
        self.logger = make_logger(self.config.NAME, level=self.config.LOGLEVEL)

        self.queue = asyncio.Queue(self.config.QUEUE_SIZE)
        self.http_semaphore = asyncio.Semaphore(self.config.HTTP_CONCURENCY_LIMIT)
        self._session_pool = [None for _ in range(self.config.HTTP_CONCURENCY_LIMIT)]
        self._session_query_count = 0

    @asyncio.coroutine
    def request(self, method, url, **kwargs):
        url = self.get_full_url(url)
        session_arg = kwargs.pop('session', None)
        status_only = kwargs.pop('status_only', False)
        raise_for_status = kwargs.pop('raise_for_status', True)
        response = None
        error_msg = None
        req_uuid = str(uuid.uuid4())
        for retry_num in range(self.config.MAX_RETRIES):
            with (yield from self.http_semaphore):
                with self.use_request_session(session_arg) as session:
                    full_url = self.get_full_url(url, params=kwargs.get('params', {}))
                    try:
                        start_time = datetime.utcnow()
                        response = yield from asyncio.wait_for(
                            session.request(method, url, **kwargs),
                            self.config.CONNECT_TIMEOUT
                        )
                        if not status_only:
                            yield from response.read()
                    except asyncio.TimeoutError as e:
                        error_msg = 'Request timed out'
                        self.timeout_count += 1
                        if self.timeout_count > self.config.MAX_TIMEOUT_COUNT:
                            self.reset_session(session)
                    except aiohttp.ClientError as e:
                        error_msg = 'Request connection error: {}'.format(e)
                    except aiohttp.ServerDisconnectedError as e:
                        error_msg = 'Server disconnected error: {}'.format(e)
                    else:
                        error_msg = None
                        self.timeout_count = 0
                        break
                    finally:
                        self.log_request({
                            'req_uuid': req_uuid,
                            'method': method,
                            'kwargs': kwargs,
                            'session_id': id(session),
                            'url': full_url,
                            'status': response.status if response else None,
                            'retry': retry_num,
                            'message': error_msg,
                            'timestamp': start_time,
                            'duration': int((datetime.utcnow() - start_time).total_seconds() * 1000)
                        })
                        try:
                            if response is not None:
                                yield from response.release()
                        except (aiohttp.DisconnectedError, RuntimeError):
                            # Ignore disconnect errors on release
                            # Ignore pause_reading errors
                            pass
            if error_msg is not None:
                self.logger.warn(error_msg)
        if response is None:
            raise HttpConnectionError(error_msg)
        if raise_for_status:
            self.check_status(response, url)
        return response

    def check_status(self, response, url):
        http_error_msg = ''
        if 400 <= response.status < 500:
            http_error_msg = '%s Client Error for url: %s' % (response.status, url)

        elif 500 <= response.status < 600:
            http_error_msg = '%s Server Error for url: %s' % (response.status, url)

        if http_error_msg:
            response.close()
            raise HttpError(code=response.status, message=http_error_msg, headers=response.headers)

    def get_default_session_kwargs(self):
        return {
            'headers': self.get_default_session_headers()
        }

    def get_default_session_headers(self):
        return {'User-Agent': self.config.DEFAULT_USER_AGENT}

    def create_session(self, **kwargs):
        connector = self.get_connector()
        request_kwargs = self.get_default_session_kwargs()
        request_kwargs.update(kwargs)
        self.logger.debug('Creating new session with %s and %s', connector, request_kwargs)
        return aiohttp.ClientSession(connector=connector, **request_kwargs)

    @contextmanager
    def get_session(self, **kwargs):
        try:
            session = self.create_session(**kwargs)
            yield session
        finally:
            session.close()

    def reset_session(self, session):
        session.close()

    def get_session_from_pool(self):
        pool_size = len(self._session_pool)
        pool_index = self._session_query_count % pool_size
        session = self._session_pool[pool_index]

        if session is not None and getattr(session, '_use_count', 0) > self.config.REUSE_SESSION_COUNT:
            session.close()

        if session is None or session.closed:
            self._session_pool[pool_index] = self.create_session()
            session = self._session_pool[pool_index]
            session._use_count = 0
        return session

    @contextmanager
    def use_request_session(self, session=None):
        current_session = session or self.get_session_from_pool()
        try:
            yield current_session
        finally:
            self._session_query_count += 1
            if not hasattr(current_session, '_use_count'):
                current_session._use_count = 1
            else:
                current_session._use_count += 1

    def get_connector(self):
        if self.config.PROXY is not None:
            conn = aiohttp.connector.ProxyConnector(
                proxy=self.config.PROXY,
                conn_timeout=self.config.CONNECT_TIMEOUT,
                limit=self.config.CONNECTOR_LIMIT)
        else:
            conn = aiohttp.TCPConnector(
                verify_ssl=False,
                conn_timeout=self.config.CONNECT_TIMEOUT,
                limit=self.config.CONNECTOR_LIMIT
            )
        return conn

    @asyncio.coroutine
    def get(self, *args, **kwargs):
        response = yield from self.request('GET', *args, **kwargs)
        return response

    @asyncio.coroutine
    def get_text(self, url='', *args, **kwargs):
        response = None
        url = self.get_full_url(url)
        encoding = kwargs.pop('encoding', self.config.ENCODING)
        cache = kwargs.pop('cache', False)
        if cache:
            cache_id = get_cache_id(url, *args, **kwargs)
            cached_result = yield from self.get_cached_content(cache_id)
            if cached_result is not None:
                return cached_result
        response = yield from self.get(url, *args, **kwargs)
        text = yield from response.text(encoding=encoding)
        if cache:
            yield from self.set_cached_content(cache_id, response.url, text)
        return text

    @asyncio.coroutine
    def get_json(self, *args, **kwargs):
        text = yield from self.get_text(*args, **kwargs)
        return json.loads(text)

    @asyncio.coroutine
    def get_dom(self, *args, **kwargs):
        text = yield from self.get_text(*args, **kwargs)
        return html.fromstring(text)

    @asyncio.coroutine
    def post(self, *args, **kwargs):
        response = yield from self.request('POST', *args, **kwargs)
        return response

    @asyncio.coroutine
    def consume_queue(self):
        """ Use get_nowait construct until Py 3.4.4 """
        try:
            self.consumer_count += 1
            while True:
                try:
                    if self.finished():
                        return
                    coro, args, kwargs, meta = self.queue.get_nowait()
                    self.tasks_running += 1
                    try:
                        yield from self.run_task(coro, *args, **kwargs)
                    except Exception:
                        if self.storage_enabled(coro):
                            if meta is not None and meta.get('tried') < self.config.TASK_RETRY_COUNT:
                                yield from self.add_to_queue(coro, args, kwargs, meta)
                    finally:
                        self.tasks_running -= 1
                        if hasattr(self.queue, 'task_done'):
                            self.queue.task_done()
                except asyncio.QueueEmpty:
                    if self.finished():
                        return
                    yield from asyncio.sleep(0.5)
        finally:
            self.consumer_count -= 1

    @asyncio.coroutine
    def run_many(self, coro_arg, generator):
        generator = args_kwargs_iterator(generator)
        generator = add_func_to_iterator(coro_arg, generator)
        done, pending = yield from asyncio.wait(list(
            asyncio.ensure_future(self.run_one(coro, *args, **kwargs)) for coro, (args, kwargs) in generator
        ))
        assert len(pending) == 0
        return (d.result() for d in done)

    @asyncio.coroutine
    def run_one(self, coro, *args, **kwargs):
        if not asyncio.iscoroutinefunction(coro):
            raise Exception('Given task %s is not a coroutine! Decorate it with @scrapa.async', coro)
        result = yield from self.run_task(coro, *args, **kwargs)
        return result

    @asyncio.coroutine
    def run_task(self, coro, *args, **kwargs):
        failed = False
        done = False
        value = None
        exception = None
        try:
            self.tasks_running += 1
            result = (yield from coro(*args, **kwargs))
        except Exception as e:
            self.logger.exception(e)
            exception = traceback.format_exc(exception)
            failed = True
            raise e
        else:
            value = result
            done = True
            return result
        finally:
            self.tasks_running -= 1
            if self.storage_enabled(coro):
                yield from self.store_task_result(
                    self.config.NAME,
                    coro, args, kwargs,
                    done, failed,
                    value, exception
                )

    @asyncio.coroutine
    def schedule_many(self, coro_arg, generator):
        generator = args_kwargs_iterator(generator)
        generator = add_func_to_iterator(coro_arg, generator)
        for coro, (args, kwargs) in generator:
            yield from self.schedule_one(coro, *args, **kwargs)

    @asyncio.coroutine
    def schedule_one(self, coro, *args, **kwargs):
        if not asyncio.iscoroutinefunction(coro):
            raise Exception('Given task %s is not a coroutine! Decorate it with @scrapa.async', coro)
        should_run = yield from self.prepare_schedule(coro, args, kwargs)
        if should_run:
            yield from self.add_to_queue(coro, args, kwargs)

    @asyncio.coroutine
    def add_to_queue(self, coro, args, kwargs, meta=None):
        yield from self.queue.put((coro, args, kwargs, meta))

    @asyncio.coroutine
    def queue_pending_tasks(self):
        storage = yield from self.get_storage()
        tasks = yield from storage.get_pending_tasks(self.config.NAME)
        for task_dict in tasks:
            yield from self.add_to_queue(
                getattr(self, task_dict['task_name']),
                task_dict['args'],
                task_dict['kwargs'],
                meta=task_dict['meta']
            )

    @asyncio.coroutine
    def prepare_schedule(self, coro, args, kwargs):
        should_run = True
        if self.storage_enabled(coro):
            should_run = yield from self.store_task(coro, args, kwargs)
        return should_run

    @asyncio.coroutine
    def get_storage(self):
        if self.storage is None:
            self.storage = self.config.STORAGE
            yield from self.storage.create()
        return self.storage

    def storage_enabled(self, coro):
        return getattr(coro, 'scrapa_store', False) and self.config.STORAGE_ENABLED

    @asyncio.coroutine
    def store_task(self, coro, args, kwargs):
        storage = yield from self.get_storage()
        should_run = yield from storage.store_task(self.config.NAME, coro, args, kwargs)
        return should_run

    @asyncio.coroutine
    def store_task_result(self, *args, **kwargs):
        storage = yield from self.get_storage()
        yield from storage.store_task_result(*args, **kwargs)

    @asyncio.coroutine
    def store_result(self, result_id, kind, result):
        storage = yield from self.get_storage()
        yield from storage.store_result(self.config.NAME, result_id, kind, result)

    @asyncio.coroutine
    def get_cached_content(self, cache_id):
        storage = yield from self.get_storage()
        result = yield from storage.get_cached_content(cache_id)
        return result

    @asyncio.coroutine
    def set_cached_content(self, cache_id, url, content):
        storage = yield from self.get_storage()
        yield from storage.set_cached_content(cache_id, url, content)

    def finished(self):
        if self.stopping:
            return True
        return self.queue_finished()

    def queue_finished(self):
        qsize = self.queue.qsize()
        return qsize == 0 and self.tasks_running == 0

    @asyncio.coroutine
    def progress(self):
        while True:
            if not self.finished():
                self.logger.info('Tasks queued: %d, running: %d' % (
                                 self.queue.qsize(), self.tasks_running))
                self.log_progress({
                    'queue_size': self.queue.qsize(),
                    'task_count': self.tasks_running,
                    'consumer_count': self.consumer_count
                })
            if self.finished() and self.consumer_count == 0:
                yield from self.queue_pending_tasks()
                if self.finished():
                    break
            yield from asyncio.sleep(self.config.PROGRESS_INTERVAL)
        if self.stopping:
            self.logger.info('Stopping, cleaning up...')
        else:
            self.logger.info('All tasks finished, cleaning up...')
        yield from self.clean_up()

    @asyncio.coroutine
    def clean_up(self):
        for session in self._session_pool:
            if session is not None and not session.closed:
                session.close()
        if self.config.ENABLE_WEBSERVER:
            yield from self.websocket_handler.close_server()
        return None

    def get_full_url(self, url, params=None):
        if self.config.BASE_URL and not url.startswith(('http://', 'https://')):
            return urljoin(self.config.BASE_URL, url)
        if params is not None:
            url_parts = urlsplit(url)
            url_dict = vars(url_parts)
            qs_list = parse_qsl(url_dict['query'])
            qs_list += list(params.items())
            qs_string = urlencode(qs_list)
            url_dict['query'] = qs_string
            # url_dict is an OrderedDict, that's why this works
            url = urlunsplit(url_dict.values())
        return url

    def start(self):
        raise NotImplementedError

    def run_from_cli(self):
        args = self.get_command_line_args()
        getattr(self, args['command_name'])(**args)

    def get_command_line_args(self):
        parser = argparse.ArgumentParser(description='Scrapa arguments.')

        subparsers = parser.add_subparsers(dest="command_name")

        scraper = subparsers.add_parser('scrape')
        scraper.add_argument('--clear', dest='clear', action='store_true',
                           default=False,
                           help='Clear all stored tasks for this scraper')
        scraper.add_argument('--start', dest='start', action='store_true',
                           default=False,
                           help='Run start even if tasks are present')
        scraper.add_argument('--clear-cache', dest='clear_cache',
                            action='store_true',
                            default=False,
                            help='Clear the http cache')
        scraper.add_argument('--loglevel', dest='loglevel',
                            default=ScrapaConfig.LOGLEVEL,
                            help='Loglevel: one of DEBUG, INFO, WARN, ERROR.')

        dump_tasks = subparsers.add_parser('dump_tasks')
        dump_tasks.add_argument('-s', '--split', default=1, type=int,
                                help='Split tasks in number of files.')
        dump_tasks.add_argument('-o', '--output',
                                help='base name to use for output.')

        load_tasks = subparsers.add_parser('load_tasks')
        load_tasks.add_argument('-f', '--filename',
                                help='Filename to read from, defaults to stdin.')

        args = parser.parse_args()
        args = vars(args)

        if args.get('command_name') is None:
            args = scraper.parse_args()
            args = vars(args)
            args['command_name'] = 'scrape'

        return args

    def scrape(self, **kwargs):
        self.init_configuration(kwargs)
        loop = asyncio.get_event_loop()

        self.add_signal_handler(loop)

        try:
            loop.run_until_complete(self.check_start(**kwargs))
        finally:
            loop.close()
            self.logger.info('Done.')

    def dump_tasks(self, split=1, output=None, prefix='scrapa-', **kwargs):
        if output is None:
            output = prefix

        loop = asyncio.get_event_loop()
        storage = loop.run_until_complete(self.get_storage())
        total_count = loop.run_until_complete(
                        storage.get_pending_task_count(self.config.NAME))
        tasks = loop.run_until_complete(storage.get_pending_tasks(self.config.NAME))

        tasks_per_file = math.ceil(total_count / split)
        task_counter = 0
        outfile = None
        file_counter = 0
        for task_dict in tasks:
            if outfile is None:
                file_counter += 1
                filename = '{}{:03d}.json'.format(output, file_counter)
                outfile = open(filename, 'w')
            outfile.write(json_dumps({
                'scraper_name': self.config.NAME,
                'task_name': task_dict['task_name'],
                'args': task_dict['args'],
                'kwargs': task_dict['kwargs'],
                'meta': task_dict['kwargs']
            }, indent=None))
            outfile.write('\n')
            task_counter += 1
            if task_counter > tasks_per_file:
                outfile.close()
                outfile = None
                task_counter = 0

    def load_tasks(self, filename=None, **kwargs):
        loop = asyncio.get_event_loop()
        storage = loop.run_until_complete(self.get_storage())

        if filename is None:
            task_file = open(filename, encoding='utf-8')
        else:
            task_file = sys.stdin
        count = 0
        already = 0
        for line in task_file:
            task = json_loads(line)
            result = loop.run_until_complete(
                storage.store_task(
                    task['scraper_name'],
                    getattr(self, task['task_name']),
                    task['args'],
                    task['kwargs']
                ))
            count += 1
            if not result:
                already += 1
        print('Loaded {} tasks with {} already present.'.format(count, already))

    @asyncio.coroutine
    def check_start(self, start=False, clear=False, clear_cache=False, **kwargs):
        storage = yield from self.get_storage()
        task_count = yield from storage.get_task_count(self.config.NAME)
        if clear_cache:
            yield from storage.clear_cache()

        if start or clear or task_count == 0:
            if clear and task_count > 0:
                self.logger.info('Deleting %s tasks...', task_count)
                yield from storage.clear_tasks(self.config.NAME)
            self.logger.info('Starting scraper from scratch')
            yield from self.run_start()
        else:
            pending_task_count = yield from storage.get_pending_task_count(self.config.NAME)
            if pending_task_count == 0:
                self.logger.info('All %d tasks are complete.', task_count)
                return
            self.logger.info('Resuming scraper with %d/%d pending tasks',
                             pending_task_count, task_count)
            yield from self.queue_pending_tasks()
            yield from self.run()

    @asyncio.coroutine
    def run(self, start_coro=None):
        if self.config.ENABLE_WEBSERVER:
            self.websocket_handler = yield from add_websocket_handler(self.logger)

        consumers = []
        if self.config.ENABLE_QUEUE:
            consumers = [self.consume_queue() for _ in range(self.config.CONSUMER_COUNT)]

        start = []
        if start_coro is not None:
            start = [self.run_task(start_coro)]

        yield from asyncio.wait([self.progress()] + start + consumers)

    @asyncio.coroutine
    def run_start(self):
        yield from self.run(start_coro=self.start)

    def interrupt(self):
        loop = asyncio.get_event_loop()
        self.remove_signal_handler(loop)
        try:
            print('Tasks are still running, do you want to stop? [y, n]')
            yes_no = input()
            self.add_signal_handler(loop)
            if yes_no.lower() != 'y':
                self.logger.info('Continue...')
                return
            self.logger.info('Stopping...')
            self.stopping = True
        except KeyboardInterrupt:
            self.logger.info('Force shutdown!')
            self.stop()

    def stop(self):
        loop = asyncio.get_event_loop()
        loop.stop()

    def add_signal_handler(self, loop):
        for signame in ('SIGINT',):
            loop.add_signal_handler(getattr(signal, signame), self.interrupt)

    def remove_signal_handler(self, loop):
        for signame in ('SIGINT',):
            loop.remove_signal_handler(getattr(signal, signame))

    def log_request(self, info):
        self.logger.debug('request', extra={'scrapa': {
            'scraper': self.config.NAME,
            'type': 'request',
            'data': info
        }})

    def log_progress(self, info):
        self.logger.debug('progress', extra={'scrapa': {
            'scraper': self.config.NAME,
            'type': 'progress',
            'data': info
        }})
