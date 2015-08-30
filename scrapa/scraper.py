import asyncio
from contextlib import contextmanager
import signal
from urllib.parse import urljoin

import aiohttp
from lxml import html

from .logger import make_logger
from .exceptions import HTTPConnectionError, HTTPError
from .utils import args_kwargs_iterator, add_func_to_iterator

if not hasattr(asyncio, 'ensure_future'):
    asyncio.ensure_future = asyncio.async


class Scraper(object):
    PROGRESS_INTERVAL = 5
    CONNECTOR_LIMIT = 10
    HTTP_CONCURENCY_LIMIT = 10
    CONNECT_TIMEOUT = 30
    MAX_RETRIES = 3
    QUEUE_SIZE = 0
    CONSUMER_COUNT = 10
    REUSE_SESSION = True

    def __init__(self, name='scraper', encoding='utf-8', **kwargs):
        self.name = name
        self.encoding = encoding
        self.session = None
        self.reuse_session = self.REUSE_SESSION
        self.logger = make_logger(self.name)
        self.queue = asyncio.Queue(self.QUEUE_SIZE)
        self.consumer_count = 0
        self.tasks_running = 0
        self.connector_limit = kwargs.get('connector_limit',
                                          self.CONNECTOR_LIMIT)
        self.http_concurrency_limit = kwargs.get('http_concurrency_limit',
                                                 self.HTTP_CONCURENCY_LIMIT)
        self.http_semaphore = asyncio.Semaphore(self.http_concurrency_limit)

    @asyncio.coroutine
    def request(self, method, url, **kwargs):
        url = self.get_full_url(url)
        response = None
        for retry_num in range(self.MAX_RETRIES):
            with (yield from self.http_semaphore):
                with self.get_session() as session:
                    self.logger.info('GET %s', url)
                    try:
                        response = yield from asyncio.wait_for(
                            session.request(method, url, **kwargs),
                            self.CONNECT_TIMEOUT
                        )
                    except asyncio.TimeoutError as e:
                        error_msg = 'Request timed out'
                    except aiohttp.ClientError as e:
                        error_msg = 'Request connection error: {}'.format(e)
                    except aiohttp.ServerDisconnectedError as e:
                        error_msg = 'Server disconnected error: {}'.format(e)
                    else:
                        error_msg = None
                        break
                if error_msg is not None:
                    self.logger.error(error_msg)
        if response is None:
            raise HTTPConnectionError(error_msg)
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
            raise HTTPError(http_error_msg, response=self)

    def create_session(self):
        conn = self.get_connector()
        return aiohttp.ClientSession(connector=conn)

    @contextmanager
    def get_session(self):
        try:
            if self.session is None:
                self.session = self.create_session()
            yield self.session
        finally:
            if not self.reuse_session:
                self.session.close()
                self.session = None

    def get_connector(self):
        conn = aiohttp.TCPConnector(
            verify_ssl=False,
            conn_timeout=self.CONNECT_TIMEOUT,
            limit=self.connector_limit
        )
        return conn

    @asyncio.coroutine
    def get(self, *args, **kwargs):
        response = yield from self.request('GET', *args, **kwargs)
        return response

    @asyncio.coroutine
    def get_dom(self, *args, **kwargs):
        encoding = kwargs.pop('encoding', self.encoding)
        response = yield from self.get(*args, **kwargs)
        try:
            text = yield from response.text(encoding=encoding)
            return html.fromstring(text)
        finally:
            yield from response.release()

    @asyncio.coroutine
    def consume_queue(self):
        """ Use get_nowait construct until Py 3.4.4 """
        try:
            self.consumer_count += 1
            while True:
                try:
                    coro, args, kwargs = self.queue.get_nowait()
                    self.tasks_running += 1
                    yield from self.run_one(coro, *args, **kwargs)
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
        try:
            result = (yield from coro(*args, **kwargs))
        except Exception as e:
            self.logger.exception(e)
        else:
            return result

    @asyncio.coroutine
    def schedule_many(self, coro_arg, generator):
        generator = args_kwargs_iterator(generator)
        generator = add_func_to_iterator(coro_arg, generator)
        for coro, (args, kwargs) in generator:
            yield from self.schedule_one(coro, *args, **kwargs)

    @asyncio.coroutine
    def schedule_one(self, coro, *args, **kwargs):
        yield from self.queue.put((coro, args, kwargs))

    def finished(self):
        qsize = self.queue.qsize()
        return qsize == 0 and self.tasks_running == 0

    @asyncio.coroutine
    def progress(self):
        while True:
            yield from asyncio.sleep(self.PROGRESS_INTERVAL)
            if not self.finished():
                self.logger.info('Tasks queued: %d, running: %d' % (
                                 self.queue.qsize(), self.tasks_running))
            if self.finished() and self.consumer_count == 0:
                break
        self.logger.info('All tasks finished.')

    def get_full_url(self, url):
        if not url.startswith(('http://', 'https://')):
            return urljoin(self.BASE_URL, url)
        return url

    def start(self):
        raise NotImplementedError

    def interrupt(self):
        loop = asyncio.get_event_loop()
        self.remove_signal_handler(loop)
        try:
            print('Tasks are still running, do you want to abort? [y, n]')
            yes_no = input()
            if yes_no.lower() == 'n':
                self.add_signal_handler(loop)
                return
        except KeyboardInterrupt:
            pass
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

    def run(self):
        loop = asyncio.get_event_loop()

        self.add_signal_handler(loop)

        consumers = [self.consume_queue() for _ in range(self.CONSUMER_COUNT)]
        try:
            loop.run_until_complete(asyncio.wait([
                self.schedule_one(self.start),
                self.progress()
            ] + consumers))
        finally:
            if self.session is not None:
                self.session.close()
            loop.close()
