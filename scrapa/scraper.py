import asyncio
from datetime import datetime, timedelta
import signal
from collections import Counter

from .cli import CommandLineMixin
from .config import ConfigurationMixin
from .logger import LoggingMixin, add_websocket_handler
from .queue import QueueMixin
from .request import RequestMixin
from .storage import StorageMixin


if not hasattr(asyncio, 'ensure_future'):
    asyncio.ensure_future = asyncio.async


class Scraper(ConfigurationMixin,
              CommandLineMixin,
              LoggingMixin,
              RequestMixin,
              QueueMixin,
              StorageMixin,
              object):
    def __init__(self, **kwargs):
        self.config_kwargs = kwargs

    async def progress(self):
        while True:
            if not self.finished():
                duration_seconds = (datetime.utcnow() - self.stats['start_time']).seconds
                tasks_per_second = 0
                if duration_seconds > 0:
                    tasks_per_second = self.stats['counter']['tasks_succeeded'] / duration_seconds
                eta = None
                if tasks_per_second > 0:
                    eta = timedelta(seconds=self.queue.qsize() / tasks_per_second)
                self.logger.info('{success}/{tried} tasks succeeded out of {queued} (running: {running}). ETA: {eta}'.format(
                     queued=self.queue.qsize(),
                     running=self.tasks_running,
                     success=self.stats['counter']['tasks_succeeded'],
                     tried=self.stats['counter']['tasks_tried'],
                     eta=eta
                ))
                self.log_progress({
                    'queue_size': self.queue.qsize(),
                    'task_count': self.tasks_running,
                    'consumer_count': self.consumer_count
                })
            if self.stopping:
                break
            if self.queue_finished():
                await self.queue_pending_tasks()
                if self.queue_finished():
                    break
            await asyncio.sleep(self.config.PROGRESS_INTERVAL)
        if self.stopping:
            self.logger.info('Stopping, cleaning up...')
        else:
            self.logger.info('All tasks finished, cleaning up...')
        await self.clean_up()

    async def clean_up(self):
        self.terminate_consumers = True
        for session in self._session_pool:
            if session is not None and not session.closed:
                session.close()
        if self.config.ENABLE_WEBSERVER:
            await self.websocket_handler.close_server()
        return None

    def start(self):
        raise NotImplementedError

    def scrape(self, **kwargs):
        self.init_configuration(kwargs)
        loop = asyncio.get_event_loop()

        self.add_signal_handler(loop)

        try:
            loop.run_until_complete(self.check_start(**kwargs))
        finally:
            loop.close()
            self.logger.info('Done.')

    async def check_start(self, start=False, clear=False, clear_cache=False, **kwargs):
        storage = await self.get_storage()
        task_count = await storage.get_task_count(self.config.NAME)
        if clear_cache:
            await storage.clear_cache()

        self.configure(**kwargs)

        if start or clear or task_count == 0:
            if clear and task_count > 0:
                self.logger.info('Deleting %s tasks...', task_count)
                await storage.clear_tasks(self.config.NAME)
            self.logger.info('Starting scraper from scratch')
            await self.run_start()
        else:
            pending_task_count = await storage.get_pending_task_count(self.config.NAME)
            if pending_task_count == 0:
                self.logger.info('All %d tasks are complete.', task_count)
                return
            self.logger.info('Resuming scraper with %d/%d pending tasks',
                             pending_task_count, task_count)
            await self.queue_pending_tasks()
            await self.run()

    async def run(self, start_coro=None):
        if self.config.ENABLE_WEBSERVER:
            self.websocket_handler = await add_websocket_handler(self.logger)

        consumers = []
        if self.config.ENABLE_QUEUE:
            self.terminate_consumers = False
            consumers = [self.consume_queue() for _ in range(self.config.CONSUMER_COUNT)]

        start = []
        if start_coro is not None:
            start = [self.run_task(start_coro)]

        self.stats = {'counter': Counter(), 'start_time': datetime.utcnow()}

        await asyncio.wait([self.progress()] + start + consumers)

    async def run_start(self):
        await self.run(start_coro=self.start)

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
