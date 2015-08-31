import asyncio

from .base import BaseStorage


class DummyStorage(BaseStorage):
    @asyncio.coroutine
    def create(self):
        pass

    @asyncio.coroutine
    def create_task(self, scraper_name, coro, args, kwargs):
        pass

    @asyncio.coroutine
    def clear_tasks(self, scraper_name):
        pass

    @asyncio.coroutine
    def get_task_count(self, scraper_name):
        return 0

    @asyncio.coroutine
    def get_pending_task_count(self, scraper_name):
        return 0

    @asyncio.coroutine
    def get_pending_tasks(self, scraper_name, instance):
        return []

    @asyncio.coroutine
    def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        pass

    @asyncio.coroutine
    def store_result(self, scraper_name, result_id, kind, result):
        pass

    @asyncio.coroutine
    def get_cached_content(self, cache_id):
        pass

    @asyncio.coroutine
    def set_cached_content(self, cache_id, url, content):
        pass

    @asyncio.coroutine
    def clear_cache(self):
        pass
