import asyncio
import hashlib
import json


class BaseStorage(object):
    def get_task_id(self, coro, args, kwargs):
        task_id = hashlib.md5()
        task_id.update(coro.__name__.encode('utf-8'))
        task_id.update(json.dumps(args, sort_keys=True).encode('utf-8'))
        task_id.update(json.dumps(kwargs, sort_keys=True).encode('utf-8'))
        return task_id.hexdigest()

    @asyncio.coroutine
    def create(self):
        raise NotImplementedError

    @asyncio.coroutine
    def store_task(self, scraper_name, coro, args, kwargs):
        raise NotImplementedError

    @asyncio.coroutine
    def clear_tasks(self, scraper_name):
        raise NotImplementedError

    @asyncio.coroutine
    def get_task_count(self, scraper_name):
        raise NotImplementedError

    @asyncio.coroutine
    def get_pending_task_count(self, scraper_name):
        raise NotImplementedError

    @asyncio.coroutine
    def get_pending_tasks(self, scraper_name, instance):
        raise NotImplementedError

    @asyncio.coroutine
    def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        raise NotImplementedError

    @asyncio.coroutine
    def store_result(self, scraper_name, result_id, kind, result):
        raise NotImplementedError

    @asyncio.coroutine
    def get_cached_content(self, cache_id):
        raise NotImplementedError

    @asyncio.coroutine
    def set_cached_content(self, cache_id, url, content):
        raise NotImplementedError

    @asyncio.coroutine
    def clear_cache(self):
        raise NotImplementedError
