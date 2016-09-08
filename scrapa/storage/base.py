import hashlib
import json


class GeneratorWrapper(object):
    def __init__(self, gen):
        self.gen = gen

    def __iter__(self):
        yield from self.gen


class BaseStorage(object):
    def get_task_id(self, coro, args, kwargs):
        task_id = hashlib.md5()
        task_id.update(coro.__name__.encode('utf-8'))
        task_id.update(json.dumps(args, sort_keys=True).encode('utf-8'))
        dump_kwargs = {k: v for k, v in kwargs.items() if k not in coro.store_exclude}
        task_id.update(json.dumps(dump_kwargs, sort_keys=True).encode('utf-8'))
        return task_id.hexdigest()

    async def create(self):
        raise NotImplementedError

    async def store_task(self, scraper_name, coro, args, kwargs):
        """Return True if stored, False if already stored."""
        raise NotImplementedError

    async def clear_tasks(self, scraper_name):
        raise NotImplementedError

    async def get_task_count(self, scraper_name):
        raise NotImplementedError

    async def get_pending_task_count(self, scraper_name):
        raise NotImplementedError

    async def get_pending_tasks(self, scraper_name):
        raise NotImplementedError

    async def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        raise NotImplementedError

    async def has_result(self, scraper_name, result_id, kind):
        raise NotImplementedError

    async def get_result(self, scraper_name, result_id, kind):
        raise NotImplementedError

    async def store_result(self, scraper_name, result_id, kind, result):
        raise NotImplementedError

    async def get_cached_content(self, cache_id):
        raise NotImplementedError

    async def set_cached_content(self, cache_id, url, content):
        raise NotImplementedError

    async def clear_cache(self):
        raise NotImplementedError
