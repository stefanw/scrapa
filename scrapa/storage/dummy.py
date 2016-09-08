from .base import BaseStorage


class DummyStorage(BaseStorage):
    async def create(self):
        pass

    async def store_task(self, scraper_name, coro, args, kwargs):
        return True

    async def clear_tasks(self, scraper_name):
        pass

    async def get_task_count(self, scraper_name):
        return 0

    async def get_pending_task_count(self, scraper_name):
        return 0

    async def get_pending_tasks(self, scraper_name):
        return []

    async def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        return False

    async def has_result(self, scraper_name, result_id, kind):
        return False

    async def get_result(self, scraper_name, result_id, kind):
        raise ValueError

    async def store_result(self, scraper_name, result_id, kind, result):
        return True

    async def get_cached_content(self, cache_id):
        pass

    async def set_cached_content(self, cache_id, url, content):
        pass

    async def clear_cache(self):
        pass
