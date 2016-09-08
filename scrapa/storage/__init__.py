from .base import BaseStorage  # noqa
from .dummy import DummyStorage  # noqa
try:
    from .database import DatabaseStorage  # noqa
except ImportError:
    print('No sqlalchemy installed')
    pass

try:
    from .asyncpg import AsyncPostgresStorage  # noqa
except ImportError:
    print('No psycopg2 installed')
    pass


class StorageMixin():
    async def get_storage(self):
        if self.storage is None:
            self.storage = self.config.STORAGE
            await self.storage.create()
        return self.storage

    def storage_enabled(self, coro):
        return getattr(coro, 'store', False) and self.config.STORAGE_ENABLED

    async def store_task(self, coro, args, kwargs):
        storage = await self.get_storage()
        should_run = await storage.store_task(self.config.NAME, coro, args, kwargs)
        return should_run

    async def store_task_result(self, *args, **kwargs):
        storage = await self.get_storage()
        await storage.store_task_result(*args, **kwargs)

    async def has_result(self, result_id, kind):
        storage = await self.get_storage()
        return await storage.has_result(self.config.NAME, result_id, kind)

    async def get_result(self, result_id, kind):
        storage = await self.get_storage()
        return await storage.get_result(self.config.NAME, result_id, kind)

    async def store_result(self, result_id, kind, result):
        storage = await self.get_storage()
        await storage.store_result(self.config.NAME, result_id, kind, result)

    async def get_cached_content(self, cache_id):
        storage = await self.get_storage()
        result = await storage.get_cached_content(cache_id)
        return result

    async def set_cached_content(self, cache_id, url, response):
        content = await response.read()
        storage = await self.get_storage()
        await storage.set_cached_content(cache_id, url, content)
