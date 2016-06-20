from datetime import datetime

from aiopg.sa import create_engine
import psycopg2
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable, CreateIndex

from .base import BaseStorage, GeneratorWrapper as GW
from ..utils import json_loads, json_dumps


metadata = sa.MetaData()

task_table = sa.Table('scrapa_task', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('scraper_name', sa.String(255)),
    sa.Column('task_id', sa.String(255)),
    sa.Column('name', sa.String(255)),
    sa.Column('args', sa.Text()),
    sa.Column('kwargs', sa.Text()),
    sa.Column('created', sa.DateTime),
    sa.Column('last_tried', sa.DateTime, nullable=True),
    sa.Column('tried', sa.Integer, default=0),
    sa.Column('done', sa.Boolean, default=False),
    sa.Column('failed', sa.Boolean, default=False),
    sa.Column('value', sa.Text, nullable=True),
    sa.Column('exception', sa.Text, nullable=True),
)

task_index = sa.Index('scrapa_task__scraper_name_task_id', task_table.c.scraper_name, task_table.c.task_id, unique=True)

result_table = sa.Table('scrapa_result', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('scraper_name', sa.String(255)),
    sa.Column('result_id', sa.String(1024)),
    sa.Column('kind', sa.String(255)),
    sa.Column('result', sa.Text),
)

result_index = sa.Index('scrapa_result__scraper_name_result_id_kind', result_table.c.scraper_name, result_table.c.result_id, result_table.c.kind, unique=True)

cache_table = sa.Table('scrapa_cache', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('cache_id', sa.String(255)),
    sa.Column('url', sa.String(1024)),
    sa.Column('created', sa.DateTime),
    sa.Column('content', sa.LargeBinary),
)

cache_index = sa.Index('scrapa_cache__cache_id', cache_table.c.cache_id, unique=True)


class AsyncPostgresStorage(BaseStorage):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    async def create(self):
        schema_name = self.kwargs.pop('schema_name', 'public')
        self.engine = await create_engine(**self.kwargs)
        async with self.engine.acquire() as conn:
            tables = ((task_table, task_index),
                      (cache_table, cache_index),
                      (result_table, result_index)
            )
            for table, index in tables:
                exists = await conn.scalar('''SELECT EXISTS (
                   SELECT 1
                   FROM   information_schema.tables
                   WHERE  table_schema = '{table_schema}'
                   AND    table_name = '{table_name}'
                );'''.format(table_schema=schema_name,
                             table_name=table.name))
                if not exists:
                    tr = await conn.begin()
                    create_statement = str(CreateTable(table).compile(self.engine))
                    create_statement = create_statement.replace('CREATE TABLE',
                            'CREATE TABLE IF NOT EXISTS')
                    await conn.execute(create_statement)
                    create_index = str(CreateIndex(index).compile(self.engine))
                    await conn.execute(create_index)
                    await tr.commit()

    async def store_task(self, scraper_name, coro, args, kwargs):
        task_id = self.get_task_id(coro, args, kwargs)
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(task_table.insert().values(
                    scraper_name=scraper_name,
                    task_id=task_id,
                    name=coro.__name__,
                    args=json_dumps(args),
                    kwargs=json_dumps(kwargs),
                    created=datetime.now(),
                    tried=0,
                    done=False,
                    failed=False,
                    value=None,
                    exception=None
                ))
                return True
        except psycopg2.IntegrityError:
            # Task already exists
            return False

    async def clear_tasks(self, scraper_name):
        async with self.engine.acquire() as conn:
            await conn.execute(
                task_table.delete(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                ))
            )

    async def get_task_count(self, scraper_name):
        async with self.engine.acquire() as conn:
            count = await conn.scalar(
                task_table.count(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                ))
            )
        return count

    async def get_pending_task_count(self, scraper_name):
        async with self.engine.acquire() as conn:
            count = await conn.scalar(
                task_table.count(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                    task_table.c.done == False  # noqa
                ))
            )
        return count

    async def get_pending_tasks(self, scraper_name):
        async with self.engine.acquire() as conn:
            result = await conn.execute(
                task_table.select().where(
                    sa.and_(
                        task_table.c.scraper_name == scraper_name,
                        task_table.c.done == False  # noqa
                    )
                )
            )
        return GW({
            'task_name': task.name,
            'args': json_loads(task.args),
            'kwargs': json_loads(task.kwargs),
            'meta': {'tried': task.tried}
        } for task in result)

    async def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        task_id = self.get_task_id(coro, args, kwargs)
        async with self.engine.acquire() as conn:
            await conn.execute(
                task_table.update().where(
                    sa.and_(
                        task_table.c.scraper_name == scraper_name,
                        task_table.c.task_id == task_id
                    )
                ).values(**{'done': done, 'failed': failed,
                        'last_tried': datetime.now(), 'value': json_dumps(value),
                        'exception': exception, 'tried': task_table.c.tried + 1})
            )

    async def has_result(self, scraper_name, result_id, kind):
        query = sa.and_(
            result_table.c.scraper_name == scraper_name,
            result_table.c.kind == kind,
            result_table.c.result_id == result_id
        )
        async with self.engine.acquire() as conn:
            result_obj = await conn.execute(
                result_table.select().where(
                    query
                )
            )
            return result_obj.rowcount > 0

    async def store_result(self, scraper_name, result_id, kind, result):
        query = sa.and_(
            result_table.c.scraper_name == scraper_name,
            result_table.c.kind == kind,
            result_table.c.result_id == result_id
        )
        async with self.engine.acquire() as conn:
            result_obj = await conn.execute(
                result_table.select().where(
                    query
                )
            )
            if result_obj.rowcount > 0:
                result_obj = list(result_obj)[0]
                result_value = json_loads(result_obj.result)
                if isinstance(result_value, dict) and isinstance(result, dict):
                    result_value.update(result)
                else:
                    result_value = result
                await conn.execute(
                    result_table.update().where(
                        query
                    ).values(result=json_dumps(result_value))
                )
                return False
            else:
                await conn.execute(result_table.insert().values(
                    scraper_name=scraper_name,
                    kind=kind,
                    result_id=result_id,
                    result=json_dumps(result)
                ))
                return True

    async def get_cached_content(self, cache_id):
        async with self.engine.acquire() as conn:
            result = await conn.execute(
                cache_table.select().where(
                    cache_table.c.cache_id == cache_id
                )
            )
            if result.rowcount > 0:
                return list(result)[0].content.tobytes()
            return None

    async def set_cached_content(self, cache_id, url, content):
        async with self.engine.acquire() as conn:
            await conn.execute(cache_table.insert().values(
                cache_id=cache_id,
                url=url,
                created=datetime.now(),
                content=content
            ))

    async def clear_cache(self):
        async with self.engine.acquire() as conn:
            await conn.execute(
                cache_table.delete()
            )
