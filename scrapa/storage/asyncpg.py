import asyncio
import json
from datetime import datetime

from aiopg.sa import create_engine
import psycopg2
import sqlalchemy as sa
from sqlalchemy.schema import CreateTable, CreateIndex

from .base import BaseStorage


metadata = sa.MetaData()

task_table = sa.Table('scrapa_tasks', metadata,
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

task_index = sa.Index('scrapa_tasks__scraper_name_task_id', task_table.c.scraper_name, task_table.c.task_id, unique=True)

result_table = sa.Table('scrapa_result', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('scraper_name', sa.String(255)),
    sa.Column('result_id', sa.String(255)),
    sa.Column('kind', sa.String(255)),
    sa.Column('result', sa.Text),
)

result_index = sa.Index('scrapa_result__scraper_name_result_id_kind', result_table.c.scraper_name, result_table.c.result_id, result_table.c.kind, unique=True)

cache_table = sa.Table('scrapa_cache', metadata,
    sa.Column('id', sa.Integer, primary_key=True),
    sa.Column('cache_id', sa.String(255)),
    sa.Column('url', sa.String(1024)),
    sa.Column('created', sa.DateTime),
    sa.Column('content', sa.Text),
)

cache_index = sa.Index('scrapa_cache__cache_id', cache_table.c.cache_id, unique=True)


class AsyncPostgresStorage(BaseStorage):
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @asyncio.coroutine
    def create(self):
        schema_name = self.kwargs.pop('schema_name', 'public')
        self.engine = yield from create_engine(**self.kwargs)
        with (yield from self.engine) as conn:
            tables = ((task_table, task_index),
                      (cache_table, cache_index),
                      (result_table, result_index)
            )
            for table, index in tables:
                exists = yield from conn.scalar('''SELECT EXISTS (
                   SELECT 1
                   FROM   information_schema.tables
                   WHERE  table_schema = '{table_schema}'
                   AND    table_name = '{table_name}'
                );'''.format(table_schema=schema_name,
                             table_name=table.name))
                if not exists:
                    tr = yield from conn.begin()
                    create_statement = str(CreateTable(table).compile(self.engine))
                    create_statement = create_statement.replace('CREATE TABLE',
                            'CREATE TABLE IF NOT EXISTS')
                    yield from conn.execute(create_statement)
                    create_index = str(CreateIndex(index).compile(self.engine))
                    yield from conn.execute(create_index)
                    yield from tr.commit()

    @asyncio.coroutine
    def store_task(self, scraper_name, coro, args, kwargs):
        task_id = self.get_task_id(coro, args, kwargs)
        try:
            with (yield from self.engine) as conn:
                yield from conn.execute(task_table.insert().values(
                    scraper_name=scraper_name,
                    task_id=task_id,
                    name=coro.__name__,
                    args=json.dumps(args),
                    kwargs=json.dumps(kwargs),
                    created=datetime.now(),
                    tried=0,
                    done=False,
                    failed=False,
                    value=None,
                    exception=None
                ))
        except psycopg2.IntegrityError:
            # Task already exists
            pass

    @asyncio.coroutine
    def clear_tasks(self, scraper_name):
        with (yield from self.engine) as conn:
            yield from conn.execute(
                task_table.delete(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                ))
            )

    @asyncio.coroutine
    def get_task_count(self, scraper_name):
        with (yield from self.engine) as conn:
            count = yield from conn.scalar(
                task_table.count(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                ))
            )
        return count

    @asyncio.coroutine
    def get_pending_task_count(self, scraper_name):
        with (yield from self.engine) as conn:
            count = yield from conn.scalar(
                task_table.count(sa.and_(
                    task_table.c.scraper_name == scraper_name,
                    task_table.c.done == False  # noqa
                ))
            )
        return count

    @asyncio.coroutine
    def get_pending_tasks(self, scraper_name, instance):
        with (yield from self.engine) as conn:
            result = yield from conn.execute(
                task_table.select().where(
                    sa.and_(
                        task_table.c.scraper_name == scraper_name,
                        task_table.c.done == False  # noqa
                    )
                )
            )
        return ({
            'coro': getattr(instance, task.name),
            'args': json.loads(task.args),
            'kwargs': json.loads(task.kwargs),
            'meta': {'tried': task.tried}
        } for task in result)

    @asyncio.coroutine
    def store_task_result(self, scraper_name, coro, args, kwargs, done, failed,
                          value, exception):
        task_id = self.get_task_id(coro, args, kwargs)
        with (yield from self.engine) as conn:
            yield from conn.execute(
                task_table.update().where(
                    sa.and_(
                        task_table.c.scraper_name == scraper_name,
                        task_table.c.task_id == task_id
                    )
                ).values(**{'done': done, 'failed': failed,
                        'last_tried': datetime.now(), 'value': json.dumps(value),
                        'exception': exception, 'tried': task_table.c.tried + 1})
            )

    @asyncio.coroutine
    def store_result(self, scraper_name, result_id, kind, result):
        query = sa.and_(
            result_table.c.scraper_name == scraper_name,
            result_table.c.kind == kind,
            result_table.c.result_id == result_id
        )
        with (yield from self.engine) as conn:
            result_obj = yield from conn.execute(
                result_table.select().where(
                    query
                )
            )
            if result_obj.rowcount > 0:
                result_obj = list(result_obj)[0]
                result_value = json.loads(result_obj.result)
                if isinstance(result_value, dict) and isinstance(result, dict):
                    result_value.update(result)
                else:
                    result_value = result
                yield from conn.execute(
                    result_table.update().where(
                        query
                    ).values(result=json.dumps(result_value))
                )
            else:
                yield from conn.execute(result_table.insert().values(
                    scraper_name=scraper_name,
                    kind=kind,
                    result_id=result_id,
                    result=json.dumps(result)
                ))

    @asyncio.coroutine
    def get_cached_content(self, cache_id):
        with (yield from self.engine) as conn:
            result = yield from conn.execute(
                cache_table.select().where(
                    cache_table.c.cache_id == cache_id
                )
            )
            if result.rowcount > 0:
                return list(result)[0].content
            return None

    @asyncio.coroutine
    def set_cached_content(self, cache_id, url, content):
        with (yield from self.engine) as conn:
            yield from conn.execute(cache_table.insert().values(
                cache_id=cache_id,
                url=url,
                created=datetime.now(),
                content=content
            ))

    @asyncio.coroutine
    def clear_cache(self):
        with (yield from self.engine) as conn:
            yield from conn.execute(
                cache_table.delete()
            )
