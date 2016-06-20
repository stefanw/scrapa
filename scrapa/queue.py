import asyncio
import traceback
import sys
try:
    import ipdb as pdb
except ImportError:
    import pdb

from .utils import args_kwargs_iterator, add_func_to_iterator


class QueueMixin():
    def finished(self):
        if self.stopping:
            return True
        return self.queue_finished()

    def queue_finished(self):
        qsize = self.queue.qsize()
        return qsize == 0 and self.tasks_running == 0

    async def consume_queue(self):
        """ Use get_nowait construct until Py 3.4.4 """
        try:
            self.consumer_count += 1
            while True:
                try:
                    if self.terminate_consumers:
                        return
                    coro, args, kwargs, meta = self.queue.get_nowait()
                    self.tasks_running += 1
                    try:
                        await self.run_task(coro, *args, **kwargs)
                    except Exception:
                        if self.storage_enabled(coro):
                            if meta is not None and meta.get('tried') < self.config.TASK_RETRY_COUNT:
                                await self.add_to_queue(coro, args, kwargs, meta)
                    finally:
                        self.tasks_running -= 1
                        if hasattr(self.queue, 'task_done'):
                            self.queue.task_done()
                except asyncio.QueueEmpty:
                    await asyncio.sleep(0.5)
        finally:
            self.consumer_count -= 1

    async def run_many(self, coro_arg, generator):
        generator = args_kwargs_iterator(generator)
        generator = add_func_to_iterator(coro_arg, generator)
        done, pending = await asyncio.wait(list(
            asyncio.ensure_future(self.run_one(coro, *args, **kwargs)) for coro, (args, kwargs) in generator
        ))
        assert len(pending) == 0
        return (d.result() for d in done)

    async def run_one(self, coro, *args, **kwargs):
        if not asyncio.iscoroutinefunction(coro):
            raise Exception('Given task %s is not a coroutine! Decorate it with @scrapa.async', coro)
        result = await self.run_task(coro, *args, **kwargs)
        return result

    async def run_task(self, coro, *args, **kwargs):
        failed = False
        done = False
        value = None
        exception = None
        try:
            self.stats['counter']['tasks_tried'] += 1
            self.tasks_running += 1
            self.log_task_start({
                'task_name': coro.__name__,
                'args': args,
                'kwargs': kwargs,
            })
            result = (await coro(*args, **kwargs))
            self.log_task_end({
                'task_name': coro.__name__,
                'args': args,
                'kwargs': kwargs,
                'result': str(result)
            })
        except Exception as e:
            self.logger.error('Exception running %s(*%s, **%s)',
                              coro.__name__, args, kwargs)
            # self.logger.exception(e)
            self.stats['counter']['tasks_failed'] += 1
            self.log_exception(e)

            if self.config.DEBUG_EXCEPTIONS:
                _, _, tb = sys.exc_info()
                pdb.post_mortem(tb)

            exception = traceback.format_exc(exception)
            failed = True
            raise e
        else:
            self.stats['counter']['tasks_succeeded'] += 1
            value = result
            done = True
            return result
        finally:
            self.tasks_running -= 1
            if self.storage_enabled(coro):
                await self.store_task_result(
                    self.config.NAME,
                    coro, args, kwargs,
                    done, failed,
                    str(value), exception
                )

    async def schedule_many(self, coro_arg, generator):
        count = 0
        schedule_count = 0
        generator = args_kwargs_iterator(generator)
        generator = add_func_to_iterator(coro_arg, generator)
        for coro, (args, kwargs) in generator:
            scheduled = await self.schedule_one(coro, *args, **kwargs)
            count += 1
            if scheduled:
                schedule_count += 1
        self.logger.info('Scheduled %s tasks (%s already present)',
                         schedule_count, count - schedule_count)

    async def schedule_one(self, coro, *args, **kwargs):
        if not asyncio.iscoroutinefunction(coro):
            raise Exception('Given task %s is not a coroutine! Decorate it with @scrapa.async', coro)
        should_run = await self.prepare_schedule(coro, args, kwargs)
        if should_run:
            await self.add_to_queue(coro, args, kwargs)
            return True
        return False

    async def add_to_queue(self, coro, args, kwargs, meta=None):
        await self.queue.put((coro, args, kwargs, meta))

    async def queue_pending_tasks(self):
        storage = await self.get_storage()
        tasks = await storage.get_pending_tasks(self.config.NAME)
        for task_dict in tasks:
            await self.add_to_queue(
                getattr(self, task_dict['task_name']),
                task_dict['args'],
                task_dict['kwargs'],
                meta=task_dict['meta']
            )

    async def prepare_schedule(self, coro, args, kwargs):
        should_run = True
        if self.storage_enabled(coro):
            should_run = await self.store_task(coro, args, kwargs)
        return should_run
