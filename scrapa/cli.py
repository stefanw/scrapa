import argparse
import asyncio
import math
import sys

from .config import ScrapaConfig
from .utils import json_dumps, json_loads


class CommandLineMixin():
    def run_from_cli(self):
        args = self.get_command_line_args()
        getattr(self, args['command_name'])(**args)

    def get_command_line_args(self):
        parser = argparse.ArgumentParser(description='Scrapa arguments.')

        subparsers = parser.add_subparsers(dest="command_name")

        scraper = subparsers.add_parser('scrape')
        scraper.add_argument('--clear', dest='clear', action='store_true',
                           default=False,
                           help='Clear all stored tasks for this scraper')
        scraper.add_argument('--start', dest='start', action='store_true',
                           default=False,
                           help='Run start even if tasks are present')
        scraper.add_argument('--clear-cache', dest='clear_cache',
                            action='store_true',
                            default=False,
                            help='Clear the http cache')
        scraper.add_argument('--loglevel', dest='loglevel',
                            default=ScrapaConfig.LOGLEVEL,
                            help='Loglevel: one of DEBUG, INFO, WARN, ERROR.')
        self.add_arguments(scraper)

        dump_tasks = subparsers.add_parser('dump_tasks')
        dump_tasks.add_argument('-s', '--split', default=1, type=int,
                                help='Split tasks in number of files.')
        dump_tasks.add_argument('-o', '--output',
                                help='base name to use for output.')

        load_tasks = subparsers.add_parser('load_tasks')
        load_tasks.add_argument('-f', '--filename',
                                help='Filename to read from, defaults to stdin.')

        args = parser.parse_args()
        args = vars(args)

        if args.get('command_name') is None:
            args = scraper.parse_args()
            args = vars(args)
            args['command_name'] = 'scrape'

        return args

    def add_arguments(self, parser):
        pass

    def configure(self, **kwargs):
        pass

    def dump_tasks(self, split=1, output=None, prefix='scrapa-', **kwargs):
        if output is None:
            output = prefix

        loop = asyncio.get_event_loop()
        storage = loop.run_until_complete(self.get_storage())
        total_count = loop.run_until_complete(
                        storage.get_pending_task_count(self.config.NAME))
        tasks = loop.run_until_complete(storage.get_pending_tasks(self.config.NAME))

        tasks_per_file = math.ceil(total_count / split)
        task_counter = 0
        outfile = None
        file_counter = 0
        for task_dict in tasks:
            if outfile is None:
                file_counter += 1
                filename = '{}{:03d}.json'.format(output, file_counter)
                outfile = open(filename, 'w')
            outfile.write(json_dumps({
                'scraper_name': self.config.NAME,
                'task_name': task_dict['task_name'],
                'args': task_dict['args'],
                'kwargs': task_dict['kwargs'],
                'meta': task_dict['kwargs']
            }, indent=None))
            outfile.write('\n')
            task_counter += 1
            if task_counter > tasks_per_file:
                outfile.close()
                outfile = None
                task_counter = 0

    def load_tasks(self, filename=None, **kwargs):
        loop = asyncio.get_event_loop()
        storage = loop.run_until_complete(self.get_storage())

        if filename is None:
            task_file = open(filename, encoding='utf-8')
        else:
            task_file = sys.stdin
        count = 0
        already = 0
        for line in task_file:
            task = json_loads(line)
            result = loop.run_until_complete(
                storage.store_task(
                    task['scraper_name'],
                    getattr(self, task['task_name']),
                    task['args'],
                    task['kwargs']
                ))
            count += 1
            if not result:
                already += 1
        print('Loaded {} tasks with {} already present.'.format(count, already))
