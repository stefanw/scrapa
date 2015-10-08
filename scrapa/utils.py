import asyncio
from datetime import datetime
import functools
import hashlib
from itertools import repeat
import json


class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        super(DateTimeDecoder, self).__init__(
                object_hook=self.dict_to_object, *args, **kargs)

    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

        type = d.pop('__type__')
        try:
            dateobj = datetime(**d)
            return dateobj
        except:
            d['__type__'] = type
            return d


class DateTimeEncoder(json.JSONEncoder):
    """ Instead of letting the default encoder convert datetime to string,
        convert datetime objects into a dict, which can be decoded by the
        DateTimeDecoder
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return {
                '__type__': 'datetime',
                'iso': obj.isoformat(),
                'year': obj.year,
                'month': obj.month,
                'day': obj.day,
                'hour': obj.hour,
                'minute': obj.minute,
                'second': obj.second,
                'microsecond': obj.microsecond,
            }
        else:
            return super(DateTimeEncoder, self).default(obj)


def json_dumps(obj, indent=2):
    return json.dumps(obj, cls=DateTimeEncoder, indent=indent, sort_keys=True)


def json_loads(obj):
    return json.loads(obj, cls=DateTimeDecoder)


def args_kwargs_iterator(iterator):
    """
    Converts items in an iterator to args tuples and kwargs dictionaries:
    - if the item is a 2-tuple and the first item in the tuple is a tuple and
      the second is a dict, then treat the first item as args, second as kwargs
    - else if the item is a tuple, then treat the tuple as args, empty kwargs
    - else treat the item as a single argument

    """
    for args_kwargs in iterator:
        if isinstance(args_kwargs, tuple):
            if len(args_kwargs) == 2 and (
                    isinstance(args_kwargs[0], tuple) and
                    isinstance(args_kwargs[1], dict)):
                args, kwargs = args_kwargs
            else:
                args = args_kwargs
                kwargs = {}
        else:
            args = (args_kwargs,)
            kwargs = {}
        yield args, kwargs


def add_func_to_iterator(coro_arg, iterator):
    try:
        coro_iter = iter(coro_arg)
    except TypeError:
        coro_iter = repeat(coro_arg)
    yield from zip(coro_iter, iterator)


def get_cache_id(url, *args, **kwargs):
    cache_id = hashlib.md5()
    cache_id.update(url.encode('utf-8'))
    cache_id.update(json.dumps(kwargs.get('params', ''), sort_keys=True).encode('utf-8'))
    return cache_id.hexdigest()


def doublewrap(f):
    '''
    a decorator decorator, allowing the decorator to be used as:
    @decorator(with, arguments, and=kwargs)
    or
    @decorator
    Credits: https://stackoverflow.com/questions/653368/
    '''
    @functools.wraps(f)
    def new_dec(*args, **kwargs):
        if len(args) == 1 and len(kwargs) == 0 and callable(args[0]):
            # actual decorated function
            return f(args[0])
        else:
            # decorator arguments
            return lambda realf: f(realf, *args, **kwargs)
    return new_dec


@doublewrap
def async(f, store=False):
    if store:
        f.scrapa_store = True
    return asyncio.coroutine(f)
