import asyncio
import hashlib
import functools
import json

from itertools import repeat


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
