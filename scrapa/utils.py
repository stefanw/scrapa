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
