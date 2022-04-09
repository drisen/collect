import functools


def add_args(func, *moreargs, **kwargs):
    @functools.wraps(func)
    def wrapper(*args):
        return func(*args, *moreargs, **kwargs)
    return wrapper


def print_args(*args, **kwargs):
    print(f"args={args}, kwargs={kwargs}")


myfunc = functools.partial(print_args, kw1='kw1', kw2='kw2')
yourfunc = functools.partial(print_args, 'c', kw1='1kw', kw2='2kw')

myfunc('a', 'b')
yourfunc('a', 'b')
myfunc('a')
myfunc()
