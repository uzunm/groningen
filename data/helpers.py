import functools

def compose_two(func1, func2):
     def composition(*args, **kwargs):
        return func1(func2(*args, **kwargs))
     return composition
def compose(*funcs):
    return functools.reduce(compose_two, funcs)
def pipe(*funcs):
    return functools.reduce(compose_two, reversed(funcs))