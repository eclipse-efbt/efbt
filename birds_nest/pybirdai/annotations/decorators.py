import functools

def lineage(dependencies: set[str]):
    
    def decorator_lineage(func):
        @functools.wraps(func)
        def wrapper_lineage(*args, **kwargs):
            value = func(*args, **kwargs)
            return value
        return wrapper_lineage
    return decorator_lineage

    

    