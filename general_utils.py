
import time


def time_function(func):
    """ This function decorator times the function and print the runtime
    """
    def decorator_wrapper(*args, **kwargs):
        print(f'-- Function {func.__name__} starts... --')
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        print(f'-- Function {func.__name__} executed in {(t2 - t1):.4f}s --')
        return result
    return decorator_wrapper
