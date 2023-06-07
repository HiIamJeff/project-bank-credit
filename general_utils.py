
import time
from contextlib import contextmanager
import os


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


@contextmanager
def cd(new_dir):
    """ Change directory with rollback to current working directory after the work is done
    """
    prev_dir = os.getcwd()
    os.chdir(os.path.expanduser(new_dir))
    try:
        yield
    finally:
        os.chdir(prev_dir)


def generate_available_period():
    """ Generate a list with all available periods ("MM/YYYY") in processed folder for the demo
    """
    with cd('data/processed/'):
        list_dir = [i for i in os.listdir() if i.isnumeric()]
        list_period = []

        for y in list_dir:
            with cd(f'{y}/'):
                for m in [i for i in os.listdir() if i.isnumeric()]:
                    s = y + '/' + m  # 2019/04
                    list_period.append(s)
    return list_period
