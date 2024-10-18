import time
from functools import wraps

# Decorator===================================================================================================

def simple_logger(function):
    def wrapper(*args, **kwargs):
        print(f"----- {function.__name__}: start -----")
        output = function(*args, **kwargs)
        print(f"----- {function.__name__}: end -----")
        return output
    return wrapper

# Keep the original function’s name and documentation===========================================================

def logger(function):
    @wraps(function)
    def wrapper(*args, **kwargs):
        """wrapper documentation"""
        print(f"----- {function.__name__}: start -----")
        output = function(*args, **kwargs)
        print(f"----- {function.__name__}: end -----")
        return output
    return wrapper

# @logger
# def add_two_numbers(a, b):
#     """this function adds two numbers"""
#     return a + b

# Repeat function===================================================================================================

def repeat(number_of_times):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(number_of_times):
                func(*args, **kwargs)
        return wrapper
    return decorate

# @repeat(5)
# def dummy():
#     print("hello")

# Execution Time===================================================================================================

def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        print(f'{func.__name__} took {end - start:.2f} seconds to complete')
        return result
    return wrapper

# @timeit
# def process_data():
#     time.sleep(1)
# process_data()
# process_data took 1.000012 seconds to complete

# Retry===================================================================================================

def retry(retry_time, sleep_time):
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, sleep_time+1):
                try:
                    return func(*args, **kwargs)
                except:
                    print(f'{func.__name__} Error occured! Wait {sleep_time} sec for retry ({retry_time-i} retry remain)')
                    time.sleep(sleep_time)
            return 'Always Failed!'
        return wrapper
    return decorate

# @retry(3, 3)
# def divide():
#     return 0 / 3

# Counter===================================================================================================

def counter(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.count += 1
        result = func(*args, **kwargs)
        print(f'{func.__name__} has been executed {wrapper.count} times')
        return result
    wrapper.count = 0
    return wrapper

# @counter
# def say_hi():
#     print('Hi')
