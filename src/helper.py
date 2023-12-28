import logging
from config import LOCAL
import sys
import traceback
from functools import wraps


def fetchIdinHostName(hostName):
    '''
    Fetches the id of a server from its hostname.
    @param hostName: the hostname of the server
    @return: the id of the server
    '''
    return int(hostName[6:8]) if LOCAL else int(hostName[13:15])

def handle_exceptions(func):
    '''
    Decorator to handle exceptions.
    @param func: the function to be decorated
    @return: the decorated function
    '''
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()

            logging.error(
                f'{e} in {func.__name__} in file {traceback.extract_tb(exc_tb)[-1][0]} at line {traceback.extract_tb(exc_tb)[-1][1]}')
    return wrapper

def locked(func):
    '''
    Decorator to lock a function.
    @param func: the function to be decorated
    @return: the decorated function
    '''
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.list_lock:
            return func(self, *args, **kwargs)
    return wrapper