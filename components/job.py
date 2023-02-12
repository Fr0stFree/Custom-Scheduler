from enum import Enum
import datetime as dt
from uuid import uuid4
import logging
from components.exceptions import *


def coroutine(func):
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return wrapper


class Job:
    class Status(Enum):
        PENDING = 1
        DONE = 2
        FAILED = 3

    def __init__(self, func: callable, args=tuple(), start_at: dt.datetime = dt.datetime.now(),
                 max_working_time: dt.timedelta = dt.timedelta.min, tries: int = 1,
                 dependencies: list['Job'] = [], **kwargs):
        self._id = uuid4()
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._result = None
        self._error = None
        self._status = self.Status.PENDING
        self._start_at = start_at
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies

    def run(self):
        self._tries -= 1
        try:
            self._result = self._func(*self._args, **self._kwargs)
            self._status = self.Status.DONE
        except Exception as e:
            self._error = e
            self._status = self.Status.FAILED
    
    def restart(self):
        self._status = self.Status.PENDING
        self._result = None
        self._error = None
    
    @property
    def has_tries(self):
        return self._tries > 0
    
    @property
    def result(self):
        return self._result
    
    @property
    def error(self):
        return self._error
    
    @property
    def start_at(self) -> dt.datetime:
        return self._start_at
    
    @property
    def dependencies(self) -> list['Job']:
        return self._dependencies
    
    @property
    def max_working_time(self) -> dt.timedelta:
        return self._max_working_time
    
    @property
    def status(self) -> Status:
        return self._status
    
    def failed(self, error: Exception):
        self._status = self.Status.FAILED
        self._error = error
    
    def __repr__(self):
        return f'<Job: {self._id}, {self._func.__name__}, {self._status.name}>'
    
    def __str__(self):
        return f'{self._func.__name__}'
      
    def __lt__(self, other):
        if other in self._dependencies:
            return True
        if self._start_at < other.start_at:
            return True
        return False


    