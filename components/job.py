import threading
from enum import Enum
import datetime as dt
from uuid import uuid4
import logging

from .utils import coroutine, dispatch
from components.exceptions import *


logger = logging.getLogger(__name__)


class Job:
    class Status(Enum):
        PENDING = 1
        RUNNING = 2
        DONE = 3
        FAILED = 3

    def __init__(self, func: callable, args=tuple(), start_at: dt.datetime = dt.datetime.now(),
                 scheduler=None, tries: int = 1, dependencies: list['Job'] = None,
                 dependency_event=None, **kwargs):
        self._id = uuid4()
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._scheduler = scheduler
        self._result = None
        self._error = None
        self._status = self.Status.PENDING
        self._start_at = start_at
        # self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies
        self._dependency_event = dependency_event

    def run(self):
        if self._dependency_event:
            while not self._dependency_event.is_set():
                self._dependency_event.wait()
                
        self._status = self.Status.RUNNING
        dispatch('on_job_started', self._scheduler, job=self)
        try:
            self._result = self._func(*self._args, **self._kwargs)
            self._status = self.Status.DONE
            dispatch('on_job_done', self._scheduler, job=self)
        except Exception as exc:
            self._error = exc
            self._status = self.Status.FAILED
            dispatch('on_job_failed', self._scheduler, job=self)

    def restart(self):
        self._tries -= 1
        self._start_at = dt.datetime.now() + dt.timedelta(seconds=5)
        self._status = self.Status.PENDING
        self._result = None
        self._error = None
    
    @property
    def id(self):
        return self._id
    
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
    def status(self) -> Status:
        return self._status
    
    @property
    def tries(self):
        return self._tries
    
    @property
    def dependencies(self):
        return self._dependencies
    
    @property
    def dependency_event(self):
        return self._dependency_event
    
    @dependency_event.setter
    def dependency_event(self, event: threading.Event):
        self._dependency_event = event
    
    @property
    def scheduler(self):
        return self._scheduler
    
    @scheduler.setter
    def scheduler(self, scheduler):
        self._scheduler = scheduler
    
    def failed(self, error: Exception):
        logger.debug('Job %s set to failed', self)
        self._status = self.Status.FAILED
        self._error = error

    def stop(self):
        self._status = self.Status.PENDING
        self._result = None
        self._error = None
    
    def __repr__(self):
        return f'<Job: {self._id}, {self._func.__name__}, {self._status.name}>'
    
    def __str__(self):
        return f'{self._func.__name__} {self._id}'
      
    def __lt__(self, other: 'Job') -> bool:
        if other in self._dependencies:
            return True
        if self._start_at < other.start_at:
            return True
        return False


    