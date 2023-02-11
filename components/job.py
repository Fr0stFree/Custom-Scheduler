from enum import Enum
import datetime as dt

from components.exceptions import *


class Job:
    class Status(Enum):
        PENDING = 1
        DONE = 2
        FAILED = 3

    def __init__(self, func: callable, args=tuple(), start_at: dt.datetime = dt.datetime.now(),
                 max_working_time: dt.timedelta = None, tries: int = 1,
                 dependencies: list['Job'] = [], **kwargs):
        self._func = func(*args, **kwargs)
        self._result = None
        self._status = self.Status.PENDING
        self._start_at = start_at
        self._max_working_time = max_working_time
        self._tries = tries
        self._dependencies = dependencies

    def run(self):
        try:
            self._result = next(self._func)
        except StopIteration:
            self._status = self.Status.DONE
        except Exception as exc:
            self._tries -= 1
            self._result = exc
    
    @property
    def result(self):
        return self._result
    
    @property
    def status(self) -> Status:
        return self._status
    
    @property
    def start_at(self) -> dt.datetime:
        return self._start_at
    
    @property
    def dependencies(self) -> list['Job']:
        return self._dependencies

    def __repr__(self):
        return f'<Job: {self._func.__name__}>'
    
    def depends_on(self, job: 'Job') -> bool:
        return job in self._dependencies
    
    def __lt__(self, other):
        if other.depends_on(self):
            return True
        if self._start_at < other.start_at:
            return True
        return False

    @property
    def ready(self) -> bool:
        conditions = [
            self._start_at <= dt.datetime.now(),
            all([job.done for job in self._dependencies]),
            self.status == self.Status.PENDING,
        ]
        return all(conditions)
    
    @property
    def done(self) -> bool:
        return self.status == self.Status.DONE

    