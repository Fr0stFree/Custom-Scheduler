import time
import logging

from .utils import coroutine
from .job import Job
from components.exceptions import *


logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, pool_size=10):
        self._queue = []
        self._storage = []
        self._pool_size = pool_size
        self._running = True
        self._validator = self._validator()
        self._executor = self._executor()
        self._planner = self._planner()
        
    def schedule(self, task) -> None:
        if len(self._queue) >= self._pool_size:
            raise ValueError('Queue is full')
        self._planner.send(task)
    
    @coroutine
    def _planner(self):
        while True:
            job = yield
            if not self._queue:
                self._queue.append(job)
            else:
                for i, task in enumerate(self._queue):
                    if job < task:
                        self._queue.insert(i, job)
                        break
                else:
                    self._queue.append(job)

    def run(self):
        while self._running:
            try:
                job = self._queue.pop(0)
            except IndexError:
                print('Queue is empty')
                time.sleep(1)
            else:
                self._validator.send(job)
    
    @coroutine
    def _validator(self):
        while True:
            job = yield
            if not job.has_tries:
                logger.warning('Job %s has no more tries', job)
                self._storage.append(job)
            elif job.start_at > dt.datetime.now():
                logger.info('Job %s is not ready yo run yet', job)
                self._planner.send(job)
            elif not all([task.status == Job.Status.DONE for task in job.dependencies]):
                logger.info('Job %s has uncompleted dependencies', job)
                self._planner.send(job)
            elif job.max_working_time > dt.datetime.now() - job.start_at:
                logger.warning('Job %s exceeded max working time', job)
                job.failed(error=JobExceededMaxWorkingTime())
                self._storage.append(job)
            else:
                self._executor.send(job)
    
    @coroutine
    def _executor(self):
        while True:
            job = yield
            job.run()
            match job.status:
                case Job.Status.DONE:
                    logger.info('Job %s is done. Result: %s', job, job.result)
                    self._storage.append(job)
                case Job.Status.FAILED:
                    job.restart()
                    self._validator.send(job)
                case Job.Status.PENDING:
                    self._planner.send(job)
                
                    
                    
                
        
        


    def restart(self):
        pass

    def stop(self):
        pass
