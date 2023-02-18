import datetime as dt
import logging
import os
import pickle
import threading
import time

from .exceptions import JobDependencyHasFailed
from .utils import coroutine, Singleton, subscribe, dispatch


logger = logging.getLogger(__name__)


class Scheduler(metaclass=Singleton):
    def __init__(self, pool_size: int = 10):
        self._pending = []
        self._running = {}
        self._completed = {}
        self._is_active: bool = True
        self._pool_size = pool_size
        self._executor = self._executor()

    def schedule(self, *tasks) -> None:
        for task in tasks:
            if len(self._pending) + len(self._running) >= self._pool_size:
                raise ValueError('Queue is full')

            task.scheduler = self
            for i, job in enumerate(self._pending):
                if task < job:
                    self._pending.insert(i, task)
                    break
            else:
                self._pending.append(task)
            dispatch('on_job_scheduled', self, task=task, position=self._pending.index(task))

    @subscribe('on_job_done')
    def job_done(self, job):
        self._running.pop(job.id)
        self._completed[job.id] = job.result

        for other_job in self._pending + list(self._running.values()):
            if job in other_job.dependencies:
                print(other_job.dependencies)
                other_job.dependencies.remove(job)
                if not other_job.dependencies:
                    other_job.dependency_event.set()

    @subscribe('on_job_failed')
    def job_failed(self, job):
        self._running.pop(job.id)
        if job.tries > 0:
            job.restart()
            self.schedule(job)
            return

        self._completed[job.id] = job.error
        for other_job in self._pending + list(self._running.values()):
            if job in other_job.dependencies:
                other_job.failed(error=JobDependencyHasFailed(job))
                other_job.dependency_event.set()

    def run(self):
        dispatch('on_scheduler_started', self)
        while self._is_active:
            try:
                job = self._pending.pop(0)
            except IndexError:
                time.sleep(1)
                if not any([self._pending, self._running]):
                    self.stop()
                
            else:
                self._executor.send(job)
                time.sleep(.1)

    @coroutine
    def _executor(self):
        while True:
            job = yield

            if job.start_at > dt.datetime.now():
                thread = threading.Timer(interval=job.start_at.timestamp() - time.time(),
                                         function=job.run)
            else:
                thread = threading.Thread(target=job.run)

            self._running[job.id] = job
            thread.start()

    def stop(self):
        dispatch('on_scheduler_stopping', self)
        self._is_active = False
        for job in self._pending + list(self._running.values()):
            job.stop()
        self._executor.close()
        del self._executor

        with open('scheduler.pickle', 'wb') as f:
            pickle.dump(self, f)
        dispatch('on_scheduler_stopped', self)

    @classmethod
    def load(cls):
        with open('scheduler.pickle', 'rb') as f:
            scheduler = pickle.load(f)
        os.remove('scheduler.pickle')
        scheduler._executor = scheduler._executor()
        scheduler._is_active = True
        return scheduler
