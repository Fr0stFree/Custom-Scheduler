import time

from components.exceptions import *


def coroutine(func):
    def wrapper(*args, **kwargs):
        gen = func(*args, **kwargs)
        next(gen)
        return gen
    return wrapper


class Scheduler:
    def __init__(self, pool_size=10):
        self._queue = []
        self._pool_size = pool_size
    
    def schedule(self, task):
        if len(self._queue) >= self._pool_size:
            raise ValueError('Queue is full')

        if not self._queue:
            return self._queue.append(task)

        for i, job in enumerate(self._queue):
            if task < job:
                return self._queue.insert(i, task)
        self._queue.append(task)

    def run(self):
        while True:
            try:
                job = self._queue.pop(0)
            except IndexError:
                print('Queue is empty')
                time.sleep(1)
            else:
                job.run()
                if not job.done and job.ready:
                    self.schedule(job)
                    

        
        


    def restart(self):
        pass

    def stop(self):
        pass
