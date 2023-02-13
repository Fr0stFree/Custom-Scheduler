import datetime as dt


class JobDependencyHasFailed(Exception):
    def __init__(self, job):
        self.message = f'Job %s dependency has failed.' % job


class JobExceededMaxWorkingTime(Exception):
    def __init__(self):
        self.message = f'Job exceeded max working time'


class JobExceededMaxTries(Exception):
    def __init__(self):
        self.message = f'Job exceeded max tries'