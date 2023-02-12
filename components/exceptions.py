import datetime as dt


class JobIsNotReadyYet(Exception):
    def __init__(self, timeout=dt.timedelta(seconds=1)):
        self.message = f'Job is not ready yet. Timeout: {timeout}'
        self.timeout = timeout


class JobHasDependencies(Exception):
    def __init__(self, dependencies):
        self.message = f'Job has dependencies: {dependencies}'
        

class JobExceededMaxWorkingTime(Exception):
    def __init__(self):
        self.message = f'Job exceeded max working time'


class JobExceededMaxTries(Exception):
    def __init__(self):
        self.message = f'Job exceeded max tries'