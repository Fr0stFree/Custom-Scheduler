class JobDependencyHasFailed(Exception):
    def __init__(self, job):
        self.message = f'Job %s dependency has failed.' % job
        super().__init__(self.message)
