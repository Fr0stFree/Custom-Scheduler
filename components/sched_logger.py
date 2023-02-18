import logging

from .utils import subscribe


logger = logging.getLogger(__name__)


@subscribe('on_scheduler_started')
def scheduler_started(*args):
    scheduler = args[0]
    logger.debug('Scheduler has started. Current queue: %s, max size: %s',
                 len(scheduler._pending), scheduler._pool_size)


@subscribe('on_job_scheduled')
def job_scheduled(*args, **kwargs):
    job = kwargs.get('task')
    position = kwargs.get('position') + 1
    scheduler = args[0]
    logger.debug('Job %s is scheduled at position %s/%s', job, position, len(scheduler._pending))


@subscribe('on_job_started')
def job_started(*args, **kwargs):
    job = kwargs.get('job')
    logger.info('Job %s has started', job)

@subscribe('on_job_done')
def job_done(*args, **kwargs):
    job = kwargs.get('job')
    logger.info('Job %s is done, result: %s', job, job.result)


@subscribe('on_job_failed')
def job_failed(*args, **kwargs):
    job = kwargs.get('job')
    if job.tries > 0:
        logger.warning('Job %s is failed, error: %s. Restarting job... (%s tries left)',
                       job, job.error, job.tries)
    else:
        logger.warning('Job %s is failed, error: %s. No more tries left', job, job.error)
