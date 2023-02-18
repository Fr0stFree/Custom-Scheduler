import random
import datetime as dt

import pytest

from components.exceptions import JobDependencyHasFailed
from components.scheduler import Scheduler
from components.job import Job


@pytest.fixture
def scheduler():
    return Scheduler()


def test_scheduler_able_to_handle_successful_job(scheduler):
    job = Job(lambda: 1)
    scheduler.schedule(job)
    scheduler.run()
    assert job.result == 1
    assert job.error is None
    assert job.status == Job.Status.DONE
    assert job.tries == 1
    assert scheduler._completed[job.id] == job.result
    assert len(scheduler._completed) == 1
    assert scheduler._pending == []
    assert scheduler._running == {}
    

def test_scheduler_able_handle_failed_job(scheduler):
    job = Job(lambda: 1 / 0)
    scheduler.schedule(job)
    scheduler.run()
    assert isinstance(job.error, ZeroDivisionError)
    assert job.status == Job.Status.FAILED
    assert job.tries == 0
    assert job.result is None
    assert scheduler._completed[job.id] == job.error
    assert len(scheduler._completed) == 1
    assert scheduler._pending == []
    assert scheduler._running == {}
    

def test_scheduler_able_to_handle_independent_jobs(scheduler):
    job_1 = Job(sum, args=([1, 2, 3],))
    job_2 = Job(sum, args=([2, 3, 4],))
    job_3 = Job(sum, args=([3, 4, 5],))
    jobs = [job_1, job_2, job_3]
    scheduler.schedule(*jobs)
    scheduler.run()
    assert all([job.status == Job.Status.DONE for job in jobs])
    assert all([job.tries == 1 for job in jobs])
    assert all([job_1.result == 6, job_2.result == 9, job_3.result == 12])
    assert all([job.error is None for job in jobs])
    assert all([scheduler._completed[job.id] == job.result for job in jobs])
    assert len(scheduler._completed) == 3
    assert scheduler._pending == []
    assert scheduler._running == {}


def test_scheduler_able_to_handle_dependent_jobs(scheduler):
    job_1 = Job(lambda: 'hello ')
    job_2 = Job(lambda: 'world', dependencies=[job_1])
    job_3 = Job(lambda: '!', dependencies=[job_2, job_1])
    jobs = [job_1, job_2, job_3]
    random.shuffle(jobs)
    scheduler.schedule(*jobs)
    assert scheduler._pending == [job_1, job_2, job_3]
    scheduler.run()
    assert ''.join(scheduler._completed.values()) == 'hello world!'
    assert all([job.status == Job.Status.DONE for job in [job_1, job_2, job_3]])
    assert all([job.error is None for job in jobs])
    assert all([scheduler._completed[job.id] == job.result for job in jobs])
    assert len(scheduler._completed) == 3
    assert scheduler._pending == []
    assert scheduler._running == {}


def test_scheduler_able_to_schedule_jobs_by_time(scheduler):
    job_1 = Job(lambda: 1, start_at=dt.datetime.now() + dt.timedelta(seconds=1))
    job_2 = Job(lambda: 2, start_at=dt.datetime.now() + dt.timedelta(seconds=2))
    job_3 = Job(lambda: 3, start_at=dt.datetime.now() + dt.timedelta(seconds=3))
    jobs = [job_1, job_2, job_3]
    random.shuffle(jobs)
    scheduler.schedule(*jobs)
    assert scheduler._pending == [job_1, job_2, job_3]
    scheduler.run()
    assert all([job.status == Job.Status.DONE for job in jobs])
    assert all([job.tries == 1 for job in jobs])
    assert all([job.error is None for job in jobs])
    assert all([scheduler._completed[job.id] == job.result for job in jobs])
    assert len(scheduler._completed) == 3
    assert scheduler._pending == []
    assert scheduler._running == {}


def test_scheduler_able_to_handle_failed_dependency(scheduler):
    job_1 = Job(lambda: 1 / 0)
    job_2 = Job(lambda: 1, dependencies=[job_1])
    job_3 = Job(lambda: 1, dependencies=[job_2])
    jobs = [job_1, job_2, job_3]
    scheduler.schedule(*jobs)
    scheduler.run()
    assert all([job.status == Job.Status.FAILED for job in jobs])
    assert all([job.tries == 0 for job in jobs])
    assert isinstance(job_1.error, ZeroDivisionError)
    assert isinstance(job_2.error, JobDependencyHasFailed)
    assert isinstance(job_3.error, JobDependencyHasFailed)
    assert all([scheduler._completed[job.id] == job.error for job in jobs])
    assert len(scheduler._completed) == 3
    assert scheduler._pending == []
    assert scheduler._running == {}