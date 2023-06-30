"""Tests isolating only the Scheduler."""
from aiida.schedulers import SchedulerError
import pytest

from aiida_firecrest.scheduler import FirecrestScheduler
from aiida_firecrest.transport import FirecrestTransport
from aiida_firecrest.utils_test import FirecrestConfig


@pytest.fixture(name="transport")
def _transport(firecrest_server: FirecrestConfig):
    transport = FirecrestTransport(
        url=firecrest_server.url,
        token_uri=firecrest_server.token_uri,
        client_id=firecrest_server.client_id,
        client_secret=firecrest_server.client_secret,
        client_machine=firecrest_server.machine,
        small_file_size_mb=firecrest_server.small_file_size_mb,
    )
    transport.chdir(firecrest_server.scratch_path)
    yield transport


def test_get_jobs_empty(transport: FirecrestTransport):
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    with pytest.raises(SchedulerError, match="Failed to retrieve job"):
        scheduler.get_jobs(["unknown"])

    assert isinstance(scheduler.get_jobs(), list)


def test_submit_job(transport: FirecrestTransport):
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    with pytest.raises(SchedulerError, match="invalid path"):
        scheduler.submit_job(transport.getcwd(), "unknown.sh")

    # create a job script in a folder
    transport.mkdir("test_submission")
    transport.chdir("test_submission")
    transport.write_binary("job.sh", b"#!/bin/bash\n\necho 'hello world'")

    job_id = scheduler.submit_job(transport.getcwd(), "job.sh")
    assert isinstance(job_id, str)
