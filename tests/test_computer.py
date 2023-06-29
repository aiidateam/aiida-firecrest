"""Tests for setting up an AiiDA computer for Firecrest,
and basic functionality of the Firecrest transport and scheduler plugins.
"""
from pathlib import Path

from aiida import orm
import pytest

from aiida_firecrest.utils_test import FirecrestConfig


@pytest.fixture(name="firecrest_computer")
def _firecrest_computer(firecrest_server: FirecrestConfig):
    """Create and return a computer configured for Firecrest.

    Note, the computer is not stored in the database.
    """
    computer = orm.Computer(
        label="test_computer",
        description="test computer",
        hostname="-",
        workdir=firecrest_server.scratch_path,
        transport_type="firecrest",
        scheduler_type="firecrest",
    )
    computer.set_minimum_job_poll_interval(5)
    computer.set_default_mpiprocs_per_machine(1)
    computer.configure(
        url=firecrest_server.url,
        token_uri=firecrest_server.token_uri,
        client_id=firecrest_server.client_id,
        client_secret=firecrest_server.client_secret,
        client_machine=firecrest_server.machine,
        small_file_size_mb=firecrest_server.small_file_size_mb,
    )
    return computer


@pytest.mark.usefixtures("aiida_profile_clean")
def test_whoami(firecrest_computer: orm.Computer):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    assert isinstance(transport.whoami(), str)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_create_temp_file(firecrest_computer: orm.Computer, tmp_path: Path):
    """Check if it is possible to create a temporary file
    and then delete it in the work directory.
    """
    transport = firecrest_computer.get_transport()
    authinfo = firecrest_computer.get_authinfo(orm.User.collection.get_default())
    workdir = authinfo.get_workdir().format(username=transport.whoami())
    transport.chdir(workdir)

    tmp_path.joinpath("test.txt").write_text("test")
    transport.putfile(str(tmp_path.joinpath("test.txt")), "test.txt")

    assert transport.path_exists("test.txt")

    transport.getfile("test.txt", str(tmp_path.joinpath("test2.txt")))

    assert tmp_path.joinpath("test2.txt").read_text() == "test"

    transport.remove("test.txt")

    assert not transport.path_exists("test.txt")


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_jobs(firecrest_computer: orm.Computer):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    scheduler = firecrest_computer.get_scheduler()
    scheduler.set_transport(transport)
    assert isinstance(scheduler.get_jobs(), list)
