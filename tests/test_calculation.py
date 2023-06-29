"""Test for running calculations on a FireCREST computer."""
from aiida import engine, manage, orm
from aiida.engine.processes.calcjobs.tasks import MAX_ATTEMPTS_OPTION
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
def test_calculation(firecrest_computer: orm.Computer):
    """Test running a simple calculation."""
    firecrest_computer.store()

    code = orm.InstalledCode(
        label="test_code",
        description="test code",
        default_calc_job_plugin="core.arithmetic.add",
        computer=firecrest_computer,
        filepath_executable="/bin/sh",
    )
    code.store()

    builder = code.get_builder()
    builder.x = orm.Int(1)
    builder.y = orm.Int(2)
    # TODO currently uploading via firecrest changes _aiidasubmit.sh to aiidasubmit.sh 😱
    # https://github.com/eth-cscs/firecrest/issues/191
    builder.metadata.options.submit_script_filename = "aiidasubmit.sh"

    # TODO reset in fixture?
    manage.get_config().set_option(MAX_ATTEMPTS_OPTION, 1)

    _, node = engine.run_get_node(builder)
    assert node.is_finished_ok
