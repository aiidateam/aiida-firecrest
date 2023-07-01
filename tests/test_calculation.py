"""Test for running calculations on a FireCREST computer."""
from pathlib import Path

from aiida import common, engine, manage, orm
from aiida.common.folders import Folder
from aiida.engine.processes.calcjobs.tasks import MAX_ATTEMPTS_OPTION
from aiida.manage.tests.pytest_fixtures import EntryPointManager
from aiida.parsers import Parser
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
    computer.store()
    return computer


@pytest.mark.usefixtures("aiida_profile_clean")
def test_calculation_basic(firecrest_computer: orm.Computer):
    """Test running a simple `arithmetic.add` calculation."""
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

    # TODO reset in fixture? also the calculation seems to hang on errors still
    # rather than failing immediately
    manage.get_config().set_option(MAX_ATTEMPTS_OPTION, 1)

    _, node = engine.run_get_node(builder)
    assert node.is_finished_ok


@pytest.mark.usefixtures("aiida_profile_clean")
def test_calculation_file_transfer(
    firecrest_computer: orm.Computer, entry_points: EntryPointManager
):
    """Test a more calculation, with multiple files uploaded/retrieved."""
    entry_points.add(MultiFileCalcjob, "aiida.calculations:testing.multifile")
    entry_points.add(NoopParser, "aiida.parsers:testing.noop")

    code = orm.InstalledCode(
        label="test_code",
        description="test code",
        default_calc_job_plugin="testing.multifile",
        computer=firecrest_computer,
        filepath_executable="/bin/sh",
    )
    code.store()

    builder = code.get_builder()

    # TODO reset in fixture?
    manage.get_config().set_option(MAX_ATTEMPTS_OPTION, 1)

    node: orm.CalcJobNode
    _, node = engine.run_get_node(builder)
    assert node.is_finished_ok

    if (retrieved := node.get_retrieved_node()) is None:
        raise RuntimeError("No retrieved node found")

    paths = sorted([str(p) for p in retrieved.base.repository.glob()])
    assert paths == [
        "_scheduler-stderr.txt",
        "_scheduler-stdout.txt",
        "folder1",
        "folder1/a",
        "folder1/a/b.txt",
        "folder1/a/c.txt",
        "folder2",
        "folder2/x",
        "folder2/y",
        "folder2/y/z",
    ]


class MultiFileCalcjob(engine.CalcJob):
    """A complex CalcJob that creates/retrieves multiple files."""

    @classmethod
    def define(cls, spec):
        """Define the process specification."""
        super().define(spec)
        spec.input(
            "metadata.options.submit_script_filename",
            valid_type=str,
            default="aiidasubmit.sh",
        )
        spec.inputs["metadata"]["options"]["resources"].default = {
            "num_machines": 1,
            "num_mpiprocs_per_machine": 1,
        }
        spec.input(
            "metadata.options.parser_name", valid_type=str, default="testing.noop"
        )
        spec.exit_code(400, "ERROR", message="Calculation failed.")

    def prepare_for_submission(self, folder: Folder) -> common.CalcInfo:
        """Prepare the calculation job for submission."""
        codeinfo = common.CodeInfo()
        codeinfo.code_uuid = self.inputs.code.uuid

        path = Path(folder.get_abs_path("a")).parent
        for subpath in [
            "i.txt",
            "j.txt",
            "folder1/a/b.txt",
            "folder1/a/c.txt",
            "folder1/a/c.in",
            "folder1/c.txt",
            "folder2/x",
            "folder2/y/z",
        ]:
            path.joinpath(subpath).parent.mkdir(parents=True, exist_ok=True)
            path.joinpath(subpath).touch()

        calcinfo = common.CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.retrieve_list = [("folder1/*/*.txt", ".", 99), ("folder2", ".", 99)]

        return calcinfo


class NoopParser(Parser):
    """Parser that does absolutely nothing!"""

    def parse(self, **kwargs):
        pass
