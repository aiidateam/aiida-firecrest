################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
"""Test for running calculations on a FireCREST computer."""

from pathlib import Path

from aiida import common, engine, manage, orm
from aiida.common.folders import Folder
from aiida.engine.processes.calcjobs.tasks import MAX_ATTEMPTS_OPTION
from aiida.tools.pytest_fixtures.entry_points import EntryPointManager
from aiida.parsers import Parser
import pytest

from aiida_firecrest.utils import FcPath


@pytest.fixture(name="no_retries")
def _no_retries():
    """Remove calcjob retries, to make failing the test faster."""
    # TODO calculation seems to hang on errors still
    max_attempts = manage.get_config().get_option(MAX_ATTEMPTS_OPTION)
    manage.get_config().set_option(MAX_ATTEMPTS_OPTION, 1)
    yield
    manage.get_config().set_option(MAX_ATTEMPTS_OPTION, max_attempts)


@pytest.mark.timeout(180)
@pytest.mark.usefixtures("aiida_profile_clean", "no_retries")
def test_calculation_basic(firecrest_computer: orm.Computer, firecrest_config):
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
    custom_scheduler_commands = "\n".join(
        firecrest_config.builder_metadata_options_custom_scheduler_commands
    )
    builder.metadata.options.custom_scheduler_commands = custom_scheduler_commands

    _, node = engine.run_get_node(builder)
    assert node.is_finished_ok


@pytest.mark.usefixtures("aiida_profile_clean", "no_retries")
def test_calculation_file_transfer(
    firecrest_computer: orm.Computer, entry_points: EntryPointManager, tmpdir: Path
):
    """Test a calculation, with multiple files copied/uploaded/retrieved."""
    # add temporary entry points
    entry_points.add(MultiFileCalcjob, "aiida.calculations:testing.multifile")
    entry_points.add(NoopParser, "aiida.parsers:testing.noop")

    # add a remote file which is used by remote_copy_list
    touched_file = Path(tmpdir / "remote_copy.txt")
    touched_file.write_text("touch")
    transport = firecrest_computer.get_transport()
    transport.put(
        str(touched_file), FcPath(firecrest_computer.get_workdir()) / "remote_copy.txt"
    )

    # setup the calculation
    code = orm.InstalledCode(
        label="test_code",
        description="test code",
        default_calc_job_plugin="testing.multifile",
        computer=firecrest_computer,
        filepath_executable="/bin/sh",
    )
    code.store()
    builder = code.get_builder()

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
        "folder2/remote_copy.txt",
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
            path.joinpath(subpath).write_text("touch")

        calcinfo = common.CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.retrieve_list = [("folder1/*/*.txt", ".", 99), ("folder2", ".", 99)]
        comp: orm.Computer = self.inputs.code.computer
        calcinfo.remote_copy_list = [
            (
                comp.uuid,
                f"{comp.get_workdir()}/remote_copy.txt",
                "folder2/remote_copy.txt",
            )
        ]
        # TODO also add remote_symlink_list

        return calcinfo


class NoopParser(Parser):
    """Parser that does absolutely nothing!"""

    def parse(self, **kwargs):
        pass
