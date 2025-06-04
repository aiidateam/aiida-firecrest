###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
from pathlib import Path
import textwrap
from time import sleep

from aiida import orm
from aiida.schedulers import SchedulerError
from aiida.schedulers.datastructures import CodeRunMode, JobTemplate
import pytest

from aiida_firecrest.scheduler import FirecrestScheduler


@pytest.mark.usefixtures("aiida_profile_clean")
def test_submit_job(firecrest_computer: orm.Computer, firecrest_config, tmpdir: Path):
    """Test submitting a job to the scheduler.
    Note: this test relies on a functional transport.put() method."""

    transport = firecrest_computer.get_transport()
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    # raise error if file not found
    with pytest.raises(SchedulerError):
        scheduler.submit_job(firecrest_config.workdir, "unknown.sh")

    custom_scheduler_commands = "\n    ".join(
        firecrest_config.builder_metadata_options_custom_scheduler_commands
    )

    shell_script = f"""
    #!/bin/bash
    #SBATCH --no-requeue
    #SBATCH --job-name="aiida-1928"
    #SBATCH --get-user-env
    #SBATCH --output=_scheduler-stdout.txt
    #SBATCH --error=_scheduler-stderr.txt
    #SBATCH --nodes=1
    #SBATCH --ntasks-per-node=1
    {custom_scheduler_commands}

    echo 'hello world'
    """

    dedented_script = textwrap.dedent(shell_script).strip()
    Path(tmpdir / "job.sh").write_text(dedented_script)
    remote_ = Path(firecrest_config.workdir).joinpath("job.sh")
    transport.put(tmpdir / "job.sh", remote_)

    job_id = scheduler.submit_job(firecrest_config.workdir, "job.sh")

    assert isinstance(job_id, str)


@pytest.mark.timeout(180)
@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_and_kill_jobs(
    firecrest_computer: orm.Computer, firecrest_config, tmpdir: Path
):
    """Test getting and killing jobs from the scheduler.
    We test the two together for performance reasons, as this test might run against
      a real server and we don't want to leave parasitic jobs behind.
      also less billing for the user.
    Note: this test relies on a functional transport.put() method.
    """
    import time

    transport = firecrest_computer.get_transport()
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    # verify that no error is raised in the case of an invalid job id 000
    scheduler.get_jobs(["000"])

    custom_scheduler_commands = "\n    ".join(
        firecrest_config.builder_metadata_options_custom_scheduler_commands
    )
    shell_script = f"""
    #!/bin/bash
    #SBATCH --no-requeue
    #SBATCH --job-name="aiida-1929"
    #SBATCH --get-user-env
    #SBATCH --output=_scheduler-stdout.txt
    #SBATCH --error=_scheduler-stderr.txt
    #SBATCH --nodes=1
    #SBATCH --ntasks-per-node=1
    {custom_scheduler_commands}

    sleep 180
    """

    joblist = []
    dedented_script = textwrap.dedent(shell_script).strip()
    Path(tmpdir / "job.sh").write_text(dedented_script)
    remote_ = Path(firecrest_config.workdir).joinpath("job.sh")
    transport.put(tmpdir / "job.sh", remote_)

    for _ in range(5):
        joblist.append(scheduler.submit_job(firecrest_config.workdir, "job.sh"))

    # test pagaination is working
    scheduler._DEFAULT_PAGE_SIZE = 2
    result = scheduler.get_jobs(joblist)
    assert len(result) == 5
    for i in range(5):
        assert result[i].job_id in joblist
        # TODO: one could check states as well

    # test kill jobs
    for jobid in joblist:
        scheduler.kill_job(jobid)

    # sometimes it takes time for the server to actually kill the jobs
    timeout_kill = 5  # seconds
    start_time = time.time()
    while time.time() - start_time < timeout_kill:
        result = scheduler.get_jobs(joblist)
        if not len(result):
            break
        sleep(0.5)

    assert not len(result)


def test_write_script_full():
    # to avoid false positive (overwriting on existing file),
    # we check the output of the script instead of using `file_regression``
    expectaion = """
    #!/bin/bash
    #SBATCH -H
    #SBATCH --requeue
    #SBATCH --mail-user=True
    #SBATCH --mail-type=BEGIN
    #SBATCH --mail-type=FAIL
    #SBATCH --mail-type=END
    #SBATCH --job-name="test_job"
    #SBATCH --get-user-env
    #SBATCH --output=test.out
    #SBATCH --error=test.err
    #SBATCH --partition=test_queue
    #SBATCH --account=test_account
    #SBATCH --qos=test_qos
    #SBATCH --nice=100
    #SBATCH --nodes=1
    #SBATCH --ntasks-per-node=1
    #SBATCH --time=01:00:00
    #SBATCH --mem=1
    test_command
    """
    expectation_flat = "\n".join(
        line.strip() for line in expectaion.splitlines()
    ).strip("\n")
    scheduler = FirecrestScheduler()
    template = JobTemplate(
        {
            "job_resource": scheduler.create_job_resource(
                num_machines=1, num_mpiprocs_per_machine=1
            ),
            "codes_info": [],
            "codes_run_mode": CodeRunMode.SERIAL,
            "submit_as_hold": True,
            "rerunnable": True,
            "email": True,
            "email_on_started": True,
            "email_on_terminated": True,
            "job_name": "test_job",
            "import_sys_environment": True,
            "sched_output_path": "test.out",
            "sched_error_path": "test.err",
            "queue_name": "test_queue",
            "account": "test_account",
            "qos": "test_qos",
            "priority": 100,
            "max_wallclock_seconds": 3600,
            "max_memory_kb": 1024,
            "custom_scheduler_commands": "test_command",
        }
    )
    try:
        assert scheduler.get_submit_script(template).rstrip() == expectation_flat
    except AssertionError:
        print(scheduler.get_submit_script(template).rstrip())
        print(expectaion)
        raise


def test_write_script_minimal():
    # to avoid false positive (overwriting on existing file),
    # we check the output of the script instead of using `file_regression``
    expectaion = """
    #!/bin/bash
    #SBATCH --no-requeue
    #SBATCH --error=slurm-%j.err
    #SBATCH --nodes=1
    #SBATCH --ntasks-per-node=1
    """

    expectation_flat = "\n".join(
        line.strip() for line in expectaion.splitlines()
    ).strip("\n")
    scheduler = FirecrestScheduler()
    template = JobTemplate(
        {
            "job_resource": scheduler.create_job_resource(
                num_machines=1, num_mpiprocs_per_machine=1
            ),
            "codes_info": [],
            "codes_run_mode": CodeRunMode.SERIAL,
        }
    )

    try:
        assert scheduler.get_submit_script(template).rstrip() == expectation_flat
    except AssertionError:
        print(scheduler.get_submit_script(template).rstrip())
        print(expectaion)
        raise
