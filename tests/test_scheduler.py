from pathlib import Path

from aiida import orm
from aiida.schedulers.datastructures import CodeRunMode, JobTemplate
import pytest

from aiida_firecrest.scheduler import FirecrestScheduler
from conftest import Values


@pytest.mark.usefixtures("aiida_profile_clean")
def test_submit_job(firecrest_computer: orm.Computer, tmp_path: Path):
    transport = firecrest_computer.get_transport()
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    with pytest.raises(FileNotFoundError):
        scheduler.submit_job(transport.getcwd(), "unknown.sh")

    _script = Path(tmp_path / "job.sh")
    _script.write_text("#!/bin/bash\n\necho 'hello world'")

    job_id = scheduler.submit_job(transport.getcwd(), _script)
    # this is how aiida expects the job_id to be returned
    assert isinstance(job_id, str)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get_jobs(firecrest_computer: orm.Computer):
    transport = firecrest_computer.get_transport()
    scheduler = FirecrestScheduler()
    scheduler.set_transport(transport)

    # test pagaination
    scheduler._DEFAULT_PAGE_SIZE = 2
    Values._DEFAULT_PAGE_SIZE = 2

    joblist = ["111", "222", "333", "444", "555"]
    result = scheduler.get_jobs(joblist)
    assert len(result) == 5
    for i in range(5):
        assert result[i].job_id == str(joblist[i])
        # TODO: one could check states as well


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
    expectaion_flat = "\n".join(line.strip() for line in expectaion.splitlines()).strip(
        "\n"
    )
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
        assert scheduler.get_submit_script(template).rstrip() == expectaion_flat
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

    expectaion_flat = "\n".join(line.strip() for line in expectaion.splitlines()).strip(
        "\n"
    )
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
        assert scheduler.get_submit_script(template).rstrip() == expectaion_flat
    except AssertionError:
        print(scheduler.get_submit_script(template).rstrip())
        print(expectaion)
        raise
