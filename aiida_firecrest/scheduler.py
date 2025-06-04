"""Scheduler interface."""

from __future__ import annotations

import datetime
import itertools
from pathlib import Path
import re
import string
import time
from typing import TYPE_CHECKING, Any, ClassVar

from aiida.engine.processes.exit_code import ExitCode
from aiida.schedulers import Scheduler, SchedulerError
from aiida.schedulers.datastructures import JobInfo, JobState, JobTemplate
from aiida.schedulers.plugins.slurm import _TIME_REGEXP, SlurmJobResource
from firecrest.FirecrestException import FirecrestException

from .utils import convert_header_exceptions

if TYPE_CHECKING:
    from aiida_firecrest.transport import FirecrestTransport


class FirecrestScheduler(Scheduler):
    """Scheduler interface for FirecREST.
    It must be used together with the 'firecrest' transport plugin.
    """

    transport: FirecrestTransport
    _job_resource_class = SlurmJobResource
    _features: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "can_query_by_user": False,
    }
    _logger = Scheduler._logger.getChild("firecrest")
    _DEFAULT_PAGE_SIZE = 25

    def _get_submit_script_header(self, job_tmpl: JobTemplate) -> str:
        """
        Return the submit script header, using the parameters from the
        job_tmpl.

        :params job_tmpl: an JobTemplate instance with relevant parameters set.

        TODO: truncate the title if too long
        """
        # adapted from Slurm plugin
        lines = []
        if job_tmpl.submit_as_hold:
            lines.append("#SBATCH -H")

        if job_tmpl.rerunnable:
            lines.append("#SBATCH --requeue")
        else:
            lines.append("#SBATCH --no-requeue")

        if job_tmpl.email:
            # If not specified, but email events are set, SLURM
            # sends the mail to the job owner by default
            lines.append(f"#SBATCH --mail-user={job_tmpl.email}")

        if job_tmpl.email_on_started:
            lines.append("#SBATCH --mail-type=BEGIN")
        if job_tmpl.email_on_terminated:
            lines.append("#SBATCH --mail-type=FAIL")
            lines.append("#SBATCH --mail-type=END")

        if job_tmpl.job_name:
            # The man page does not specify any specific limitation
            # on the job name.
            # Just to be sure, I remove unwanted characters, and I
            # trim it to length 128

            # I leave only letters, numbers, dots, dashes and underscores
            # Note: I don't compile the regexp, I am going to use it only once
            job_title = re.sub(r"[^a-zA-Z0-9_.-]+", "", job_tmpl.job_name)

            # prepend a 'j' (for 'job') before the string if the string
            # is now empty or does not start with a valid charachter
            if not job_title or (
                job_title[0] not in string.ascii_letters + string.digits
            ):
                job_title = f"j{job_title}"

            # Truncate to the first 128 characters
            # Nothing is done if the string is shorter.
            job_title = job_title[:128]

            lines.append(f'#SBATCH --job-name="{job_title}"')

        if job_tmpl.import_sys_environment:
            lines.append("#SBATCH --get-user-env")

        if job_tmpl.sched_output_path:
            lines.append(f"#SBATCH --output={job_tmpl.sched_output_path}")

        if job_tmpl.sched_join_files:
            # TODO: manual says:  # pylint: disable=fixme
            # By  default both standard output and standard error are directed
            # to a file of the name "slurm-%j.out", where the "%j" is replaced
            # with  the  job  allocation  number.
            # See that this automatic redirection works also if
            # I specify a different --output file
            if job_tmpl.sched_error_path:
                self.logger.info(
                    "sched_join_files is True, but sched_error_path is set in "
                    "SLURM script; ignoring sched_error_path"
                )
        else:
            if job_tmpl.sched_error_path:
                lines.append(f"#SBATCH --error={job_tmpl.sched_error_path}")
            else:
                # To avoid automatic join of files
                lines.append("#SBATCH --error=slurm-%j.err")

        if job_tmpl.queue_name:
            lines.append(f"#SBATCH --partition={job_tmpl.queue_name}")

        if job_tmpl.account:
            lines.append(f"#SBATCH --account={job_tmpl.account}")

        if job_tmpl.qos:
            lines.append(f"#SBATCH --qos={job_tmpl.qos}")

        if job_tmpl.priority:
            #  Run the job with an adjusted scheduling priority  within  SLURM.
            #  With no adjustment value the scheduling priority is decreased by
            #  100. The adjustment range is from -10000 (highest  priority)  to
            #  10000  (lowest  priority).
            lines.append(f"#SBATCH --nice={job_tmpl.priority}")

        if not job_tmpl.job_resource:
            raise ValueError(
                "Job resources (as the num_machines) are required for the SLURM scheduler plugin"
            )

        lines.append(f"#SBATCH --nodes={job_tmpl.job_resource.num_machines}")
        if job_tmpl.job_resource.num_mpiprocs_per_machine:
            lines.append(
                f"#SBATCH --ntasks-per-node={job_tmpl.job_resource.num_mpiprocs_per_machine}"
            )

        if job_tmpl.job_resource.num_cores_per_mpiproc:
            lines.append(
                f"#SBATCH --cpus-per-task={job_tmpl.job_resource.num_cores_per_mpiproc}"
            )

        if job_tmpl.max_wallclock_seconds is not None:
            try:
                tot_secs = int(job_tmpl.max_wallclock_seconds)
                if tot_secs <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError(
                    "max_wallclock_seconds must be "
                    "a positive integer (in seconds)! It is instead '{}'"
                    "".format(job_tmpl.max_wallclock_seconds)
                ) from None
            days = tot_secs // 86400
            tot_hours = tot_secs % 86400
            hours = tot_hours // 3600
            tot_minutes = tot_hours % 3600
            minutes = tot_minutes // 60
            seconds = tot_minutes % 60
            if days == 0:
                lines.append(f"#SBATCH --time={hours:02d}:{minutes:02d}:{seconds:02d}")
            else:
                lines.append(
                    f"#SBATCH --time={days:d}-{hours:02d}:{minutes:02d}:{seconds:02d}"
                )

        # It is the memory per node, not per cpu!
        if job_tmpl.max_memory_kb:
            try:
                physical_memory_kb = int(job_tmpl.max_memory_kb)
                if physical_memory_kb <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError(
                    "max_memory_kb must be a positive integer (in kB)! "
                    f"It is instead `{job_tmpl.max_memory_kb}`"
                ) from None
            # --mem: Specify the real memory required per node in MegaBytes.
            # --mem and  --mem-per-cpu  are  mutually exclusive.
            lines.append(f"#SBATCH --mem={physical_memory_kb // 1024}")

        if job_tmpl.custom_scheduler_commands:
            lines.append(job_tmpl.custom_scheduler_commands)

        return "\n".join(lines)

    def submit_job(self, working_directory: str, filename: str) -> str | ExitCode:
        transport = self.transport
        with convert_header_exceptions({"machine": transport._machine}):
            try:
                result = transport._client.submit(
                    transport._machine,
                    script_remote_path=str(Path(working_directory).joinpath(filename)),
                )
            except FirecrestException as exc:
                raise SchedulerError(str(exc)) from exc
        return str(result["jobid"])

    def get_jobs(
        self,
        jobs: list[str] | None = None,
        user: str | None = None,
        as_dict: bool = False,
    ) -> list[JobInfo] | dict[str, JobInfo]:
        results = []
        transport = self.transport

        with convert_header_exceptions({"machine": transport._machine}):
            # TODO handle pagination (pageSize, pageNumber) if many jobs
            # This will do pagination
            try:
                for page_iter in itertools.count():
                    results += transport._client.poll_active(
                        transport._machine,
                        jobs,
                        page_number=page_iter,
                        page_size=self._DEFAULT_PAGE_SIZE,
                    )
                    if len(results) < self._DEFAULT_PAGE_SIZE * (page_iter + 1):
                        break
            except FirecrestException as exc:
                # TODO: check what type of error is returned and handle it properly
                if "Invalid job id" not in str(exc):
                    # firecrest returns error if the job is completed, while aiida expect a silent return
                    raise SchedulerError(str(exc)) from exc
        job_list = []
        for raw_result in results:
            # TODO: probably the if below is not needed, because recently
            # the server should return only the jobs of the current user
            if user is not None and raw_result["user"] != user:
                continue
            this_job = JobInfo()  # type: ignore

            this_job.job_id = raw_result["jobid"]
            # TODO: firecrest does not return the annotation, so set to an empty string.
            # To be investigated how important that is.
            this_job.annotation = ""

            job_state_raw = raw_result["state"]

            try:
                job_state_string = _MAP_STATUS_SLURM[job_state_raw]
            except KeyError:
                self.logger.warning(
                    f"Unrecognized job_state '{job_state_raw}' for job id {this_job.job_id}"
                )
                job_state_string = JobState.UNDETERMINED
            # QUEUED_HELD states are not specific states in SLURM;
            # they are instead set with state QUEUED, and then the
            # annotation tells if the job is held.
            # I check for 'Dependency', 'JobHeldUser',
            # 'JobHeldAdmin', 'BeginTime'.
            # Other states should not bring the job in QUEUED_HELD, I believe
            # (the man page of slurm seems to be incomplete, for instance
            # JobHeld* are not reported there; I also checked at the source code
            # of slurm 2.6 on github (https://github.com/SchedMD/slurm),
            # file slurm/src/common/slurm_protocol_defs.c,
            # and these seem all the states to be taken into account for the
            # QUEUED_HELD status).
            # There are actually a few others, like possible
            # failures, or partition-related reasons, but for the moment I
            # leave them in the QUEUED state.
            if job_state_string == JobState.QUEUED and this_job.annotation in [
                "Dependency",
                "JobHeldUser",
                "JobHeldAdmin",
                "BeginTime",
            ]:
                job_state_string = JobState.QUEUED_HELD

            this_job.job_state = job_state_string

            # The rest is optional

            this_job.job_owner = raw_result["user"]

            try:
                this_job.num_machines = int(raw_result["nodes"])
            except ValueError:
                self.logger.warning(
                    "The number of allocated nodes is not "
                    "an integer ({}) for job id {}!".format(
                        raw_result["nodes"], this_job.job_id
                    )
                )

            # See issue https://github.com/aiidateam/aiida-firecrest/issues/39
            # try:
            #     this_job.num_mpiprocs = int(thisjob_dict['number_cpus'])
            # except ValueError:
            #     self.logger.warning(
            #         'The number of allocated cores is not '
            #         'an integer ({}) for job id {}!'.format(
            # thisjob_dict['number_cpus'], this_job.job_id)
            #     )

            # ALLOCATED NODES HERE
            # string may be in the format
            # nid00[684-685,722-723,748-749,958-959]
            # therefore it requires some parsing, that is unnecessary now.
            # I just store is as a raw string for the moment, and I leave
            # this_job.allocated_machines undefined
            if this_job.job_state == JobState.RUNNING:
                this_job.allocated_machines_raw = raw_result["nodelist"]

            this_job.queue_name = raw_result["partition"]

            try:
                time_left = self._convert_time(raw_result["time_left"])
                start_time = self._convert_time(raw_result["start_time"])

                if time_left is None or start_time is None:
                    this_job.requested_wallclock_time_seconds = 0
                else:
                    this_job.requested_wallclock_time_seconds = time_left + start_time

            except ValueError:
                self.logger.warning(
                    f"Couldn't parse the time limit for job id {this_job.job_id}"
                )

            # Only if it is RUNNING; otherwise it is not meaningful,
            # and may be not set (in my test, it is set to zero)
            if this_job.job_state == JobState.RUNNING:
                try:
                    wallclock_time_seconds = self._convert_time(
                        raw_result["start_time"]
                    )
                    if wallclock_time_seconds is not None:
                        this_job.wallclock_time_seconds = wallclock_time_seconds
                    else:
                        this_job.wallclock_time_seconds = 0
                except ValueError:
                    self.logger.warning(
                        f"Couldn't parse time_used for job id {this_job.job_id}"
                    )

            # dispatch_time is not returned explicitly by the FirecREST server
            # see: https://github.com/aiidateam/aiida-firecrest/issues/40
            #     try:
            #         this_job.dispatch_time = self._parse_time_string(thisjob_dict['dispatch_time'])
            #     except ValueError:
            #         self.logger.warning(f'Error parsing dispatch_time for job id {this_job.job_id}')

            try:
                this_job.submission_time = self._parse_time_string(raw_result["time"])
            except ValueError:
                self.logger.warning(
                    f"Couldn't parse submission_time for job id {this_job.job_id}"
                )

            this_job.title = raw_result["name"]

            # Everything goes here anyway for debugging purposes
            this_job.raw_data = raw_result

            # Double check of redundant info
            # Not really useful now, allocated_machines in this
            # version of the plugin is never set
            if (
                this_job.allocated_machines is not None
                and this_job.num_machines is not None
                and len(this_job.allocated_machines) != this_job.num_machines
            ):
                self.logger.error(
                    "The length of the list of allocated "
                    "nodes ({}) is different from the "
                    "expected number of nodes ({})!".format(
                        len(this_job.allocated_machines), this_job.num_machines
                    )
                )

            # I append to the list of jobs to return
            job_list.append(this_job)

        if as_dict:
            return {job.job_id: job for job in job_list}

        return job_list

    def kill_job(self, jobid: str) -> bool:
        transport = self.transport
        with convert_header_exceptions({"machine": transport._machine}):
            transport._client.cancel(transport._machine, jobid)
        return True

    def _convert_time(self, string: str) -> int | None:
        """
        Note: this function was copied from the Slurm scheduler plugin
        Convert a string in the format DD-HH:MM:SS to a number of seconds.
        """
        if string == "UNLIMITED":
            return 2147483647  # == 2**31 - 1, largest 32-bit signed integer (68 years)

        if string == "NOT_SET" or string == "N/A":
            return None

        groups = _TIME_REGEXP.match(string)
        if groups is None:
            raise ValueError("Unrecognized format for time string.")

        groupdict = groups.groupdict()
        # should not raise a ValueError, they all match digits only
        days = int(groupdict["days"] if groupdict["days"] is not None else 0)
        hours = int(groupdict["hours"] if groupdict["hours"] is not None else 0)
        mins = int(groupdict["minutes"] if groupdict["minutes"] is not None else 0)
        secs = int(groupdict["seconds"] if groupdict["seconds"] is not None else 0)

        return days * 86400 + hours * 3600 + mins * 60 + secs

    def _parse_time_string(
        self, string: str, fmt: str = "%Y-%m-%dT%H:%M:%S"
    ) -> datetime.datetime:
        """
        Note: this function was copied from the Slurm scheduler plugin
        Parse a time string in the format returned from qstat -f and
        returns a datetime object.
        """

        try:
            time_struct = time.strptime(string, fmt)
        except Exception as exc:
            self.logger.debug(
                f"Unable to parse time string {string}, the message was {exc}"
            )
            raise ValueError("Problem parsing the time string.") from exc

        # I convert from a time_struct to a datetime object going through
        # the seconds since epoch, as suggested on stackoverflow:
        # http://stackoverflow.com/questions/1697815
        return datetime.datetime.fromtimestamp(time.mktime(time_struct))


# see https://slurm.schedmd.com/squeue.html#lbAG
# note firecrest returns full names, not abbreviations
_MAP_STATUS_SLURM = {
    "CA": JobState.DONE,
    "CANCELLED": JobState.DONE,
    "CD": JobState.DONE,
    "COMPLETED": JobState.DONE,
    "CF": JobState.QUEUED,
    "CONFIGURING": JobState.QUEUED,
    "CG": JobState.RUNNING,
    "COMPLETING": JobState.RUNNING,
    "F": JobState.DONE,
    "FAILED": JobState.DONE,
    "NF": JobState.DONE,
    "NODE_FAIL": JobState.DONE,
    "PD": JobState.QUEUED,
    "PENDING": JobState.QUEUED,
    "PR": JobState.DONE,
    "PREEMPTED": JobState.DONE,
    "R": JobState.RUNNING,
    "RUNNING": JobState.RUNNING,
    "S": JobState.SUSPENDED,
    "SUSPENDED": JobState.SUSPENDED,
    "TO": JobState.DONE,
    "TIMEOUT": JobState.DONE,
}
