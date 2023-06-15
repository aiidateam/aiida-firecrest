###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Scheduler interface."""
from __future__ import annotations

import re
import string
from typing import TYPE_CHECKING

from aiida.engine.processes.exit_code import ExitCode
from aiida.schedulers import Scheduler
from aiida.schedulers.datastructures import JobInfo, JobTemplate
from aiida.schedulers.plugins.slurm import SlurmJobResource

if TYPE_CHECKING:
    from aiida_firecrest.transport import FirecrestTransport


class FirecrestScheduler(Scheduler):
    """Scheduler interface for FirecREST."""

    _job_resource_class = SlurmJobResource

    transport: FirecrestTransport

    def _get_submit_script_header(self, job_tmpl: JobTemplate) -> str:
        """
        Return the submit script header, using the parameters from the
        job_tmpl.

        Args:
           job_tmpl: an JobTemplate instance with relevant parameters set.

        TODO: truncate the title if too long
        """
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
                )
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
                    f"max_memory_kb must be a positive integer (in kB)! It is instead `{job_tmpl.max_memory_kb}`"
                )
            # --mem: Specify the real memory required per node in MegaBytes.
            # --mem and  --mem-per-cpu  are  mutually exclusive.
            lines.append(f"#SBATCH --mem={physical_memory_kb // 1024}")

        if job_tmpl.custom_scheduler_commands:
            lines.append(job_tmpl.custom_scheduler_commands)

        return "\n".join(lines)

    def submit_job(self, working_directory: str, filename: str) -> str | ExitCode:
        return self.transport.submit_job(working_directory, filename)

    def get_jobs(
        self,
        jobs: list[str] | None = None,
        user: str | None = None,
        as_dict: bool = False,
    ) -> list[JobInfo] | dict[str, JobInfo]:
        return self.transport.get_jobs(jobs, user, as_dict)

    def kill_job(self, jobid: str) -> bool:
        return self.transport.kill_job(jobid)
