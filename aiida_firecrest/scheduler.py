###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Scheduler interface."""
from aiida.schedulers import Scheduler
from aiida.schedulers.plugins.slurm import SlurmJobResource


class FirecrestScheduler(Scheduler):
    """Scheduler interface for FirecREST."""

    _job_resource_class = SlurmJobResource

    def _get_submit_script_header(self, job_tmpl):
        raise NotImplementedError

    def _get_joblist_command(self, jobs=None, user=None):
        raise NotImplementedError

    def _parse_joblist_output(self, retval, stdout, stderr):
        raise NotImplementedError

    def _get_submit_command(self, submit_script):
        raise NotImplementedError

    def _parse_submit_output(self, retval, stdout, stderr):
        raise NotImplementedError

    def _get_kill_command(self, jobid):
        raise NotImplementedError

    def _parse_kill_output(self, retval, stdout, stderr):
        raise NotImplementedError
