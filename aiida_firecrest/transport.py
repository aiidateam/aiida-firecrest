###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Transport interface."""
from __future__ import annotations

import os
import posixpath
from pathlib import Path, PurePosixPath
from typing import Any, NamedTuple, TypedDict

import firecrest as f7t
from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.cmdline.params.types.path import AbsolutePathOrEmptyParamType
from aiida.engine.processes.exit_code import ExitCode
from aiida.schedulers.datastructures import JobInfo, JobState
from aiida.schedulers.plugins.slurm import _MAP_STATUS_SLURM
from aiida.transports import Transport
from click.types import ParamType
from firecrest.FirecrestException import HeaderException


class ValidAuthOption(TypedDict, total=False):  # type: ignore
    option: OverridableOption | None  # existing option
    switch: bool  # whether the option is a boolean flag
    type: type[Any] | ParamType  # noqa: A003
    default: Any
    non_interactive_default: bool  # whether option should provide a default in non-interactive mode
    prompt: str  # for interactive CLI
    help: str  # noqa: A003


def login_decorator(func):
    def _decorator(self, *args, **kwargs):
        return self.keycloak.account_login(func)(self, *args, **kwargs)

    return _decorator


class FirecrestAuthorizationClass:
    def __init__(self, token_uri: str, client_id: str, client_secret: str):
        self.keycloak = f7t.ClientCredentialsAuth(
            client_id,
            client_secret,
            token_uri,
        )

    # TODO maybe only check if token is valid on Transport.open? (rather than on every call)
    @login_decorator
    def get_access_token(self):
        return self.keycloak.get_access_token()


class StatResult(NamedTuple):
    """Result of a stat call."""

    st_mode: int  # protection bits,
    # st_ino: int  # inode number,
    # st_dev: int  # device,
    # st_nlink: int  # number of hard links,
    st_uid: int  # user id of owner,
    st_gid: int  # group id of owner,
    st_size: int  # size of file, in bytes,


class FirecrestTransport(Transport):
    """Transport interface for FirecREST."""

    _valid_auth_options: list[tuple[str, dict]] = [
        (
            "url",
            {
                "type": str,
                "non_interactive_default": True,
                "prompt": "Server URL",
                "help": "URL to FirecREST server",
            },
        ),
        (
            "token_uri",
            {
                "type": str,
                "non_interactive_default": True,
                "prompt": "Token URI",
                "help": "URI for retrieving FirecREST authentication tokens",
            },
        ),
        (
            "client_id",
            {
                "type": str,
                "non_interactive_default": True,
                "prompt": "Client ID",
                "help": "FirecREST client ID",
            },
        ),
        (
            "secret_path",
            {
                "type": AbsolutePathOrEmptyParamType(dir_okay=False, exists=True),
                "non_interactive_default": True,
                "prompt": "Secret key file",
                "help": "Absolute path to file containing FirecREST client secret",
            },
            # TODO: format of secret file, and lookup secret by default in ~/.firecrest/secrets.json
        ),
    ]

    def __init__(
        self,
        *,
        url: str,
        token_uri: str,
        client_id: str,
        client_secret: str | Path,
        machine: str,
        **kwargs: Any,
    ):
        super().__init__()
        self._machine = machine
        self._url = url
        self._token_uri = token_uri
        self._client_id = client_id

        secret = (
            client_secret.read_text()
            if isinstance(client_secret, Path)
            else client_secret
        )

        self._client = f7t.Firecrest(
            firecrest_url=self._url,
            authorization=FirecrestAuthorizationClass(
                token_uri=self._token_uri, client_id=client_id, client_secret=secret
            ),
        )

        self._cwd = PurePosixPath()

    def _get_path(self, *path: str) -> str:
        # TODO ensure all remote paths are manipulated with posixpath
        return posixpath.normpath(self._cwd.joinpath(*path))

    def open(self):  # noqa: A003
        # TODO allow for batch connections in pyfirecrest?
        pass

    def close(self):
        pass

    def getcwd(self) -> str:
        return str(self._cwd)

    def chdir(self, path: str, check_exists: bool = True) -> None:
        if check_exists:
            try:
                self._client.file_type(self._machine, path)
            except HeaderException as exc:
                if "X-Invalid-Path" in exc.responses[-1].headers:
                    raise FileNotFoundError(path)
                raise
        self._cwd = PurePosixPath(path)

    def normalize(self, path="."):
        return posixpath.normpath(path)

    def chmod(self, path: str, mode: str):
        self._client.chmod(self._machine, self._get_path(path), mode=mode)

    def chown(self, path, uid: str, gid: str):
        self._client.chown(self._machine, self._get_path(path), owner=uid, group=gid)

    def copy(self, remotesource, remotedestination, dereference=False, recursive=True):
        raise NotImplementedError

    def copyfile(self, remotesource, remotedestination, dereference=False):
        self._client.copy(
            self._machine,
            self._get_path(remotesource),
            self._get_path(remotedestination),
        )

    def copytree(self, remotesource, remotedestination, dereference=False):
        raise NotImplementedError

    def get(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def gettree(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def path_exists(self, path: str) -> bool:
        try:
            self._client.file_type(self._machine, self._get_path(path))
        except HeaderException as exc:
            if "X-Invalid-Path" in exc.responses[-1].headers:
                return False
            raise
        return True

    # TODO: once https://github.com/eth-cscs/firecrest/pull/133 is deployed,
    # then this can be used for stat, get_attribute, isdir, isfile, etc

    def _convert_st_mode(self, data: dict) -> int:
        # created from `ls -l -A --time-style=+%Y-%m-%dT%H:%M:%S`, e.g.
        # -rw-------  1 username usergroup 57 2021-12-02T10:42:00 file.txt
        # type, permissions, # of links (not recorded), user, group, size, last_modified, name

        ftype = {
            "b": "0060",  # block device
            "c": "0020",  # character device
            "d": "0040",  # directory
            "l": "0120",  # Symbolic link
            "s": "0140",  # Socket.
            "p": "0010",  # FIFO
            "-": "0100",  # Regular file
        }
        p = data["permissions"]
        r = lambda x: 4 if x == "r" else 0  # noqa: E731
        w = lambda x: 2 if x == "w" else 0  # noqa: E731
        x = lambda x: 1 if x == "x" else 0  # noqa: E731
        p_int = (
            ((r(p[0]) + w(p[1]) + x(p[2])) * 100)
            + ((r(p[3]) + w(p[4]) + x(p[5])) * 10)
            + ((r(p[6]) + w(p[7]) + x(p[8])) * 100)
        )
        mode = int(ftype[data["type"]] + str(p_int), 8)

        return mode

    def stat(self, path: str) -> dict:
        """Retrieve information about a file on the remote system"""
        # TODO lstat
        dirname, filename = posixpath.split(self._get_path(path))
        try:
            output = self._client.list_files(self._machine, dirname, show_hidden=True)
        except HeaderException as exc:
            if "X-Invalid-Path" in exc.responses[-1].headers:
                raise FileNotFoundError(posixpath.join(dirname, filename))
            raise
        for data in output:
            if data["name"] == filename:
                data["mode"] = self._convert_st_mode(data)
                return data
        raise FileNotFoundError(posixpath.join(dirname, filename))

    def get_attribute(self, path):
        raise NotImplementedError

    def isdir(self, path: str) -> bool:
        try:
            ftype = self._client.file_type(self._machine, self._get_path(path))
        except HeaderException as exc:
            if "X-Invalid-Path" in exc._responses[-1].headers:
                return False
            raise
        return ftype == "directory"

    def isfile(self, path: str) -> bool:
        try:
            ftype = self._client.file_type(self._machine, self._get_path(path))
        except HeaderException as exc:
            if "X-Invalid-Path" in exc.responses[-1].headers:
                return False
            raise
        return ftype != "directory"

    def listdir(self, path: str = ".", pattern: str | None = None) -> list[str]:
        if pattern is not None:
            raise NotImplementedError("pattern matching")
        try:
            output = self._client.list_files(
                self._machine, self._get_path(path), show_hidden=True
            )
        except HeaderException as exc:
            if "X-Invalid-Path" in exc.responses[-1].headers:
                raise FileNotFoundError(self._get_path(path))
            raise
        return [data["name"] for data in output]

    # TODO handle ignore_existing and failure modes for makedir/makedirs

    def makedirs(self, path: str, ignore_existing: bool = False) -> None:
        self._client.mkdir(self._machine, self._get_path(path), p=True)

    def mkdir(self, path: str, ignore_existing: bool = False) -> None:
        self._client.mkdir(self._machine, self._get_path(path), p=False)

    def getfile(self, remotepath: str, localpath: str, *args, **kwargs):
        # TODO simple_download should maybe allow to just write to handle
        # TODO handle large files (maybe use .parameters() to decide if file is large)
        self._client.simple_download(
            self._machine, self._get_path(remotepath), localpath
        )
        # TODO use client.checksum?

    def put(self, localpath, remotepath, *args, **kwargs):
        # TODO ssh does a lot more
        if os.path.isdir(localpath):
            self.puttree(localpath, remotepath)
        elif os.path.isfile(localpath):
            if self.isdir(remotepath):
                remote = os.path.join(remotepath, os.path.split(localpath)[1])
                self.putfile(localpath, remote)
            else:
                self.putfile(localpath, remotepath)

    def putfile(self, localpath: str, remotepath: str, *args, **kwargs):
        # local checks
        if not Path(localpath).is_absolute():
            raise ValueError("The localpath must be an absolute path")

        # TODO handle large files (maybe use .parameters() to decide if file is large)
        # TODO pyfirecrest requires the remotepath to be a directory & takes the name from localpath
        # (see issue #5)
        remotepathlib = PurePosixPath(self._get_path(remotepath))
        assert Path(localpath).name == remotepathlib.name
        # note this allows overwriting
        self._client.simple_upload(self._machine, localpath, str(remotepathlib.parent))

    def puttree(self, localpath: str | Path, remotepath: str, *args, **kwargs):
        localpath = Path(localpath)

        # local checks
        if not localpath.is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not localpath.exists():
            raise OSError("The localpath does not exists")
        if not localpath.is_dir():
            raise ValueError(f"Input localpath is not a folder: {localpath}")

        for dirpath, _, filenames in os.walk(localpath):
            # Get the relative path
            rel_folder = os.path.relpath(path=dirpath, start=localpath)

            if not self.path_exists(os.path.join(remotepath, rel_folder)):
                self.mkdir(os.path.join(remotepath, rel_folder))

            for filename in filenames:
                localfile_path = os.path.join(localpath, rel_folder, filename)
                remotefile_path = os.path.join(remotepath, rel_folder, filename)
                self.putfile(localfile_path, remotefile_path)

    def remove(self, path: str):
        self._client.simple_delete(self._machine, self._get_path(path))

    def rename(self, oldpath: str, newpath: str):
        self._client.mv(self._machine, self._get_path(oldpath), self._get_path(newpath))

    def rmdir(self, path: str):
        self._client.simple_delete(self._machine, self._get_path(path))

    def rmtree(self, path: str):
        self._client.simple_delete(self._machine, self._get_path(path))

    def symlink(self, remotesource: str, remotedestination: str):
        self._client.symlink(
            self._machine,
            self._get_path(remotesource),
            self._get_path(remotedestination),
        )

    def gotocomputer_command(self, remotedir: str):
        raise NotImplementedError

    def _exec_command_internal(self, command, **kwargs):
        raise NotImplementedError

    def exec_command_wait_bytes(self, command, stdin=None, **kwargs):
        raise NotImplementedError

    # pass through from scheduler

    def submit_job(self, working_directory: str, filename: str) -> str | ExitCode:
        """Submit a job.

        :param working_directory: The absolute filepath to the working directory where the job is to be executed.
        :param filename: The filename of the submission script relative to the working directory.
        :return: a string with the job ID or an exit code if the submission failed because the submission script is
            invalid and the job should be terminated.
        """
        result = self._client.submit(
            self._machine, self._get_path(working_directory, filename), local_file=False
        )
        return str(result["jobid"])

    def get_jobs(
        self,
        jobs: list[str] | None = None,
        user: str | None = None,
        as_dict: bool = False,
    ) -> list[JobInfo] | dict[str, JobInfo]:
        """Return the list of currently active jobs.

        :param jobs: A list of jobs to check; only these are checked.
        :param user: A string with a user: only jobs of this user are checked.
        :param as_dict: If False (default), a list of JobInfo objects is returned. If True, a dictionary is
            returned, where the job_id is the key and the values are the JobInfo objects.
        :returns: List of active jobs.
        """
        result = self._client.poll_active(self._machine, jobs)
        job_list = []
        for raw_result in result:
            if user is not None and raw_result["user"] != user:
                continue

            this_job = JobInfo()

            this_job.job_id = raw_result["jobid"]
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

            # try:
            #     this_job.num_mpiprocs = int(thisjob_dict['number_cpus'])
            # except ValueError:
            #     self.logger.warning(
            #         'The number of allocated cores is not '
            #         'an integer ({}) for job id {}!'.format(thisjob_dict['number_cpus'], this_job.job_id)
            #     )

            # ALLOCATED NODES HERE
            # string may be in the format
            # nid00[684-685,722-723,748-749,958-959]
            # therefore it requires some parsing, that is unnecessary now.
            # I just store is as a raw string for the moment, and I leave
            # this_job.allocated_machines undefined
            # if this_job.job_state == JobState.RUNNING:
            #     this_job.allocated_machines_raw = thisjob_dict['allocated_machines']

            this_job.queue_name = raw_result["partition"]

            # try:
            #     walltime = (self._convert_time(thisjob_dict['time_limit']))
            #     this_job.requested_wallclock_time_seconds = walltime  # pylint: disable=invalid-name
            # except ValueError:
            #     self.logger.warning(f'Error parsing the time limit for job id {this_job.job_id}')

            # Only if it is RUNNING; otherwise it is not meaningful,
            # and may be not set (in my test, it is set to zero)
            # if this_job.job_state == JobState.RUNNING:
            #     try:
            #         this_job.wallclock_time_seconds = (self._convert_time(thisjob_dict['time_used']))
            #     except ValueError:
            #         self.logger.warning(f'Error parsing time_used for job id {this_job.job_id}')

            #     try:
            #         this_job.dispatch_time = self._parse_time_string(thisjob_dict['dispatch_time'])
            #     except ValueError:
            #         self.logger.warning(f'Error parsing dispatch_time for job id {this_job.job_id}')

            # try:
            #     this_job.submission_time = self._parse_time_string(thisjob_dict['submission_time'])
            # except ValueError:
            #     self.logger.warning(f'Error parsing submission_time for job id {this_job.job_id}')

            this_job.title = raw_result["name"]

            # Everything goes here anyway for debugging purposes
            this_job.raw_data = raw_result

            # Double check of redundant info
            # Not really useful now, allocated_machines in this
            # version of the plugin is never set
            if (
                this_job.allocated_machines is not None
                and this_job.num_machines is not None
            ):
                if len(this_job.allocated_machines) != this_job.num_machines:
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
        """Kill a job with a given job ID.

        :param jobid: The job ID of the job to kill.
        :return: True if the job was successfully killed, False otherwise.
        """
        self._client.cancel(self._machine, jobid)
        return True
