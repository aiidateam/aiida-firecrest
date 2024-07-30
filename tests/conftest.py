from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import stat
from typing import Any, Callable
from urllib.parse import urlparse

from aiida import orm
import firecrest
import firecrest.path
import pytest
import requests


class Values:
    _DEFAULT_PAGE_SIZE: int = 25


@pytest.fixture
def firecrest_computer(firecrest_config):
    """Create and return a computer configured for Firecrest.

    Note, the computer is not stored in the database.
    """

    # create a temp directory and set it as the workdir

    computer = orm.Computer(
        label="test_computer",
        description="test computer",
        hostname="-",
        workdir=firecrest_config.workdir,
        transport_type="firecrest",
        scheduler_type="firecrest",
    )
    computer.set_minimum_job_poll_interval(5)
    computer.set_default_mpiprocs_per_machine(1)
    computer.configure(
        url=firecrest_config.url,
        token_uri=firecrest_config.token_uri,
        client_id=firecrest_config.client_id,
        client_secret=firecrest_config.client_secret,
        compute_resource=firecrest_config.compute_resource,
        small_file_size_mb=firecrest_config.small_file_size_mb,
        temp_directory=firecrest_config.temp_directory,
        api_version=firecrest_config.api_version,
    )
    return computer


class MockFirecrest:
    """Mocks py:class:`pyfirecrest.Firecrest`."""

    def __init__(self, firecrest_url, *args, **kwargs):
        self._firecrest_url = firecrest_url
        self.args = args
        self.kwargs = kwargs

        self.whoami = whoami
        self.list_files = list_files
        self.stat = stat_
        self.mkdir = mkdir
        self.simple_delete = simple_delete
        self.parameters = parameters
        self.symlink = symlink
        self.checksum = checksum
        self.simple_download = simple_download
        self.simple_upload = simple_upload
        self.compress = compress
        self.extract = extract
        self.copy = copy
        self.submit = submit
        self.poll_active = poll_active


class MockClientCredentialsAuth:
    """Mocks py:class:`pyfirecrest.ClientCredentialsAuth`."""

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@dataclass
class ComputerFirecrestConfig:
    """Configuration of a computer using FirecREST as transport plugin."""

    url: str
    token_uri: str
    client_id: str
    client_secret: str
    compute_resource: str
    temp_directory: str
    workdir: str
    api_version: str
    small_file_size_mb: float = 1.0


class RequestTelemetry:
    """A to gather telemetry on requests.
    This class is stale and not used in the current implementation.
    We keep it here for future use, if needed.
    """

    def __init__(self) -> None:
        self.counts = {}

    def wrap(
        self,
        method: Callable[..., requests.Response],
        url: str | bytes,
        **kwargs: Any,
    ) -> requests.Response:
        """Wrap a requests method to gather telemetry."""
        endpoint = urlparse(url if isinstance(url, str) else url.decode("utf-8")).path
        self.counts.setdefault(endpoint, 0)
        self.counts[endpoint] += 1
        return method(url, **kwargs)


@pytest.fixture(scope="function")
def firecrest_config(
    request: pytest.FixtureRequest,
    monkeypatch,
    tmp_path: Path,
):
    """
    If a config file is provided it sets up a client environment with the information
    in the config file and uses pyfirecrest to communicate with the server.
    ┌─────────────────┐───►┌─────────────┐───►┌──────────────────┐
    │ aiida_firecrest │    │ pyfirecrest │    │ FirecREST server │
    └─────────────────┘◄───└─────────────┘◄───└──────────────────┘

    if a config file is not provided, it monkeypatches pyfirecrest so we never
    actually communicate with a server.
    ┌─────────────────┐───►┌─────────────────────────────┐
    │ aiida_firecrest │    │ pyfirecrest (monkeypatched) │
    └─────────────────┘◄───└─────────────────────────────┘
    """
    config_path: str | None = request.config.getoption("--firecrest-config")
    no_clean: bool = request.config.getoption("--firecrest-no-clean")
    # record_requests: bool = request.config.getoption("--firecrest-requests")
    # record_requests: bool = request.config.getoption("--firecrest-requests")
    # TODO: record_requests is un-maintained after PR#36, and practically not used.
    # But let's keep it commented for future use, if needed.

    if config_path is not None:
        # telemetry: RequestTelemetry | None = None
        # if given, use this config
        with open(config_path, encoding="utf8") as handle:
            config = json.load(handle)
        config = ComputerFirecrestConfig(**config)
        # # rather than use the scratch_path directly, we use a subfolder,
        # # which we can then clean
        config.workdir = config.workdir + "/pytest_tmp"
        config.temp_directory = config.temp_directory + "/pytest_tmp"

        # # we need to connect to the client here,
        # # to ensure that the scratch path exists and is empty
        client = firecrest.Firecrest(
            firecrest_url=config.url,
            authorization=firecrest.ClientCredentialsAuth(
                config.client_id,
                Path(config.client_secret).read_text().strip(),
                config.token_uri,
            ),
        )
        client.mkdir(config.compute_resource, config.workdir, p=True)
        client.mkdir(config.compute_resource, config.temp_directory, p=True)

        # if record_requests:
        #     telemetry = RequestTelemetry()
        #     monkeypatch.setattr(requests, "get", partial(telemetry.wrap, requests.get))
        #     monkeypatch.setattr(
        #         requests, "post", partial(telemetry.wrap, requests.post)
        #     )
        #     monkeypatch.setattr(requests, "put", partial(telemetry.wrap, requests.put))
        #     monkeypatch.setattr(
        #         requests, "delete", partial(telemetry.wrap, requests.delete)
        # )
        yield config
        # # Note this shouldn't really work, for folders but it does :shrug:
        # # because they use `rm -r`:
        # # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L347
        if not no_clean:
            client.simple_delete(config.compute_resource, config.workdir)
            client.simple_delete(config.compute_resource, config.temp_directory)

        # if telemetry is not None:
        #     test_name = request.node.name
        #     pytestconfig.stash.setdefault("firecrest_requests", {})[
        #         test_name
        #     ] = telemetry.counts
    else:
        # if no_clean or record_requests:
        if no_clean:
            raise ValueError(
                "--firecrest-{no-clean,requests} options are only available"
                " when a config file is passed using --firecrest-config."
            )

        monkeypatch.setattr(firecrest, "Firecrest", MockFirecrest)
        monkeypatch.setattr(
            firecrest, "ClientCredentialsAuth", MockClientCredentialsAuth
        )

        # dummy config
        _temp_directory = tmp_path / "temp"
        _temp_directory.mkdir()

        Path(tmp_path / ".firecrest").mkdir()
        _secret_path = Path(tmp_path / ".firecrest/secretabc")
        _secret_path.write_text("secret_string")

        workdir = tmp_path / "scratch"
        workdir.mkdir()

        yield ComputerFirecrestConfig(
            url="https://URI",
            token_uri="https://TOKEN_URI",
            client_id="CLIENT_ID",
            client_secret=str(_secret_path),
            compute_resource="MACHINE_NAME",
            workdir=str(workdir),
            small_file_size_mb=1.0,
            temp_directory=str(_temp_directory),
            api_version="2",
        )


def submit(
    machine: str,
    script_str: str | None = None,
    script_remote_path: str | None = None,
    script_local_path: str | None = None,
    local_file=False,
):
    if local_file:
        raise DeprecationWarning("local_file is not supported")

    if script_remote_path and not Path(script_remote_path).exists():
        raise FileNotFoundError(f"File {script_remote_path} does not exist")
    job_id = random.randint(10000, 99999)

    # Filter out lines starting with '#SBATCH'
    with open(script_remote_path) as file:
        lines = file.readlines()
    command = "".join([line for line in lines if not line.strip().startswith("#")])

    # Make the dummy files
    for line in lines:
        if "--error" in line:
            error_file = line.split("=")[1].strip()
            (Path(script_remote_path).parent / error_file).touch()
        elif "--output" in line:
            output_file = line.split("=")[1].strip()
            (Path(script_remote_path).parent / output_file).touch()

    # Execute the job, this is useful for test_calculation.py
    if "aiida.in" in command:
        # skip blank command like: '/bin/bash'
        os.chdir(Path(script_remote_path).parent)
        os.system(command)

    return {"jobid": job_id}


def poll_active(machine: str, jobs: list[str], page_number: int = 0):
    response = []
    # 12 satets are defined in firecrest
    states = [
        "TIMEOUT",
        "SUSPENDED",
        "PREEMPTED",
        "CANCELLED",
        "NODE_FAIL",
        "PENDING",
        "FAILED",
        "RUNNING",
        "CONFIGURING",
        "QUEUED",
        "COMPLETED",
        "COMPLETING",
    ]
    for i in range(len(jobs)):
        response.append(
            {
                "job_data_err": "",
                "job_data_out": "",
                "job_file": "somefile.sh",
                "job_file_err": "somefile-stderr.txt",
                "job_file_out": "somefile-stdout.txt",
                "job_info_extra": "Job info returned successfully",
                "jobid": f"{jobs[i]}",
                "name": "aiida-45",
                "nodelist": "nid00049",
                "nodes": "1",
                "partition": "normal",
                "start_time": "0:03",
                "state": states[i % 12],
                "time": "2024-06-21T10:44:42",
                "time_left": "29:57",
                "user": "Prof. Wang",
            }
        )

    return response[
        page_number
        * Values._DEFAULT_PAGE_SIZE : (page_number + 1)
        * Values._DEFAULT_PAGE_SIZE
    ]


def whoami(machine: str):
    assert machine == "MACHINE_NAME"
    return "test_user"


def list_files(
    machine: str, target_path: str, recursive: bool = False, show_hidden: bool = False
):
    # this is mimiking the expected behaviour from the firecrest code.

    content_list = []
    for root, dirs, files in os.walk(target_path):
        if not recursive and root != target_path:
            continue
        for name in dirs + files:
            full_path = os.path.join(root, name)
            relative_path = (
                Path(os.path.relpath(root, target_path)).joinpath(name).as_posix()
            )
            if os.path.islink(full_path):
                content_type = "l"
                link_target = (
                    os.readlink(full_path) if os.path.islink(full_path) else None
                )
            elif os.path.isfile(full_path):
                content_type = "-"
                link_target = None
            elif os.path.isdir(full_path):
                content_type = "d"
                link_target = None
            else:
                content_type = "NON"
                link_target = None
            permissions = stat.filemode(Path(full_path).lstat().st_mode)[1:]
            if name.startswith(".") and not show_hidden:
                continue
            content_list.append(
                {
                    "name": relative_path,
                    "type": content_type,
                    "link_target": link_target,
                    "permissions": permissions,
                }
            )

    return content_list


def stat_(machine: str, targetpath: firecrest.path, dereference=True):
    stats = os.stat(
        targetpath, follow_symlinks=bool(dereference) if dereference else False
    )
    return {
        "ino": stats.st_ino,
        "dev": stats.st_dev,
        "nlink": stats.st_nlink,
        "uid": stats.st_uid,
        "gid": stats.st_gid,
        "size": stats.st_size,
        "atime": stats.st_atime,
        "mtime": stats.st_mtime,
        "ctime": stats.st_ctime,
    }


def mkdir(
    machine: str, target_path: str, p: bool = False, ignore_existing: bool = False
):
    target = Path(target_path)
    target.mkdir(exist_ok=ignore_existing, parents=p)


def simple_delete(machine: str, target_path: str):
    if not Path(target_path).exists():
        raise FileNotFoundError(f"File or folder {target_path} does not exist")
    if os.path.isdir(target_path):
        shutil.rmtree(target_path)
    else:
        os.remove(target_path)


def symlink(machine: str, target_path: str, link_path: str):
    # this is how firecrest does it
    os.system(f"ln -s {target_path}  {link_path}")


def simple_download(machine: str, remote_path: str, local_path: str):
    # this procedure is complecated in firecrest, but I am simplifying it here
    # we don't care about the details of the download, we just want to make sure
    # that the aiida-firecrest code is calling the right functions at right time
    if Path(remote_path).is_dir():
        raise IsADirectoryError(f"{remote_path} is a directory")
    if not Path(remote_path).exists():
        raise FileNotFoundError(f"{remote_path} does not exist")
    os.system(f"cp {remote_path} {local_path}")


def simple_upload(
    machine: str, local_path: str, remote_path: str, file_name: str | None = None
):
    # this procedure is complecated in firecrest, but I am simplifying it here
    # we don't care about the details of the upload, we just want to make sure
    # that the aiida-firecrest code is calling the right functions at right time
    if Path(local_path).is_dir():
        raise IsADirectoryError(f"{local_path} is a directory")
    if not Path(local_path).exists():
        raise FileNotFoundError(f"{local_path} does not exist")
    if file_name:
        remote_path = os.path.join(remote_path, file_name)
    os.system(f"cp {local_path} {remote_path}")


def copy(machine: str, source_path: str, target_path: str):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/utilities/utilities.py#L451C1-L452C1
    os.system(f"cp --force -dR --preserve=all -- '{source_path}' '{target_path}'")


def compress(
    machine: str, source_path: str, target_path: str, dereference: bool = True
):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/utilities/utilities.py#L460
    basedir = os.path.dirname(source_path)
    file_path = os.path.basename(source_path)
    deref = "--dereference" if dereference else ""
    os.system(f"tar {deref} -czf '{target_path}' -C '{basedir}' '{file_path}'")


def extract(machine: str, source_path: str, target_path: str):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/common/cscs_api_common.py#L1110C18-L1110C65
    os.system(f"tar -xf '{source_path}' -C '{target_path}'")


def checksum(machine: str, remote_path: str) -> int:
    if not remote_path.exists():
        return False
    # Firecrest uses sha256
    sha256_hash = hashlib.sha256()
    with open(remote_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()


def parameters():
    # note: I took this from https://firecrest-tds.cscs.ch/ or https://firecrest.cscs.ch/
    # if code is not working but test passes, it means you need to update this dictionary
    # with the latest FirecREST parameters
    return {
        "compute": [
            {
                "description": "Type of resource and workload manager used in compute microservice",
                "name": "WORKLOAD_MANAGER",
                "unit": "",
                "value": "Slurm",
            }
        ],
        "storage": [
            {
                "description": "Type of object storage, like `swift`, `s3v2` or `s3v4`.",
                "name": "OBJECT_STORAGE",
                "unit": "",
                "value": "s3v4",
            },
            {
                "description": "Expiration time for temp URLs.",
                "name": "STORAGE_TEMPURL_EXP_TIME",
                "unit": "seconds",
                "value": "86400",
            },
            {
                "description": "Maximum file size for temp URLs.",
                "name": "STORAGE_MAX_FILE_SIZE",
                "unit": "MB",
                "value": "5120",
            },
            {
                "description": "Available filesystems through the API.",
                "name": "FILESYSTEMS",
                "unit": "",
                "value": [
                    {
                        "mounted": ["/project", "/store", "/scratch/snx3000tds"],
                        "system": "dom",
                    },
                    {
                        "mounted": ["/project", "/store", "/capstor/scratch/cscs"],
                        "system": "pilatus",
                    },
                ],
            },
        ],
        "utilities": [
            {
                "description": "The maximum allowable file size for various operations of the utilities microservice",
                "name": "UTILITIES_MAX_FILE_SIZE",
                "unit": "MB",
                "value": "5",
            },
            {
                "description": (
                    "Maximum time duration for executing the commands "
                    "in the cluster for the utilities microservice."
                ),
                "name": "UTILITIES_TIMEOUT",
                "unit": "seconds",
                "value": "5",
            },
        ],
    }
