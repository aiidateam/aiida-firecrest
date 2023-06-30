"""Utilities mainly for testing."""
from __future__ import annotations

from dataclasses import dataclass
import io
from json import dumps as json_dumps
from pathlib import Path
import shutil
import subprocess
from typing import Any, BinaryIO
from urllib.parse import urlparse

import requests
from requests.models import Response


@dataclass
class FirecrestConfig:
    """Configuration returned from tests fixtures."""

    url: str
    token_uri: str
    client_id: str
    client_secret: str
    machine: str
    scratch_path: str
    small_file_size_mb: float = 1.0


class FirecrestMockServer:
    """A mock server to imitate Firecrest (v1.12.0).

    This minimally mimics accessing the filesystem and submitting jobs,
    enough to make tests pass, without having to run a real Firecrest server.
    """

    def __init__(
        self, tmpdir: Path, url: str = "https://test.com", machine: str = "test"
    ) -> None:
        self._url = url
        self._url_parsed = urlparse(url)
        self._machine = machine
        self._scratch = tmpdir / "scratch"
        self._scratch.mkdir()
        self._client_id = "test_client_id"
        self._client_secret = "test_client_secret"
        self._token_url = "https://test.auth.com/token"
        self._token_url_parsed = urlparse(self._token_url)
        self._username = "test_user"

        self._slurm_job_id_counter = 0
        self._slurm_jobs: dict[str, dict[str, Any]] = {}

        self._task_id_counter = 0
        self._tasks: dict[str, Task] = {}

    @property
    def scratch(self) -> Path:
        return self._scratch

    @property
    def config(self) -> FirecrestConfig:
        return FirecrestConfig(
            url=self._url,
            token_uri=self._token_url,
            client_id=self._client_id,
            client_secret=self._client_secret,
            machine=self._machine,
            scratch_path=str(self._scratch.absolute()),
        )

    def mock_request(
        self,
        url: str | bytes,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, tuple[str, BinaryIO]] | None = None,
        **kwargs: Any,
    ) -> Response:
        response = Response()
        response.encoding = "utf-8"
        response.url = url if isinstance(url, str) else url.decode("utf-8")
        parsed = urlparse(response.url)
        endpoint = parsed.path

        if parsed.netloc == self._token_url_parsed.netloc:
            if endpoint != "/token":
                raise requests.exceptions.InvalidURL(f"Unknown endpoint: {endpoint}")
            response.status_code = 200
            response.raw = io.BytesIO(
                json_dumps(
                    {
                        "access_token": "test_access_token",
                        "expires_in": 3600,
                    }
                ).encode(response.encoding)
            )
            return response

        if parsed.netloc != self._url_parsed.netloc:
            raise requests.exceptions.InvalidURL(
                f"{parsed.netloc} != {self._url_parsed.netloc}"
            )

        if endpoint == "/utilities/whoami":
            add_success_response(response, 200, self._username)
        elif endpoint == "/utilities/stat":
            utilities_stat(params or {}, response)
        elif endpoint == "/utilities/file":
            utilities_file(params or {}, response)
        elif endpoint == "/utilities/ls":
            utilities_ls(params or {}, response)
        elif endpoint == "/utilities/symlink":
            utilities_symlink(data or {}, response)
        elif endpoint == "/utilities/mkdir":
            utilities_mkdir(data or {}, response)
        elif endpoint == "/utilities/rm":
            utilities_rm(data or {}, response)
        elif endpoint == "/utilities/copy":
            utilities_copy(data or {}, response)
        elif endpoint == "/utilities/chmod":
            utilities_chmod(data or {}, response)
        # elif endpoint == "/utilities/chown":
        #     utilities_chown(data or {}, response)
        elif endpoint == "/utilities/upload":
            utilities_upload(data or {}, files or {}, response)
        elif endpoint == "/utilities/download":
            utilities_download(params or {}, response)
        elif endpoint == "/compute/jobs/path":
            self.compute_jobs_path(data or {}, response)
        elif endpoint == "/compute/jobs":
            self.compute_jobs(params or {}, response)
        elif endpoint.startswith("/tasks/"):
            self.handle_task(endpoint[7:], response)
        elif endpoint == "/storage/xfer-external/upload":
            self.storage_xfer_external_upload(data or {}, response)
        else:
            raise requests.exceptions.InvalidURL(f"Unknown endpoint: {endpoint}")

        return response

    def new_task_id(self) -> str:
        self._task_id_counter += 1
        return f"{self._task_id_counter}"

    def new_slurm_job_id(self) -> str:
        """Generate a new SLURM job ID."""
        self._slurm_job_id_counter += 1
        return f"{self._slurm_job_id_counter}"

    def handle_task(self, task_id: str, response: Response) -> Response:
        if task_id not in self._tasks:
            return add_json_response(
                response, 404, {"error": f"Task {task_id} does not exist"}
            )

        task = self._tasks[task_id]

        if isinstance(task, ActiveSchedulerJobsTask):
            if task.jobs is not None:
                for job_id in task.jobs or []:
                    if job_id not in self._slurm_jobs:
                        return add_json_response(
                            response,
                            400,
                            {
                                "description": "Failed to retrieve job information",
                                "error": f"{job_id} is not a valid job ID",
                            },
                        )

            # Note because we always run jobs straight away (see self.compute_jobs_path),
            # then we can assume that there are never any active jobs.
            # TODO add some basic way to simulate active jobs
            job_data: dict[str, Any] = {}

            return add_json_response(
                response,
                200,
                {
                    "task": {
                        "task_id": task_id,
                        "service": "compute",
                        "status": "200",
                        "description": "Finished successfully",
                        "data": job_data,
                        "user": self._username,
                        # "task_url": "...",
                        # "hash_id": "50d3ca603f3e4e095107ff01107f6f28",
                        # "created_at": "2023-06-29T08:04:56",
                        # "last_modify": "2023-06-29T08:04:56",
                        # "updated_at": "2023-06-29T08:04:56",
                    }
                },
            )

        if isinstance(task, ScheduledJobTask):
            return add_json_response(
                response,
                200,
                {
                    "task": {
                        "data": {
                            "job_data_err": "",
                            "job_data_out": "",
                            "job_file": str(task.script_path),
                            "job_file_err": str(str(task.stderr_path)),
                            "job_file_out": str(str(task.stdout_path)),
                            "job_info_extra": "Job info returned successfully",
                            "jobid": task.job_id,
                            "result": "Job submitted",
                        },
                        "description": "Finished successfully",
                        "service": "compute",
                        "status": "200",
                        "task_id": task.task_id,
                        "user": self._username,
                        # "task_url": "...",
                        # "hash_id": "...",
                        # "created_at": "2023-06-29T16:30:24",
                        # "last_modify": "2023-06-29T16:30:25",
                        # "updated_at": "2023-06-29T16:30:25",
                    }
                },
            )

        if isinstance(task, StorageXferExternalUploadTask):
            # to mock this once the Form URL is retrieved (110), we move straight to the
            # "Download from Object Storage to server has finished" (114) for the next request
            # this skips statuses 111, 112 and 113 (see pyfirecrest.ExternalUpload)
            # and so we are assuming that the file is uploaded to the server

            if not task.form_retrieved:
                task.form_retrieved = True
                return add_json_response(
                    response,
                    200,
                    {
                        "task": {
                            "data": {
                                # "hash_id": "fd690c43e6ee509359b9e2c3237f4cc5",
                                "msg": {
                                    "command": "echo 'mock'",
                                    # "command": "curl --show-error -s -i -X POST http://192.168.220.19:9000/service-account-firecrest-sample -F 'key=fd690c43e6ee509359b9e2c3237f4cc5/file.txt' -F 'x-amz-algorithm=AWS4-HMAC-SHA256' -F 'x-amz-credential=storage_access_key/202306/us-east-1/s3/aws4_request' -F 'x-amz-date=20230630T155026Z' -F 'policy=xxx' -F 'x-amz-signature=yyy' -F file=@/private/var/folders/t2/xbl15_3n4tsb1vr_ccmmtmbr0000gn/T/pytest-of-chrisjsewell/pytest-340/test_putfile_large0/file.txt",  # noqa: E501
                                    "parameters": {
                                        # "data": {
                                        #     "key": "fd690c43e6ee509359b9e2c3237f4cc5/file.txt",
                                        #     "policy": "xxx",
                                        #     "x-amz-algorithm": "AWS4-HMAC-SHA256",
                                        #     "x-amz-credential": "storage_access_key/202306/us-east-1/s3/aws4_request",
                                        #     "x-amz-date": "20230630T155026Z",
                                        #     "x-amz-signature": "yyy",
                                        # },
                                        "files": str(task.target),
                                        "headers": {},
                                        "json": {},
                                        "method": "POST",
                                        "params": {},
                                        # "url": "http://192.168.220.19:9000/service-account-firecrest-sample",
                                    },
                                },
                                "status": "111",
                                "source": str(task.source),
                                "target": str(task.target),
                                # "system_addr": "192.168.220.12:22",
                                # "system_name": "cluster",
                                # "trace_id": "d5a45c91e0390fa4f4f5296ddb4bf511:6f925a63f3e6ac4f:84525bc7447d25a4:1",
                                "user": self._username,
                            },
                            "description": "Form URL from Object Storage received",
                            "service": "storage",
                            "status": "111",
                            "user": self._username,
                            "task_id": task.task_id,
                            # "hash_id": "...",
                            # "task_url": "...",
                            # "created_at": "2023-06-30T15:50:26",
                            # "last_modify": "2023-06-30T15:50:26",
                            # "updated_at": "2023-06-30T15:50:26",
                        },
                    },
                )
            else:
                shutil.copy(task.source, task.target)
                return add_json_response(
                    response,
                    200,
                    {
                        "task": {
                            "data": "Download from Object Storage to server has finished",
                            "description": "Download from Object Storage to server has finished",
                            "service": "storage",
                            "status": "114",
                            "task_id": task.task_id,
                            "user": self._username,
                            "hash_id": "598608eef16ecdbe4de7612566291754",
                            "updated_at": "2023-06-30T16:25:18",
                            "last_modify": "2023-06-30T16:25:18",
                            "created_at": "2023-06-30T16:24:39",
                            "task_url": "...",
                        }
                    },
                )

        raise NotImplementedError(f"Unknown task type: {type(task)}")

    def compute_jobs(self, params: dict[str, Any], response: Response) -> None:
        # TODO pageSize pageNumber
        jobs: None | list[str] = params["jobs"].split(",") if "jobs" in params else None
        task_id = self.new_task_id()
        self._tasks[task_id] = ActiveSchedulerJobsTask(task_id=task_id, jobs=jobs)
        add_json_response(
            response,
            200,
            {
                "success": "Task created",
                "task_id": task_id,
                "task_url": "notset",
            },
        )

    def compute_jobs_path(self, data: dict[str, Any], response: Response) -> Response:
        script_path = Path(data["targetPath"])
        if not script_path.is_file():
            return add_json_response(
                response,
                400,
                {"description": "Failed to submit job", "data": "File does not exist"},
                {"X-Invalid-Path": f"{script_path} is an invalid path."},
            )

        job_id = self.new_slurm_job_id()

        # read the file
        script_content = script_path.read_text("utf8")

        # TODO this could be more rigorous

        # check that the first line is a shebang
        if not script_content.startswith("#!/bin/bash"):
            return add_json_response(
                response,
                400,
                {
                    "description": "Finished with errors",
                    "data": "First line must be a shebang `#!/bin/bash`",
                },
            )

        # get all sbatch options (see https://slurm.schedmd.com/sbatch.html)
        sbatch_options: dict[str, Any] = {}

        for line in script_content.splitlines():
            if not line.startswith("#SBATCH"):
                continue
            arg = line[7:].strip().split("=", 1)
            if len(arg) == 1:
                assert arg[0].startswith("--"), f"Invalid sbatch option: {arg[0]}"
                sbatch_options[arg[0][2:]] = True
            elif len(arg) == 2:
                assert arg[0].startswith("--"), f"Invalid sbatch option: {arg[0]}"
                sbatch_options[arg[0][2:]] = arg[1].strip()

        # set stdout and stderror file
        out_file = error_file = "slurm-%j.out"
        if "output" in sbatch_options:
            out_file = sbatch_options["output"]
        if "error" in sbatch_options:
            error_file = sbatch_options["error"]
        out_file = out_file.replace("%j", job_id)
        error_file = error_file.replace("%j", job_id)

        # we now just run the job straight away and blocking, no scheduling
        # run the script in a subprocess, in the script's directory
        # pipe stdout and stderr to the slurm output file
        script_path.chmod(0o755)  # make sure the script is executable
        if out_file == error_file:
            with open(script_path.parent / out_file, "w") as out:
                subprocess.run(
                    [str(script_path)],
                    cwd=script_path.parent,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                )
        else:
            with open(script_path.parent / out_file, "w") as out, open(
                script_path.parent / error_file, "w"
            ) as err:
                subprocess.run(
                    [str(script_path)], cwd=script_path.parent, stdout=out, stderr=err
                )

        task_id = self.new_task_id()
        self._tasks[task_id] = ScheduledJobTask(
            task_id=task_id,
            job_id=job_id,
            script_path=script_path,
            stdout_path=script_path.parent / out_file,
            stderr_path=script_path.parent / error_file,
        )
        self._slurm_jobs[job_id] = {}
        return add_json_response(
            response,
            201,
            {"success": "Task created", "task_url": "notset", "task_id": task_id},
        )

    def storage_xfer_external_upload(
        self, data: dict[str, Any], response: Response
    ) -> None:
        source = Path(data["sourcePath"])
        target = Path(data["targetPath"])
        if not target.parent.exists():
            response.status_code = 400
            response.headers["X-Not-Found"] = ""
            return
        task_id = self.new_task_id()
        self._tasks[task_id] = StorageXferExternalUploadTask(task_id, source, target)
        add_json_response(
            response,
            201,
            {
                "success": "Task created",
                "task_id": task_id,
                "task_url": "...",
            },
        )


@dataclass
class Task:
    task_id: str


@dataclass
class ActiveSchedulerJobsTask(Task):
    jobs: list[str] | None


@dataclass
class ScheduledJobTask(Task):
    job_id: str
    script_path: Path
    stdout_path: Path
    stderr_path: Path


@dataclass
class StorageXferExternalUploadTask(Task):
    source: Path
    target: Path
    form_retrieved: bool = False


def add_json_response(
    response: Response,
    status_code: int,
    json_data: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> Response:
    response.status_code = status_code
    response.raw = io.BytesIO(json_dumps(json_data).encode(response.encoding or "utf8"))
    if headers:
        response.headers.update(headers)
    return response


def add_success_response(
    response: Response, status_code: int, output: Any = ""
) -> Response:
    return add_json_response(
        response,
        status_code,
        {
            "description": "success",
            "output": output,
        },
    )


def utilities_file(params: dict[str, Any], response: Response) -> None:
    path = Path(params["targetPath"])
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    add_success_response(response, 200, "directory" if path.is_dir() else "text")


def utilities_symlink(data: dict[str, Any], response: Response) -> None:
    target = Path(data["targetPath"])
    link = Path(data["linkPath"])
    if not target.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    if link.exists():
        response.status_code = 400
        response.headers["X-Exists"] = ""
        return
    link.symlink_to(target)
    add_success_response(response, 201)


def utilities_stat(params: dict[str, Any], response: Response) -> None:
    path = Path(params["targetPath"])
    dereference = params.get("dereference", False)
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    result = path.stat() if dereference else path.lstat()
    add_success_response(
        response,
        200,
        {
            "mode": result.st_mode,
            "uid": result.st_uid,
            "gid": result.st_gid,
            "size": result.st_size,
            "atime": result.st_atime,
            "mtime": result.st_mtime,
            "ctime": result.st_ctime,
            "nlink": result.st_nlink,
            "ino": result.st_ino,
            "dev": result.st_dev,
        },
    )


def utilities_ls(params: dict[str, Any], response: Response) -> None:
    path = Path(params["targetPath"])
    if not path.is_dir():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    add_success_response(response, 200, [{"name": f.name} for f in path.iterdir()])


def utilities_chmod(data: dict[str, Any], response: Response) -> None:
    path = Path(data["targetPath"])
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    path.chmod(int(data["mode"], 8))
    add_success_response(response, 200)


def utilities_mkdir(data: dict[str, Any], response: Response) -> None:
    path = Path(data["targetPath"])
    if path.exists():
        response.status_code = 400
        response.headers["X-Exists"] = ""
        return
    path.mkdir(parents=data.get("p", False))
    add_success_response(response, 201)


def utilities_rm(data: dict[str, Any], response: Response) -> None:
    path = Path(data["targetPath"])
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    path.unlink()
    add_success_response(response, 204)


def utilities_copy(data: dict[str, Any], response: Response) -> None:
    source = Path(data["sourcePath"])
    target = Path(data["targetPath"])
    if not source.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    if target.exists():
        response.status_code = 400
        response.headers["X-Exists"] = ""
        return
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        shutil.copy2(source, target)
    add_success_response(response, 201)


def utilities_upload(
    data: dict[str, Any], files: dict[str, tuple[str, BinaryIO]], response: Response
) -> None:
    # TODO files["file"] can be a tuple (name, stream) or just a stream with a name
    fname, fbuffer = files["file"]
    path = Path(data["targetPath"]) / fname
    if not path.parent.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return

    max_size_bytes = 5 * 1024 * 1024
    fbytes = fbuffer.read()
    if len(fbytes) > max_size_bytes:
        add_json_response(
            response,
            413,
            {
                "description": f"Failed to upload file. The file is over {max_size_bytes} bytes"
            },
        )
        return

    path.write_bytes(fbytes)
    add_success_response(response, 201)


def utilities_download(params: dict[str, Any], response: Response) -> None:
    path = Path(params["sourcePath"])
    if not path.is_file():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    response.status_code = 200
    response.raw = io.BytesIO(path.read_bytes())
