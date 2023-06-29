"""Utilities mainly for testing."""
from __future__ import annotations

from dataclasses import dataclass
import io
from json import dumps as json_dumps
from pathlib import Path
import shutil
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
    small_file_size_mb: float = 5.0


class FirecrestMockServer:
    """A mock server to imitate Firecrest (v1.12.0)."""

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

        self._slurm_jobs: dict[str, dict[str, Any]] = {}

        self._task_id_counter = 0
        self._tasks: dict[str, SchedulerJobsTask | ScheduledJobTask] = {}

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

    def new_task_id(self) -> str:
        self._task_id_counter += 1
        return f"{self._task_id_counter}"

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
            assert data is not None
            script_path = Path(data["targetPath"])
            if not script_path.is_file():
                return add_json_response(
                    response,
                    400,
                    {"description": "Failed to submit job"},
                    {"X-Invalid-Path": f"{script_path} is an invalid path."},
                )
            # TODO implement running a script and saving the output
            job_id = "test_job_id"
            task_id = self.new_task_id()
            self._tasks[task_id] = ScheduledJobTask(task_id, job_id)
            add_json_response(
                response,
                201,
                {"success": "Task created", "task_url": "notset", "task_id": task_id},
            )
        elif endpoint == "/compute/jobs":
            assert params is not None
            # TODO pageSize pageNumber
            jobs: None | list[str] = (
                params["jobs"].split(",") if "jobs" in params else None
            )
            task_id = self.new_task_id()
            self._tasks[task_id] = SchedulerJobsTask(task_id=task_id, jobs=jobs)
            add_json_response(
                response,
                200,
                {
                    "success": "Task created",
                    "task_id": task_id,
                    "task_url": "notset",
                },
            )
        elif endpoint.startswith("/tasks/"):
            return self.handle_task(endpoint[7:], response)
        else:
            raise requests.exceptions.InvalidURL(f"Unknown endpoint: {endpoint}")

        return response

    def handle_task(self, task_id: str, response: Response) -> Response:
        if task_id not in self._tasks:
            return add_json_response(
                response, 404, {"error": f"Task {task_id} does not exist"}
            )

        task = self._tasks.pop(task_id)

        if isinstance(task, SchedulerJobsTask):
            job_data: dict[str, Any] = {}
            if task.jobs is None:
                # TODO get all jobs
                pass
            else:
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

                    # TODO get job data for job_id

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
            # TODO
            pass

        raise NotImplementedError(f"Unknown task type: {type(task)}")


@dataclass
class SchedulerJobsTask:
    task_id: str
    jobs: list[str] | None


@dataclass
class ScheduledJobTask:
    task_id: str
    job_id: str


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
    path.write_bytes(fbuffer.read())
    add_success_response(response, 201)


def utilities_download(params: dict[str, Any], response: Response) -> None:
    path = Path(params["sourcePath"])
    if not path.is_file():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    response.status_code = 200
    response.raw = io.BytesIO(path.read_bytes())


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
