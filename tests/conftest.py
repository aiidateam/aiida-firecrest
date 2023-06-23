import dataclasses
import io
from json import dumps as json_dumps
from json import load as json_load
from pathlib import Path
from typing import Any, BinaryIO, Dict, Optional, Union
from urllib.parse import urlparse

import pytest
import requests
from requests.models import Response


@pytest.fixture(scope="function")
def firecrest_server(request, monkeypatch, tmp_path: Path):
    """A fixture which provides a mock Firecrest server to test against."""
    config_path = request.config.getoption("--firecrest-config")
    if config_path is not None:
        # if given, use this config
        with open(config_path) as handle:
            config = json_load(handle)
        # TODO how to ensure clean server
        yield FirecrestConfig(**config)
    else:
        # otherwise use mock server
        server = FirecrestMockServer(tmp_path)
        monkeypatch.setattr(requests, "get", server.mock_request)
        monkeypatch.setattr(requests, "post", server.mock_request)
        monkeypatch.setattr(requests, "put", server.mock_request)
        yield server.config


@dataclasses.dataclass
class FirecrestConfig:
    """Configuration returned from fixture"""

    url: str
    token_uri: str
    client_id: str
    client_secret: str
    machine: str
    scratch_path: str


class FirecrestMockServer:
    """A mock server for Firecrest."""

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
        url: Union[str, bytes],
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict[str, BinaryIO]] = None,
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

        if endpoint == "/utilities/file":
            utilities_file(params or {}, data or {}, response)
        elif endpoint == "/utilities/mkdir":
            utilities_mkdir(data or {}, response)
        elif endpoint == "/utilities/ls":
            utilities_ls(params or {}, response)
        elif endpoint == "/utilities/chmod":
            utilities_chmod(data or {}, response)
        # elif endpoint == "/utilities/chown":
        #     utilities_chown(data or {}, response)
        elif endpoint == "/utilities/upload":
            utilities_upload(data or {}, files or {}, response)
        else:
            raise requests.exceptions.InvalidURL(f"Unknown endpoint: {endpoint}")

        return response


def utilities_file(
    params: Dict[str, Any], data: Dict[str, Any], response: Response
) -> None:
    path = Path(params["targetPath"])
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    response.status_code = 200
    return_data = {
        "description": "success",
        "output": "directory" if path.is_dir() else "text",
    }
    response.raw = io.BytesIO(json_dumps(return_data).encode(response.encoding))


def utilities_ls(params: Dict[str, Any], response: Response) -> None:
    path = Path(params["targetPath"])
    if not path.is_dir():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    response.status_code = 200
    return_data = {
        "description": "success",
        "output": [{"name": f.name} for f in path.iterdir()],
    }
    response.raw = io.BytesIO(json_dumps(return_data).encode(response.encoding))


def utilities_chmod(data: Dict[str, Any], response: Response) -> None:
    path = Path(data["targetPath"])
    if not path.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    path.chmod(int(data["mode"], 8))
    response.status_code = 200
    return_data = {"description": "success"}
    response.raw = io.BytesIO(json_dumps(return_data).encode(response.encoding))


def utilities_mkdir(data: Dict[str, Any], response: Response) -> None:
    path = Path(data["targetPath"])
    if path.exists():
        response.status_code = 400
        response.headers["X-Exists"] = ""
        return
    path.mkdir(parents=data.get("p", False))
    response.status_code = 201
    response.raw = io.BytesIO(b"{}")


def utilities_upload(
    data: Dict[str, Any], files: Dict[str, BinaryIO], response: Response
) -> None:
    path = Path(data["targetPath"]) / Path(files["file"].name).name
    if not path.parent.exists():
        response.status_code = 400
        response.headers["X-Invalid-Path"] = ""
        return
    path.write_bytes(files["file"].read())
    response.status_code = 201
    response.raw = io.BytesIO(b"{}")
