"""Transport interface."""
from __future__ import annotations

import fnmatch
import os
from pathlib import Path
import platform
import posixpath
import time
from typing import Any, ClassVar, TypedDict

from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.transports import Transport
from aiida.transports.transport import validate_positive_number
from aiida.transports.util import FileAttribute
from click.types import ParamType
import firecrest as f7t

from .remote_path import FcPath, convert_header_exceptions


class ValidAuthOption(TypedDict, total=False):  # type: ignore
    option: OverridableOption | None  # existing option
    switch: bool  # whether the option is a boolean flag
    type: type[Any] | ParamType
    default: Any
    non_interactive_default: bool  # whether option should provide a default in non-interactive mode
    prompt: str  # for interactive CLI
    help: str


class FirecrestTransport(Transport):
    """Transport interface for FirecREST."""

    # override these options, because they don't really make sense for a REST-API,
    # so we don't want the user having to provide them
    # - `use_login_shell` you can't run bash on a REST-API
    # - `safe_interval` there is no connection overhead for a REST-API
    #   although ideally you would rate-limit the number of requests,
    #   but this would ideally require some "global" rate limiter,
    #   across all transport instances
    # TODO upstream issue
    # TODO also open an issue that the `verdi computer test won't work with a REST-API`
    _common_auth_options: ClassVar[list[Any]] = []
    _DEFAULT_SAFE_OPEN_INTERVAL = 0.0

    _valid_auth_options: ClassVar[list[tuple[str, dict]]] = [
        (
            "url",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Server URL",
                "help": "URL to the FirecREST server",
            },
        ),
        (
            "token_uri",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Token URI",
                "help": "URI for retrieving FirecREST authentication tokens",
            },
        ),
        (
            "client_id",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Client ID",
                "help": "FirecREST client ID",
            },
        ),
        (
            "client_secret",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Client Secret",
                "help": "FirecREST client secret",
            },
        ),
        # (
        #     # TODO: format of secret file, and lookup secret by default in ~/.firecrest/secrets.json
        #     "secret_path",
        #     {
        #         "type": AbsolutePathOrEmptyParamType(dir_okay=False, exists=True),
        #         "non_interactive_default": False,
        #         "prompt": "Secret key file",
        #         "help": "Absolute path to file containing FirecREST client secret",
        #     },
        # ),
        (
            "client_machine",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Client Machine",
                "help": "FirecREST machine secret",
            },
        ),
        (
            # TODO you could potentially get this dynamically from server
            # (via /status/parameters)
            "small_file_size_mb",
            {
                "type": float,
                "default": 5.0,  # limit set on the server is usually this
                "non_interactive_default": True,
                "prompt": "Maximum file size for direct transfer (MB)",
                "help": "Below this size, file bytes will be sent in a single API call.",
                "callback": validate_positive_number,
            },
        ),
    ]

    def __init__(
        self,
        *,
        url: str,
        token_uri: str,
        client_id: str,
        client_secret: str | Path,
        client_machine: str,
        small_file_size_mb: float,
        # note, machine is provided by default,
        # for the hostname, but we don't use that
        # TODO ideally hostname would not be necessary on a computer
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        assert isinstance(url, str), "url must be a string"
        assert isinstance(token_uri, str), "token_uri must be a string"
        assert isinstance(client_id, str), "client_id must be a string"
        assert isinstance(
            client_secret, (str, Path)
        ), "client_secret must be a string or Path"

        assert isinstance(client_machine, str), "client_machine must be a string"
        assert isinstance(
            small_file_size_mb, float
        ), "small_file_size_mb must be a float"

        self._machine = client_machine
        self._url = url
        self._token_uri = token_uri
        self._client_id = client_id
        self._small_file_size_bytes = int(small_file_size_mb * 1024 * 1024)

        secret = (
            client_secret.read_text()
            if isinstance(client_secret, Path)
            else client_secret
        )

        self._client = f7t.Firecrest(
            firecrest_url=self._url,
            authorization=f7t.ClientCredentialsAuth(client_id, secret, token_uri),
        )

        self._cwd: FcPath = FcPath(self._client, self._machine, "/")

    # TODO if this is missing is causes plugin info to fail on verdi
    is_process_function = False

    @classmethod
    def get_description(cls) -> str:
        """Used by verdi to describe the plugin."""
        return (
            "A plugin to connect to a FirecREST server.\n"
            "It must be used together with the 'firecrest' scheduler plugin.\n"
            "Authentication parameters:\n"
        ) + "\n".join(
            [f"  {k}: {v.get('help', '')}" for k, v in cls.auth_options.items()]
        )

    def open(self):  # noqa: A003
        pass

    def close(self):
        pass

    def getcwd(self) -> str:
        return str(self._cwd)

    def _get_path(self, *path: str) -> str:
        return posixpath.normpath(self._cwd.joinpath(*path))

    def chdir(self, path) -> None:
        new_path = self._cwd.joinpath(path)
        if not new_path.is_dir():
            raise OSError(f"'{new_path}' is not a valid directory")
        self._cwd = new_path

    def normalize(self, path="."):
        return posixpath.normpath(path)

    def chmod(self, path: str, mode: str) -> None:
        self._cwd.joinpath(path).chmod(mode)

    def chown(self, path, uid: str, gid: str) -> None:
        self._cwd.joinpath(path).chown(uid, gid)

    def path_exists(self, path: str) -> bool:
        return self._cwd.joinpath(path).exists()

    def get_attribute(self, path: str) -> FileAttribute:
        result = self._cwd.joinpath(path).stat()
        return FileAttribute(
            {
                "st_size": result.st_size,
                "st_uid": result.st_uid,
                "st_gid": result.st_gid,
                "st_mode": result.st_mode,
                "st_atime": result.st_atime,
                "st_mtime": result.st_mtime,
            }
        )

    def isdir(self, path: str) -> bool:
        return self._cwd.joinpath(path).is_dir()

    def isfile(self, path: str) -> bool:
        return self._cwd.joinpath(path).is_file()

    def listdir(self, path: str = ".", pattern: str | None = None) -> list[str]:
        names = [p.name for p in self._cwd.joinpath(path).iterdir()]
        if pattern is not None:
            names = fnmatch.filter(names, pattern)
        return names

    def write_binary(self, path: str, data: bytes) -> None:
        """Write bytes to a file on the remote."""
        # Note this is not part of the Transport interface, but is useful for testing
        # TODO will fail for files exceeding small_file_size_mb
        self._cwd.joinpath(path).write_bytes(data)

    def read_binary(self, path: str) -> bytes:
        """Read bytes from a file on the remote."""
        # Note this is not part of the Transport interface, but is useful for testing
        # TODO will fail for files exceeding small_file_size_mb
        return self._cwd.joinpath(path).read_bytes()

    def symlink(self, remotesource: str, remotedestination: str) -> None:
        source = self._cwd.joinpath(remotesource)
        destination = self._cwd.joinpath(remotedestination)
        destination.symlink_to(source)

    def copyfile(
        self, remotesource: str, remotedestination: str, dereference: bool = False
    ) -> None:
        source = self._cwd.joinpath(remotesource).enable_cache()
        destination = self._cwd.joinpath(remotedestination).enable_cache()

        if not source.exists():
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not source.is_file():
            raise FileNotFoundError(f"Source is not a file: {source}")

        if not dereference and source.is_symlink():
            destination.symlink_to(source)
        else:
            source.copy_to(destination)

    def copytree(
        self, remotesource: str, remotedestination: str, dereference: bool = False
    ) -> None:
        source = self._cwd.joinpath(remotesource).enable_cache().enable_cache()
        destination = (
            self._cwd.joinpath(remotedestination).enable_cache().enable_cache()
        )

        if not source.exists():
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not source.is_dir():
            raise FileNotFoundError(f"Source is not a directory: {source}")

        if not dereference and source.is_symlink():
            destination.symlink_to(source)
        else:
            source.copy_to(destination)

    def copy(
        self,
        remotesource: str,
        remotedestination: str,
        dereference: bool = False,
        recursive: bool = True,
    ) -> None:
        if not recursive:
            # TODO this appears to not actually be used upstream, so just remove there
            raise NotImplementedError("Non-recursive copy not implemented")
        source = self._cwd.joinpath(remotesource).enable_cache()
        destination = self._cwd.joinpath(remotedestination).enable_cache()

        if not source.exists():
            raise FileNotFoundError(f"Source file does not exist: {source}")

        if not dereference and source.is_symlink():
            destination.symlink_to(source)
        else:
            source.copy_to(destination)

    def makedirs(self, path: str, ignore_existing: bool = False) -> None:
        self._cwd.joinpath(path).mkdir(parents=True, exist_ok=ignore_existing)

    def mkdir(self, path: str, ignore_existing: bool = False) -> None:
        self._cwd.joinpath(path).mkdir(exist_ok=ignore_existing)

    # TODO check symlink handling for get methods
    # TODO do get/put methods need to handle glob patterns?

    def getfile(
        self, remotepath: str | FcPath, localpath: str | Path, *args: Any, **kwargs: Any
    ) -> None:
        local = Path(localpath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        remote = (
            remotepath
            if isinstance(remotepath, FcPath)
            else self._cwd.joinpath(remotepath).enable_cache()
        )
        if not remote.is_file():
            raise FileNotFoundError(f"Source file does not exist: {remote}")
        # TODO handle files exceeding small_file_size_mb
        with convert_header_exceptions({"machine": self._machine, "path": remote}):
            self._client.simple_download(self._machine, str(remote), localpath)
        # TODO use client.checksum to confirm?

    def gettree(
        self, remotepath: str | FcPath, localpath: str | Path, *args: Any, **kwargs: Any
    ) -> None:
        local = Path(localpath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        if local.is_file():
            raise OSError("Cannot copy a directory into a file")
        local.mkdir(parents=True, exist_ok=True)

        remote = (
            remotepath
            if isinstance(remotepath, FcPath)
            else self._cwd.joinpath(remotepath).enable_cache()
        )
        if not remote.is_dir():
            raise OSError(f"Source is not a directory: {remote}")
        for remote_item in remote.iterdir():
            local_item = local.joinpath(remote_item.name)
            if remote_item.is_dir():
                self.gettree(remote_item, local_item)
            else:
                self.getfile(remote_item, local_item)

    def get(self, remotepath: str, localpath: str, *args: Any, **kwargs: Any) -> None:
        remote = self._cwd.joinpath(remotepath).enable_cache()
        if remote.is_dir():
            self.gettree(remote, localpath)
        elif remote.is_file():
            self.getfile(remote, localpath)
        else:
            raise FileNotFoundError(f"Source file does not exist: {remote}")

    def putfile(self, localpath: str, remotepath: str, *args, **kwargs):
        if not Path(localpath).is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not Path(localpath).is_file():
            raise ValueError(f"Input localpath is not a file: {localpath}")
        local_size = Path(localpath).stat().st_size
        remote = self._cwd.joinpath(remotepath).enable_cache()
        # note this allows overwriting of existing files
        with convert_header_exceptions({"machine": self._machine, "path": remote}):
            if local_size >= self._small_file_size_bytes:
                up_obj = self._client.external_upload(
                    self._machine, localpath, str(remote)
                )
                if (
                    os.environ.get("FIRECREST_LOCAL_TESTING")
                    and platform.system() == "Darwin"
                ):
                    # TODO when using the demo server on a Mac, the wrong IP is provided
                    up_obj.object_storage_data["command"] = up_obj.object_storage_data[
                        "command"
                    ].replace("192.168.220.19", "localhost")

                # TODO the following is a very basic implementation of uploading a large file
                # ideally though, if uploading multiple large files (i.e. in puttree),
                # we would want to probably use asyncio, to concurrently upload,
                # then wait for all to finish

                # this uploads the file to the "staging area"
                # TODO this calls curl in a subcommand, but you could also use the python requests library
                # see: https://github.com/chrisjsewell/fireflow/blob/d45d41a0aced6502b7946c5557712a3c3cb1bebb/src/fireflow/process.py#L177
                up_obj.finish_upload()
                # this waits for the file in the staging area to be moved to the final location
                while up_obj.in_progress:
                    # TODO add a timeout and optimise the sleep time (and allow configurability?)
                    time.sleep(0.1)
            else:
                self._client.simple_upload(
                    self._machine, localpath, str(remote.parent), remote.name
                )

        # TODO use client.checksum to confirm upload is not corrupted?

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

    def remove(self, path: str) -> None:
        self._cwd.joinpath(path).unlink()

    def rename(self, oldpath: str, newpath: str) -> None:
        self._cwd.joinpath(oldpath).rename(self._cwd.joinpath(newpath))

    def rmdir(self, path: str):
        # TODO check if empty
        self._cwd.joinpath(path).rmtree()

    def rmtree(self, path: str) -> None:
        self._cwd.joinpath(path).rmtree()

    def whoami(self) -> str:
        return self._cwd.whoami()

    def gotocomputer_command(self, remotedir: str):
        # TODO remove from interface
        raise NotImplementedError

    def _exec_command_internal(self, command, **kwargs):
        # TODO remove from interface
        raise NotImplementedError

    def exec_command_wait_bytes(self, command, stdin=None, **kwargs):
        # TODO remove from interface
        raise NotImplementedError


def validate_non_empty_string(ctx, param, value):  # pylint: disable=unused-argument
    """Validate that the number passed to this parameter is a positive number.

    :param ctx: the `click.Context`
    :param param: the parameter
    :param value: the value passed for the parameter
    :raises `click.BadParameter`: if the value is not a positive number
    """
    if not isinstance(value, str) or not value.strip():
        from click import BadParameter

        raise BadParameter(f"{value} is not string or is empty")

    return value
