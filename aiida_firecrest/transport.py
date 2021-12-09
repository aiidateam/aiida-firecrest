###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Transport interface."""
import os
import posixpath
from pathlib import Path, PurePosixPath
from typing import Any, List, NamedTuple, Optional, Tuple, Type, Union

import firecrest as f7t
from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.cmdline.params.types.path import AbsolutePathOrEmptyParamType
from aiida.transports import Transport
from click.types import ParamType
from firecrest.FirecrestException import HeaderException

try:
    from typing import TypedDict  # type: ignore
except ImportError:
    from typing_extensions import TypedDict


class ValidAuthOption(TypedDict, total=False):  # type: ignore
    option: Optional[OverridableOption]  # existing option
    switch: bool  # whether the option is a boolean flag
    type: Union[Type[Any], ParamType]  # noqa: A003
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
        self.keycloak = f7t.ClientCredentialsAuthorization(
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

    _valid_auth_options: List[Tuple[str, dict]] = [
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
        client_secret: Union[str, Path],
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

    def _get_path(self, path: str) -> str:
        # TODO ensure all remote paths are manipulated with posixpath
        return posixpath.normpath(self._cwd.joinpath(path))

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
        # -rw-------  1 chrisjsewell staff 57 2021-12-02T10:42:00 file.txt
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
            output = self._client.list_files(self._machine, dirname, showhidden=True)
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

    def listdir(self, path: str = ".", pattern: Optional[str] = None) -> List[str]:
        if pattern is not None:
            raise NotImplementedError("pattern matching")
        try:
            output = self._client.list_files(
                self._machine, self._get_path(path), showhidden=True
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

    def puttree(self, localpath: Union[str, Path], remotepath: str, *args, **kwargs):
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
        self._client.rename(
            self._machine, self._get_path(oldpath), self._get_path(newpath)
        )

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
