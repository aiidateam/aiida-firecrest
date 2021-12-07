###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Transport interface."""
import posixpath
from pathlib import Path, PurePosixPath
from typing import Any, List, Optional, Tuple, Type, Union

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
        return posixpath.normpath(self._cwd.joinpath(path))

    def open(self):  # noqa: A003
        # TODO allow for batch connections in pyfirecrest?
        pass

    def close(self):
        pass

    def getcwd(self) -> str:
        return str(self._cwd)

    def chdir(self, path: str) -> None:
        raise NotImplementedError

    def normalize(self, path="."):
        raise NotImplementedError

    def chmod(self, path: str, mode: str):
        self._client.chmod(self._machine, self._get_path(path), mode=mode)

    def chown(self, path, uid: str, gid: str):
        self._client.chmod(self._machine, self._get_path(path), owner=uid, group=gid)

    def copy(self, remotesource, remotedestination, dereference=False, recursive=True):
        raise NotImplementedError

    def copyfile(self, remotesource, remotedestination, dereference=False):
        raise NotImplementedError

    def copytree(self, remotesource, remotedestination, dereference=False):
        raise NotImplementedError

    def _exec_command_internal(self, command, **kwargs):
        raise NotImplementedError

    def exec_command_wait_bytes(self, command, stdin=None, **kwargs):
        raise NotImplementedError

    def get(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def gettree(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    # TODO: once https://github.com/eth-cscs/firecrest/pull/133 is deployed,
    # then this can be used for get_attribute, isdir, isfile, etc

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

    def getfile(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def put(self, localpath, remotepath, *args, **kwargs):
        raise NotImplementedError

    def putfile(self, localpath: str, remotepath: str, *args, **kwargs):
        # TODO handle large files (maybe use .parameters() to decide if file is large)
        # TODO pyfirecrest requires the remotepath to be a directory & takes the name from localpath
        remotepathlib = PurePosixPath(self._get_path(remotepath))
        assert Path(localpath).name == remotepathlib.name
        # note this allows overwriting
        self._client.simple_upload(self._machine, localpath, str(remotepathlib.parent))

    def puttree(self, localpath, remotepath, *args, **kwargs):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def rename(self, oldpath, newpath):
        raise NotImplementedError

    def rmdir(self, path):
        raise NotImplementedError

    def rmtree(self, path):
        raise NotImplementedError

    def gotocomputer_command(self, remotedir):
        raise NotImplementedError

    def symlink(self, remotesource, remotedestination):
        raise NotImplementedError

    def path_exists(self, path: str) -> bool:
        try:
            self._client.file_type(self._machine, self._get_path(path))
        except HeaderException as exc:
            if "X-Invalid-Path" in exc.responses[-1].headers:
                return False
            raise
        return True
