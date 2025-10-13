################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
"""Transport interface."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
import fnmatch
import hashlib
import os
from pathlib import Path, PurePosixPath
import posixpath
import stat
import sys
import tarfile
from typing import Any, ClassVar, TypedDict
import uuid

from aiida.cmdline.params.options.interactive import InteractiveOption
from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.transports.transport import (
    AsyncTransport,
    Transport,
    has_magic,
    validate_positive_number,
)
from aiida.transports.util import FileAttribute
from click.core import Context
from click.types import ParamType
from firecrest import ClientCredentialsAuth
from firecrest.v2 import AsyncFirecrest, Firecrest
from packaging.version import InvalidVersion, Version, parse

from aiida_firecrest.utils import FcPath, TPath_Extended, convert_header_exceptions

_MINIMUM_API_VERSION = "2.2.8"  # minimum supported version of FirecREST API
_SMALL_FILE_SIZE_MB = 10.0


class ValidAuthOption(TypedDict, total=False):
    option: OverridableOption | None  # existing option
    switch: bool  # whether the option is a boolean flag
    type: type[Any] | ParamType
    default: Any
    non_interactive_default: (
        bool  # whether option should provide a default in non-interactive mode
    )
    prompt: str  # for interactive CLI
    help: str
    callback: Callable[..., Any]  # for validation


def _create_secret_file(ctx: Context, param: InteractiveOption, value: str) -> str:
    """Create a secret file if the value is not a path to a secret file.
    The path should be absolute, if it is not, the file will be created in ~/.firecrest.
    """
    import click

    possible_path = Path(value)
    if os.path.isabs(possible_path):
        if not possible_path.exists():
            raise click.BadParameter(f"Secret file not found at {value}")
        secret_path = possible_path

    else:
        Path("~/.firecrest").expanduser().mkdir(parents=True, exist_ok=True)
        _ = uuid.uuid4()
        secret_path = Path(f"~/.firecrest/secret_{_}").expanduser()
        while secret_path.exists():
            # instead of a random number one could use the label or pk of the computer being configured
            secret_path = Path(f"~/.firecrest/secret_{_}").expanduser()
        secret_path.write_text(value)
        click.echo(
            click.style("Fireport: ", bold=True, fg="magenta")
            + f"Client Secret stored at {secret_path}"
        )
    return str(secret_path)


def _validate_temp_directory(ctx: Context, param: InteractiveOption, value: str) -> str:
    """Validate the temp directory on the server.
    If it does not exist, create it.
    If it is not empty, get a confirmation from the user to empty it.
    """

    import click

    firecrest_url = ctx.params["url"]
    token_uri = ctx.params["token_uri"]
    client_id = ctx.params["client_id"]
    compute_resource = ctx.params["compute_resource"]
    secret = ctx.params["client_secret"]

    transport = FirecrestTransport(
        url=firecrest_url,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=secret,
        compute_resource=compute_resource,
        temp_directory=value,
        billing_account="irrelevant",  # billing_account is irrelevant here
        max_io_allowed=8,  # max_io_allowed is irrelevant here
        checksum_check=False,  # checksum_check is irrelevant here
    )

    # Temp directory routine
    if transport.isfile(transport._temp_directory):
        raise click.BadParameter("Temp directory cannot be a file")

    if transport.path_exists(transport._temp_directory):
        if transport.listdir(transport._temp_directory):
            # if not configured:
            confirm = click.confirm(
                f"Temp directory {transport._temp_directory} is not empty. Do you want to flush it?"
            )
            if confirm:
                transport.rmtree(transport._temp_directory)
                transport.mkdir(transport._temp_directory)
            else:
                click.echo("Please provide an empty temp directory on the server.")
                raise click.BadParameter(
                    f"Temp directory {transport._temp_directory} is not empty"
                )

    else:
        try:
            transport.mkdir(transport._temp_directory, ignore_existing=True)
        except Exception as e:
            raise click.BadParameter(
                f"Could not create temp directory {transport._temp_directory} on server: {e}"
            ) from e
    click.echo(
        click.style("Fireport: ", bold=True, fg="magenta")
        + f"Temp directory is set to {value}"
    )

    return value


def _dynamic_info_direct_size(
    ctx: Context, param: InteractiveOption, value: float
) -> float:
    """Get dynamic information from the server, if the user enters 0 for the small_file_size_mb.
    This is done by connecting to the server and getting the value of UTILITIES_MAX_FILE_SIZE.
    Below this size, file bytes will be sent in a single API call. Above this size,
    the file will be downloaded(uploaded) from(to) the object store and downloaded in chunks.

    :param ctx: the `click.Context`
    :param param: the parameter
    :param value: the value passed for the parameter

    :return: the value of small_file_size_mb.

    """

    if value > 0:
        return value
    # parameter endpoint is not supported in v2
    # The code below is commented for now, and it has to adopt and come back,
    # once this issue is resolved:
    # https://github.com/eth-cscs/pyfirecrest/issues/162

    # import click
    # firecrest_url = ctx.params["url"]
    # token_uri = ctx.params["token_uri"]
    # client_id = ctx.params["client_id"]
    # compute_resource = ctx.params["compute_resource"]
    # secret = ctx.params["client_secret"]
    # temp_directory = ctx.params["temp_directory"]

    # transport = FirecrestTransport(
    #     url=firecrest_url,
    #     token_uri=token_uri,
    #     client_id=client_id,
    #     client_secret=secret,
    #     compute_resource=compute_resource,
    #     temp_directory=temp_directory,
    #     small_file_size_mb=0.0,
    #     billing_account="irrelevant",  # billing_account is irrelevant here
    # )

    # parameters = transport._client.parameters()
    # utilities_max_file_size = next(
    #     (
    #         item
    #         for item in parameters["utilities"]
    #         if item["name"] == "UTILITIES_MAX_FILE_SIZE"
    #     ),
    #     None,
    # )
    # small_file_size_mb = (
    #     float(utilities_max_file_size["value"])
    #     if utilities_max_file_size is not None
    #     else 5.0
    # )
    # click.echo(
    #     click.style("Fireport: ", bold=True, fg="magenta")
    #     + f"Maximum file size for direct transfer: {small_file_size_mb} MB"
    # )
    small_file_size_mb = 5.0

    return small_file_size_mb


class FirecrestTransport(AsyncTransport):  # type: ignore[misc]
    """Transport interface for FirecREST.
    Must be used together with the 'firecrest' scheduler plugin."""

    # We override these options, because they don't really make sense for a REST-API,
    # - `use_login_shell` you can't run bash on a REST-API
    # - `safe_interval` there is no connection overhead for a REST-API
    _common_auth_options: ClassVar[list[Any]] = []
    _DEFAULT_SAFE_OPEN_INTERVAL = 0.0
    _DEFAULT_max_io_allowed = 8

    _valid_auth_options: ClassVar[list[tuple[str, ValidAuthOption]]] = [
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
                "help": "FirecREST client secret or Absolute path to an existing FirecREST Secret Key",
                "callback": _create_secret_file,
            },
        ),
        (
            "compute_resource",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Compute resource (Machine)",
                "help": "Compute resources, for example 'daint', 'eiger', etc.",
            },
        ),
        (
            "billing_account",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Billing account for time consuming operations",
                "help": "According to the FirecREST documentation, operations longer than 5 seconds have to be"
                " submitted as a job, therefore you need to specify a billing account.",
            },
        ),
        (
            "temp_directory",
            {
                "type": str,
                "non_interactive_default": False,
                "prompt": "Temp directory on server",
                "help": "A temp directory on server for creating temporary files (compression, extraction, etc.)",
                "callback": _validate_temp_directory,
            },
        ),
        (
            "max_io_allowed",
            {
                "type": int,
                "default": _DEFAULT_max_io_allowed,
                "prompt": "Maximum number of concurrent I/O operations",
                "help": "Depends on various factors, such as your network bandwidth, the server load, etc."
                " (An experimental number)",
                "non_interactive_default": True,
                "callback": validate_positive_number,
            },
        ),
        (
            "checksum_check",
            {
                "type": bool,
                "default": False,
                "prompt": "Whether to validate checksum for each file transferred. Note: This can slow down the Transport plugin.",
                "non_interactive_default": True,
            },
        ),
    ]

    def __init__(
        self,
        *,
        url: str,
        token_uri: str,
        client_id: str,
        client_secret: str,
        compute_resource: str,
        temp_directory: str,
        billing_account: str,
        max_io_allowed: int,
        checksum_check: bool,
        **kwargs: Any,
    ):
        """Construct a FirecREST transport object.

        :param url: URL to the FirecREST server
        :param token_uri: URI for retrieving FirecREST authentication tokens
        :param client_id: FirecREST client ID
        :param client_secret: FirecREST client secret or str(Absolute path) to an existing FirecREST Secret Key
        :param compute_resource: Compute resources, for example 'daint', 'eiger', etc.
        :param temp_directory: A temp directory on server for creating temporary files (compression, extraction, etc.)
        :param billing_account: Billing account for time consuming operations
        :param max_io_allowed: Maximum number of concurrent I/O operations.
        :param kwargs: Additional keyword arguments

        """

        # there is no overhead for "opening" a connection to a REST-API,
        # but still allow the user to set a safe interval if they really want to
        kwargs.setdefault("safe_interval", 0)
        super().__init__(**kwargs)

        assert isinstance(url, str), "url must be a string"
        assert isinstance(token_uri, str), "token_uri must be a string"
        assert isinstance(client_id, str), "client_id must be a string"
        assert isinstance(client_secret, str), "client_secret must be a string"
        assert isinstance(compute_resource, str), "compute_resource must be a string"
        assert isinstance(temp_directory, str), "temp_directory must be a string"
        assert isinstance(billing_account, str), "billing_account must be a string"
        assert isinstance(max_io_allowed, int), "max_io_allowed must be an integer"

        self._machine = compute_resource
        self._url = url
        self._token_uri = token_uri
        self._small_file_size_bytes = int(_SMALL_FILE_SIZE_MB * 1024 * 1024)

        self._payoff_override: bool | None = None
        self._concurrent_io: int = 0

        secret = (
            Path(client_secret).read_text().strip()
            if Path(client_secret).exists()
            else client_secret
        )

        try:
            self.async_client = AsyncFirecrest(
                firecrest_url=self._url,
                authorization=ClientCredentialsAuth(client_id, secret, token_uri),
            )
            self.blocking_client = Firecrest(
                firecrest_url=self._url,
                authorization=ClientCredentialsAuth(client_id, secret, token_uri),
            )
        except Exception as e:
            raise ValueError(f"Could not connect to FirecREST server: {e}") from e

        self._temp_directory = FcPath(temp_directory)
        self._api_version: Version = self._get_firecrest_version()

        if self._api_version < parse("1.16.0"):
            self._payoff_override = False

        self.billing_account = billing_account
        self._max_io_allowed = max_io_allowed
        self.checksum_check = checksum_check

        # this makes no sense for firecrest, but we need to set this to True
        # otherwise the aiida-core will complain that the transport is not open:
        # aiida-core/src/aiida/orm/utils/remote:clean_remote()
        self._is_open = True

    def _get_firecrest_version(self) -> Version:
        """
        Find the version of the FirecREST server.
        Will connect to the server and get the version of the FirecREST server.

        returns: version of the FirecREST server.

        :raises ValueError: if the version is not supported
        :raises RuntimeError: if the version could not be retrieved
        """

        try:
            _version = self.blocking_client.server_version()
        except Exception as e:
            raise RuntimeError(
                "Could not get the version of the FirecREST server.\nPerhaps you have inserted wrong credentials?"
            ) from e

        if _version is None:
            raise RuntimeError(
                "Could not get the version of the FirecREST server, it returned None.\nPerhaps you have inserted wrong credentials?"
            )

        try:
            parsed_version = parse(_version)
        except InvalidVersion as err:
            raise ValueError(
                f"Cannot parse the retrieved version from the server: {_version}"
            ) from err

        if parsed_version < parse(_MINIMUM_API_VERSION):
            raise ValueError(
                f"FirecREST api version {_version} is not supported,"
                f" minimum supported version is {_MINIMUM_API_VERSION}"
            )

        return parsed_version

    def __str__(self) -> str:
        """Return the name of the plugin."""
        return self.__class__.__name__

    @property
    def max_io_allowed(self) -> int:
        return self._max_io_allowed

    async def _lock(self, sleep_time: float = 0.5) -> None:
        while self._concurrent_io >= self.max_io_allowed:
            await asyncio.sleep(sleep_time)
        self._concurrent_io += 1

    async def _unlock(self) -> None:
        self._concurrent_io -= 1

    @property
    def is_open(self) -> bool:
        return self._is_open

    @property
    def payoff_override(self) -> bool | None:
        return self._payoff_override

    @payoff_override.setter
    def payoff_override(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise ValueError("payoff_override must be a boolean value")
        self._payoff_override = value

    async def open_async(self) -> None:
        """Open the transport.
        This is a no-op for the REST-API, as there is no connection to open.
        """
        pass

    async def close_async(self) -> None:
        """Close the transport.
        This is a no-op for the REST-API, as there is no connection to close.
        """
        pass

    async def chmod_async(self, path: TPath_Extended, mode: int) -> None:
        """Change the mode of a path to the numeric mode.

        Note, if the path points to a symlink,
        the symlink target's permissions are changed.
        """

        path = str(path)
        # note: according to https://www.gnu.org/software/coreutils/manual/html_node/chmod-invocation.html#chmod-invocation
        # chmod never changes the permissions of symbolic links,
        # i.e. this is chmod, not lchmod
        if not isinstance(mode, int):
            raise TypeError("mode must be an integer")
        with convert_header_exceptions(
            {"X-Invalid-Mode": lambda p: ValueError(f"invalid mode: {mode}")}
        ):
            await self.async_client.chmod(self._machine, path, str(mode))

    async def chown_async(self, path: TPath_Extended, uid: int, gid: int) -> None:
        raise NotImplementedError

    async def _stat(self, path: TPath_Extended) -> os.stat_result:
        """Return stat info for this path.

        If the path is a symbolic link,
        stat will examine the file the link points to.
        """

        path = str(path)
        with convert_header_exceptions():
            stats = await self.async_client.stat(self._machine, path, dereference=True)
        return os.stat_result(
            (
                stats["mode"],
                stats["ino"],
                stats["dev"],
                stats["nlink"],
                stats["uid"],
                stats["gid"],
                stats["size"],
                stats["atime"],
                stats["mtime"],
                stats["ctime"],
            )
        )

    async def _lstat(self, path: TPath_Extended) -> os.stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """

        path = str(path)
        with convert_header_exceptions():
            stats = await self.async_client.stat(self._machine, path, dereference=False)
        return os.stat_result(
            (
                stats["mode"],
                stats["ino"],
                stats["dev"],
                stats["nlink"],
                stats["uid"],
                stats["gid"],
                stats["size"],
                stats["atime"],
                stats["mtime"],
                stats["ctime"],
            )
        )

    async def path_exists_async(self, path: TPath_Extended) -> bool:
        """Check if a path exists on the remote."""

        path = str(path)
        try:
            await self._stat(path)
        except FileNotFoundError:
            return False
        return True

    async def get_attribute_async(self, path: TPath_Extended) -> FileAttribute:
        """Get the attributes of a file."""

        path = str(path)
        result = await self._stat(path)
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

    async def isdir_async(self, path: TPath_Extended) -> bool:
        """Check if a path is a directory."""

        path = str(path)
        try:
            st_mode = (await self._stat(path)).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISDIR(st_mode)

    async def isfile_async(self, path: TPath_Extended) -> bool:
        """Check if a path is a file."""

        path = str(path)
        try:
            st_mode = (await self._stat(path)).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISREG(st_mode)

    async def listdir_async(
        self,
        path: TPath_Extended,
        pattern: str | None = None,
        recursive: bool = False,
        hidden: bool = True,
    ) -> list[str]:
        """List the contents of a directory. Returns filenames (or relative paths).

        :param path: this should be an absolute path.
        :param pattern: Unix shell-style wildcards to match the pattern:
            - `*` matches everything
            - `?` matches any single character
            - `[seq]` matches any character in seq
            - `[!seq]` matches any character not in seq
        :param recursive: If True, list directories recursively
        """

        path = str(path)
        if not recursive and (await self.isdir_async(path)) and not path.endswith("/"):
            # This is just to match the behavior of ls
            path += "/"

        with convert_header_exceptions():
            results = await self.async_client.list_files(
                self._machine, path, show_hidden=hidden, recursive=recursive
            )
        # names are relative to path
        names = [result["name"] for result in results]

        if pattern is not None:
            names = fnmatch.filter(names, pattern)
        return names

    # TODO the default implementations of glob / iglob could be overridden

    async def makedirs_async(
        self, path: TPath_Extended, ignore_existing: bool = False
    ) -> None:
        """Make directories on the remote."""

        path = str(path)
        exists = await self.path_exists_async(path)
        if not ignore_existing and exists:
            # Note: FirecREST does not raise an error if the directory already exists, and parent is True.
            # which makes sense, but following the Superclass, we should raise an OSError in that case.
            # AiiDA expects an OSError, instead of a FileExistsError
            raise OSError(f"'{path}' already exists")

        if ignore_existing and exists:
            return

        # firecrest does not support `exist_ok`, it's somehow blended into `parents`
        # see: https://github.com/eth-cscs/firecrest/issues/202
        await self.mkdir_async(path, ignore_existing=True)

    async def mkdir_async(
        self, path: TPath_Extended, ignore_existing: bool = False
    ) -> None:
        """Make a directory on the remote."""

        path = str(path)
        try:
            with convert_header_exceptions():
                # Note see: https://github.com/eth-cscs/firecrest/issues/172
                # Also see: https://github.com/eth-cscs/firecrest/issues/202
                # firecrest does not support `exist_ok`, it's somehow blended into `parents`
                await self.async_client.mkdir(
                    self._machine, path, create_parents=ignore_existing
                )

        except FileExistsError as err:
            if not ignore_existing:
                raise OSError(f"'{path}' already exists") from err
            raise

    async def normalize_async(self, path: TPath_Extended) -> str:
        """Normalize a path on the remote."""

        # TODO: this might be buggy
        path = str(path)
        return posixpath.normpath(path)

    async def symlink_async(
        self, remotesource: TPath_Extended, remotedestination: TPath_Extended
    ) -> None:
        """Create a symbolic link between the remote source and the remote
        destination.

        :param remotesource: where the link is pointing to, must be absolute.
        :param remotedestination: where the link is going to be created, must be absolute
        """

        # remotesource and remotedestination are non descriptive names.
        # We use those only because the functions signature should be the same as the one in superclass

        link_path = str(remotedestination)
        source_path = str(remotesource)

        if not PurePosixPath(source_path).is_absolute():
            raise ValueError("target(remotesource) must be an absolute path")
        with convert_header_exceptions():
            await self.async_client.symlink(self._machine, source_path, link_path)

    async def copyfile_async(
        self,
        remotesource: TPath_Extended,
        remotedestination: TPath_Extended,
        dereference: bool = False,
    ) -> None:
        """Copy a file on the remote. FirecREST does not support symlink copying.

        :param dereference: If True, copy the target of the symlink instead of the symlink itself.
        """

        source = str(remotesource)
        destination = str(remotedestination)

        if not (await self.path_exists_async(source)):
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not (await self.isfile_async(source)):
            raise ValueError(f"Source is not a file: {source}")
        if not (await self.path_exists_async(destination)) and not (
            await self.isfile_async(source)
        ):
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        await self._copy_to(source, destination, dereference)
        # I removed symlink copy, becasue it's really not a file copy, it's a link copy
        # and aiida-ssh have it in buggy manner, prrobably it's not used anyways

    async def _copy_to(
        self, source: TPath_Extended, target: TPath_Extended, dereference: bool
    ) -> None:
        """Copy source path to the target path. Both paths must be on remote.

        Works for both files and directories (in which case the whole tree is copied).
        """

        source = str(source)
        target = str(target)
        with convert_header_exceptions():
            # Note although this endpoint states that it is only for directories,
            # it actually uses `cp -r`:
            # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L320
            await self.async_client.cp(
                system_name=self._machine,
                source_path=source,
                target_path=target,
                dereference=dereference,
                account=self.billing_account,
                blocking=True,
            )

    async def copytree_async(
        self,
        remotesource: TPath_Extended,
        remotedestination: TPath_Extended,
        dereference: bool = False,
    ) -> None:
        """Copy a directory on the remote. FirecREST does not support symlink copying.

        :param dereference: If True, copy the target of the symlink instead of the symlink itself.
        """
        # TODO: check if deference is set to False, symlinks will be functional after the copy in Firecrest server.

        source = remotesource
        destination = remotedestination

        if not (await self.path_exists_async(source)):
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not (await self.isdir_async(source)):
            raise ValueError(f"Source is not a directory: {source}")
        if not (await self.path_exists_async(destination)):
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        await self._copy_to(source, destination, dereference)

    async def copy_async(
        self,
        remotesource: TPath_Extended,
        remotedestination: TPath_Extended,
        dereference: bool = False,
        recursive: bool = True,
    ) -> None:
        """Copy a file or directory on the remote. FirecREST does not support symlink copying.

        :param recursive: If True, copy directories recursively.
        note that the non-recursive option is not implemented in FirecREST server.
        And it's not used in upstream, anyways...

        :param dereference: If True, copy the target of the symlink instead of the symlink itself.
        """
        # TODO: investigate overwrite (?)

        remotesource = str(remotesource)
        remotedestination = str(remotedestination)

        if not recursive:
            # TODO this appears to not actually be used upstream, so just remove there
            raise NotImplementedError("Non-recursive copy not implemented")

        if has_magic(str(remotesource)):
            async for item in self.iglob_async(remotesource):
                # item is of str type, so we need to split it to get the file name
                filename = (
                    item.split("/")[-1] if (await self.isfile_async(item)) else ""
                )
                await self.copy_async(
                    item,
                    remotedestination + filename,
                    dereference=dereference,
                    recursive=recursive,
                )
            return

        if not (await self.path_exists_async(remotesource)):
            raise FileNotFoundError(f"Source does not exist: {remotesource}")
        if not (await self.path_exists_async(remotedestination)) and not (
            await self.isfile_async(remotesource)
        ):
            raise FileNotFoundError(f"Destination does not exist: {remotedestination}")

        await self._copy_to(remotesource, remotedestination, dereference)

    # TODO do get/put methods need to handle glob patterns?
    # Apparently not, but I'm not clear how glob() iglob() are going to behave here. We may need to implement them.

    async def getfile_async(
        self,
        remotepath: TPath_Extended,
        localpath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a file from the remote.

        :param dereference: If True, follow symlinks.
            note: we don't support downloading symlinks, so dereference should always be True

        """
        remotepath = FcPath(remotepath)
        local = Path(localpath)

        if not dereference:
            raise NotImplementedError(
                "Getting symlinks with `dereference=False` is not supported"
            )

        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")

        if not (await self.isfile_async(remotepath)):
            raise FileNotFoundError(f"Source file does not exist: {remotepath}")

        with convert_header_exceptions():
            await self._lock()
            await self.async_client.download(
                self._machine,
                str(remotepath),
                str(local),
                account=self.billing_account,
                blocking=True,
            )
            await self._unlock()

        if self.checksum_check:
            await self._validate_checksum_async(local, remotepath)

    async def _validate_checksum_async(
        self, localpath: TPath_Extended, remotepath: TPath_Extended
    ) -> None:
        """Validate the checksum of a file.
        Useful for checking if a file was transferred correctly.
        it uses sha256 hash to compare the checksum of the local and remote files.

        Raises: FileNotFoundError: If the remote file does not exist or is not a file.
        Raises: ValueError: If the checksums do not match. Or when the remote algorithm is not supported.
        """

        local = Path(localpath)
        remotepath = str(remotepath)

        if not (await self.isfile_async(remotepath)):
            raise FileNotFoundError(
                f"Remote file does not exist or is not a file: {remotepath}"
            )

        sha256_hash = hashlib.sha256()
        with open(local, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        local_hash = sha256_hash.hexdigest()

        results = await self.async_client.checksum(self._machine, remotepath)

        remote_hash = str(results["checksum"])
        remote_algorithm = str(results["algorithm"])

        if remote_algorithm.lower() != "sha256":
            raise ValueError(
                f"Unsupported checksum algorithm on remote: {remote_algorithm}."
                "Only sha256 is supported. Please open an issue on https://github.com/aiidateam/aiida-firecrest/issues"
                "sharing this error message, to support other algorithms."
            )

        try:
            assert local_hash == remote_hash
        except AssertionError as e:
            raise ValueError(
                f"Checksum mismatch between local and remote files: {local} and {remotepath}"
            ) from e

    def _validate_checksum(self, *args: Any, **kwargs: Any) -> None:
        # This is a blocking wrapper for the async method _validate_checksum_async.
        # Not part of the Transport interface, but used internally.
        return self.run_command_blocking(self._validate_checksum_async, *args, **kwargs)  # type: ignore

    async def _gettreetar(
        self,
        remotepath: TPath_Extended,
        localpath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a directory from the remote as a gzip file and extract it locally.
        This is useful for downloading a directory with many files,
        as it might be more efficient than downloading each file individually.
        Note that this method is not part of the Transport interface, and is not meant to be used publicly.

        :param dereference: If True, follow symlinks.
        """

        remotepath = str(remotepath)
        localpath = str(localpath)

        _ = uuid.uuid4()
        remote_path_temp = self._temp_directory.joinpath(f"temp_{_}.gzip")

        # Compress
        await self.async_client.compress(
            self._machine, remotepath, str(remote_path_temp), dereference=dereference
        )
        # Download
        localpath_temp = Path(localpath).joinpath(f"temp_{_}.gzip")
        try:
            await self.getfile_async(remote_path_temp, localpath_temp)
        finally:
            await self.remove_async(remote_path_temp)

        # Extract the downloaded file locally
        try:
            # with tarfile.open(localpath_temp, "r") as tar:
            #     members = [m for m in tar.getmembers() if m.name.startswith(remotepath.name)]
            #     for member in members:
            #         member.name = os.path.relpath(member.name, remotepath.name)
            #         tar.extract(member, path=localpath)
            os.system(f"tar -xf '{localpath_temp}' --strip-components=1 -C {localpath}")
        finally:
            localpath_temp.unlink()

    async def gettree_async(
        self,
        remotepath: TPath_Extended,
        localpath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a directory from the remote.

        :param dereference: If True, follow symlinks.
            note: dereference should be always True, otherwise the symlinks will not be functional.
        """

        remotepath = FcPath(remotepath)
        local = Path(localpath)

        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        if local.is_file():
            raise OSError("Cannot copy a directory into a file")

        if not (await self.isdir_async(remotepath)):
            raise OSError(f"Source is not a directory: {remotepath}")

        # this block is added only to mimick the behavior that aiida expects
        if local.exists():
            # Destination directory already exists, create remote directory name inside it
            local = local.joinpath(remotepath.name)
            local.mkdir(parents=True, exist_ok=True)
        else:
            # Destination directory does not exist, create and move content abc inside it
            local.mkdir(parents=True, exist_ok=False)

        if await self.payoff(remotepath):
            # in this case send a request to the server to tar the files and then download the tar file
            # unfortunately, the server does not provide a deferenced tar option, yet.
            await self._gettreetar(remotepath, local, dereference=dereference)
        else:
            # otherwise download the files one by one
            for remote_item in await self.listdir_async(remotepath, recursive=True):
                # remote_item is a relative path,
                remote_item_abs = remotepath.joinpath(remote_item).resolve()
                local_item = local.joinpath(remote_item)
                if dereference and (await self.is_symlink_async(remote_item_abs)):
                    target_path = await self._get_target(remote_item_abs)
                    if not Path(target_path).is_absolute():
                        target_path = remote_item_abs.parent.joinpath(
                            target_path
                        ).resolve()

                    if await self.isdir_async(target_path):
                        await self.gettree_async(
                            target_path, local_item, dereference=True
                        )
                else:
                    target_path = remote_item_abs

                if not (await self.isdir_async(target_path)):
                    await self.getfile_async(target_path, local_item)
                else:
                    local_item.mkdir(parents=True, exist_ok=True)

    async def _get_target(self, path: TPath_Extended) -> FcPath:
        """gets the target of a symlink.
        Note: path must be a symlink, we don't check that, here."""

        path = str(path)
        # results = self.async_client.list_files(self._machine, path,
        #                                   show_hidden=True,
        #                                   recursive=False,
        #                                   dereference=False)
        # dereference=False is not supported in firecrest v1,
        # so we have to do a workaround
        results = await self.async_client.list_files(
            self._machine, str(Path(path).parent), show_hidden=True, recursive=False
        )
        # filter results to get the symlink with the given path
        results = [result for result in results if result["name"] == Path(path).name]

        if not results:
            raise FileNotFoundError(f"Symlink target does not exist: {path}")
        if len(results) != 1:
            raise ValueError(
                f"Expected a single symlink target, got {len(results)}: {path}"
            )

        return FcPath(results[0]["linkTarget"])

    # Not part of the aiida-core::Transport interface, but very useful.
    async def is_symlink_async(self, path: TPath_Extended) -> bool:
        """Whether path is a symbolic link."""

        path = str(path)
        try:
            st_mode = (await self._lstat(path)).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISLNK(st_mode)

    def is_symlink(self, *args: Any, **kwargs: Any) -> bool:
        return self.run_command_blocking(self.is_symlink_async, *args, **kwargs)  # type: ignore

    async def get_async(
        self,
        remotepath: TPath_Extended,
        localpath: TPath_Extended,
        ignore_nonexisting: bool = False,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a file or directory from the remote.

        :param ignore_nonexisting: If True, do not raise an error if the source file does not exist.
        :param dereference: If True, follow symlinks.
            note: dereference should be always True, otherwise the symlinks will not be functional.
        """

        remotepath = FcPath(remotepath)
        localpath = str(localpath)

        if await self.isdir_async(remotepath):
            await self.gettree_async(remotepath, localpath)
        elif await self.isfile_async(remotepath):
            await self.getfile_async(remotepath, localpath)
        elif has_magic(str(remotepath)):
            async for item in self.iglob_async(remotepath):
                # item is of str type, so we need to split it to get the file name
                filename = (
                    item.split("/")[-1] if (await self.isfile_async(item)) else ""
                )
                await self.get_async(
                    item,
                    localpath + filename,
                    dereference=dereference,
                    ignore_nonexisting=ignore_nonexisting,
                )
            return
        elif not ignore_nonexisting:
            raise FileNotFoundError(f"Source file does not exist: {remotepath}")

    async def putfile_async(
        self,
        localpath: TPath_Extended,
        remotepath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a file from the remote.

        :param dereference: If True, follow symlinks.
            note: we don't support uploading symlinks, so dereference is always should be True

        """

        localpath = Path(localpath)
        remotepath = FcPath(remotepath)

        if not dereference:
            raise NotImplementedError(
                "Getting symlinks with `dereference=False` is not supported"
            )

        if not localpath.is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not localpath.is_file():
            if not localpath.exists():
                raise FileNotFoundError(f"Local file does not exist: {localpath}")
            raise ValueError(f"Input localpath is not a file {localpath}")

        if await self.isdir_async(remotepath):
            raise ValueError(f"Destination is a directory: {remotepath}")

        # note this allows overwriting of existing files
        with convert_header_exceptions():
            await self._lock()
            await self.async_client.upload(
                self._machine,
                str(localpath),
                str(remotepath.parent),
                str(remotepath.name),
                account=self.billing_account,
                blocking=True,
            )
            await self._unlock()

        if self.checksum_check:
            await self._validate_checksum_async(localpath, remotepath)

    async def payoff(self, path: TPath_Extended) -> bool:
        """
        This function will be used to determine whether to tar the files before downloading
        """
        # After discussing with the pyfirecrest team, it seems that server has some sort
        # of serialization and "penalty" for sending multiple requests asycnhronusly or in a short time window.
        # It responses in 1, 1.5, 3, 5, 7 seconds!
        # So right now, I think if the number of files is more than 3, it pays off to tar everything

        path = str(path)

        # If payoff_override is set, return its value
        if self.payoff_override is not None:
            return bool(self.payoff_override)

        # This is a workaround to determine if the path is local or remote
        if Path(path).exists():
            return len(os.listdir(path)) > 3

        return len(await self.listdir_async(path, recursive=True)) > 3

    async def _puttreetar(
        self,
        localpath: TPath_Extended,
        remotepath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a directory to the remote by sending as gzip file in backend.
        This is useful for uploading a directory with many files,
        as it might be more efficient than uploading each file individually.
        Note that this method is not part of the Transport interface, and is not meant to be used publicly.

        :param dereference: If True, follow symlinks. If False, symlinks are ignored from sending over.
        """
        # this function will be used to send a folder as a gzip file to the server and extract it on the server

        _ = uuid.uuid4()

        localpath = Path(localpath)
        remotepath = str(remotepath)

        tarpath = localpath.parent.joinpath(f"temp_{_}.gzip")
        remote_path_temp = self._temp_directory.joinpath(f"temp_{_}.gzip")
        with tarfile.open(tarpath, "w:gz", dereference=dereference) as gzip:
            for root, _, files in os.walk(localpath, followlinks=dereference):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, localpath)
                    gzip.add(full_path, arcname=relative_path)
        # Upload
        try:
            await self.putfile_async(tarpath, remote_path_temp)
        finally:
            tarpath.unlink()

        # Attempt extract
        try:
            await self.async_client.extract(
                self._machine, str(remote_path_temp), remotepath
            )
        finally:
            await self.remove_async(remote_path_temp)

    async def puttree_async(
        self,
        localpath: TPath_Extended,
        remotepath: TPath_Extended,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a directory to the remote.

        :param dereference: If True, follow symlinks.
            note: dereference should be always True, otherwise the symlinks
              will not be functional, therfore not supported.
        """
        if not dereference:
            raise NotImplementedError

        localpath = Path(localpath)
        remote = FcPath(remotepath)

        if not localpath.is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not localpath.exists():
            raise OSError("The localpath does not exists")
        if not localpath.is_dir():
            raise ValueError(f"Input localpath is not a directory: {localpath}")

        # this block is added only to mimick the behavior that aiida expects
        if await self.path_exists_async(remote):
            # Destination directory already exists, create local directory name inside it
            remote = remote.joinpath(localpath.name)
            await self.mkdir_async(remote, ignore_existing=False)
        else:
            # Destination directory does not exist, create and move content abc inside it
            await self.mkdir_async(remote, ignore_existing=False)

        if await self.payoff(localpath):
            # in this case send send everything as a tar file
            await self._puttreetar(localpath, remote)
        else:
            # otherwise send the files one by one
            for dirpath, _, filenames in os.walk(localpath, followlinks=dereference):
                rel_folder = os.path.relpath(path=dirpath, start=localpath)

                rm_parent_now = remote.joinpath(rel_folder)
                await self.mkdir_async(rm_parent_now, ignore_existing=True)

                for filename in filenames:
                    localfile_path = os.path.join(localpath, rel_folder, filename)
                    remotefile_path = rm_parent_now.joinpath(filename)
                    await self.putfile_async(localfile_path, remotefile_path)

    async def put_async(
        self,
        localpath: TPath_Extended,
        remotepath: TPath_Extended,
        ignore_nonexisting: bool = False,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a file or directory to the remote.

        :param ignore_nonexisting: If True, do not raise an error if the source file does not exist.
        :param dereference: If True, follow symlinks.
            note: dereference should be always True, otherwise the symlinks will not be functional.
        """
        # TODO ssh does a lot more
        # update on the TODO: I made a manual test with ssh.
        # added some extra care in puttree and gettree and now it's working fine

        if not dereference:
            raise NotImplementedError

        local = Path(localpath)
        remotepath = str(remotepath)
        if not local.is_absolute():
            raise ValueError("The localpath must be an absolute path")

        if has_magic(str(localpath)):
            async for item in self.iglob_async(localpath):
                # item is of str type, so we need to split it to get the file name
                filename = (
                    item.split("/")[-1] if (await self.isfile_async(item)) else ""
                )
                await self.put_async(
                    item,
                    remotepath + filename,
                    dereference=dereference,
                    ignore_nonexisting=ignore_nonexisting,
                )
            return

        if not Path(local).exists() and not ignore_nonexisting:
            raise FileNotFoundError(f"Source file does not exist: {localpath}")

        if local.is_dir():
            await self.puttree_async(localpath, remotepath)
        elif local.is_file():
            await self.putfile_async(localpath, remotepath)

    async def remove_async(self, path: TPath_Extended) -> None:
        """Remove the file at the given path. This only works on files."""

        path = str(path)

        if await self.isfile_async(path):
            with convert_header_exceptions():
                await self.async_client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        elif not (await self.path_exists_async(path)):
            # if the path does not exist, we do not raise an error
            return
        else:
            raise OSError(f"Path is not a file: {path}")

    async def rename_async(
        self, oldpath: TPath_Extended, newpath: TPath_Extended
    ) -> None:
        """Rename a file or directory on the remote."""

        oldpath = str(oldpath)
        newpath = str(newpath)

        with convert_header_exceptions():
            await self.async_client.mv(
                self._machine,
                oldpath,
                newpath,
                account=self.billing_account,
                blocking=True,
            )

    async def rmdir_async(self, path: TPath_Extended) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, an OSError is raised."""

        path = str(path)

        if len(await self.listdir_async(path)) == 0:
            with convert_header_exceptions():
                await self.async_client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        else:
            raise OSError(f"Directory not empty: {path}")

    async def rmtree_async(self, path: TPath_Extended) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, it will be removed recursively, equivalent to `rm -rf`.
        It does not raise an error if the directory does not exist.
        """
        # note firecrest uses `rm -rf`,

        path = str(path)

        if await self.isdir_async(path):
            with convert_header_exceptions():
                await self.async_client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        elif not (await self.path_exists_async(path)):
            # if the path does not exist, we do not raise an error
            return
        else:
            raise OSError(f"Path is not a directory: {path}")

    async def whoami_async(self) -> str:
        """Return the username of the current user.
        return None if the username cannot be determined.
        """
        # whoami is not supported in v2:
        # https://github.com/eth-cscs/pyfirecrest/issues/160
        # return self.async_client.whoami(machine=self._machine)
        return str(
            (await self.async_client.userinfo(system_name=self._machine))["user"][
                "name"
            ]
        )

    def gotocomputer_command(self, remotedir: TPath_Extended) -> str:
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support gotocomputer_command")

    async def exec_command_wait_async(  # type: ignore[no-untyped-def]
        self,
        command: str,
        stdin=None,
        encoding: str = "utf-8",
        workdir: TPath_Extended | None = None,
        **kwargs,
    ):
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support command execution")

    ## This methods that could be put in AsyncTransport, abstract class
    async def listdir_withattributes_async(
        self, path: TPath_Extended, pattern: str | None = None
    ) -> list[dict[str, Any]]:
        """Return a list of the names of the entries in the given path.
        The list is in arbitrary order. It does not include the special
        entries '.' and '..' even if they are present in the directory.

        :param path: path to list (default to '.')
            It must be an absolute path.
        :param pattern: if used, listdir returns a list of files matching
                            filters in Unix style. Unix only.
        :type path:  :class:`Path <pathlib.Path>`, :class:`PurePosixPath <pathlib.PurePosixPath>`, or `str`
        :type pattern: str
        :return: a list of dictionaries, one per entry.
            The schema of the dictionary is
            the following::

                {
                   'name': String,
                   'attributes': FileAttributeObject,
                   'isdir': Bool
                }

            where 'name' is the file or folder directory, and any other information is metadata
            (if the file is a folder, a directory, ...). 'attributes' behaves as the output of
            transport.get_attribute(); isdir is a boolean indicating if the object is a directory or not.
        """
        path = str(path)
        retlist = []
        path_resolved = Path(path).resolve().as_posix()

        for file_name in await self.listdir_async(path_resolved):
            filepath = os.path.join(path_resolved, file_name)
            attributes = await self.get_attribute_async(filepath)
            retlist.append(
                {
                    "name": file_name,
                    "attributes": attributes,
                    "isdir": await self.isdir_async(filepath),
                }
            )
        return retlist

    ## This methods that could be put in AsyncTransport, abstract class
    async def copy_from_remote_to_remote_async(
        self,
        transportdestination: Transport,
        remotesource: TPath_Extended,
        remotedestination: TPath_Extended,
        **kwargs: Any,
    ) -> None:
        """Copy files or folders from a remote computer to another remote computer, asynchronously.

        :param transportdestination: transport to be used for the destination computer
        :param remotesource: path to the remote source directory / file
        :param remotedestination: path to the remote destination directory / file
        :param kwargs: keyword parameters passed to the call to transportdestination.put,
            except for 'dereference' that is passed to self.get

        :type transportdestination: :class:`Transport <aiida.transports.transport.Transport>`,
        :type remotesource:  :class:`Path <pathlib.Path>`, :class:`PurePosixPath <pathlib.PurePosixPath>`, or `str`
        :type remotedestination:  :class:`Path <pathlib.Path>`, :class:`PurePosixPath <pathlib.PurePosixPath>`, or `str`

        .. note:: the keyword 'dereference' SHOULD be set to False for the
         final put (onto the destination), while it can be set to the
         value given in kwargs for the get from the source. In that
         way, a symbolic link would never be followed in the final
         copy to the remote destination. That way we could avoid getting
         unknown (potentially malicious) files into the destination computer.
         HOWEVER, since dereference=False is currently NOT
         supported by all plugins, we still force it to True for the final put.

        .. note:: the supported keys in kwargs are callback, dereference,
           overwrite and ignore_nonexisting.
        """
        from aiida.common.folders import SandboxFolder

        kwargs_get = {
            "callback": None,
            "dereference": kwargs.pop("dereference", True),
            "overwrite": True,
            "ignore_nonexisting": False,
        }
        kwargs_put = {
            "callback": kwargs.pop("callback", None),
            "dereference": True,
            "overwrite": kwargs.pop("overwrite", True),
            "ignore_nonexisting": kwargs.pop("ignore_nonexisting", False),
        }

        if kwargs:
            self.logger.error("Unknown parameters passed to copy_from_remote_to_remote")

        with SandboxFolder() as sandbox:
            await self.get_async(remotesource, sandbox.abspath, **kwargs_get)
            # Then we scan the full sandbox directory with get_content_list,
            # because copying directly from sandbox.abspath would not work
            # to copy a single file into another single file, and copying
            # from sandbox.get_abs_path('*') would not work for files
            # beginning with a dot ('.').
            for filename in sandbox.get_content_list():
                await transportdestination.put_async(
                    os.path.join(sandbox.abspath, filename),
                    remotedestination,
                    **kwargs_put,
                )

    async def glob_async(self, pathname: TPath_Extended) -> list[str]:
        """Return a list of paths matching a pathname pattern.

        The pattern may contain simple shell-style wildcards a la fnmatch.

        :param pathname: the pathname pattern to match. It should only be an absolute path.

        :type pathname:  :class:`Path <pathlib.Path>`, :class:`PurePosixPath <pathlib.PurePosixPath>`, or `str`

        :return: a list of paths matching the pattern.
        """
        pathname_str = str(pathname)
        result = []
        async for item in self.iglob_async(pathname_str):
            result.append(item)
        return result

    async def iglob_async(self, pathname: TPath_Extended) -> AsyncGenerator[str]:
        """Return an iterator which yields the paths matching a pathname pattern.

        The pattern may contain simple shell-style wildcards a la fnmatch.

        :param pathname: the pathname pattern to match.
        """
        pathname_str = str(pathname)

        if not has_magic(pathname_str):
            if await self.path_exists_async(pathname_str):
                yield pathname_str
            return
        dirname, basename = os.path.split(pathname_str)

        if has_magic(dirname):
            dirs = [
                d
                async for d in self.iglob_async(dirname)
                if (await self.isdir_async(d))
            ]
        else:
            dirs = [dirname] if (await self.isdir_async(dirname)) else []

        glob_in_dir = self.glob1 if has_magic(basename) else self.glob0
        for dirname in dirs:
            for name in await glob_in_dir(dirname, basename):
                yield os.path.join(dirname, name)

    async def glob1(self, dirname: str, pattern: str) -> list[str]:
        """Match subpaths of dirname against pattern.

        :param dirname: path to the directory
        :param pattern: pattern to match against
        """

        if isinstance(pattern, str) and not isinstance(dirname, str):
            dirname = dirname.decode(
                sys.getfilesystemencoding() or sys.getdefaultencoding()
            )
        try:
            names = await self.listdir_async(dirname, recursive=False)
        except OSError:
            return []
        if pattern[0] != ".":
            names = [name for name in names if name[0] != "."]
        return fnmatch.filter(names, pattern)

    async def glob0(self, dirname: str, basename: str) -> list[str]:
        """Wrap basename i a list if it is empty or if dirname/basename is an existing path, else return empty list.

        :param dirname: path to the directory
        :param basename: basename to match against
        """
        if basename == "":
            # `os.path.split()` returns an empty basename for paths ending with a
            # directory separator.  'q*x/' should match only directories.
            if await self.isdir_async(dirname):
                return [basename]
        elif await self.path_exists_async(os.path.join(dirname, basename)):
            return [basename]
        return []

    async def compress_async(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "Compressing files is not supported by firecrest transport, for now. "
        )

    async def extract_async(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "Extracting files is not supported by firecrest transport, for now. "
        )
