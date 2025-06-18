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

import fnmatch
import hashlib
import os
from pathlib import Path, PurePosixPath
import posixpath
import stat
import tarfile
from typing import Any, Callable, ClassVar, TypedDict
import uuid

from aiida.cmdline.params.options.interactive import InteractiveOption
from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.transports.transport import BlockingTransport, has_magic
from aiida.transports.util import FileAttribute
from click.core import Context
from click.types import ParamType
from firecrest import ClientCredentialsAuth  # type: ignore[attr-defined]
from firecrest.v2 import Firecrest  # type: ignore[attr-defined]
from packaging.version import Version, parse

from aiida_firecrest.utils import FcPath, TPath_Extended, convert_header_exceptions


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
        small_file_size_mb=1.0,  # small_file_size_mb is irrelevant here
        api_version="100.0.0",  # version is irrelevant here
        billing_account="irrelevant",  # billing_account is irrelevant here
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
                for item in transport.listdir(transport._temp_directory):
                    # TODO: maybe do recursive delete
                    transport.remove(transport._temp_directory.joinpath(item))
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


def _dynamic_info_firecrest_version(
    ctx: Context, param: InteractiveOption, value: str
) -> str:
    """
    Find the version of the FirecREST server.
    This is a callback function for click, interface.
    It's called during the parsing of the command line arguments, during `verdi computer configure` command.
    If the user enters "None" as value, this function will connect to the server and get the version of the FirecREST server.
    Otherwise, it will check if the version is supported.
    """

    import click
    from packaging.version import InvalidVersion

    if value != "None" and value != "0":
        try:
            parse(value)
        except InvalidVersion as err:
            # raise in case the version is not valid, e.g. latest, stable, etc.
            raise click.BadParameter(f"Invalid input {value}") from err

        if parse(value) < parse("2.2.8"):
            raise click.BadParameter(f"FirecREST api version {value} is not supported")
        # If version is provided by the user, and it's supported, we will just return it.
        # No print confirmation is needed, to keep things less verbose.
        return value

    # The code below is a functional dynamic version retrieval from the server.
    # However, Firecrest v2 unlike v1, does not provide the api-version of the server.
    # An issue is opened: https://github.com/eth-cscs/pyfirecrest/issues/157
    # TODO: once the issue is resolved, adopt the code below to retrieve the version dynamically.

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
    #     api_version="100.0.0",  # version is irrelevant here
    # )

    # parameters = transport._client.parameters()
    # try:
    #     info = next(
    #         (
    #             item
    #             for item in parameters["general"]
    #             if item["name"] == "FIRECREST_VERSION"
    #         ),
    #         None,
    #     )
    #     if info is not None:
    #         _version = str(info["value"])
    #     else:
    #         raise KeyError
    # except KeyError as err:
    #     click.echo("Could not get the version of the FirecREST server")
    #     raise click.Abort() from err

    # if parse(_version) < parse("1.15.0"):
    #     click.echo(f"FirecREST api version {_version} is not supported")
    #     raise click.Abort()

    # # for the sake of uniformity, we will print the version in style only if dynamically retrieved.
    # click.echo(
    #     click.style("Fireport: ", bold=True, fg="magenta") + f"FirecRESTapi: {_version}"
    # )
    # return _version

    click.echo(
        "Due to a bug in FirecREST v2 api version cannot be fetched from the server."
        "It's now set to 2.0.0, and user input is ignored."
    )
    return "2.0.0"  # default version, if the user enters 0 or None


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
    #     api_version="100.0.0",  # version is irrelevant here
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


class FirecrestTransport(BlockingTransport):
    """Transport interface for FirecREST.
    It must be used together with the 'firecrest' scheduler plugin."""

    # override these options, because they don't really make sense for a REST-API,
    # so we don't want the user having to provide them
    # - `use_login_shell` you can't run bash on a REST-API
    # - `safe_interval` there is no connection overhead for a REST-API
    #   although ideally you would rate-limit the number of requests,
    #   but this would ideally require some "global" rate limiter,
    #   across all transport instances
    # TODO upstream issue
    _common_auth_options: ClassVar[list[Any]] = []  # type: ignore[misc]
    _DEFAULT_SAFE_OPEN_INTERVAL = 0.0

    _valid_auth_options: ClassVar[list[tuple[str, ValidAuthOption]]] = [  # type: ignore[misc]
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
            "api_version",
            {
                "type": str,
                "default": "2.2.8",
                "non_interactive_default": True,
                "prompt": "FirecREST api version.",
                "help": "The version of the FirecREST api deployed on the server",
                "callback": _dynamic_info_firecrest_version,
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
            "small_file_size_mb",
            {
                "type": float,
                "default": 0,
                "non_interactive_default": True,
                "prompt": "Maximum file size for direct transfer (MB) [Enter 0 to get this info from server]",
                "help": "Below this size, file bytes will be sent in a single API call.",
                "callback": _dynamic_info_direct_size,
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
        small_file_size_mb: float,
        api_version: str,
        billing_account: str,
        # note, machine is provided by default,
        # for the hostname, but we don't use that
        # TODO ideally hostname would not be necessary on a computer
        **kwargs: Any,
    ):
        """Construct a FirecREST transport object.

        :param url: URL to the FirecREST server
        :param token_uri: URI for retrieving FirecREST authentication tokens
        :param client_id: FirecREST client ID
        :param client_secret: FirecREST client secret or str(Absolute path) to an existing FirecREST Secret Key
        :param compute_resource: Compute resources, for example 'daint', 'eiger', etc.
        :param small_file_size_mb: Maximum file size for direct transfer (MB)
        :param temp_directory: A temp directory on server for creating temporary files (compression, extraction, etc.)
        :param kwargs: Additional keyword arguments
        """

        # there is no overhead for "opening" a connection to a REST-API,
        # but still allow the user to set a safe interval if they really want to
        kwargs.setdefault("safe_interval", 0)
        super().__init__(**kwargs)  # type: ignore

        assert isinstance(url, str), "url must be a string"
        assert isinstance(token_uri, str), "token_uri must be a string"
        assert isinstance(client_id, str), "client_id must be a string"
        assert isinstance(client_secret, str), "client_secret must be a string"
        assert isinstance(compute_resource, str), "compute_resource must be a string"
        assert isinstance(temp_directory, str), "temp_directory must be a string"
        assert isinstance(api_version, str), "api_version must be a string"
        assert isinstance(
            small_file_size_mb, float
        ), "small_file_size_mb must be a float"

        self._machine = compute_resource
        self._url = url
        self._token_uri = token_uri
        self._client_id = client_id
        self._small_file_size_bytes = int(small_file_size_mb * 1024 * 1024)

        self._payoff_override: bool | None = None

        secret = (
            Path(client_secret).read_text().strip()
            if Path(client_secret).exists()
            else client_secret
        )

        try:
            self._client = Firecrest(
                firecrest_url=self._url,
                authorization=ClientCredentialsAuth(client_id, secret, token_uri),
            )
        except Exception as e:
            raise ValueError(f"Could not connect to FirecREST server: {e}") from e

        self._temp_directory = FcPath(temp_directory)

        self._api_version: Version = parse(api_version)

        if self._api_version < parse("1.16.0"):
            self._payoff_override = False

        self.billing_account = billing_account

        # this makes no sense for firecrest, but we need to set this to True
        # otherwise the aiida-core will complain that the transport is not open:
        # aiida-core/src/aiida/orm/utils/remote:clean_remote()
        self._is_open = True

        self.checksum_check = False

    def __str__(self) -> str:
        """Return the name of the plugin."""
        return self.__class__.__name__

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

    def open(self) -> None:
        """Open the transport.
        This is a no-op for the REST-API, as there is no connection to open.
        """
        pass

    def close(self) -> None:
        """Close the transport.
        This is a no-op for the REST-API, as there is no connection to close.
        """
        pass

    def chmod(self, path: TPath_Extended, mode: int) -> None:
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
            self._client.chmod(self._machine, path, str(mode))

    def chown(self, path: TPath_Extended, uid: int, gid: int) -> None:
        raise NotImplementedError

    def _stat(self, path: TPath_Extended) -> os.stat_result:
        """Return stat info for this path.

        If the path is a symbolic link,
        stat will examine the file the link points to.
        """

        path = str(path)
        with convert_header_exceptions():
            stats = self._client.stat(self._machine, path, dereference=True)
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

    def _lstat(self, path: TPath_Extended) -> os.stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """

        path = str(path)
        with convert_header_exceptions():
            stats = self._client.stat(self._machine, path, dereference=False)
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

    def path_exists(self, path: TPath_Extended) -> bool:
        """Check if a path exists on the remote."""

        path = str(path)
        try:
            self._stat(path)
        except FileNotFoundError:
            return False
        return True

    def get_attribute(self, path: TPath_Extended) -> FileAttribute:
        """Get the attributes of a file."""

        path = str(path)
        result = self._stat(path)
        return FileAttribute(  # type: ignore
            {
                "st_size": result.st_size,
                "st_uid": result.st_uid,
                "st_gid": result.st_gid,
                "st_mode": result.st_mode,
                "st_atime": result.st_atime,
                "st_mtime": result.st_mtime,
            }
        )

    def isdir(self, path: TPath_Extended) -> bool:
        """Check if a path is a directory."""

        path = str(path)
        try:
            st_mode = self._stat(path).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISDIR(st_mode)

    def isfile(self, path: TPath_Extended) -> bool:
        """Check if a path is a file."""

        path = str(path)
        try:
            st_mode = self._stat(path).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISREG(st_mode)

    def listdir(  # type: ignore[override]
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
        if not recursive and self.isdir(path) and not path.endswith("/"):
            # This is just to match the behavior of ls
            path += "/"

        with convert_header_exceptions():
            results = self._client.list_files(
                self._machine, path, show_hidden=hidden, recursive=recursive
            )
        # names are relative to path
        names = [result["name"] for result in results]

        if pattern is not None:
            names = fnmatch.filter(names, pattern)
        return names

    # TODO the default implementations of glob / iglob could be overridden

    def makedirs(self, path: TPath_Extended, ignore_existing: bool = False) -> None:
        """Make directories on the remote."""

        path = str(path)
        exists = self.path_exists(path)
        if not ignore_existing and exists:
            # Note: FirecREST does not raise an error if the directory already exists, and parent is True.
            # which makes sense, but following the Superclass, we should raise an OSError in that case.
            # AiiDA expects an OSError, instead of a FileExistsError
            raise OSError(f"'{path}' already exists")

        if ignore_existing and exists:
            return

        # firecrest does not support `exist_ok`, it's somehow blended into `parents`
        # see: https://github.com/eth-cscs/firecrest/issues/202
        self.mkdir(path, ignore_existing=True)

    def mkdir(self, path: TPath_Extended, ignore_existing: bool = False) -> None:
        """Make a directory on the remote."""

        path = str(path)
        try:
            with convert_header_exceptions():
                # Note see: https://github.com/eth-cscs/firecrest/issues/172
                # Also see: https://github.com/eth-cscs/firecrest/issues/202
                # firecrest does not support `exist_ok`, it's somehow blended into `parents`
                self._client.mkdir(self._machine, path, create_parents=ignore_existing)

        except FileExistsError as err:
            if not ignore_existing:
                raise OSError(f"'{path}' already exists") from err
            raise

    def normalize(self, path: TPath_Extended) -> str:  # type: ignore[override]
        """Normalize a path on the remote."""

        # TODO: this might be buggy
        path = str(path)
        return posixpath.normpath(path)

    def symlink(
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
            self._client.symlink(self._machine, source_path, link_path)

    def copyfile(
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

        if not self.path_exists(source):
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not self.isfile(source):
            raise ValueError(f"Source is not a file: {source}")
        if not self.path_exists(destination) and not self.isfile(source):
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        self._copy_to(source, destination, dereference)
        # I removed symlink copy, becasue it's really not a file copy, it's a link copy
        # and aiida-ssh have it in buggy manner, prrobably it's not used anyways

    def _copy_to(
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
            self._client.cp(
                system_name=self._machine,
                source_path=source,
                target_path=target,
                dereference=dereference,
                account=self.billing_account,
                blocking=True,
            )

    def copytree(
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

        if not self.path_exists(source):
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not self.isdir(source):
            raise ValueError(f"Source is not a directory: {source}")
        if not self.path_exists(destination):
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        self._copy_to(source, destination, dereference)

    def copy(
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
            for item in self.iglob(remotesource):  # type: ignore
                # item is of str type, so we need to split it to get the file name
                filename = item.split("/")[-1] if self.isfile(item) else ""
                self.copy(
                    item,
                    remotedestination + filename,
                    dereference=dereference,
                    recursive=recursive,
                )
            return

        if not self.path_exists(remotesource):
            raise FileNotFoundError(f"Source does not exist: {remotesource}")
        if not self.path_exists(remotedestination) and not self.isfile(remotesource):
            raise FileNotFoundError(f"Destination does not exist: {remotedestination}")

        self._copy_to(remotesource, remotedestination, dereference)

    # TODO do get/put methods need to handle glob patterns?
    # Apparently not, but I'm not clear how glob() iglob() are going to behave here. We may need to implement them.

    def getfile(
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

        if not self.isfile(remotepath):
            raise FileNotFoundError(f"Source file does not exist: {remotepath}")
        # if not local.exists():
        #     local.mkdir(parents=True)
        with convert_header_exceptions():
            self._client.download(
                self._machine,
                str(remotepath),
                str(local),
                account=self.billing_account,
                blocking=True,
            )

        if self.checksum_check:
            self._validate_checksum(local, remotepath)

    def _validate_checksum(
        self, localpath: TPath_Extended, remotepath: TPath_Extended
    ) -> None:
        """Validate the checksum of a file.
        Useful for checking if a file was transferred correctly.
        it uses sha256 hash to compare the checksum of the local and remote files.

        Raises: ValueError: If the checksums do not match.
        """

        local = Path(localpath)
        remotepath = str(remotepath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")

        if not self.isfile(remotepath):
            raise FileNotFoundError(
                f"Cannot calculate checksum for a directory: {remotepath}"
            )

        sha256_hash = hashlib.sha256()
        with open(local, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        local_hash = sha256_hash.hexdigest()

        # https://github.com/eth-cscs/pyfirecrest/issues/159
        # checksum's return type has changed in v2 from str to dict. This might be a mistake
        remote_hash = str(self._client.checksum(self._machine, remotepath))

        try:
            assert local_hash == remote_hash
        except AssertionError as e:
            raise ValueError(
                f"Checksum mismatch between local and remote files: {local} and {remotepath}"
            ) from e

    def _gettreetar(
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
        self._client.compress(
            self._machine, remotepath, str(remote_path_temp), dereference=dereference
        )
        # Download
        localpath_temp = Path(localpath).joinpath(f"temp_{_}.gzip")
        try:
            self.getfile(remote_path_temp, localpath_temp)
        finally:
            self.remove(remote_path_temp)

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

    def gettree(
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

        if not self.isdir(remotepath):
            raise OSError(f"Source is not a directory: {remotepath}")

        # this block is added only to mimick the behavior that aiida expects
        if local.exists():
            # Destination directory already exists, create remote directory name inside it
            local = local.joinpath(remotepath.name)
            local.mkdir(parents=True, exist_ok=True)
        else:
            # Destination directory does not exist, create and move content abc inside it
            local.mkdir(parents=True, exist_ok=False)

        if self.payoff(remotepath):
            # in this case send a request to the server to tar the files and then download the tar file
            # unfortunately, the server does not provide a deferenced tar option, yet.
            self._gettreetar(remotepath, local, dereference=dereference)
        else:
            # otherwise download the files one by one
            for remote_item in self.listdir(remotepath, recursive=True):
                # remote_item is a relative path,
                remote_item_abs = remotepath.joinpath(remote_item).resolve()
                local_item = local.joinpath(remote_item)
                if dereference and self.is_symlink(remote_item_abs):
                    target_path = self._get_target(remote_item_abs)
                    if not Path(target_path).is_absolute():
                        target_path = remote_item_abs.parent.joinpath(
                            target_path
                        ).resolve()

                    if self.isdir(target_path):
                        self.gettree(target_path, local_item, dereference=True)
                else:
                    target_path = remote_item_abs

                if not self.isdir(target_path):
                    self.getfile(target_path, local_item)
                else:
                    local_item.mkdir(parents=True, exist_ok=True)

    def _get_target(self, path: TPath_Extended) -> FcPath:
        """gets the target of a symlink.
        Note: path must be a symlink, we don't check that, here."""

        path = str(path)
        # results = self._client.list_files(self._machine, path,
        #                                   show_hidden=True,
        #                                   recursive=False,
        #                                   dereference=False)
        # dereference=False is not supported in firecrest v1,
        # so we have to do a workaround
        results = self._client.list_files(
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

    def is_symlink(self, path: TPath_Extended) -> bool:
        """Whether path is a symbolic link."""

        path = str(path)
        try:
            st_mode = self._lstat(path).st_mode
        except FileNotFoundError:
            return False
        return stat.S_ISLNK(st_mode)

    def get(
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

        if self.isdir(remotepath):
            self.gettree(remotepath, localpath)
        elif self.isfile(remotepath):
            self.getfile(remotepath, localpath)
        elif has_magic(str(remotepath)):
            for item in self.iglob(str(remotepath)):  # type: ignore
                # item is of str type, so we need to split it to get the file name
                filename = item.split("/")[-1] if self.isfile(item) else ""
                self.get(
                    item,
                    localpath + filename,
                    dereference=dereference,
                    ignore_nonexisting=ignore_nonexisting,
                )
            return
        elif not ignore_nonexisting:
            raise FileNotFoundError(f"Source file does not exist: {remotepath}")

    def putfile(
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

        if self.isdir(remotepath):
            raise ValueError(f"Destination is a directory: {remotepath}")

        # note this allows overwriting of existing files
        with convert_header_exceptions():
            self._client.upload(
                self._machine,
                str(localpath),
                str(remotepath.parent),
                str(remotepath.name),
                account=self.billing_account,
                blocking=True,
            )

        if self.checksum_check:
            self._validate_checksum(localpath, str(remotepath))

    def payoff(self, path: TPath_Extended) -> bool:
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

        return len(self.listdir(path, recursive=True)) > 3

    def _puttreetar(
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
            self.putfile(tarpath, remote_path_temp)
        finally:
            tarpath.unlink()

        # Attempt extract
        try:
            self._client.extract(self._machine, str(remote_path_temp), remotepath)
        finally:
            self.remove(remote_path_temp)

    def puttree(
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
        if self.path_exists(remote):
            # Destination directory already exists, create local directory name inside it
            remote = remote.joinpath(localpath.name)
            self.mkdir(remote, ignore_existing=False)
        else:
            # Destination directory does not exist, create and move content abc inside it
            self.mkdir(remote, ignore_existing=False)

        if self.payoff(localpath):
            # in this case send send everything as a tar file
            self._puttreetar(localpath, remote)
        else:
            # otherwise send the files one by one
            for dirpath, _, filenames in os.walk(localpath, followlinks=dereference):
                rel_folder = os.path.relpath(path=dirpath, start=localpath)

                rm_parent_now = remote.joinpath(rel_folder)
                self.mkdir(rm_parent_now, ignore_existing=True)

                for filename in filenames:
                    localfile_path = os.path.join(localpath, rel_folder, filename)
                    remotefile_path = rm_parent_now.joinpath(filename)
                    self.putfile(localfile_path, remotefile_path)

    def put(
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
            for item in self.iglob(str(localpath)):  # type: ignore
                # item is of str type, so we need to split it to get the file name
                filename = item.split("/")[-1] if self.isfile(item) else ""
                self.put(
                    item,
                    remotepath + filename,
                    dereference=dereference,
                    ignore_nonexisting=ignore_nonexisting,
                )
            return

        if not Path(local).exists() and not ignore_nonexisting:
            raise FileNotFoundError(f"Source file does not exist: {localpath}")

        if local.is_dir():
            self.puttree(localpath, remotepath)
        elif local.is_file():
            self.putfile(localpath, remotepath)

    def remove(self, path: TPath_Extended) -> None:
        """Remove a file or directory on the remote."""
        # note firecrest uses `rm -f`,

        path = str(path)

        if self.isfile(path):
            with convert_header_exceptions():
                self._client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        elif not self.path_exists(path):
            # if the path does not exist, we do not raise an error
            return
        else:
            raise OSError(f"Path is not a file: {path}")

    def rename(self, oldpath: TPath_Extended, newpath: TPath_Extended) -> None:
        """Rename a file or directory on the remote."""

        oldpath = str(oldpath)
        newpath = str(newpath)

        with convert_header_exceptions():
            self._client.mv(
                self._machine,
                oldpath,
                newpath,
                account=self.billing_account,
                blocking=True,
            )

    def rmdir(self, path: TPath_Extended) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, an OSError is raised."""

        path = str(path)

        if len(self.listdir(path)) == 0:
            with convert_header_exceptions():
                self._client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        else:
            raise OSError(f"Directory not empty: {path}")

    def rmtree(self, path: TPath_Extended) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, it will be removed recursively, equivalent to `rm -rf`.
        It does not raise an error if the directory does not exist.
        """
        # note firecrest uses `rm -rf`,

        path = str(path)

        if self.isdir(path):
            with convert_header_exceptions():
                self._client.rm(
                    self._machine, path, account=self.billing_account, blocking=True
                )
        elif not self.path_exists(path):
            # if the path does not exist, we do not raise an error
            return
        else:
            raise OSError(f"Path is not a directory: {path}")

    def whoami(self) -> str:
        """Return the username of the current user.
        return None if the username cannot be determined.
        """
        # whoami is not supported in v2:
        # https://github.com/eth-cscs/pyfirecrest/issues/160
        # return self._client.whoami(machine=self._machine)
        return str(self._client.userinfo(system_name=self._machine)["user"]["name"])

    def gotocomputer_command(self, remotedir: TPath_Extended) -> str:
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support gotocomputer_command")

    def _exec_command_internal(self, command: str, **kwargs: Any) -> Any:  # type: ignore[override]
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support command execution")

    def exec_command_wait_bytes(  # type: ignore[no-untyped-def]
        self, command: str, stdin=None, workdir: TPath_Extended | None = None, **kwargs
    ):
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support command execution")
