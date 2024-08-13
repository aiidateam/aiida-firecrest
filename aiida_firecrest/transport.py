"""Transport interface."""

from __future__ import annotations

from contextlib import suppress
import fnmatch
import hashlib
import os
from pathlib import Path
import posixpath
import tarfile
from typing import Any, Callable, ClassVar, TypedDict
import uuid

from aiida.cmdline.params.options.interactive import InteractiveOption
from aiida.cmdline.params.options.overridable import OverridableOption
from aiida.transports import Transport
from aiida.transports.util import FileAttribute
from click.core import Context
from click.types import ParamType
from firecrest import ClientCredentialsAuth, Firecrest  # type: ignore[attr-defined]
from firecrest.path import FcPath
from packaging.version import Version, parse


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
    )

    # Temp directory routine
    if transport._cwd.joinpath(
        transport._temp_directory
    ).is_file():  # self._temp_directory.is_file():
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
    """Find the version of the FirecREST server."""
    # note: right now, unfortunately, the version is not exposed in the API.
    # See issue https://github.com/eth-cscs/firecrest/issues/204
    # so here we just develope a workaround to get the version from the server
    # basically we check if extract/compress endpoint is available

    import click

    if value != "0":
        if parse(value) < parse("1.15.0"):
            raise click.BadParameter(f"FirecREST api version {value} is not supported")
        return value

    firecrest_url = ctx.params["url"]
    token_uri = ctx.params["token_uri"]
    client_id = ctx.params["client_id"]
    compute_resource = ctx.params["compute_resource"]
    secret = ctx.params["client_secret"]
    temp_directory = ctx.params["temp_directory"]

    transport = FirecrestTransport(
        url=firecrest_url,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=secret,
        compute_resource=compute_resource,
        temp_directory=temp_directory,
        small_file_size_mb=0.0,
        api_version="100.0.0",  # version is irrelevant here
    )
    try:
        transport.listdir(transport._cwd.joinpath(temp_directory), recursive=True)
        _version = "1.16.0"
    except Exception:
        # all sort of exceptions can be raised here, but we don't care. Since this is just a workaround
        _version = "1.15.0"

    click.echo(
        click.style("Fireport: ", bold=True, fg="magenta")
        + f"Deployed version of FirecREST api: v{_version}"
    )
    return _version


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
    import click

    if value > 0:
        return value

    firecrest_url = ctx.params["url"]
    token_uri = ctx.params["token_uri"]
    client_id = ctx.params["client_id"]
    compute_resource = ctx.params["compute_resource"]
    secret = ctx.params["client_secret"]
    temp_directory = ctx.params["temp_directory"]

    transport = FirecrestTransport(
        url=firecrest_url,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=secret,
        compute_resource=compute_resource,
        temp_directory=temp_directory,
        small_file_size_mb=0.0,
        api_version="100.0.0",  # version is irrelevant here
    )

    parameters = transport._client.parameters()
    utilities_max_file_size = next(
        (
            item
            for item in parameters["utilities"]
            if item["name"] == "UTILITIES_MAX_FILE_SIZE"
        ),
        None,
    )
    small_file_size_mb = (
        float(utilities_max_file_size["value"])
        if utilities_max_file_size is not None
        else 5.0
    )
    click.echo(
        click.style("Fireport: ", bold=True, fg="magenta")
        + f"Maximum file size for direct transfer: {small_file_size_mb} MB"
    )

    return small_file_size_mb


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
                "default": "0",
                "non_interactive_default": True,
                "prompt": "FirecREST api version [Enter 0 to get this info from server]",
                "help": "The version of the FirecREST api deployed on the server",
                "callback": _dynamic_info_firecrest_version,
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

        secret = Path(client_secret).read_text().strip()
        try:
            self._client = Firecrest(
                firecrest_url=self._url,
                authorization=ClientCredentialsAuth(client_id, secret, token_uri),
            )
        except Exception as e:
            raise ValueError(f"Could not connect to FirecREST server: {e}") from e

        self._cwd: FcPath = FcPath(self._client, self._machine, "/", cache_enabled=True)
        self._temp_directory = self._cwd.joinpath(temp_directory)

        self._api_version: Version = parse(api_version)

        if self._api_version < parse("1.16.0"):
            self._payoff_override = False

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

    def getcwd(self) -> str:
        """Return the current working directory."""
        return str(self._cwd)

    def _get_path(self, *path: str) -> str:
        """Return the path as a string."""
        return posixpath.normpath(self._cwd.joinpath(*path))  # type: ignore

    def chdir(self, path: str) -> None:
        """Change the current working directory."""
        # with open('/home/khosra_a/check_me', 'a') as f:
        #     f.write(f"chdir: {path}, {type(path)}\n")
        new_path = self._cwd.joinpath(path)
        if not new_path.is_dir():
            raise OSError(f"'{new_path}' is not a valid directory")
        self._cwd = new_path

    def chmod(self, path: str, mode: str) -> None:
        """Change the mode of a file."""
        self._cwd.joinpath(path).chmod(mode)

    def chown(self, path: str, uid: str, gid: str) -> None:
        """Change the owner of a file."""
        self._cwd.joinpath(path).chown(uid, gid)

    def path_exists(self, path: str) -> bool:
        """Check if a path exists on the remote."""
        return self._cwd.joinpath(path).exists()  # type: ignore

    def get_attribute(self, path: str) -> FileAttribute:
        """Get the attributes of a file."""
        result = self._cwd.joinpath(path).stat()
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

    def isdir(self, path: str) -> bool:
        """Check if a path is a directory."""
        return self._cwd.joinpath(path).is_dir()  # type: ignore

    def isfile(self, path: str) -> bool:
        """Check if a path is a file."""
        return self._cwd.joinpath(path).is_file()  # type: ignore

    def listdir(
        self, path: str = ".", pattern: str | None = None, recursive: bool = False
    ) -> list[str]:
        """List the contents of a directory.

        :param path: this could be relative or absolute path.
            Note igolb() will usually call this with relative path.

        :param pattern: Unix shell-style wildcards to match the pattern:
            - `*` matches everything
            - `?` matches any single character
            - `[seq]` matches any character in seq
            - `[!seq]` matches any character not in seq
        :param recursive: If True, list directories recursively
        """
        path_abs = self._cwd.joinpath(path)
        names = [p.relpath(path_abs) for p in path_abs.iterdir(recursive=recursive)]
        if pattern is not None:
            names = fnmatch.filter(names, pattern)
        return names

    # TODO the default implementations of glob / iglob could be overridden
    # to be more performant, using cached FcPaths and https://github.com/chrisjsewell/virtual-glob

    def makedirs(self, path: str, ignore_existing: bool = False) -> None:
        """Make directories on the remote."""
        new_path = self._cwd.joinpath(path)
        if not ignore_existing and new_path.exists():
            # Note: FirecREST does not raise an error if the directory already exists, and parent is True.
            # which makes sense, but following the Superclass, we should raise an OSError in that case.
            # AiiDA expects an OSError, instead of a FileExistsError
            raise OSError(f"'{path}' already exists")
        self._cwd.joinpath(path).mkdir(parents=True, exist_ok=ignore_existing)

    def mkdir(self, path: str, ignore_existing: bool = False) -> None:
        """Make a directory on the remote."""
        try:
            self._cwd.joinpath(path).mkdir(exist_ok=ignore_existing)
        except FileExistsError as err:
            raise OSError(f"'{path}' already exists") from err

    def normalize(self, path: str = ".") -> str:
        """Resolve the path."""
        return posixpath.normpath(path)

    def write_binary(self, path: str, data: bytes) -> None:
        """Write bytes to a file on the remote."""
        # Note this is not part of the Transport interface, but is useful for testing
        # TODO will fail for files exceeding small_file_size_mb
        self._cwd.joinpath(path).write_bytes(data)

    def read_binary(self, path: str) -> bytes:
        """Read bytes from a file on the remote."""
        # Note this is not part of the Transport interface, but is useful for testing
        # TODO will fail for files exceeding small_file_size_mb
        return self._cwd.joinpath(path).read_bytes()  # type: ignore

    def symlink(self, remotesource: str, remotedestination: str) -> None:
        """Create a symlink on the remote."""
        source = self._cwd.joinpath(remotesource)
        destination = self._cwd.joinpath(remotedestination)
        destination.symlink_to(source)

    def copyfile(
        self, remotesource: str, remotedestination: str, dereference: bool = False
    ) -> None:
        """Copy a file on the remote. FirecREST does not support symlink copying.

        :param dereference: If True, copy the target of the symlink instead of the symlink itself.
        """
        source = self._cwd.joinpath(
            remotesource
        )  # .enable_cache() it's removed from from path.py to be investigated
        destination = self._cwd.joinpath(
            remotedestination
        )  # .enable_cache() it's removed from from path.py to be investigated
        if dereference:
            raise NotImplementedError("copyfile() does not support symlink dereference")
        if not source.exists():
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not source.is_file():
            raise ValueError(f"Source is not a file: {source}")
        if not destination.exists() and not source.is_file():
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        self._copy_to(source, destination)
        # I removed symlink copy, becasue it's really not a file copy, it's a link copy
        # and aiida-ssh have it in buggy manner, prrobably it's not used anyways

    def _copy_to(self, source: FcPath, target: FcPath) -> None:
        """Copy source path to the target path. Both paths must be on remote.

        Works for both files and directories (in which case the whole tree is copied).
        """
        with self._cwd.convert_header_exceptions():
            # Note although this endpoint states that it is only for directories,
            # it actually uses `cp -r`:
            # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L320
            self._client.copy(self._machine, str(source), str(target))

    def copytree(
        self, remotesource: str, remotedestination: str, dereference: bool = False
    ) -> None:
        """Copy a directory on the remote. FirecREST does not support symlink copying.

        :param dereference: If True, copy the target of the symlink instead of the symlink itself.
        """
        # TODO: check if deference is set to False, symlinks will be functional after the copy in Firecrest server.

        source = self._cwd.joinpath(
            remotesource
        )  # .enable_cache().enable_cache() it's removed from from path.py to be investigated
        destination = self._cwd.joinpath(
            remotedestination
        )  # .enable_cache().enable_cache() it's removed from from path.py to be investigated
        if dereference:
            raise NotImplementedError(
                "Dereferencing not implemented in FirecREST server"
            )
        if not source.exists():
            raise FileNotFoundError(f"Source file does not exist: {source}")
        if not source.is_dir():
            raise ValueError(f"Source is not a directory: {source}")
        if not destination.exists():
            raise FileNotFoundError(f"Destination file does not exist: {destination}")

        self._copy_to(source, destination)

    def copy(
        self,
        remotesource: str,
        remotedestination: str,
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

        if not recursive:
            # TODO this appears to not actually be used upstream, so just remove there
            raise NotImplementedError("Non-recursive copy not implemented")
        if dereference:
            raise NotImplementedError(
                "Dereferencing not implemented in FirecREST server"
            )

        if self.has_magic(str(remotesource)):  # type: ignore
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

        source = self._cwd.joinpath(
            remotesource
        )  # .enable_cache() it's removed from from path.py to be investigated
        destination = self._cwd.joinpath(
            remotedestination
        )  # .enable_cache() it's removed from from path.py to be investigated

        if not source.exists():
            raise FileNotFoundError(f"Source does not exist: {source}")
        if not destination.exists() and not source.is_file():
            raise FileNotFoundError(f"Destination does not exist: {destination}")

        self._copy_to(source, destination)

    # TODO do get/put methods need to handle glob patterns?
    # Apparently not, but I'm not clear how glob() iglob() are going to behave here. We may need to implement them.

    def getfile(
        self,
        remotepath: str | FcPath,
        localpath: str | Path,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a file from the remote.

        :param dereference: If True, follow symlinks.
            note: we don't support downloading symlinks, so dereference should always be True

        """
        if not dereference:
            raise NotImplementedError(
                "Getting symlinks with `dereference=False` is not supported"
            )

        local = Path(localpath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        remote = (
            remotepath
            if isinstance(remotepath, FcPath)
            else self._cwd.joinpath(
                remotepath
            )  # .enable_cache() it's removed from from path.py to be investigated
        )
        if not remote.is_file():
            raise FileNotFoundError(f"Source file does not exist: {remote}")
        remote_size = remote.lstat().st_size
        # if not local.exists():
        #     local.mkdir(parents=True)
        with self._cwd.convert_header_exceptions():
            if remote_size < self._small_file_size_bytes:
                self._client.simple_download(self._machine, str(remote), localpath)
            else:
                # TODO the following is a very basic implementation of downloading a large file
                # ideally though, if downloading multiple large files (i.e. in gettree),
                # we would want to probably use asyncio,
                # to concurrently initiate internal file transfers to the object store (a.k.a. "staging area")
                # and downloading from the object store to the local machine

                # I investigated asyncio, but it's not performant for this use case.
                # Becasue in the end, FirecREST server ends up serializing the requests.
                # see here: https://github.com/eth-cscs/pyfirecrest/issues/94
                down_obj = self._client.external_download(self._machine, str(remote))
                down_obj.finish_download(local)

        if self.checksum_check:
            self._validate_checksum(local, remote)

    def _validate_checksum(
        self, localpath: str | Path, remotepath: str | FcPath
    ) -> None:
        """Validate the checksum of a file.
        Useful for checking if a file was transferred correctly.
        it uses sha256 hash to compare the checksum of the local and remote files.

        Raises: ValueError: If the checksums do not match.
        """

        local = Path(localpath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        remote = (
            remotepath
            if isinstance(remotepath, FcPath)
            else self._cwd.joinpath(
                remotepath
            )  # .enable_cache() it's removed from from path.py to be investigated
        )
        if not remote.is_file():
            raise FileNotFoundError(
                f"Cannot calculate checksum for a directory: {remote}"
            )

        sha256_hash = hashlib.sha256()
        with open(local, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        local_hash = sha256_hash.hexdigest()

        remote_hash = self._client.checksum(self._machine, remote)

        try:
            assert local_hash == remote_hash
        except AssertionError as e:
            raise ValueError(
                f"Checksum mismatch between local and remote files: {local} and {remote}"
            ) from e

    def _gettreetar(
        self,
        remotepath: str | FcPath,
        localpath: str | Path,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a directory from the remote as a tar file and extract it locally.
        This is useful for downloading a directory with many files,
        as it might be more efficient than downloading each file individually.
        Note that this method is not part of the Transport interface, and is not meant to be used publicly.

        :param dereference: If True, follow symlinks.
        """

        _ = uuid.uuid4()
        remote_path_temp = self._temp_directory.joinpath(f"temp_{_}.tar")

        # Compress
        self._client.compress(
            self._machine, str(remotepath), remote_path_temp, dereference=dereference
        )

        # Download
        localpath_temp = Path(localpath).joinpath(f"temp_{_}.tar")
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
        remotepath: str | FcPath,
        localpath: str | Path,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Get a directory from the remote.

        :param dereference: If True, follow symlinks.
            note: dereference should be always True, otherwise the symlinks will not be functional.
        """

        local = Path(localpath)
        if not local.is_absolute():
            raise ValueError("Destination must be an absolute path")
        if local.is_file():
            raise OSError("Cannot copy a directory into a file")

        remote = (
            remotepath
            if isinstance(remotepath, FcPath)
            else self._cwd.joinpath(
                remotepath
            )  # .enable_cache() it's removed from from path.py to be investigated
        )
        local = Path(localpath)

        if not remote.is_dir():
            raise OSError(f"Source is not a directory: {remote}")

        # this block is added only to mimick the behavior that aiida expects
        if local.exists():
            # Destination directory already exists, create remote directory name inside it
            local = local.joinpath(remote.name)
            local.mkdir(parents=True, exist_ok=True)
        else:
            # Destination directory does not exist, create and move content abc inside it
            local.mkdir(parents=True, exist_ok=False)

        if self.payoff(remote):
            # in this case send a request to the server to tar the files and then download the tar file
            # unfortunately, the server does not provide a deferenced tar option, yet.
            self._gettreetar(remote, local, dereference=dereference)
        else:
            # otherwise download the files one by one
            for remote_item in remote.iterdir(recursive=True):
                local_item = local.joinpath(remote_item.relpath(remote))
                if dereference and remote_item.is_symlink():
                    target_path = remote_item._cache.link_target
                    if not Path(target_path).is_absolute():
                        target_path = remote_item.parent.joinpath(target_path).resolve()

                    target_path = self._cwd.joinpath(target_path)
                    if target_path.is_dir():
                        self.gettree(target_path, local_item, dereference=True)
                else:
                    target_path = remote_item

                if not target_path.is_dir():
                    self.getfile(target_path, local_item)
                else:
                    local_item.mkdir(parents=True, exist_ok=True)

    def get(
        self,
        remotepath: str,
        localpath: str,
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
        remote = self._cwd.joinpath(
            remotepath
        )  # .enable_cache() it's removed from from path.py to be investigated

        if remote.is_dir():
            self.gettree(remote, localpath)
        elif remote.is_file():
            self.getfile(remote, localpath)
        elif self.has_magic(str(remotepath)):  # type: ignore
            for item in self.iglob(remotepath):  # type: ignore
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
            raise FileNotFoundError(f"Source file does not exist: {remote}")

    def putfile(
        self,
        localpath: str | Path,
        remotepath: str | FcPath,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a file from the remote.

        :param dereference: If True, follow symlinks.
            note: we don't support uploading symlinks, so dereference is always should be True

        """

        if not dereference:
            raise NotImplementedError(
                "Getting symlinks with `dereference=False` is not supported"
            )

        localpath = Path(localpath)
        if not localpath.is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not localpath.is_file():
            if not localpath.exists():
                raise FileNotFoundError(f"Local file does not exist: {localpath}")
            raise ValueError(f"Input localpath is not a file {localpath}")
        remote = self._cwd.joinpath(
            str(remotepath)
        )  # .enable_cache() it's removed from from path.py to be investigated

        if remote.is_dir():
            raise ValueError(f"Destination is a directory: {remote}")

        local_size = localpath.stat().st_size
        # note this allows overwriting of existing files
        with self._cwd.convert_header_exceptions():
            if local_size < self._small_file_size_bytes:
                self._client.simple_upload(
                    self._machine, str(localpath), str(remote.parent), remote.name
                )
            else:
                # TODO the following is a very basic implementation of uploading a large file
                # ideally though, if uploading multiple large files (i.e. in puttree),
                # we would want to probably use asyncio,
                # to concurrently upload to the object store (a.k.a. "staging area"),
                # then wait for all files to finish being transferred to the target location

                # I investigated asyncio, but it's not performant for this use case.
                # Becasue in the end, FirecREST server ends up serializing the requests.
                # see here: https://github.com/eth-cscs/pyfirecrest/issues/94
                up_obj = self._client.external_upload(
                    self._machine, str(localpath), str(remote)
                )
                up_obj.finish_upload()

        if self.checksum_check:
            self._validate_checksum(localpath, str(remote))

    def payoff(self, path: str | FcPath | Path) -> bool:
        """
        This function will be used to determine whether to tar the files before downloading
        """
        # After discussing with the pyfirecrest team, it seems that server has some sort
        # of serialization and "penalty" for sending multiple requests asycnhronusly or in a short time window.
        # It responses in 1, 1.5, 3, 5, 7 seconds!
        # So right now, I think if the number of files is more than 3, it pays off to tar everything

        # If payoff_override is set, return its value
        if self.payoff_override is not None:
            return bool(self.payoff_override)

        return (
            isinstance(path, FcPath)
            and len(self.listdir(str(path), recursive=True)) > 3
        ) or (isinstance(path, Path) and len(os.listdir(path)) > 3)

    def _puttreetar(
        self,
        localpath: str | Path,
        remotepath: str | FcPath,
        dereference: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Put a directory to the remote by sending as tar file in backend.
        This is useful for uploading a directory with many files,
        as it might be more efficient than uploading each file individually.
        Note that this method is not part of the Transport interface, and is not meant to be used publicly.

        :param dereference: If True, follow symlinks. If False, symlinks are ignored from sending over.
        """
        # this function will be used to send a folder as a tar file to the server and extract it on the server

        _ = uuid.uuid4()

        localpath = Path(localpath)
        tarpath = localpath.parent.joinpath(f"temp_{_}.tar")
        remote_path_temp = self._temp_directory.joinpath(f"temp_{_}.tar")
        with tarfile.open(tarpath, "w", dereference=dereference) as tar:
            for root, _, files in os.walk(localpath, followlinks=dereference):
                for file in files:
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, localpath)
                    tar.add(full_path, arcname=relative_path)

        # Upload
        try:
            self.putfile(tarpath, remote_path_temp)
        finally:
            tarpath.unlink()

        # Attempt extract
        try:
            self._client.extract(self._machine, remote_path_temp, str(remotepath))
        finally:
            self.remove(remote_path_temp)

    def puttree(
        self,
        localpath: str | Path,
        remotepath: str,
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
        remote = self._cwd.joinpath(remotepath)

        if not localpath.is_absolute():
            raise ValueError("The localpath must be an absolute path")
        if not localpath.exists():
            raise OSError("The localpath does not exists")
        if not localpath.is_dir():
            raise ValueError(f"Input localpath is not a directory: {localpath}")

        # this block is added only to mimick the behavior that aiida expects
        if remote.exists():
            # Destination directory already exists, create local directory name inside it
            remote = self._cwd.joinpath(remote, localpath.name)
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
        localpath: str,
        remotepath: str,
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
        if not local.is_absolute():
            raise ValueError("The localpath must be an absolute path")

        if self.has_magic(str(localpath)):  # type: ignore
            for item in self.iglob(localpath):  # type: ignore
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

    def remove(self, path: str | FcPath) -> None:
        """Remove a file or directory on the remote."""
        self._cwd.joinpath(str(path)).unlink()

    def rename(self, oldpath: str, newpath: str) -> None:
        """Rename a file or directory on the remote."""
        self._cwd.joinpath(oldpath).rename(self._cwd.joinpath(newpath))

    def rmdir(self, path: str) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, an OSError is raised."""

        if len(self.listdir(path)) == 0:
            self._cwd.joinpath(path).rmtree()
        else:
            raise OSError(f"Directory not empty: {path}")

    def rmtree(self, path: str) -> None:
        """Remove a directory on the remote.
        If the directory is not empty, it will be removed recursively, equivalent to `rm -rf`.
        It does not raise an error if the directory does not exist.
        """
        # TODO: suppress is to mimick the behaviour of `aiida-ssh`` transport, TODO: raise an issue on aiida
        with suppress(FileNotFoundError):
            self._cwd.joinpath(path).rmtree()

    def whoami(self) -> str | None:
        """Return the username of the current user.
        return None if the username cannot be determined.
        """
        return self._client.whoami(machine=self._machine)

    def gotocomputer_command(self, remotedir: str) -> str:
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support gotocomputer_command")

    def _exec_command_internal(self, command: str, **kwargs: Any) -> Any:
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support command execution")

    def exec_command_wait_bytes(
        self, command: str, stdin: Any = None, **kwargs: Any
    ) -> Any:
        """Not possible for REST-API.
        It's here only because it's an abstract method in the base class."""
        # TODO remove from interface
        raise NotImplementedError("firecrest does not support command execution")
