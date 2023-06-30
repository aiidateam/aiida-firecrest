"""A pathlib.Path-like object for accessing the file system via the Firecrest API.

Note this is independent of AiiDA,
in fact it is awaiting possible inclusion in pyfirecrest:
https://github.com/eth-cscs/pyfirecrest/pull/43
"""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from functools import lru_cache
from io import BytesIO
import os
from pathlib import PurePosixPath
import stat
import tempfile
from typing import TypeVar

from firecrest import ClientCredentialsAuth, Firecrest

from .utils import convert_header_exceptions

# Note in python 3.11 could use typing.Self
SelfTv = TypeVar("SelfTv", bound="FcPath")


@dataclass
class ModeCache:
    """A cache of the path's mode.

    This can be useful for limiting calls to the API,
    particularly for paths generated via `/utilities/ls`
    which already provides the st_mode for each file
    """

    st_mode: int | None = None
    """The st_mode of the path, dereferencing symlinks."""
    lst_mode: int | None = None
    """The st_mode of the path, without dereferencing symlinks."""

    def reset(self) -> None:
        """Reset the cache."""
        self.st_mode = None
        self.lst_mode = None


class FcPath(os.PathLike):
    """A pathlib.Path-like object for accessing the file system via the Firecrest API."""

    __slots__ = ("_client", "_machine", "_path", "_cache_enabled", "_cache")

    def __init__(
        self,
        client: Firecrest,
        machine: str,
        path: str | PurePosixPath,
        *,
        cache_enabled: bool = False,
        _cache: None | ModeCache = None,
    ) -> None:
        """Construct a new FcPath instance.

        :param client: A Firecrest client object
        :param machine: The machine name
        :param path: The absolute path to the file or directory
        :param cache_enabled: Enable caching of path statistics
            This enables caching of path statistics, like mode,
            which can be useful if you are using multiple methods on the same path,
            as it avoids making multiple calls to the API.
            You should only use this if you are sure that the file system is not being modified.

        """
        self._client = client
        self._machine = machine
        self._path = PurePosixPath(path)
        if not self._path.is_absolute():
            raise ValueError(f"Path must be absolute: {str(self._path)!r}")
        self._cache_enabled = cache_enabled
        self._cache = _cache or ModeCache()

    @classmethod
    def from_env_variables(
        cls, machine: str, path: str | PurePosixPath, *, cache_enabled: bool = False
    ):
        """Convenience method, to construct a new FcPath using environmental variables.

        The following environment variables are required:
        - FIRECREST_URL
        - FIRECREST_CLIENT_ID
        - FIRECREST_CLIENT_SECRET
        - AUTH_TOKEN_URL
        """
        auth_obj = ClientCredentialsAuth(
            os.environ["FIRECREST_CLIENT_ID"],
            os.environ["FIRECREST_CLIENT_SECRET"],
            os.environ["AUTH_TOKEN_URL"],
        )
        client = Firecrest(os.environ["FIRECREST_URL"], authorization=auth_obj)
        return cls(client, machine, path, cache_enabled=cache_enabled)

    @property
    def client(self) -> Firecrest:
        """The Firecrest client object."""
        return self._client

    @property
    def machine(self) -> str:
        """The machine name."""
        return self._machine

    @property
    def path(self) -> str:
        """Return the string representation of the path on the machine."""
        return str(self._path)

    @property
    def pure_path(self) -> PurePosixPath:
        """Return the pathlib representation of the path on the machine."""
        return self._path

    @property
    def cache_enabled(self) -> bool:
        """Enable caching of path statistics.

        This enables caching of path statistics, like mode,
        which can be useful if you are using multiple methods on the same path,
        as it avoids making multiple calls to the API.

        You should only use this if you are sure that the file system is not being modified.
        """
        return self._cache_enabled

    @cache_enabled.setter
    def cache_enabled(self, value: bool) -> None:
        self._cache_enabled = value

    def enable_cache(self: SelfTv) -> SelfTv:
        """Enable caching of path statistics."""
        self._cache_enabled = True
        return self

    def clear_cache(self: SelfTv) -> SelfTv:
        """Clear the cache of path statistics."""
        self._cache.reset()
        return self

    def _new_path(
        self: SelfTv, path: PurePosixPath, *, _cache: None | ModeCache = None
    ) -> SelfTv:
        """Construct a new FcPath object from a PurePosixPath object."""
        return self.__class__(
            self._client,
            self._machine,
            path,
            cache_enabled=self._cache_enabled,
            _cache=_cache,
        )

    def __fspath__(self) -> str:
        return str(self._path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        variables = [
            repr(self._client._firecrest_url),
            repr(self._machine),
            repr(self.path),
        ]
        if self._cache_enabled:
            variables.append("CACHED")
        return f"{self.__class__.__name__}({', '.join(variables)})"

    def as_posix(self) -> str:
        """Return the string representation of the path."""
        return self._path.as_posix()

    @property
    def name(self) -> str:
        """The final path component, if any."""
        return self._path.name

    @property
    def suffix(self) -> str:
        """
        The final component's last suffix, if any.

        This includes the leading period. For example: '.txt'
        """
        return self._path.suffix

    @property
    def suffixes(self):
        """
        A list of the final component's suffixes, if any.

        These include the leading periods. For example: ['.tar', '.gz']
        """
        return self._path.suffixes

    @property
    def stem(self) -> str:
        """
        The final path component, minus its last suffix.

        If the final path component has no suffix, this is the same as name.
        """
        return self._path.stem

    def with_name(self: SelfTv, name: str) -> SelfTv:
        """Return a new path with the file name changed."""
        return self._new_path(self._path.with_name(name))

    def with_suffix(self: SelfTv, suffix: str) -> SelfTv:
        """Return a new path with the file suffix changed."""
        return self._new_path(self._path.with_suffix(suffix))

    @property
    def parts(self) -> tuple[str, ...]:
        """The components of the path."""
        return self._path.parts

    @property
    def parent(self: SelfTv) -> SelfTv:
        """The path's parent directory."""
        return self._new_path(self._path.parent)

    def is_absolute(self) -> bool:
        """Return True if the path is absolute."""
        return self._path.is_absolute()

    def __truediv__(self: SelfTv, other: str) -> SelfTv:
        return self._new_path(self._path / other)

    def joinpath(self: SelfTv, *other: str) -> SelfTv:
        """Combine this path with one or several arguments, and return a
        new path representing either a subpath (if all arguments are relative
        paths) or a totally different path (if one of the arguments is
        anchored).
        """
        return self._new_path(self._path.joinpath(*other))

    def whoami(self) -> str:
        """Return the username of the current user."""
        with convert_header_exceptions({"machine": self._machine}):
            # TODO: use self._client.whoami(self._machine)
            # requires https://github.com/eth-cscs/pyfirecrest/issues/58
            resp = self._client._get_request(
                endpoint="/utilities/whoami",
                additional_headers={"X-Machine-Name": self._machine},
            )
            return self._client._json_response([resp], 200)["output"]

    def checksum(self) -> str:
        """Return the SHA256 (256-bit) checksum of the file."""
        # this is not part of the pathlib.Path API, but is useful
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            return self._client.checksum(self._machine, self.path)

    # methods that utilise stat calls

    def _lstat_mode(self) -> int:
        """Return the st_mode of the path, not following symlinks."""
        if self._cache_enabled and self._cache.lst_mode is not None:
            return self._cache.lst_mode
        self._cache.lst_mode = self.lstat().st_mode
        if not stat.S_ISLNK(self._cache.lst_mode):
            # if the path is not a symlink,
            # then we also know the dereferenced mode
            self._cache.st_mode = self._cache.lst_mode
        return self._cache.lst_mode

    def _stat_mode(self) -> int:
        """Return the st_mode of the path, following symlinks."""
        if self._cache_enabled and self._cache.st_mode is not None:
            return self._cache.st_mode
        self._cache.st_mode = self.stat().st_mode
        return self._cache.st_mode

    def stat(self) -> os.stat_result:
        """Return stat info for this path.

        If the path is a symbolic link,
        stat will examine the file the link points to.
        """
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            stats = self._client.stat(self._machine, self.path, dereference=True)
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

    def lstat(self) -> os.stat_result:
        """
        Like stat(), except if the path points to a symlink, the symlink's
        status information is returned, rather than its target's.
        """
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            stats = self._client.stat(self._machine, self.path, dereference=False)
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

    def exists(self) -> bool:
        """Whether this path exists (follows symlinks)."""
        try:
            self.stat()
        except FileNotFoundError:
            return False
        return True

    def is_dir(self) -> bool:
        """Whether this path is a directory (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISDIR(st_mode)

    def is_file(self) -> bool:
        """Whether this path is a regular file (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISREG(st_mode)

    def is_symlink(self) -> bool:
        """Whether this path is a symbolic link."""
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISLNK(st_mode)

    def is_block_device(self) -> bool:
        """Whether this path is a block device (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISBLK(st_mode)

    def is_char_device(self) -> bool:
        """Whether this path is a character device (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISCHR(st_mode)

    def is_fifo(self) -> bool:
        """Whether this path is a FIFO (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISFIFO(st_mode)

    def is_socket(self) -> bool:
        """Whether this path is a socket (follows symlinks)."""
        try:
            st_mode = self._stat_mode()
        except FileNotFoundError:
            return False
        return stat.S_ISSOCK(st_mode)

    def iterdir(self: SelfTv, hidden=True) -> Iterator[SelfTv]:
        """Iterate over the directory entries."""
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            results = self._client.list_files(
                self._machine, self.path, show_hidden=hidden
            )
        for entry in results:
            lst_mode = _ls_to_st_mode(entry["type"], entry["permissions"])
            # if the path is not a symlink, then we also know the dereferenced mode
            st_mode = lst_mode if not stat.S_ISLNK(lst_mode) else None
            yield self._new_path(
                self._path / entry["name"],
                _cache=ModeCache(lst_mode=lst_mode, st_mode=st_mode),
            )

    # operations that modify a file

    def chmod(self, mode: int | str) -> None:
        """Change the mode of the path to the numeric mode.

        Note, if the path points to a symlink,
        the symlink target's permissions are changed.
        """
        # note: according to:
        # https://www.gnu.org/software/coreutils/manual/html_node/chmod-invocation.html#chmod-invocation
        # chmod never changes the permissions of symbolic links,
        # i.e. this is chmod, not lchmod
        if not isinstance(mode, (int, str)):
            raise TypeError("mode must be an integer")
        with convert_header_exceptions(
            {"machine": self._machine, "path": self},
            {"X-Invalid-Mode": lambda p: ValueError(f"invalid mode: {mode}")},
        ):
            self._client.chmod(self._machine, self.path, str(mode))
            self._cache.reset()

    def chown(self, uid: int | str, gid: int | str) -> None:
        """Change the owner and group id of the path to the numeric uid and gid."""
        if not isinstance(uid, (str, int)):
            raise TypeError("uid must be an integer")
        if not isinstance(gid, (str, int)):
            raise TypeError("gid must be an integer")
        with convert_header_exceptions(
            {"machine": self._machine, "path": self},
            {
                "X-Invalid-Owner": lambda p: PermissionError(f"invalid uid: {uid}"),
                "X-Invalid-Group": lambda p: PermissionError(f"invalid gid: {gid}"),
            },
        ):
            self._client.chown(self._machine, self.path, str(uid), str(gid))

    def rename(self: SelfTv, target: str | os.PathLike[str]) -> SelfTv:
        """Rename this path to the (absolute) target path.

        Returns the new Path instance pointing to the target path.
        """
        target_path = self._new_path(PurePosixPath(target))
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            self._client.mv(self._machine, self.path, target_path.path)
        return target_path

    def symlink_to(self, target: str | os.PathLike[str]) -> None:
        """Make this path a symlink pointing to the target path."""
        target_path = PurePosixPath(target)
        if not target_path.is_absolute():
            raise ValueError("target must be an absolute path")
        with convert_header_exceptions(
            {"machine": self._machine, "path": self},
            # TODO this is only here because of this bug:
            # https://github.com/eth-cscs/firecrest/issues/190
            {"X-Error": FileExistsError},
        ):
            self._client.symlink(self._machine, str(target_path), self.path)

    def copy_to(self: SelfTv, target: str | os.PathLike[str]) -> None:
        """Copy this path to the target path

        Works for both files and directories (in which case the whole tree is copied).
        """
        target_path = PurePosixPath(target)
        if not target_path.is_absolute():
            raise ValueError("target must be an absolute path")
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            # Note although this endpoint states that it is only for directories,
            # it actually uses `cp -r`:
            # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L320
            self._client.copy(self._machine, self.path, str(target_path))

    def mkdir(
        self, mode: None = None, parents: bool = False, exist_ok: bool = False
    ) -> None:
        """Create a new directory at this given path."""
        if mode is not None:
            raise NotImplementedError("mode is not supported yet")
        try:
            with convert_header_exceptions({"machine": self._machine, "path": self}):
                self._client.mkdir(self._machine, self.path, p=parents)
        except FileExistsError:
            if not exist_ok:
                raise

    def touch(self, mode: None = None, exist_ok: bool = True) -> None:
        """Create a file at this given path.

        :param mode: ignored
        :param exist_ok: if True, do not raise an exception if the path already exists
        """
        if mode is not None:
            raise NotImplementedError("mode is not supported yet")
        if self.exists():
            if exist_ok:
                return
            raise FileExistsError(self)
        try:
            _, source_path = tempfile.mkstemp()
            with convert_header_exceptions({"machine": self._machine, "path": self}):
                self._client.simple_upload(
                    self._machine, source_path, self.parent.path, self.name
                )
        finally:
            os.remove(source_path)

    def read_bytes(self) -> bytes:
        """Read the contents of the file as bytes."""
        io = BytesIO()
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            self._client.simple_download(self._machine, self.path, io)
        return io.getvalue()

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """Read the contents of the file as text."""
        return self.read_bytes().decode(encoding, errors)

    def write_bytes(self, data: bytes) -> None:
        """Write bytes to the file."""
        buffer = BytesIO(data)
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            self._client.simple_upload(
                self._machine, buffer, self.parent.path, self.name
            )

    def write_text(
        self, data: str, encoding: str = "utf-8", errors: str = "strict"
    ) -> None:
        """Write text to the file."""
        self.write_bytes(data.encode(encoding, errors))

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove this file."""
        # note /utilities/rm uses `rm -rf`,
        # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L347
        # so we have to be careful to check first what we are deleting
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            if not missing_ok:
                raise FileNotFoundError(self) from None
            return
        if stat.S_ISDIR(st_mode):
            raise IsADirectoryError(self)
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            self._client.simple_delete(self._machine, self.path)
            self._cache.reset()

    def rmtree(self) -> None:
        """Recursively delete a directory tree."""
        # note /utilities/rm uses `rm -rf`,
        # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L347
        # so we have to be careful to check first what we are deleting
        try:
            st_mode = self._lstat_mode()
        except FileNotFoundError:
            raise FileNotFoundError(self) from None
        if not stat.S_ISDIR(st_mode):
            raise NotADirectoryError(self)
        with convert_header_exceptions({"machine": self._machine, "path": self}):
            self._client.simple_delete(self._machine, self.path)
            self._cache.reset()


@lru_cache(maxsize=256)
def _ls_to_st_mode(ftype: str, permissions: str) -> int:
    """Use the return information from `utilities/ls` to create an st_mode value.

    Note, this does not dereference symlinks, and so is like lstat

    :param ftype: The file type, e.g. "-" for regular file, "d" for directory.
    :param permissions: The file permissions, e.g. "rwxr-xr-x".
    """
    ftypes = {
        "b": "0060",  # block device
        "c": "0020",  # character device
        "d": "0040",  # directory
        "l": "0120",  # Symbolic link
        "s": "0140",  # Socket.
        "p": "0010",  # FIFO
        "-": "0100",  # Regular file
    }
    if ftype not in ftypes:
        raise ValueError(f"invalid file type: {ftype}")
    p = permissions
    r = lambda x: 4 if x == "r" else 0  # noqa: E731
    w = lambda x: 2 if x == "w" else 0  # noqa: E731
    x = lambda x: 1 if x == "x" else 0  # noqa: E731
    st_mode = (
        ((r(p[0]) + w(p[1]) + x(p[2])) * 100)
        + ((r(p[3]) + w(p[4]) + x(p[5])) * 10)
        + ((r(p[6]) + w(p[7]) + x(p[8])) * 1)
    )
    return int(ftypes[ftype] + str(st_mode), 8)
