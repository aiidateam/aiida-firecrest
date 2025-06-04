###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import posixpath
from typing import Any, Callable, Union

from aiida.schedulers import SchedulerError
from aiida.transports.transport import TransportPath
from firecrest.FirecrestException import HeaderException
from firecrest.v1.BasicClient import logger as fc_logger

try:
    # available in python 3.11
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self


class FcPath:
    """A simple class to represent a path on the FirecREST server.
    Note: since this path will be used for asynchronous operations,
    and is solely used only across class:FirecrestTransport, therefore it does not
    makes sense to develop routine methods like `exists`, `is_dir`, etc.

    The only purpose of this class is to provide a simple way to represent a path
    on the FirecREST server.
    """

    def __init__(self, path: TransportPath | Self) -> None:
        self.path = str(path)

    def __str__(self) -> str:
        return str(self.path)

    def __truediv__(self, other: str) -> FcPath:
        return FcPath(posixpath.join(self.path, other))

    def __fspath__(self) -> str:
        """Return the path as a string for file system operations."""
        return str(self.path)

    @property
    def parent(self) -> FcPath:
        """Return the parent directory of this path."""
        return FcPath(posixpath.dirname(self.path))

    @property
    def name(self) -> str:
        """Return the name of this path."""
        return posixpath.basename(self.path)

    def joinpath(self, *args: str | FcPath) -> FcPath:
        """Join this path with the given arguments."""
        return FcPath(posixpath.join(self.path, *(str(arg) for arg in args)))

    def resolve(self) -> FcPath:
        """Resolve the path to an absolute path."""
        return FcPath(posixpath.abspath(self.path))


TPath_Extended = Union[TransportPath, FcPath]


@contextmanager
def disable_fc_logging() -> Iterator[None]:
    """Temporarily disable Firecrest logging.

    This is useful when calling methods that are expected to fail,
    such as `exists` or `is_dir`, as it avoids polluting the log with errors.
    """
    level = fc_logger.level
    fc_logger.setLevel(60)
    try:
        yield
    finally:
        fc_logger.setLevel(level)


@contextmanager
def convert_header_exceptions(
    data: dict[str, Any],
    updates: dict[str, Callable[[dict[str, Any]], Exception]] | None = None,
) -> Iterator[None]:
    """Catch HeaderException and re-raise as an alternative.

    Default conversions are:
    - X-Timeout: ApiTimeoutError
    - X-Machine-Does-Not-Exist: MachineDoesNotExistError
    - X-Machine-Not-Available: PermissionError
    - X-Permission-Denied: PermissionError
    - X-Not-Found: FileNotFoundError
    - X-Not-A-Directory: NotADirectoryError
    - X-Exists: FileExistsError
    - X-Invalid-Path: FileNotFoundError
    - X-A-Directory: IsADirectoryError
    - X-Size-Limit: FileSizeExceeded
    - X-Sbatch-Error: SchedulerError

    """
    converters: dict[str, Callable[[dict[str, Any]], Exception]] = {
        "X-Timeout": ApiTimeoutError,
        "X-Machine-Does-Not-Exist": MachineDoesNotExistError,
        "X-Machine-Not-Available": PermissionError,
        "X-Permission-Denied": PermissionError,
        "X-Not-Found": FileNotFoundError,
        "X-Not-A-Directory": NotADirectoryError,
        "X-Exists": FileExistsError,
        "X-Invalid-Path": FileNotFoundError,
        "X-A-Directory": IsADirectoryError,
        "X-Size-Limit": FileSizeExceededError,
        "X-Sbatch-Error": SchedulerError,
    }
    if updates is not None:
        converters.update(updates)
    with disable_fc_logging():
        try:
            yield
        except HeaderException as exc:
            for header in exc.responses[-1].headers:
                c = converters.get(header)
                if c is not None:
                    raise c(data) from exc
            raise


class ApiTimeoutError(TimeoutError):
    """The API call timed out."""

    def __init__(self, data: dict[str, Any]) -> None:
        super().__init__("API call timed out")


class MachineDoesNotExistError(ConnectionError):
    """The machine does not exist."""

    def __init__(self, data: dict[str, Any]) -> None:
        msg = "Remote machine does not exist"
        if "machine" in data:
            msg += f": {data['machine']}"
        super().__init__(msg)


class FileSizeExceededError(OSError):
    """Maximum file size exceeded."""

    def __init__(self, data: dict[str, Any]) -> None:
        msg = "Maximum file size exceeded"
        if "path" in data:
            msg += f": {data['path']}"
        super().__init__(msg)


_COMMON_HEADER_EXC: dict[str, Callable[[dict[str, Any]], Exception] | None] = {
    "X-Timeout": ApiTimeoutError,
    "X-Machine-Does-Not-Exist": MachineDoesNotExistError,
    "X-Machine-Not-Available": PermissionError,
    "X-Permission-Denied": PermissionError,
    "X-Not-Found": FileNotFoundError,
    "X-Not-A-Directory": NotADirectoryError,
    "X-Exists": FileExistsError,
    "X-Invalid-Path": FileNotFoundError,
    "X-A-Directory": IsADirectoryError,
}
