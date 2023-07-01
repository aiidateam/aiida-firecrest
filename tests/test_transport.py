"""Tests isolating only the Transport."""
from pathlib import Path
import platform

import pytest

from aiida_firecrest.transport import FirecrestTransport
from aiida_firecrest.utils_test import FirecrestConfig


@pytest.fixture(name="transport")
def _transport(firecrest_server: FirecrestConfig):
    transport = FirecrestTransport(
        url=firecrest_server.url,
        token_uri=firecrest_server.token_uri,
        client_id=firecrest_server.client_id,
        client_secret=firecrest_server.client_secret,
        client_machine=firecrest_server.machine,
        small_file_size_mb=firecrest_server.small_file_size_mb,
    )
    transport.chdir(firecrest_server.scratch_path)
    yield transport


def test_path_exists(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    assert transport.path_exists(firecrest_server.scratch_path)
    assert not transport.path_exists(firecrest_server.scratch_path + "/file.txt")


def test_get_attribute(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport
):
    transport._cwd.joinpath("test.txt").touch()
    attrs = transport.get_attribute(firecrest_server.scratch_path + "/test.txt")
    assert set(attrs) == {
        "st_size",
        "st_atime",
        "st_mode",
        "st_gid",
        "st_mtime",
        "st_uid",
    }
    assert isinstance(attrs.st_mode, int)


def test_isdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    assert transport.isdir(firecrest_server.scratch_path)
    assert not transport.isdir(firecrest_server.scratch_path + "/other")


def test_mkdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    transport.mkdir(firecrest_server.scratch_path + "/test")
    assert transport.isdir(firecrest_server.scratch_path + "/test")


def test_large_file_transfers(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport, tmp_path: Path
):
    """Large file transfers (> 5MB by default) have to be downloaded/uploaded via a different pathway."""
    content = "a" * (transport._small_file_size_bytes + 1)

    # upload
    remote_path = firecrest_server.scratch_path + "/file.txt"
    assert not transport.isfile(remote_path)
    file_path = tmp_path.joinpath("file.txt")
    file_path.write_text(content)
    transport.putfile(str(file_path), remote_path)
    assert transport.isfile(remote_path)

    # download
    if transport._url.startswith("http://localhost") and platform.system() == "Darwin":
        pytest.skip("Skipping large file download test on macOS with localhost server.")
        # TODO this is a known issue whereby a 403 is returned when trying to download the supplied file url
        # due to a signature mismatch
    new_path = tmp_path.joinpath("file2.txt")
    assert not new_path.is_file()
    transport.getfile(remote_path, new_path)
    assert new_path.is_file()
    assert new_path.read_text() == content


def test_listdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    assert transport.listdir(firecrest_server.scratch_path) == []


def test_copyfile(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    from_path = firecrest_server.scratch_path + "/copy_from.txt"
    to_path = firecrest_server.scratch_path + "/copy_to.txt"

    transport.write_binary(from_path, b"test")

    assert not transport.path_exists(to_path)
    transport.copyfile(from_path, to_path)
    assert transport.isfile(to_path)
    assert transport.read_binary(to_path) == b"test"


def test_copyfile_symlink_noderef(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport
):
    from_path = firecrest_server.scratch_path + "/copy_from.txt"
    from_path_symlink = firecrest_server.scratch_path + "/copy_from_symlink.txt"
    to_path = firecrest_server.scratch_path + "/copy_to_symlink.txt"

    transport.write_binary(from_path, b"test")
    transport.symlink(from_path, from_path_symlink)

    assert not transport.path_exists(to_path)
    transport.copyfile(from_path_symlink, to_path, dereference=False)
    assert transport.isfile(to_path)
    assert transport.read_binary(to_path) == b"test"


def test_copyfile_symlink_deref(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport
):
    from_path = firecrest_server.scratch_path + "/copy_from.txt"
    from_path_symlink = firecrest_server.scratch_path + "/copy_from_symlink.txt"
    to_path = firecrest_server.scratch_path + "/copy_to_symlink.txt"

    transport.write_binary(from_path, b"test")
    transport.symlink(from_path, from_path_symlink)

    assert not transport.path_exists(to_path)
    transport.copyfile(from_path_symlink, to_path, dereference=True)
    assert transport.isfile(to_path)
    assert transport.read_binary(to_path) == b"test"


def test_remove(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    transport.write_binary(firecrest_server.scratch_path + "/file.txt", b"test")
    assert transport.path_exists(firecrest_server.scratch_path + "/file.txt")
    transport.remove(firecrest_server.scratch_path + "/file.txt")
    assert not transport.path_exists(firecrest_server.scratch_path + "/file.txt")


def test_rmtree(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    transport.mkdir(firecrest_server.scratch_path + "/test")
    transport.write_binary(firecrest_server.scratch_path + "/test/file.txt", b"test")
    assert transport.path_exists(firecrest_server.scratch_path + "/test/file.txt")
    transport.rmtree(firecrest_server.scratch_path + "/test")
    assert not transport.path_exists(firecrest_server.scratch_path + "/test")


def test_rename(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    transport.write_binary(firecrest_server.scratch_path + "/file.txt", b"test")
    assert transport.path_exists(firecrest_server.scratch_path + "/file.txt")
    transport.rename(
        firecrest_server.scratch_path + "/file.txt",
        firecrest_server.scratch_path + "/file2.txt",
    )
    assert not transport.path_exists(firecrest_server.scratch_path + "/file.txt")
    assert transport.path_exists(firecrest_server.scratch_path + "/file2.txt")
