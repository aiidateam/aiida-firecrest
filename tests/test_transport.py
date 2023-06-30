"""Tests isolating only the Transport."""
from pathlib import Path

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


def test_isdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    assert transport.isdir(firecrest_server.scratch_path)
    assert not transport.isdir(firecrest_server.scratch_path + "/other")


def test_mkdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    transport.mkdir(firecrest_server.scratch_path + "/test")
    assert transport.isdir(firecrest_server.scratch_path + "/test")


def test_putfile(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport, tmp_path: Path
):
    to_path = firecrest_server.scratch_path + "/file.txt"
    assert not transport.isfile(to_path)
    file_path = tmp_path.joinpath("file.txt")
    file_path.write_text("test")
    transport.putfile(str(file_path), to_path)
    assert transport.isfile(to_path)
    assert transport.read_binary(to_path) == b"test"


def test_putfile_large(
    firecrest_server: FirecrestConfig, transport: FirecrestTransport, tmp_path: Path
):
    content = "a" * (transport._small_file_size_bytes + 1)
    to_path = firecrest_server.scratch_path + "/file.txt"
    assert not transport.isfile(to_path)
    file_path = tmp_path.joinpath("file.txt")
    file_path.write_text(content)
    transport.putfile(str(file_path), to_path)
    assert transport.isfile(to_path)
    # assert transport.read_binary(to_path) == content.encode("utf8")


def test_listdir(firecrest_server: FirecrestConfig, transport: FirecrestTransport):
    assert transport.listdir(firecrest_server.scratch_path) == []
    # TODO make file/folder then re-test


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
