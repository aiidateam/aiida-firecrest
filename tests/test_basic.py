from pathlib import Path
from typing import Protocol

import pytest

from aiida_firecrest.scheduler import FirecrestScheduler
from aiida_firecrest.transport import FirecrestTransport


class _ServerConfig(Protocol):
    scratch_path: str


def test_init_scheduler():
    FirecrestScheduler()


@pytest.fixture(name="transport")
def _transport(firecrest_server: _ServerConfig):
    transport = FirecrestTransport(
        url=firecrest_server.url,
        token_uri=firecrest_server.token_uri,
        client_id=firecrest_server.client_id,
        client_secret=firecrest_server.client_secret,
        machine=firecrest_server.machine,
    )
    yield transport
    # transport.rmtree(firecrest_server.scratch_path)


def test_path_exists(firecrest_server: _ServerConfig, transport: FirecrestTransport):
    assert transport.path_exists(firecrest_server.scratch_path)
    assert not transport.path_exists(firecrest_server.scratch_path + "/file.txt")


def test_isdir(firecrest_server: _ServerConfig, transport: FirecrestTransport):
    assert transport.isdir(firecrest_server.scratch_path)
    assert not transport.isdir(firecrest_server.scratch_path + "/other")


def test_mkdir(firecrest_server: _ServerConfig, transport: FirecrestTransport):
    transport.mkdir(firecrest_server.scratch_path + "/test")
    assert transport.isdir(firecrest_server.scratch_path + "/test")


def test_putfile(
    firecrest_server: _ServerConfig, transport: FirecrestTransport, tmp_path: Path
):
    assert not transport.isfile(firecrest_server.scratch_path + "/file.txt")
    file_path = tmp_path.joinpath("file.txt")
    file_path.write_text("test")
    transport.putfile(str(file_path), firecrest_server.scratch_path + "/file.txt")
    assert transport.isfile(firecrest_server.scratch_path + "/file.txt")


def test_listdir(firecrest_server: _ServerConfig, transport: FirecrestTransport):
    assert transport.listdir(firecrest_server.scratch_path) == []
    # TODO make file/folder then re-test
