from aiida_firecrest.scheduler import FirecrestScheduler
from aiida_firecrest.transport import FirecrestTransport


def test_init_scheduler():
    FirecrestScheduler()


def init_transport(firecrest_server):
    transport = FirecrestTransport(
        url=firecrest_server.url,
        token_uri=firecrest_server.token_uri,
        client_id=firecrest_server.client_id,
        client_secret=firecrest_server.client_secret,
        machine=firecrest_server.machine,
    )
    return transport


def test_init_transport(firecrest_server):
    init_transport(firecrest_server)


def test_path_exists(firecrest_server):
    transport = init_transport(firecrest_server)
    assert transport.path_exists(firecrest_server.scratch_path)
    assert not transport.path_exists(firecrest_server.scratch_path + "/file.txt")


def test_isdir(firecrest_server):
    transport = init_transport(firecrest_server)
    assert transport.isdir(firecrest_server.scratch_path)
    assert not transport.isdir(firecrest_server.scratch_path + "/other")


def test_mkdir(firecrest_server):
    transport = init_transport(firecrest_server)
    transport.mkdir(firecrest_server.scratch_path + "/test")
    assert transport.isdir(firecrest_server.scratch_path + "/test")


def test_isfile(firecrest_server):
    transport = init_transport(firecrest_server)
    assert not transport.isfile(firecrest_server.scratch_path + "/file.txt")
    # TODO make file then re-test


def test_listdir(firecrest_server):
    transport = init_transport(firecrest_server)
    assert transport.listdir(firecrest_server.scratch_path) == []
    # TODO make file/folder then re-test
