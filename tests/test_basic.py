from aiida_firecrest.scheduler import FirecrestScheduler
from aiida_firecrest.transport import FirecrestTransport


def test_init_scheduler():
    FirecrestScheduler()


def test_init_transport():
    FirecrestTransport()
