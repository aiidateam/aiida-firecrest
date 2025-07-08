"""Pytest configuration that must be at the root level."""

pytest_plugins = ["aiida.tools.pytest_fixtures"]


def pytest_addoption(parser):
    parser.addoption("--firecrest-config", action="store", help="Path to firecrest config JSON file")
    parser.addoption(
        "--firecrest-no-clean",
        action="store_true",
        help="Don't clean up server after tests (for debugging)",
    )
    parser.addoption(
        "--firecrest-requests",
        action="store_true",
        help="Collect and print telemetry data for API requests",
    )
