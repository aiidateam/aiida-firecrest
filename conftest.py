# add option to pytest
# note this file must be at the root of the project

pytest_plugins = ["aiida.manage.tests.pytest_fixtures"]


def pytest_addoption(parser):
    parser.addoption(
        "--firecrest-config", action="store", help="Path to firecrest config JSON file"
    )
    parser.addoption(
        "--firecrest-no-clean",
        action="store_true",
        help="Don't clean up server after tests (for debugging)",
    )


def pytest_report_header(config):
    if config.getoption("--firecrest-config"):
        header = [
            "Running against FirecREST server: {}".format(
                config.getoption("--firecrest-config")
            )
        ]
        if config.getoption("--firecrest-no-clean"):
            header.append("Not cleaning up FirecREST server after tests!")
        return header
    return ["Running against Mock FirecREST server"]
