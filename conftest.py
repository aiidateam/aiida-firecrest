# add option to pytest
# note this file must be at the root of the project


def pytest_addoption(parser):
    parser.addoption(
        "--firecrest-config", action="store", help="Path to firecrest config JSON file"
    )
