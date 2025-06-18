################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
"""Pytest configuration that must be at the root level."""

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
    parser.addoption(
        "--firecrest-requests",
        action="store_true",
        help="Collect and print telemetry data for API requests",
    )
