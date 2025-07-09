################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import uuid

from aiida import orm
from firecrest import ClientCredentialsAuth
from firecrest.v2 import Firecrest
import pytest


@pytest.fixture
def firecrest_computer(firecrest_config):
    """Create and return a computer configured for Firecrest.

    Note, the computer is not stored in the database.
    """

    computer = orm.Computer(
        label="test_computer",
        description="test computer",
        hostname="-",
        workdir=firecrest_config.workdir,
        transport_type="firecrest",
        scheduler_type="firecrest",
    )
    computer.set_minimum_job_poll_interval(5)
    computer.set_default_mpiprocs_per_machine(1)
    computer.configure(
        url=firecrest_config.url,
        token_uri=firecrest_config.token_uri,
        client_id=firecrest_config.client_id,
        client_secret=firecrest_config.client_secret,
        compute_resource=firecrest_config.compute_resource,
        small_file_size_mb=firecrest_config.small_file_size_mb,
        temp_directory=firecrest_config.temp_directory,
        api_version=firecrest_config.api_version,
        billing_account=firecrest_config.billing_account,
        max_io_allowed=firecrest_config.max_io_allowed,
        checksum_check=firecrest_config.checksum_check,
    )
    return computer


@dataclass
class ComputerFirecrestConfig:
    """Configuration of a computer using FirecREST as transport plugin.

    :param url: The URL of the FirecREST server.
    :param token_uri: The URI to receive  tokens.
    :param client_id: The client ID for the client credentials.
    :param client_secret: The client secret for the client credentials.
    :param compute_resource: The name of the compute resource. This is the name of the machine.
    :param temp_directory: A temporary directory on the machine for transient zip files.
    :param workdir: The aiida working directory on the machine.
    :param api_version: The version of the FirecREST API.
    :param builder_metadata_options_custom_scheduler_commands: A list of custom
           scheduler commands when submitting a job, for example
           ["#SBATCH --account=mr32",
            "#SBATCH --constraint=mc",
            "#SBATCH --mem=10K"].
    :param small_file_size_mb: The maximum file size for direct upload & download.
    :param max_io_allowed: The maximum number of I/O operations allowed.
    :param billing_account: The billing account to use for the computer.
    :param username: The username to use for the computer.
    :param mocked: If True, the configuration is mocked and no real connection is made.
    :param checksum_check: If True, checksums are checked for file transfers.
    """

    url: str
    token_uri: str
    client_id: str
    client_secret: str
    compute_resource: str
    temp_directory: str
    workdir: str
    api_version: str
    billing_account: str
    username: str
    small_file_size_mb: float = 1.0
    builder_metadata_options_custom_scheduler_commands: list[str] = field(
        default_factory=list
    )
    max_io_allowed: int = 8
    checksum_check: bool = True
    mocked: bool = False


@pytest.fixture(scope="function")
def firecrest_config(
    request: pytest.FixtureRequest,
):
    """
    A config file is necessary to run the tests with Firecrest.
    ┌─────────────────┐───►┌─────────────┐───►┌──────────────────┐
    │ aiida_firecrest │    │ pyfirecrest │    │ FirecREST server │
    └─────────────────┘◄───└─────────────┘◄───└──────────────────┘

    By default, the tests use a server installed on a docker container.
    If you want to use a real server, you just need to pass a similar file to `.firecrest-demo-config.json`,
    that includes the clients credentials using the `--firecrest-config` option.
    """

    config_path: str | None = request.config.getoption("--firecrest-config")
    no_clean: bool = request.config.getoption("--firecrest-no-clean")
    if not config_path:
        config_path = ".firecrest-demo-config.json"

    with open(config_path, encoding="utf8") as handle:
        config = json.load(handle)
    config = ComputerFirecrestConfig(**config)
    # # rather than use the scratch_path directly, we use a subfolder,
    # # which we can then clean

    config.workdir = str(Path(config.workdir) / f"pytest_tmp_{uuid.uuid4()}")
    config.temp_directory = str(
        Path(config.temp_directory) / f"pytest_tmp_{uuid.uuid4()}"
    )

    # # we need to connect to the client here,
    # # to ensure that the scratch path exists and is empty
    if Path(config.client_secret).exists():
        _secret = Path(config.client_secret).read_text().strip()
    else:
        _secret = config.client_secret
    client = Firecrest(
        firecrest_url=config.url,
        authorization=ClientCredentialsAuth(
            config.client_id,
            _secret,
            config.token_uri,
        ),
    )
    client.mkdir(config.compute_resource, config.workdir, create_parents=True)
    client.mkdir(config.compute_resource, config.temp_directory, create_parents=True)

    yield config

    if not no_clean:
        client.rm(config.compute_resource, config.workdir)
        client.rm(config.compute_resource, config.temp_directory)
        pass
