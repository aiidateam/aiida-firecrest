from __future__ import annotations

from json import load as json_load
from pathlib import Path

import firecrest as f7t
import pytest
import requests

from aiida_firecrest.utils_test import FirecrestConfig, FirecrestMockServer


@pytest.fixture(scope="function")
def firecrest_server(request, monkeypatch, tmp_path: Path):
    """A fixture which provides a mock Firecrest server to test against."""
    config_path = request.config.getoption("--firecrest-config")
    no_clean = request.config.getoption("--firecrest-no-clean")
    if config_path is not None:
        # if given, use this config
        with open(config_path, encoding="utf8") as handle:
            config = json_load(handle)
        config = FirecrestConfig(**config)
        # rather than use the scratch_path directly, we use a subfolder,
        # which we can then clean
        config.scratch_path = config.scratch_path + "/pytest_tmp"

        # we need to connect to the client here,
        # to ensure that the scratch path exists and is empty
        client = f7t.Firecrest(
            firecrest_url=config.url,
            authorization=f7t.ClientCredentialsAuth(
                config.client_id, config.client_secret, config.token_uri
            ),
        )
        client.mkdir(config.machine, config.scratch_path, p=True)
        yield config
        # Note this shouldn't really work, for folders but it does :shrug:
        # because they use `rm -r`:
        # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L347
        if not no_clean:
            client.simple_delete(config.machine, config.scratch_path)
    else:
        # otherwise use mock server
        server = FirecrestMockServer(tmp_path)
        monkeypatch.setattr(requests, "get", server.mock_request)
        monkeypatch.setattr(requests, "post", server.mock_request)
        monkeypatch.setattr(requests, "put", server.mock_request)
        monkeypatch.setattr(requests, "delete", server.mock_request)
        yield server.config
