"""Pytest configuration for the aiida-firecrest tests.

This sets up the Firecrest server to use, and telemetry for API requests.
"""
from __future__ import annotations

from functools import partial
from json import load as json_load
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from _pytest.terminal import TerminalReporter
import firecrest as f7t
import pytest
import requests
import yaml

from aiida_firecrest.utils_test import FirecrestConfig, FirecrestMockServer

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


def pytest_terminal_summary(
    terminalreporter: TerminalReporter, exitstatus: int, config: pytest.Config
):
    """Called after all tests have run."""
    data = config.stash.get("firecrest_requests", None)
    if data is None:
        return
    terminalreporter.write(
        yaml.dump(
            {"Firecrest requests telemetry": data},
            default_flow_style=False,
            sort_keys=True,
        )
    )


@pytest.fixture(scope="function")
def firecrest_server(
    pytestconfig: pytest.Config,
    request: pytest.FixtureRequest,
    monkeypatch,
    tmp_path: Path,
):
    """A fixture which provides a mock Firecrest server to test against."""
    config_path: str | None = request.config.getoption("--firecrest-config")
    no_clean: bool = request.config.getoption("--firecrest-no-clean")
    record_requests: bool = request.config.getoption("--firecrest-requests")
    telemetry: RequestTelemetry | None = None

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

        if record_requests:
            telemetry = RequestTelemetry()
            monkeypatch.setattr(requests, "get", partial(telemetry.wrap, requests.get))
            monkeypatch.setattr(
                requests, "post", partial(telemetry.wrap, requests.post)
            )
            monkeypatch.setattr(requests, "put", partial(telemetry.wrap, requests.put))
            monkeypatch.setattr(
                requests, "delete", partial(telemetry.wrap, requests.delete)
            )

        yield config
        # Note this shouldn't really work, for folders but it does :shrug:
        # because they use `rm -r`:
        # https://github.com/eth-cscs/firecrest/blob/7f02d11b224e4faee7f4a3b35211acb9c1cc2c6a/src/utilities/utilities.py#L347
        if not no_clean:
            client.simple_delete(config.machine, config.scratch_path)
    else:
        # otherwise use mock server
        server = FirecrestMockServer(tmp_path)
        if record_requests:
            telemetry = RequestTelemetry()
            mock_request = partial(telemetry.wrap, server.mock_request)
        else:
            mock_request = server.mock_request
        monkeypatch.setattr(requests, "get", mock_request)
        monkeypatch.setattr(requests, "post", mock_request)
        monkeypatch.setattr(requests, "put", mock_request)
        monkeypatch.setattr(requests, "delete", mock_request)
        yield server.config

    # save data on the server
    if telemetry is not None:
        test_name = request.node.name
        pytestconfig.stash.setdefault("firecrest_requests", {})[
            test_name
        ] = telemetry.counts


class RequestTelemetry:
    """A to gather telemetry on requests."""

    def __init__(self) -> None:
        self.counts = {}

    def wrap(
        self,
        method: Callable[..., requests.Response],
        url: str | bytes,
        **kwargs: Any,
    ) -> requests.Response:
        """Wrap a requests method to gather telemetry."""
        endpoint = urlparse(url if isinstance(url, str) else url.decode("utf-8")).path
        self.counts.setdefault(endpoint, 0)
        self.counts[endpoint] += 1
        return method(url, **kwargs)
