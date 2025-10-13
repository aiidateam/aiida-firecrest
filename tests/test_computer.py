################################################################################
# Copyright (c), The AiiDA team. All rights reserved.                          #
# This file is part of the AiiDA code.                                         #
#                                                                              #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-firecrest #
# For further information on the license, see the LICENSE.txt file             #
# For further information please visit http://www.aiida.net                    #
################################################################################
from pathlib import Path
from unittest.mock import Mock

from aiida import orm
from click import BadParameter
import pytest

import aiida_firecrest.transport as _trans


@pytest.mark.usefixtures("aiida_profile_clean")
def test_whoami(firecrest_computer: orm.Computer, firecrest_config):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    assert transport.whoami() == firecrest_config.username


def test_create_secret_file_with_existing_file(tmpdir: Path):
    from aiida_firecrest.transport import _create_secret_file

    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    result = _create_secret_file(None, None, str(secret_file))
    assert isinstance(result, str)
    assert result == str(secret_file)
    assert Path(result).read_text() == "topsecret"


def test_create_secret_file_with_nonexistent_file(tmp_path):
    from aiida_firecrest.transport import _create_secret_file

    secret_file = tmp_path / "nonexistent"
    with pytest.raises(BadParameter):
        _create_secret_file(None, None, str(secret_file))


def test_create_secret_file_with_secret_value(tmp_path, monkeypatch):
    from aiida_firecrest.transport import _create_secret_file

    secret = "topsecret!~/"
    monkeypatch.setattr(
        Path,
        "expanduser",
        lambda x: tmp_path / str(x).lstrip("~/") if str(x).startswith("~/") else x,
    )
    result = _create_secret_file(None, None, secret)
    assert Path(result).parent.parts[-1] == ".firecrest"
    assert Path(result).read_text() == secret


@pytest.mark.usefixtures("aiida_profile_clean")
def test_validate_temp_directory(
    firecrest_computer: orm.Computer, firecrest_config, monkeypatch, tmpdir: Path
):
    """
    Test the validation of the temp directory.
    Note: this test depends on a functional putfile() method, for consistency with the real server tests.
      Before running this test, make sure putfile() is working, which is tested in `test_putfile_getfile`.
    """
    from aiida_firecrest.transport import _validate_temp_directory

    monkeypatch.setattr("click.echo", lambda x: None)
    ctx = Mock()
    ctx.params = {
        "url": f"{firecrest_config.url}",
        "token_uri": f"{firecrest_config.token_uri}",
        "client_id": f"{firecrest_config.client_id}",
        "client_secret": f"{firecrest_config.client_secret}",
        "compute_resource": f"{firecrest_config.compute_resource}",
        "max_io_allowed": f"{firecrest_config.max_io_allowed}",
        "checksum_check": True,
    }

    # prepare some files and directories for testing
    transport = firecrest_computer.get_transport()
    _remote = transport._temp_directory
    _local = tmpdir
    Path(tmpdir / "_.txt").write_text("touch")
    transport.mkdir(_remote / "temp_on_server_directory")
    transport.putfile(tmpdir / "_.txt", _remote / "_.txt")
    transport.putfile(tmpdir / "_.txt", _remote / "temp_on_server_directory" / "_.txt")
    transport.mkdir(_remote / "temp_on_server_directory/ subdir")

    # should raise if is_file
    with pytest.raises(BadParameter):
        result = _validate_temp_directory(ctx, None, Path(_remote / "_.txt").as_posix())

    # should create the directory if it doesn't exist
    result = _validate_temp_directory(
        ctx, None, Path(_remote / "nonexisting").as_posix()
    )
    assert result == Path(_remote / "nonexisting").as_posix()
    assert transport.path_exists(Path(_remote / "nonexisting"))

    # should get a confirmation if the directory exists and is not empty
    monkeypatch.setattr("click.confirm", lambda x: False)
    with pytest.raises(BadParameter):
        result = _validate_temp_directory(
            ctx, None, Path(_remote / "temp_on_server_directory").as_posix()
        )

    # should delete the content if I confirm
    monkeypatch.setattr("click.confirm", lambda x: True)
    result = _validate_temp_directory(
        ctx, None, Path(_remote / "temp_on_server_directory").as_posix()
    )
    assert result == Path(_remote / "temp_on_server_directory").as_posix()
    assert not transport.path_exists(_remote / "temp_on_server_directory" / "_.txt")
    assert not transport.path_exists(_remote / "temp_on_server_directory" / "subdir")


def test_dynamic_info_direct_size(firecrest_config, monkeypatch, tmpdir: Path):
    from aiida_firecrest.transport import _dynamic_info_direct_size

    monkeypatch.setattr("click.echo", lambda x: None)
    ctx = Mock()
    ctx.params = {
        "url": f"{firecrest_config.url}",
        "token_uri": f"{firecrest_config.token_uri}",
        "client_id": f"{firecrest_config.client_id}",
        "client_secret": f"{firecrest_config.client_secret}",
        "compute_resource": f"{firecrest_config.compute_resource}",
        "temp_directory": f"{firecrest_config.temp_directory}",
        "billing_account": f"{firecrest_config.billing_account}",
        "max_io_allowed": f"{firecrest_config.max_io_allowed}",
        "checksum_check": f"{firecrest_config.checksum_check}",
    }

    # should catch UTILITIES_MAX_FILE_SIZE if value is not provided
    result = _dynamic_info_direct_size(ctx, None, 0)
    assert result == 5

    # should use the value if provided
    # note: user cannot enter negative numbers anyways, click raise as this shoule be float not str
    result = _dynamic_info_direct_size(ctx, None, 10)
    assert result == 10


@pytest.mark.usefixtures("aiida_profile_clean")
def test_dynamic_info_firecrest_version(
    firecrest_computer: orm.Computer, monkeypatch, capsys
):

    from packaging.version import parse

    transport = firecrest_computer.get_transport()

    # basic functionality test
    monkeypatch.setattr(_trans, "_MINIMUM_API_VERSION", "1")
    transport.blocking_client.server_version = lambda: "10.10.10"
    assert transport._get_firecrest_version() == parse("10.10.10")

    # raise RuntimeError if cannot get the version
    def _mocked():
        raise Exception

    transport.blocking_client.server_version = _mocked
    with pytest.raises(
        RuntimeError,
        match=r"Could not get the version of the FirecREST server.\nPerhaps you have inserted wrong credentials?",
    ):
        transport._get_firecrest_version()

    # raise RuntimeError if the version is None
    transport.blocking_client.server_version = lambda: None
    with pytest.raises(
        RuntimeError,
        match=r"Could not get the version of the FirecREST server, it returned None.\nPerhaps you have inserted wrong credentials?",
    ):
        transport._get_firecrest_version()

    # raise ValueError if the version is invalid or not parsable
    transport.blocking_client.server_version = lambda: "invalid_version"
    with pytest.raises(
        ValueError,
        match="Cannot parse the retrieved version from the server: invalid_version",
    ):
        transport._get_firecrest_version()

    # raise ValueError if the version is too old
    monkeypatch.setattr(_trans, "_MINIMUM_API_VERSION", "1.0")
    transport.blocking_client.server_version = lambda: "0.9.9"
    with pytest.raises(
        ValueError,
        match=r"FirecREST api version 0.9.9 is not supported,"
        r" minimum supported version is 1.0",
    ):
        transport._get_firecrest_version()


@pytest.mark.usefixtures("aiida_profile_clean")
def test_verdi_computer_test(firecrest_computer: orm.Computer):
    """
    Test that all test pass with `verdi computer test` command.
    Note: We probably could import /aiida/cmdline/commands/cmd_computer.py::computer_test and run that directly,
    however that's not a good idea, because it's not part of the public api.
    """

    import subprocess

    result = subprocess.run(
        ["verdi", "computer", "test", "test_computer", "--print-traceback"],
        capture_output=True,
        text=True,
    )

    if "Success: all 6 tests succeeded" not in result.stdout:
        raise AssertionError(
            "verdi computer test test_computer did not pass all tests."
            f"\nstdout:\n {result.stdout}\nstderr:\n {result.stderr}"
        )
