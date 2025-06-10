###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
from pathlib import Path
from unittest.mock import Mock

from aiida import orm
from click import BadParameter
import pytest


@pytest.mark.usefixtures("aiida_profile_clean")
def test_whoami(firecrest_computer: orm.Computer):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    assert isinstance(transport.whoami(), str)


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
        "small_file_size_mb": float(5),
        "api_version": f"{firecrest_config.api_version}",
    }

    # prepare some files and directories for testing
    transport = firecrest_computer.get_transport()
    _remote = transport._temp_directory
    _local = tmpdir
    Path(tmpdir / "_.txt").write_text("touch")
    transport.mkdir(_remote / "temp_on_server_directory")
    transport.putfile(tmpdir / "_.txt", _remote / "_.txt")
    transport.putfile(tmpdir / "_.txt", _remote / "temp_on_server_directory" / "_.txt")

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
        "api_version": f"{firecrest_config.api_version}",
    }

    # should catch UTILITIES_MAX_FILE_SIZE if value is not provided
    result = _dynamic_info_direct_size(ctx, None, 0)
    assert result == 5

    # should use the value if provided
    # note: user cannot enter negative numbers anyways, click raise as this shoule be float not str
    result = _dynamic_info_direct_size(ctx, None, 10)
    assert result == 10


def test_dynamic_info_firecrest_version(
    firecrest_config, monkeypatch, tmpdir: Path, capsys
):

    from unittest.mock import patch

    from click import Abort

    from aiida_firecrest.transport import _dynamic_info_firecrest_version
    from conftest import MockFirecrest

    ctx = Mock()
    ctx.params = {
        "url": f"{firecrest_config.url}",
        "token_uri": f"{firecrest_config.token_uri}",
        "client_id": f"{firecrest_config.client_id}",
        "client_secret": f"{firecrest_config.client_secret}",
        "compute_resource": f"{firecrest_config.compute_resource}",
        "temp_directory": f"{firecrest_config.temp_directory}",
        "api_version": f"{firecrest_config.api_version}",
    }

    # should catch FIRECREST_VERSION if value is not provided
    result = _dynamic_info_firecrest_version(ctx, None, "None")
    assert isinstance(result, str)

    # should use the value if provided
    result = _dynamic_info_firecrest_version(ctx, None, "10.10.10")
    assert result == "10.10.10"

    # raise BadParameter if the version is not supported
    with pytest.raises(
        BadParameter, match=r".*FirecREST api version 0.0.0 is not supported.*"
    ):
        _dynamic_info_firecrest_version(ctx, None, "0.0.0")

    # raise BadParameter if the input is nonsense, latest, stable, etc.
    with pytest.raises(BadParameter, match=r".*Invalid input.*"):
        _dynamic_info_firecrest_version(ctx, None, "latest")

    # For the time-being, we only run this part of the mocked test.
    # TODO: Also run and mock for th server test
    if firecrest_config.mocked:
        # in case it could not get the version from the server, should abort, as it is a required parameter.
        with patch.object(
            MockFirecrest, "parameters", autospec=True
        ) as mock_parameters:
            mock_parameters.return_value = {"there is no version key": "bye bye"}
            with pytest.raises(Abort):
                result = _dynamic_info_firecrest_version(ctx, None, "None")
            capture = capsys.readouterr()
            assert "Could not get the version of the FirecREST server" in capture.out

        # in case the version recieved from the server is not supported, should abort, as no magic input can solve this problem.
        with patch.object(
            MockFirecrest, "parameters", autospec=True
        ) as mock_parameters:
            unsupported_version = "0.1"
            mock_parameters.return_value = {
                "general": [
                    {
                        "description": "FirecREST version.",
                        "name": "FIRECREST_VERSION",
                        "unit": "",
                        "value": f"v{unsupported_version}",
                    },
                ]
            }
            with pytest.raises(Abort):
                result = _dynamic_info_firecrest_version(ctx, None, "None")
            capture = capsys.readouterr()
            assert (
                f"FirecREST api version v{unsupported_version} is not supported"
                in capture.out
            )
