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
    Path(tmpdir / "_.txt").touch()
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
    assert transport._cwd.joinpath(_remote / "nonexisting").exists()

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
    assert not transport._cwd.joinpath(
        _remote / "temp_on_server_directory" / "_.txt"
    ).exists()


def test_dynamic_info(firecrest_config, monkeypatch, tmpdir: Path):
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
