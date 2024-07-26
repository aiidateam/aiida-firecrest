from pathlib import Path
from unittest.mock import Mock

from aiida import orm
from click import BadParameter
import pytest


@pytest.mark.usefixtures("aiida_profile_clean")
def test_whoami(firecrest_computer: orm.Computer):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    assert transport.whoami() == "test_user"


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


def test_validate_temp_directory(myfirecrest, monkeypatch, tmpdir: Path):
    from aiida_firecrest.transport import _validate_temp_directory

    monkeypatch.setattr("click.echo", lambda x: None)
    # monkeypatch.setattr('click.BadParameter', lambda x: None)
    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    ctx = Mock()
    ctx.params = {
        "url": "http://test.com",
        "token_uri": "token_uri",
        "client_id": "client_id",
        "compute_resource": "compute_resource",
        "client_secret": secret_file.as_posix(),
        "small_file_size_mb": float(10),
    }

    # should raise if is_file
    Path(tmpdir / "crap.txt").touch()
    with pytest.raises(BadParameter):
        result = _validate_temp_directory(
            ctx, None, Path(tmpdir / "crap.txt").as_posix()
        )

    # should create the directory if it doesn't exist
    result = _validate_temp_directory(
        ctx, None, Path(tmpdir / "temp_on_server_directory").as_posix()
    )
    assert result == Path(tmpdir / "temp_on_server_directory").as_posix()
    assert Path(tmpdir / "temp_on_server_directory").exists()

    # should get a confirmation if the directory exists and is not empty
    Path(tmpdir / "temp_on_server_directory" / "crap.txt").touch()
    monkeypatch.setattr("click.confirm", lambda x: False)
    with pytest.raises(BadParameter):
        result = _validate_temp_directory(
            ctx, None, Path(tmpdir / "temp_on_server_directory").as_posix()
        )

    # should delete the content if I confirm
    monkeypatch.setattr("click.confirm", lambda x: True)
    result = _validate_temp_directory(
        ctx, None, Path(tmpdir / "temp_on_server_directory").as_posix()
    )
    assert result == Path(tmpdir / "temp_on_server_directory").as_posix()
    assert not Path(tmpdir / "temp_on_server_directory" / "crap.txt").exists()


def test_dynamic_info(myfirecrest, monkeypatch, tmpdir: Path):
    from aiida_firecrest.transport import _dynamic_info_direct_size

    monkeypatch.setattr("click.echo", lambda x: None)
    # monkeypatch.setattr('click.BadParameter', lambda x: None)
    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    ctx = Mock()
    ctx.params = {
        "url": "http://test.com",
        "token_uri": "token_uri",
        "client_id": "client_id",
        "compute_resource": "compute_resource",
        "client_secret": secret_file.as_posix(),
        "small_file_size_mb": float(10),
        "temp_directory": "temp_directory",
    }

    # should catch UTILITIES_MAX_FILE_SIZE if value is not provided
    result = _dynamic_info_direct_size(ctx, None, 0)
    assert result == 69

    # should use the value if provided
    # note: user cannot enter negative numbers anyways, click raise as this shoule be float not str
    result = _dynamic_info_direct_size(ctx, None, 10)
    assert result == 10
