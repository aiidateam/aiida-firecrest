from pathlib import Path
import os

import pytest
from unittest.mock import Mock
from click import BadParameter

from aiida import orm


@pytest.fixture(name="firecrest_computer")
def _firecrest_computer(myfirecrest, tmpdir: Path):
    """Create and return a computer configured for Firecrest.

    Note, the computer is not stored in the database.
    """

    # create a temp directory and set it as the workdir
    _scratch = tmpdir / "scratch"
    _scratch.mkdir()
    _temp_directory = tmpdir / "temp"

    Path(tmpdir / ".firecrest").mkdir()
    _secret_path = Path(tmpdir / ".firecrest/secret69")
    _secret_path.write_text("SECRET_STRING")

    computer = orm.Computer(
        label="test_computer",
        description="test computer",
        hostname="-",
        workdir=str(_scratch),
        transport_type="firecrest",
        scheduler_type="firecrest",
    )
    computer.set_minimum_job_poll_interval(5)
    computer.set_default_mpiprocs_per_machine(1)
    computer.configure(
        url=' https://URI',
        token_uri='https://TOKEN_URI',
        client_id='CLIENT_ID',
        client_secret=str(_secret_path),
        client_machine='MACHINE_NAME',
        small_file_size_mb=1.0,
        temp_directory=str(_temp_directory),
    )
    return computer


@pytest.mark.usefixtures("aiida_profile_clean")
def test_whoami(firecrest_computer: orm.Computer):
    """check if it is possible to determine the username."""
    transport = firecrest_computer.get_transport()
    assert transport.whoami() == 'test_user'

def test_create_secret_file_with_existing_file(tmpdir: Path):
    from aiida_firecrest.transport import FirecrestTransport 
    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    result = FirecrestTransport._create_secret_file(None, None, str(secret_file))
    assert isinstance(result, str)
    assert result == str(secret_file)
    assert Path(result).read_text() == "topsecret"

def test_create_secret_file_with_nonexistent_file(tmp_path):
    from aiida_firecrest.transport import FirecrestTransport 
    secret_file = tmp_path / "nonexistent"
    with pytest.raises(BadParameter):
        FirecrestTransport._create_secret_file(None, None, str(secret_file))

def test_create_secret_file_with_secret_value(tmp_path, monkeypatch):
    from aiida_firecrest.transport import FirecrestTransport 
    secret = "topsecret!~/"
    monkeypatch.setattr(Path, "expanduser", lambda x: tmp_path / str(x).lstrip("~/") if str(x).startswith("~/") else x)    
    result = FirecrestTransport._create_secret_file(None, None, secret)
    assert Path(result).parent.parts[-1]== ".firecrest"
    assert Path(result).read_text() == secret

def test_validate_temp_directory(myfirecrest, monkeypatch, tmpdir: Path):
    from aiida_firecrest.transport import FirecrestTransport 

    monkeypatch.setattr('click.echo', lambda x: None)
    # monkeypatch.setattr('click.BadParameter', lambda x: None)
    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    ctx = Mock()
    ctx.params = {
        'url': 'http://test.com',
        'token_uri': 'token_uri',
        'client_id': 'client_id',
        'client_machine': 'client_machine',
        'client_secret': secret_file.as_posix(),
        'small_file_size_mb': float(10)
    }

    # should raise if is_file
    Path(tmpdir / 'crap.txt').touch()
    with pytest.raises(BadParameter):
        result = FirecrestTransport._validate_temp_directory(ctx, None, Path(tmpdir /'crap.txt').as_posix())

    # should create the directory if it doesn't exist
    result = FirecrestTransport._validate_temp_directory(ctx, None, Path(tmpdir /'temp_on_server_directory').as_posix())
    assert result == Path(tmpdir /'temp_on_server_directory').as_posix()
    assert Path(tmpdir /'temp_on_server_directory').exists()

    # should get a confirmation if the directory exists and is not empty
    Path(tmpdir /'temp_on_server_directory' / 'crap.txt').touch()
    monkeypatch.setattr('click.confirm', lambda x: False)
    with pytest.raises(BadParameter):
        result = FirecrestTransport._validate_temp_directory(ctx, None, Path(tmpdir /'temp_on_server_directory').as_posix())

    # should delete the content if I confirm
    monkeypatch.setattr('click.confirm', lambda x: True)
    result = FirecrestTransport._validate_temp_directory(ctx, None, Path(tmpdir /'temp_on_server_directory').as_posix())
    assert result == Path(tmpdir /'temp_on_server_directory').as_posix()
    assert not Path(tmpdir /'temp_on_server_directory' / 'crap.txt').exists()

def test__dynamic_info(myfirecrest, monkeypatch, tmpdir: Path):
    from aiida_firecrest.transport import FirecrestTransport 

    monkeypatch.setattr('click.echo', lambda x: None)
    # monkeypatch.setattr('click.BadParameter', lambda x: None)
    secret_file = Path(tmpdir / "secret")
    secret_file.write_text("topsecret")
    ctx = Mock()
    ctx.params = {
        'url': 'http://test.com',
        'token_uri': 'token_uri',
        'client_id': 'client_id',
        'client_machine': 'client_machine',
        'client_secret': secret_file.as_posix(),
        'small_file_size_mb': float(10)
    }

    # should catch UTILITIES_MAX_FILE_SIZE if value is not provided
    result = FirecrestTransport._dynamic_info_direct_size(ctx, None, 0)
    assert result == 69

    # should use the value if provided
    # note: user cannot enter negative numbers anyways, click raise as this shoule be float not str
    result = FirecrestTransport._dynamic_info_direct_size(ctx, None, 10)
    assert result == 10


@pytest.mark.usefixtures("aiida_profile_clean")
def test_mkdir(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "sampledir"
    transport.mkdir(_scratch)
    assert _scratch.exists()

    _scratch = tmpdir / "sampledir2" / "subdir"
    transport.makedirs(_scratch)
    assert _scratch.exists()

@pytest.mark.usefixtures("aiida_profile_clean")
def test_is_file(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "samplefile"
    Path(_scratch).touch()
    
    assert transport.isfile(_scratch) == True
    assert transport.isfile("/does_not_exist") == False

@pytest.mark.usefixtures("aiida_profile_clean")
def test_is_dir(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "sampledir"
    _scratch.mkdir()

    assert transport.isdir(_scratch) == True
    assert transport.isdir("/does_not_exist") == False

@pytest.mark.usefixtures("aiida_profile_clean")
def test_normalize(firecrest_computer: orm.Computer):
    transport = firecrest_computer.get_transport()
    assert transport.normalize("/path/to/dir") == os.path.normpath("/path/to/dir")
    assert transport.normalize("path/to/dir") == os.path.normpath("path/to/dir")
    assert transport.normalize("path/to/dir/") == os.path.normpath("path/to/dir/")
    assert transport.normalize("path/to/../dir") == os.path.normpath("path/to/../dir")
    assert transport.normalize("path/to/../../dir") == os.path.normpath("path/to/../../dir")
    assert transport.normalize("path/to/../../dir/") == os.path.normpath("path/to/../../dir/")
    assert transport.normalize("path/to/../../dir/../") == os.path.normpath("path/to/../../dir/../")

@pytest.mark.usefixtures("aiida_profile_clean")
def test_remove(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "samplefile"
    Path(_scratch).touch()
    transport.remove(_scratch)
    assert not _scratch.exists()

    _scratch = tmpdir / "sampledir"
    _scratch.mkdir()
    transport.rmtree(_scratch)
    assert not _scratch.exists()

    _scratch = tmpdir / "sampledir"
    _scratch.mkdir()
    Path(_scratch / "samplefile").touch()
    with pytest.raises(OSError):
        transport.rmdir(_scratch)

    os.remove(_scratch / "samplefile")
    transport.rmdir(_scratch)
    assert not _scratch.exists()

@pytest.mark.usefixtures("aiida_profile_clean")
def test_symlink(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = Path(tmpdir / "samplefile-2sym")
    Path(_scratch).touch()
    _symlink = Path(tmpdir / "samplelink")
    transport.symlink(_scratch, _symlink)
    assert _symlink.is_symlink()
    assert _symlink.resolve() == _scratch


@pytest.mark.usefixtures("aiida_profile_clean")
def test_listdir(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "sampledir"
    _scratch.mkdir()
    # to test basics
    Path(_scratch / "file1").touch()
    Path(_scratch / "dir1").mkdir()
    Path(_scratch / ".hidden").touch()
    # to test recursive
    Path(_scratch / "dir1" / "file2").touch()

    assert set(transport.listdir(_scratch)) == set(["file1", "dir1", ".hidden"])
    assert set(transport.listdir(_scratch, recursive=True)) == set(["file1", "dir1", ".hidden",
                                                            "dir1/file2"])
    # to test symlink
    Path(_scratch / "dir1" / "dir2").mkdir()
    Path(_scratch / "dir1" / "dir2" / "file3").touch()
    os.symlink(_scratch / "dir1" / "dir2", _scratch / "dir2_link")
    os.symlink(_scratch / "dir1" / "file2", _scratch / "file_link")

    assert set(transport.listdir(_scratch, recursive=True)) == set(["file1", "dir1", ".hidden",
                                                            "dir1/file2", "dir1/dir2", "dir1/dir2/file3",
                                                            "dir2_link", "file_link"])

    assert set(transport.listdir(_scratch / "dir2_link", recursive=False)) == set(["file3"])

