from pathlib import Path
import os

import pytest
from unittest.mock import Mock, patch
from click import BadParameter

from aiida import orm


@pytest.fixture(name="firecrest_computer")
def _firecrest_computer(myfirecrest, tmpdir: Path):
    """Create and return a computer configured for Firecrest.

    Note, the computer is not stored in the database.
    """

    # create a temp directory and set it as the workdir
    _scratch = tmpdir / "scratch"
    _temp_directory = tmpdir / "temp"
    _scratch.mkdir()
    _temp_directory.mkdir()

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
    assert transport.isfile(_scratch / "does_not_exist") == False

@pytest.mark.usefixtures("aiida_profile_clean")
def test_is_dir(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _scratch = tmpdir / "sampledir"
    _scratch.mkdir()

    assert transport.isdir(_scratch) == True
    assert transport.isdir(_scratch / "does_not_exist") == False

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


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get(firecrest_computer: orm.Computer, tmpdir: Path):
    """ 
    This is minimal test is to check if get() is raising errors as expected,
    and directing to getfile() and gettree() as expected.
    Mainly just checking error handeling and folder creation.
    """
    transport = firecrest_computer.get_transport()

    _remote = tmpdir / "remotedir"
    _local = tmpdir / "localdir"
    _remote.mkdir()
    _local.mkdir()
  

    # check if the code is directing to getfile() or gettree() as expected
    with patch.object(transport, 'gettree', autospec=True) as mock_gettree:
        transport.get(_remote, _local)
    mock_gettree.assert_called_once()

    with patch.object(transport, 'gettree', autospec=True) as mock_gettree:
        os.symlink(_remote, tmpdir / "dir_link")
        transport.get(tmpdir / "dir_link", _local)
    mock_gettree.assert_called_once()

    with patch.object(transport, 'getfile', autospec=True) as mock_getfile:
        Path(_remote / "file1").write_text("file1")
        transport.get(_remote / "file1", _local / "file1")
    mock_getfile.assert_called_once()

    with patch.object(transport, 'getfile', autospec=True) as mock_getfile:
        os.symlink(_remote / "file1", _remote / "file1_link")
        transport.get(_remote / "file1_link", _local / "file1_link")
    mock_getfile.assert_called_once()

    # raise if remote file/folder does not exist
    with pytest.raises(FileNotFoundError):
        transport.get(_remote / "does_not_exist", _local)
    transport.get(_remote / "does_not_exist", _local, ignore_nonexisting=True)
    
    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.get(_remote, Path(_local).relative_to(tmpdir))
    with pytest.raises(ValueError):
        transport.get(_remote / "file1", Path(_local).relative_to(tmpdir))


@pytest.mark.usefixtures("aiida_profile_clean")
def test_getfile(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()

    _remote = tmpdir / "remotedir"
    _local = tmpdir / "localdir"
    _remote.mkdir()
    _local.mkdir()

    Path(_remote / "file1").write_text("file1")
    Path(_remote / ".hidden").write_text(".hidden")
    os.symlink(_remote / "file1", _remote / "file1_link")
    

    # raise if remote file does not exist
    with pytest.raises(FileNotFoundError):
        transport.getfile(_remote / "does_not_exist", _local)

    # raise if localfilename not provided
    with pytest.raises(IsADirectoryError):
        transport.getfile(_remote / "file1", _local)

    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.getfile(_remote / "file1", Path(_local / "file1").relative_to(tmpdir))

    # don't mix up directory with file
    with pytest.raises(FileNotFoundError):
        transport.getfile(_remote, _local / "file1")
    
    # write where I tell you to
    transport.getfile(_remote / "file1", _local / "file1")
    transport.getfile(_remote / "file1", _local / "file1-prime")
    assert Path(_local / "file1").read_text() == "file1"
    assert Path(_local / "file1-prime").read_text() == "file1"

    # always overwrite
    transport.getfile(_remote / "file1", _local / "file1")
    assert Path(_local / "file1").read_text() == "file1"

    Path(_local / "file1").write_text("notfile1")

    transport.getfile(_remote / "file1", _local / "file1")
    assert Path(_local / "file1").read_text() == "file1"

    # don't skip hidden files
    transport.getfile(_remote / ".hidden", _local / ".hidden-prime")
    assert Path(_local / ".hidden-prime").read_text() == ".hidden"

    # follow links
    transport.getfile(_remote / "file1_link", _local / "file1_link")
    assert Path(_local / "file1_link").read_text() == "file1"
    assert not Path(_local / "file1_link").is_symlink()


@pytest.mark.usefixtures("aiida_profile_clean")
def test_gettree_notar(firecrest_computer: orm.Computer, tmpdir: Path, monkeypatch):
    """ 
    This test is to check if the gettree function is working as expected. Through non tar mode.
    payoff= False in this test, so just checking if getting files one by one is working as expected.
    """
    transport = firecrest_computer.get_transport()
    transport.payoff_override = False

    _remote = tmpdir / "remotedir"
    _local = tmpdir / "localdir"
    _remote.mkdir()
    _local.mkdir()
    # a typical tree
    Path(_remote / "dir1").mkdir()
    Path(_remote / "dir2").mkdir()
    Path(_remote / "file1").write_text("file1")
    Path(_remote / ".hidden").write_text(".hidden")
    Path(_remote / "dir1" / "file2").write_text("file2")
    Path(_remote / "dir2" / "file3").write_text("file3")
    # with symlinks to a file even if pointing to a relative path
    os.symlink(_remote / "file1", _remote / "dir1" / "file1_link")
    os.symlink(Path("../file1"), _remote / "dir1" / "file10_link")
    # with symlinks to a folder even if pointing to a relative path 
    os.symlink(_remote / "dir2",  _remote / "dir1" / "dir2_link")
    os.symlink(Path("../dir2" ), _remote / "dir1" / "dir20_link")
    

    # raise if remote file does not exist
    with pytest.raises(OSError):
        transport.gettree(_remote / "does_not_exist", _local)
    
    # raise if local is a file
    with pytest.raises(OSError):
        Path(tmpdir / "isfile").touch()
        transport.gettree(_remote, tmpdir / "isfile")
    
    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.gettree(_remote, Path(_local).relative_to(tmpdir))

    # If destination directory does not exists, AiiDA expects transport make the new path as root not _remote.name
    transport.gettree(_remote, _local / "newdir")
    _root = _local / "newdir"
    # tree should be copied recursively
    assert Path(_root / "file1").read_text() == "file1"
    assert Path(_root / ".hidden").read_text() == ".hidden"
    assert Path(_root / "dir1" / "file2").read_text() == "file2"
    assert Path(_root / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_root / "dir1" / "file10_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    assert not Path(_root / "dir1" / "file1_link").is_symlink() 
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert not Path(_root / "dir1" / "file10_link").is_symlink()
    assert not Path(_root / "dir1" / "dir20_link" / "file3").is_symlink()


    # If destination directory does exists, AiiDA expects transport make _remote.name and write into it
    # however this might have changed in the newer versions of AiiDA ~ 2.6.0 (IDK)
    transport.gettree(_remote, _local / "newdir")
    _root = _local / "newdir" / Path(_remote).name
    # tree should be copied recursively
    assert Path(_root / "file1").read_text() == "file1"
    assert Path(_root / ".hidden").read_text() == ".hidden"
    assert Path(_root / "dir1" / "file2").read_text() == "file2"
    assert Path(_root / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_root / "dir1" / "file10_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    assert not Path(_root / "dir1" / "file1_link").is_symlink() 
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert not Path(_root / "dir1" / "file10_link").is_symlink()
    assert not Path(_root / "dir1" / "dir20_link" / "file3").is_symlink()

@pytest.mark.usefixtures("aiida_profile_clean")
def test_gettree_bytar(firecrest_computer: orm.Computer, tmpdir: Path):
    """ 
    This test is to check if the gettree function is working as expected. Through non tar mode.
    bytar= True in this test.
    """
    transport = firecrest_computer.get_transport()
    transport.payoff_override = True

    _remote = tmpdir / "remotedir"
    _local = tmpdir / "localdir"
    _remote.mkdir()
    _local.mkdir()
    # a typical tree
    Path(_remote / "file1").write_text("file1")
    Path(_remote / ".hidden").write_text(".hidden")
    Path(_remote / "dir1").mkdir()
    Path(_remote / "dir1" / "file2").write_text("file2")
    # with symlinks
    Path(_remote / "dir2").mkdir()
    Path(_remote / "dir2" / "file3").write_text("file3")
    os.symlink(_remote / "file1", _remote / "dir1" / "file1_link")
    os.symlink(_remote / "dir2",  _remote / "dir1" / "dir2_link")
    # if symlinks are pointing to a relative path
    os.symlink(Path("../file1"), _remote / "dir1" / "file10_link")
    os.symlink(Path("../dir2" ), _remote / "dir1" / "dir20_link")
    
    

    # raise if remote file does not exist
    with pytest.raises(OSError):
        transport.gettree(_remote / "does_not_exist", _local)
    
    # raise if local is a file
    Path(tmpdir / "isfile").touch()
    with pytest.raises(OSError):
        transport.gettree(_remote, tmpdir / "isfile")
    
    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.gettree(_remote, Path(_local).relative_to(tmpdir))

    # If destination directory does not exists, AiiDA expects transport make the new path as root not _remote.name
    transport.gettree(_remote, _local / "newdir")
    _root = _local / "newdir"
    # tree should be copied recursively
    assert Path(_root / "file1").read_text() == "file1"
    assert Path(_root / ".hidden").read_text() == ".hidden"
    assert Path(_root / "dir1" / "file2").read_text() == "file2"
    assert Path(_root / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_root / "dir1" / "file10_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    assert not Path(_root / "dir1" / "file1_link").is_symlink() 
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert not Path(_root / "dir1" / "file10_link").is_symlink()
    assert not Path(_root / "dir1" / "dir20_link" / "file3").is_symlink()


    # If destination directory does exists, AiiDA expects transport make _remote.name and write into it
    # however this might have changed in the newer versions of AiiDA ~ 2.6.0 (IDK)
    transport.gettree(_remote, _local / "newdir")
    _root = _local / "newdir" / Path(_remote).name
    # tree should be copied recursively
    assert Path(_root / "file1").read_text() == "file1"
    assert Path(_root / ".hidden").read_text() == ".hidden"
    assert Path(_root / "dir1" / "file2").read_text() == "file2"
    assert Path(_root / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_root / "dir1" / "file10_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    assert not Path(_root / "dir1" / "file1_link").is_symlink() 
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert not Path(_root / "dir1" / "file10_link").is_symlink()
    assert not Path(_root / "dir1" / "dir20_link" / "file3").is_symlink()


