from pathlib import Path
import os

import pytest
from unittest.mock import patch

from aiida import orm

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


