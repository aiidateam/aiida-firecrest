###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""
Note: order of tests is important, as some tests are dependent on the previous ones.

Note:
FirecREST cannot send over empty files, therefore I do `Path.write_text("touch")` instead of Path.touch() in all tests.
"""

import os
from pathlib import Path
from unittest.mock import patch

from aiida import orm
import pytest

from aiida_firecrest.transport import FcPath


@pytest.mark.usefixtures("aiida_profile_clean")
def test_mkdir(firecrest_computer: orm.Computer):
    transport = firecrest_computer.get_transport()
    tmpdir = Path(transport._temp_directory)

    _scratch = tmpdir / "sampledir2" / "subdir"
    transport.makedirs(_scratch)
    assert transport.path_exists(_scratch)

    _scratch = tmpdir / "sampledir"
    transport.mkdir(_scratch)
    assert transport.path_exists(_scratch)

    # raise if directory already exists
    with pytest.raises(OSError):
        transport.mkdir(tmpdir / "sampledir")
    with pytest.raises(OSError):
        transport.makedirs(tmpdir / "sampledir2")

    # don't raise if directory already exists and ignore_existing is True
    transport.mkdir(tmpdir / "sampledir", ignore_existing=True)
    transport.makedirs(tmpdir / "sampledir2", ignore_existing=True)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_is_dir(firecrest_computer: orm.Computer):
    transport = firecrest_computer.get_transport()
    tmpdir = Path(transport._temp_directory)

    _scratch = tmpdir / "sampledir"
    transport.mkdir(_scratch)

    assert transport.isdir(_scratch)
    assert not transport.isdir(_scratch / "does_not_exist")


@pytest.mark.usefixtures("aiida_profile_clean")
def test_normalize(firecrest_computer: orm.Computer):
    transport = firecrest_computer.get_transport()
    assert transport.normalize("/path/to/dir") == os.path.normpath("/path/to/dir")
    assert transport.normalize("path/to/dir") == os.path.normpath("path/to/dir")
    assert transport.normalize("path/to/dir/") == os.path.normpath("path/to/dir/")
    assert transport.normalize("path/to/../dir") == os.path.normpath("path/to/../dir")
    assert transport.normalize("path/to/../../dir") == os.path.normpath(
        "path/to/../../dir"
    )
    assert transport.normalize("path/to/../../dir/") == os.path.normpath(
        "path/to/../../dir/"
    )
    assert transport.normalize("path/to/../../dir/../") == os.path.normpath(
        "path/to/../../dir/../"
    )


@pytest.mark.usefixtures("aiida_profile_clean")
def test_putfile_getfile(firecrest_computer: orm.Computer, tmpdir: Path):
    """
    Note: putfile() and getfile() should be tested together, as they are dependent on each other.
    It's written this way to be compatible with the real server testings.
    """
    transport = firecrest_computer.get_transport()
    tmpdir_remote = FcPath(transport._temp_directory)

    _remote = tmpdir_remote / "remotedir"
    _local = tmpdir / "localdir"
    _local_download = tmpdir / "download"
    transport.mkdir(_remote)
    _local.mkdir()
    _local_download.mkdir()

    Path(_local / "file1").write_text("file1")
    Path(_local / ".hidden").write_text(".hidden")
    os.symlink(_local / "file1", _local / "file1_link")

    # raise if file does not exist
    with pytest.raises(FileNotFoundError):
        transport.putfile(_local / "does_not_exist", _remote / "file1")
        transport.getfile(_remote / "does_not_exist", _local)

    # raise if filename is not provided
    with pytest.raises(ValueError):
        transport.putfile(_local / "file1", _remote)
        transport.getfile(_remote / "file1", _local)

    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.putfile(Path(_local / "file1").relative_to(tmpdir), _remote / "file1")
        transport.getfile(_remote / "file1", Path(_local / "file1").relative_to(tmpdir))

    # don't mix up directory with file
    with pytest.raises(ValueError):
        transport.putfile(_local, _remote / "file1")
        transport.getfile(_remote, _local / "file1")

    # write where I tell you to
    # note: if you change this block, you need to change the the following two blocks as well
    transport.putfile(_local / "file1", _remote / "file1")
    transport.putfile(_local / "file1", _remote / "file1-prime")

    transport.getfile(_remote / "file1", _local_download / "file1")
    transport.getfile(_remote / "file1-prime", _local_download / "differentName")

    assert Path(_local_download / "file1").read_text() == "file1"
    assert Path(_local_download / "differentName").read_text() == "file1"

    # always overwrite for putfile
    # this block assumes "file1" has already been uploaded with the content "file1".
    #   for efficiency reasons (when the test is run against a real server). I didn't that repeat here.
    Path(_local / "file1").write_text("notfile1")
    transport.putfile(_local / "file1", _remote / "file1")
    transport.getfile(_remote / "file1", _local_download / "file1_uploaded")
    assert Path(_local_download / "file1_uploaded").read_text() == "notfile1"

    # always overwrite for getfile
    # this block assumes "file1" has already been downloaded with the content "notfile1".
    #   for efficiency reasons (when the test is run against a real server). I didn't that repeat here.
    transport.getfile(_remote / "file1", _local_download / "file1")
    assert Path(_local_download / "file1").read_text() == "notfile1"

    # don't skip hidden files
    transport.putfile(_local / ".hidden", _remote / ".hidden")
    transport.getfile(_remote / ".hidden", _local_download / ".hidden")
    assert Path(_local_download / ".hidden").read_text() == ".hidden"

    # follow links
    #   for putfile()
    Path(_local / "file1").write_text("file1")
    transport.putfile(_local / "file1_link", _remote / "file1_link")
    assert not transport.is_symlink(
        _remote / "file1_link"
    )  # should be copied as a file
    transport.getfile(_remote / "file1_link", _local_download / "file1_link")
    assert Path(_local_download / "file1_link").read_text() == "file1"
    #   for getfile()
    transport.putfile(_local / "file1", _remote / "file1")
    transport.symlink(_remote / "file1", _remote / "remote_link")
    transport.getfile(_remote / "remote_link", _local_download / "remote_link")
    assert not Path(_local_download / "remote_link").is_symlink()
    assert Path(_local_download / "remote_link").read_text() == "file1"

    # test the self.checksum_check
    with patch.object(
        transport, "_validate_checksum", autospec=True
    ) as mock_validate_checksum:
        transport.checksum_check = True
        transport.putfile(_local / "file1", _remote / "file1_checksum")
        transport.getfile(
            _remote / "file1_checksum", _local_download / "file1_checksum"
        )
    assert mock_validate_checksum.call_count == 2

    with patch.object(
        transport, "_validate_checksum", autospec=True
    ) as mock_validate_checksum:
        transport.checksum_check = False
        transport.putfile(_local / "file1", _remote / "file1_checksum2")
        transport.getfile(
            _remote / "file1_checksum2", _local_download / "file1_checksum2"
        )
    assert mock_validate_checksum.call_count == 0


@pytest.mark.usefixtures("aiida_profile_clean")
def test_remove(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()
    tmpdir_remote = Path(transport._temp_directory)

    _remote = tmpdir_remote
    _local = tmpdir

    Path(_local / "samplefile").write_text("touch")

    # remove a non-empty directory with rmtree()
    _scratch = FcPath(_remote / "sampledir")
    transport.mkdir(_scratch)
    transport.putfile(_local / "samplefile", _remote / "sampledir" / "samplefile")
    transport.rmtree(_scratch)
    assert not transport.path_exists(_scratch)

    # remove a non-empty directory should raise with rmdir()
    transport.mkdir(_remote / "sampledir")
    transport.putfile(_local / "samplefile", _remote / "sampledir" / "samplefile")
    with pytest.raises(OSError):
        transport.rmdir(_remote / "sampledir")

    # remove a file with remove()
    transport.remove(_remote / "sampledir" / "samplefile")
    assert not transport.path_exists(_remote / "sampledir" / "samplefile")

    # remove a empty directory with rmdir
    transport.rmdir(_remote / "sampledir")
    assert not transport.path_exists(_scratch)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_is_file(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()
    tmpdir_remote = Path(transport._temp_directory)

    _remote = tmpdir_remote
    _local = tmpdir

    Path(_local / "samplefile").write_text("touch")
    transport.putfile(_local / "samplefile", _remote / "samplefile")
    assert transport.isfile(_remote / "samplefile")
    assert not transport.isfile(_remote / "does_not_exist")


@pytest.mark.usefixtures("aiida_profile_clean")
def test_symlink(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()
    tmpdir_remote = Path(transport._temp_directory)

    _remote = tmpdir_remote
    _local = tmpdir

    Path(_local / "samplefile").write_text("touch")
    transport.putfile(_local / "samplefile", _remote / "samplefile")
    transport.symlink(_remote / "samplefile", _remote / "samplelink")

    assert transport.is_symlink(_remote / "samplelink")
    # TODO: check if the symlink is pointing to the correct file
    # for this we need further development of FcPath.resolve()
    # assert _symlink.resolve() == _remote / "samplefile"


@pytest.mark.usefixtures("aiida_profile_clean")
def test_listdir(firecrest_computer: orm.Computer, tmpdir: Path):
    transport = firecrest_computer.get_transport()
    tmpdir_remote = Path(transport._temp_directory)

    _remote = tmpdir_remote
    _local = tmpdir

    # test basic & recursive
    Path(_local / "file1").write_text("touch")
    Path(_local / "dir1").mkdir()
    Path(_local / ".hidden").write_text("touch")
    Path(_local / "dir1" / "file2").write_text("touch")
    transport.putfile(_local / "file1", _remote / "file1")
    transport.mkdir(_remote / "dir1")
    transport.putfile(_local / "dir1" / "file2", _remote / "dir1" / "file2")
    transport.putfile(_local / ".hidden", _remote / ".hidden")

    assert set(transport.listdir(_remote)) == {"file1", "dir1", ".hidden"}
    assert set(transport.listdir(_remote, recursive=True)) == {
        "file1",
        "dir1",
        ".hidden",
        "dir1/file2",
    }

    # to test symlink
    Path(_local / "dir1" / "dir2").mkdir()
    Path(_local / "dir1" / "dir2" / "file3").write_text("touch")
    transport.mkdir(_remote / "dir1" / "dir2")
    transport.putfile(
        _local / "dir1" / "dir2" / "file3", _remote / "dir1" / "dir2" / "file3"
    )
    transport.symlink(_remote / "dir1" / "dir2", _remote / "dir2_link")
    transport.symlink(_remote / "dir1" / "file2", _remote / "file_link")

    assert set(transport.listdir(_remote, recursive=True)) == {
        "file1",
        "dir1",
        ".hidden",
        "dir1/file2",
        "dir1/dir2",
        "dir1/dir2/file3",
        "dir2_link",
        "file_link",
    }
    # TODO: The following assert is not working as expected when testing against a real server,
    # see the open issue on FirecREST: https://github.com/eth-cscs/firecrest/issues/205
    assert set(transport.listdir(_remote / "dir2_link", recursive=False)) == {"file3"}


@pytest.mark.usefixtures("aiida_profile_clean")
def test_put(firecrest_computer: orm.Computer, tmpdir: Path):
    """
    This is minimal test is to check if put() is raising errors as expected,
    and directing to putfile() and puttree() as expected.
    Mainly just checking error handeling and folder creation.
    For faster testing, we mock the subfucntions and don't actually do it.
    """
    transport = firecrest_computer.get_transport()
    tmpdir_remote = Path(transport._temp_directory)

    _remote = tmpdir_remote / "remotedir"
    _local = tmpdir / "localdir"
    transport.mkdir(_remote)
    _local.mkdir()

    # check if the code is directing to putfile() or puttree() as expected
    with patch.object(transport, "puttree", autospec=True) as mock_puttree:
        transport.put(_local, _remote)
    mock_puttree.assert_called_once()

    with patch.object(transport, "puttree", autospec=True) as mock_puttree:
        os.symlink(_local, tmpdir / "dir_link")
        transport.put(tmpdir / "dir_link", _remote)
    mock_puttree.assert_called_once()

    with patch.object(transport, "putfile", autospec=True) as mock_putfile:
        Path(_local / "file1").write_text("file1")
        transport.put(_local / "file1", _remote / "file1")
    mock_putfile.assert_called_once()

    with patch.object(transport, "putfile", autospec=True) as mock_putfile:
        os.symlink(_local / "file1", _local / "file1_link")
        transport.put(_local / "file1_link", _remote / "file1_link")
    mock_putfile.assert_called_once()

    # raise if local file/folder does not exist
    with pytest.raises(FileNotFoundError):
        transport.put(_local / "does_not_exist", _remote)
    transport.put(_local / "does_not_exist", _remote, ignore_nonexisting=True)

    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.put(Path(_local).relative_to(tmpdir), _remote)
    with pytest.raises(ValueError):
        transport.put(Path(_local / "file1").relative_to(tmpdir), _remote)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_get(firecrest_computer: orm.Computer, tmpdir: Path):
    """
    This is minimal test is to check if get() is raising errors as expected,
    and directing to getfile() and gettree() as expected.
    Mainly just checking error handeling and folder creation.
    For faster testing, we mock the subfucntions and don't actually do it.
    """
    transport = firecrest_computer.get_transport()
    tmpdir_remote = FcPath(transport._temp_directory)

    _remote = tmpdir_remote / "remotedir"
    _local = tmpdir / "localdir"
    transport.mkdir(_remote)
    _local.mkdir()

    # check if the code is directing to getfile() or gettree() as expected
    with patch.object(transport, "gettree", autospec=True) as mock_gettree:
        transport.get(_remote, _local)
    mock_gettree.assert_called_once()

    with patch.object(transport, "gettree", autospec=True) as mock_gettree:
        transport.symlink(_remote, tmpdir_remote / "dir_link")
        transport.get(tmpdir_remote / "dir_link", _local)
    mock_gettree.assert_called_once()

    with patch.object(transport, "getfile", autospec=True) as mock_getfile:
        Path(_local / "file1").write_text("file1")
        transport.putfile(_local / "file1", _remote / "file1")
        transport.get(_remote / "file1", _local / "file1")
    mock_getfile.assert_called_once()

    with patch.object(transport, "getfile", autospec=True) as mock_getfile:
        transport.symlink(_remote / "file1", _remote / "file1_link")
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


@pytest.mark.timeout(120)
@pytest.mark.parametrize("payoff", [True, False])
@pytest.mark.usefixtures("aiida_profile_clean")
def test_puttree(firecrest_computer: orm.Computer, tmpdir: Path, payoff: bool):
    """
    This test is to check `puttree` through both recursive transfer and tar mode.
    payoff= False would put files one by one.
    payoff= True  would use the tar mode.
    Note: this test depends on a functional getfile() method, for consistency with the real server tests.
      Before running this test, make sure getfile() is working, which is tested in `test_putfile_getfile`.
    """
    transport = firecrest_computer.get_transport()
    transport.payoff_override = payoff

    # Note:
    # SSH transport behaviour
    # transport.put('somepath/abc', 'someremotepath/') == transport.put('somepath/abc', 'someremotepath')
    # transport.put('somepath/abc', 'someremotepath/') != transport.put('somepath/abc/', 'someremotepath/')
    # transport.put('somepath/abc', 'someremotepath/67') --> if 67 not exist, create and move content abc
    #  inside it (someremotepath/67)
    # transport.put('somepath/abc', 'someremotepath/67') --> if 67 exist, create abc inside it (someremotepath/67/abc)
    # transport.put('somepath/abc', 'someremotepath/6889/abc')  -->  useless Error: OSError
    # Weired
    # SSH "bug":
    # transport.put('somepath/abc', 'someremotepath/') --> assuming someremotepath exists, make abc
    # while
    # transport.put('somepath/abc/', 'someremotepath/') --> assuming someremotepath exists, OSError:
    # cannot make someremotepath

    tmpdir_remote = Path(transport._temp_directory)
    _remote = tmpdir_remote / "remotedir"
    _local = tmpdir / "localdir"
    _local_download = tmpdir / "download"
    transport.mkdir(_remote)
    _local.mkdir()
    _local_download.mkdir()
    # a typical tree
    Path(_local / "dir1").mkdir()
    Path(_local / "dir2").mkdir()
    Path(_local / "file1").write_text("file1")
    Path(_local / ".hidden").write_text(".hidden")
    Path(_local / "dir1" / "file2").write_text("file2")
    Path(_local / "dir2" / "file3").write_text("file3")
    # with symlinks to a file even if pointing to a relative path
    os.symlink(_local / "file1", _local / "dir1" / "file1_link")
    os.symlink(Path("../file1"), _local / "dir1" / "file10_link")
    # with symlinks to a folder even if pointing to a relative path
    os.symlink(_local / "dir2", _local / "dir1" / "dir2_link")
    os.symlink(Path("../dir2"), _local / "dir1" / "dir20_link")

    # raise if local file does not exist
    with pytest.raises(OSError):
        transport.puttree(_local / "does_not_exist", _remote)

    # raise if local is a file
    with pytest.raises(ValueError):
        Path(tmpdir / "isfile").write_text("touch")
        transport.puttree(tmpdir / "isfile", _remote)

    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.puttree(Path(_local).relative_to(tmpdir), _remote)

    # If destination directory does not exists, AiiDA expects transport make the new path as root not using _local.name
    transport.puttree(_local, _remote / "newdir")
    _root = _remote / "newdir"

    # GET block: to retrieve the files we depend on a functional getfile(),
    #   this limitation is a price to pay for real server testing.
    transport.getfile(_root / "file1", _local_download / "file1")
    transport.getfile(_root / ".hidden", _local_download / ".hidden")
    Path(_local_download / "dir1").mkdir()
    Path(_local_download / "dir2").mkdir()
    transport.getfile(_root / "dir1" / "file2", _local_download / "dir1" / "file2")
    transport.getfile(_root / "dir2" / "file3", _local_download / "dir2" / "file3")
    # note links should have been dereferenced while uploading
    transport.getfile(
        _root / "dir1" / "file1_link", _local_download / "dir1" / "file1_link"
    )
    Path(_local_download / "dir1" / "dir2_link").mkdir()
    transport.getfile(
        _root / "dir1" / "dir2_link" / "file3",
        _local_download / "dir1" / "dir2_link" / "file3",
    )
    transport.getfile(
        _root / "dir1" / "file10_link", _local_download / "dir1" / "file10_link"
    )
    Path(_local_download / "dir1" / "dir20_link").mkdir()
    transport.getfile(
        _root / "dir1" / "dir20_link" / "file3",
        _local_download / "dir1" / "dir20_link" / "file3",
    )
    # End of GET block

    # ASSERT block: tree should be copied recursively
    assert Path(_local_download / "file1").read_text() == "file1"
    assert Path(_local_download / ".hidden").read_text() == ".hidden"
    assert Path(_local_download / "dir1" / "file2").read_text() == "file2"
    assert Path(_local_download / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_local_download / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_local_download / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_local_download / "dir1" / "file10_link").read_text() == "file1"
    assert (
        Path(_local_download / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    )
    assert not transport.is_symlink(_root.joinpath("dir1", "file1_link"))
    assert not transport.is_symlink(_root.joinpath("dir1", "dir2_link", "file3"))

    assert not transport.is_symlink(_root.joinpath("dir1", "file10_link"))
    assert not transport.is_symlink(_root.joinpath("dir1", "dir20_link", "file3"))
    # End of ASSERT block

    # If destination directory does exists, AiiDA expects transport make _local.name and write into it
    # however this might have changed in the newer versions of AiiDA ~ 2.6.0 (IDK)
    transport.puttree(_local, _remote / "newdir")
    _root = _remote / "newdir" / Path(_local).name

    # GET block: to retrieve the files we depend on a functional getfile(),
    #   this limitation is a price to pay for real server testing.
    _local_download = tmpdir / "download2"
    _local_download.mkdir()
    transport.getfile(_root / "file1", _local_download / "file1")
    transport.getfile(_root / ".hidden", _local_download / ".hidden")
    Path(_local_download / "dir1").mkdir()
    Path(_local_download / "dir2").mkdir()
    transport.getfile(_root / "dir1" / "file2", _local_download / "dir1" / "file2")
    transport.getfile(_root / "dir2" / "file3", _local_download / "dir2" / "file3")
    # note links should have been dereferenced while uploading
    transport.getfile(
        _root / "dir1" / "file1_link", _local_download / "dir1" / "file1_link"
    )
    Path(_local_download / "dir1" / "dir2_link").mkdir()
    transport.getfile(
        _root / "dir1" / "dir2_link" / "file3",
        _local_download / "dir1" / "dir2_link" / "file3",
    )
    transport.getfile(
        _root / "dir1" / "file10_link", _local_download / "dir1" / "file10_link"
    )
    Path(_local_download / "dir1" / "dir20_link").mkdir()
    transport.getfile(
        _root / "dir1" / "dir20_link" / "file3",
        _local_download / "dir1" / "dir20_link" / "file3",
    )
    # End of GET block

    # ASSERT block: tree should be copied recursively
    assert Path(_local_download / "file1").read_text() == "file1"
    assert Path(_local_download / ".hidden").read_text() == ".hidden"
    assert Path(_local_download / "dir1" / "file2").read_text() == "file2"
    assert Path(_local_download / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert Path(_local_download / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_local_download / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert Path(_local_download / "dir1" / "file10_link").read_text() == "file1"
    assert (
        Path(_local_download / "dir1" / "dir20_link" / "file3").read_text() == "file3"
    )
    assert not transport.is_symlink(_root.joinpath("dir1", "file1_link"))
    assert not transport.is_symlink(_root.joinpath("dir1", "dir2_link", "file3"))

    assert not transport.is_symlink(_root.joinpath("dir1", "file10_link"))
    assert not transport.is_symlink(_root.joinpath("dir1", "dir20_link", "file3"))
    # End of ASSERT block


@pytest.mark.timeout(120)
@pytest.mark.parametrize("payoff", [True, False])
@pytest.mark.usefixtures("aiida_profile_clean")
def test_gettree(firecrest_computer: orm.Computer, tmpdir: Path, payoff: bool):
    """
    This test is to check `gettree` through both recursive transfer and tar mode.
    payoff= False would get files one by one.
    payoff= True  would use the tar mode.
    Note: this test depends on a functional putfile() method, for consistency with the real server tests.
      Before running this test, make sure putfile() is working, which is tested in `test_putfile_getfile`.
    """
    transport = firecrest_computer.get_transport()
    transport.payoff_override = payoff

    # Note:
    # SSH transport behaviour, abc is a directory
    # transport.get('somepath/abc', 'someremotepath/') == transport.get('somepath/abc', 'someremotepath')
    # transport.get('somepath/abc', 'someremotepath/') == transport.get('somepath/abc/', 'someremotepath/')
    # transport.get('someremotepath/abc', 'somepath/abc')--> if abc exist, create abc inside it ('somepath/abc/abc')
    # transport.get('someremotepath/abc', 'somepath/abc')--> if abc noexist,create abc inside it ('somepath/abc')
    # transport.get('somepath/abc', 'someremotepath/6889/abc') --> create everything, make_parent = True
    tmpdir_remote = Path(transport._temp_directory)
    _remote = tmpdir_remote / "remotedir"
    transport.mkdir(_remote)
    _local = tmpdir / "localdir"
    _local_for_upload = tmpdir / "forupload"
    _local.mkdir()
    _local_for_upload.mkdir()

    # a typical tree with symlinks
    Path(_local_for_upload / "file1").write_text("file1")
    Path(_local_for_upload / ".hidden").write_text(".hidden")
    Path(_local_for_upload / "dir1").mkdir()
    Path(_local_for_upload / "dir1" / "file2").write_text("file2")
    Path(_local_for_upload / "dir2").mkdir()
    Path(_local_for_upload / "dir2" / "file3").write_text("file3")
    transport.putfile(_local_for_upload / "file1", _remote / "file1")
    transport.putfile(_local_for_upload / ".hidden", _remote / ".hidden")
    transport.mkdir(_remote / "dir1")
    transport.mkdir(_remote / "dir2")
    transport.putfile(_local_for_upload / "dir1" / "file2", _remote / "dir1" / "file2")
    transport.putfile(_local_for_upload / "dir2" / "file3", _remote / "dir2" / "file3")

    transport.symlink(_remote / "file1", _remote / "dir1" / "file1_link")
    transport.symlink(_remote / "dir2", _remote / "dir1" / "dir2_link")
    # I cannot create & check relative links, because we don't have access on the server side
    # os.symlink(Path("../file1"), _local_for_upload / "dir1" / "file10_link")
    # os.symlink(Path("../dir2"), _local_for_upload / "dir1" / "dir20_link")

    # raise if remote file does not exist
    with pytest.raises(OSError):
        transport.gettree(_remote / "does_not_exist", _local)

    # raise if local is a file
    Path(tmpdir / "isfile").write_text("touch")
    with pytest.raises(OSError):
        transport.gettree(_remote, tmpdir / "isfile")

    # raise if localpath is relative
    with pytest.raises(ValueError):
        transport.gettree(_remote, Path(_local).relative_to(tmpdir))

    # If destination directory does not exists, AiiDA expects from transport to make a new path as root not _remote.name
    transport.gettree(_remote, _local / "newdir")
    _root = _local / "newdir"
    # tree should be copied recursively
    assert Path(_root / "file1").read_text() == "file1"
    assert Path(_root / ".hidden").read_text() == ".hidden"
    assert Path(_root / "dir1" / "file2").read_text() == "file2"
    assert Path(_root / "dir2" / "file3").read_text() == "file3"
    # symlink should be followed
    assert not Path(_root / "dir1" / "file1_link").is_symlink()
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"

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
    assert not Path(_root / "dir1" / "file1_link").is_symlink()
    assert not Path(_root / "dir1" / "dir2_link" / "file3").is_symlink()
    assert Path(_root / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_root / "dir1" / "dir2_link" / "file3").read_text() == "file3"


@pytest.mark.parametrize("to_test", ["copy", "copytree"])
@pytest.mark.usefixtures("aiida_profile_clean")
def test_copy(firecrest_computer: orm.Computer, tmpdir: Path, to_test: str):
    """
    This test is to check `copy` and `copytree`,
      `copyfile` is that is only for files, and it is tested in `test_putfile_getfile`.
    Also raising errors are somewhat method specific.
    Note: this test depends on functional getfile() and putfile() methods, for consistency with the real server tests.
      Before running this test, make sure `test_putfile_getfile` has passed.
    """
    transport = firecrest_computer.get_transport()
    if to_test == "copy":
        testing = transport.copy
    elif to_test == "copytree":
        testing = transport.copytree

    tmpdir_remote = Path(transport._temp_directory)
    _remote_1 = tmpdir_remote / "remotedir_1"
    _remote_2 = tmpdir_remote / "remotedir_2"
    transport.mkdir(_remote_1)
    transport.mkdir(_remote_2)
    _for_upload = tmpdir

    # raise if source or destination does not exist
    with pytest.raises(FileNotFoundError):
        testing(_remote_1 / "does_not_exist", _remote_2)
    with pytest.raises(FileNotFoundError):
        testing(_remote_1, _remote_2 / "does_not_exist")

    # a typical tree with symlinks to a file and a folder
    Path(_for_upload / "file1").write_text("file1")
    Path(_for_upload / ".hidden").write_text(".hidden")
    Path(_for_upload / "dir1").mkdir()
    Path(_for_upload / "dir2").mkdir()
    Path(_for_upload / "dir1" / "file2").write_text("file2")
    Path(_for_upload / "dir2" / "file3").write_text("file3")

    transport.putfile(_for_upload / "file1", _remote_1 / "file1")
    transport.putfile(_for_upload / ".hidden", _remote_1 / ".hidden")
    transport.mkdir(_remote_1 / "dir1")
    transport.mkdir(_remote_1 / "dir2")
    transport.putfile(_for_upload / "dir1" / "file2", _remote_1 / "dir1" / "file2")
    transport.putfile(_for_upload / "dir2" / "file3", _remote_1 / "dir2" / "file3")
    transport.symlink(_remote_1 / "file1", _remote_1 / "dir1" / "file1_link")
    transport.symlink(_remote_1 / "dir2", _remote_1 / "dir1" / "dir2_link")
    # I cannot create & check relative links, because we don't have access on the server side
    # os.symlink(Path("../file1"), _remote_1 / "dir1" / "file10_link")
    # os.symlink(Path("../dir2"), _remote_1 / "dir1" / "dir20_link")
    testing(_remote_1, _remote_2)

    _root_2 = _remote_2 / Path(_remote_1).name

    # GET block: to retrieve the files we depend on a functional getfile(),
    #   this limitation is a price to pay for real server testing.
    _local_download = tmpdir / "download1"
    _local_download.mkdir()
    transport.getfile(_root_2 / "file1", _local_download / "file1")
    transport.getfile(_root_2 / ".hidden", _local_download / ".hidden")
    Path(_local_download / "dir1").mkdir()
    Path(_local_download / "dir2").mkdir()
    transport.getfile(_root_2 / "dir1" / "file2", _local_download / "dir1" / "file2")
    transport.getfile(_root_2 / "dir2" / "file3", _local_download / "dir2" / "file3")
    # note links should have been dereferenced while uploading
    transport.getfile(
        _root_2 / "dir1" / "file1_link", _local_download / "dir1" / "file1_link"
    )
    Path(_local_download / "dir1" / "dir2_link").mkdir()
    # TODO: The following is not working as expected when testing against a real server, see open issue on FirecREST:
    # https://github.com/eth-cscs/firecrest/issues/205
    transport.getfile(
        _root_2 / "dir1" / "dir2_link" / "file3",
        _local_download / "dir1" / "dir2_link" / "file3",
    )
    # End of GET block

    # ASSERT block: tree should be copied recursively symlink should be followed
    assert Path(_local_download / "dir1").exists()
    assert Path(_local_download / "dir2").exists()
    assert Path(_local_download / "file1").read_text() == "file1"
    assert Path(_local_download / ".hidden").read_text() == ".hidden"
    assert Path(_local_download / "dir1" / "file2").read_text() == "file2"
    assert Path(_local_download / "dir2" / "file3").read_text() == "file3"
    assert Path(_local_download / "dir1" / "dir2_link").exists()
    assert Path(_local_download / "dir1" / "file1_link").read_text() == "file1"
    assert Path(_local_download / "dir1" / "dir2_link" / "file3").read_text() == "file3"
    assert transport.is_symlink(_root_2.joinpath("dir1", "file1_link"))
    assert transport.is_symlink(_root_2.joinpath("dir1", "dir2_link"))

    # End of ASSERT block

    # raise if source is inappropriate
    if to_test == "copytree":
        with pytest.raises(ValueError):
            testing(_remote_1 / "file1", _remote_2)


@pytest.mark.usefixtures("aiida_profile_clean")
def test_copyfile(firecrest_computer: orm.Computer, tmpdir: Path):
    """
    Note: this test depends on functional getfile() and putfile() methods, for consistency with the real server tests.
      Before running this test, make sure `test_putfile_getfile` has passed.
    """

    transport = firecrest_computer.get_transport()
    testing = transport.copyfile

    tmpdir_remote = Path(transport._temp_directory)
    _remote_1 = tmpdir_remote / "remotedir_1"
    _remote_2 = tmpdir_remote / "remotedir_2"
    _for_upload = tmpdir / "forUpload"
    _for_download = tmpdir / "forDownload"
    _for_upload.mkdir()
    _for_download.mkdir()
    transport.mkdir(_remote_1)
    transport.mkdir(_remote_2)

    # raise if source or destination does not exist
    with pytest.raises(FileNotFoundError):
        testing(_remote_1 / "does_not_exist", _remote_2)
    # in this case don't raise and just create the file
    Path(_for_upload / "_").write_text("touch")
    transport.putfile(_for_upload / "_", _remote_1 / "_")
    testing(_remote_1 / "_", _remote_2 / "does_not_exist")

    # raise if source is not a file
    with pytest.raises(ValueError):
        testing(_remote_1, _remote_2)

    # main functionality, including symlinks
    Path(_for_upload / "file1").write_text("file1")
    Path(_for_upload / ".hidden").write_text(".hidden")
    Path(_for_upload / "notfile1").write_text("notfile1")
    transport.putfile(_for_upload / "file1", _remote_1 / "file1")
    transport.putfile(_for_upload / ".hidden", _remote_1 / ".hidden")
    transport.putfile(_for_upload / "notfile1", _remote_1 / "notfile1")
    transport.symlink(_remote_1 / "file1", _remote_1 / "file1_link")

    # write where I tell you to
    testing(_remote_1 / "file1", _remote_2 / "file1")
    transport.getfile(_remote_2 / "file1", _for_download / "file1")
    assert Path(_for_download / "file1").read_text() == "file1"

    # always overwrite
    testing(_remote_1 / "notfile1", _remote_2 / "file1")
    transport.getfile(_remote_2 / "file1", _for_download / "file1-prime")
    assert Path(_for_download / "file1-prime").read_text() == "notfile1"

    # don't skip hidden files
    testing(_remote_1 / ".hidden", _remote_2 / ".hidden-prime")
    transport.getfile(_remote_2 / ".hidden-prime", _for_download / ".hidden-prime")
    assert Path(_for_download / ".hidden-prime").read_text() == ".hidden"

    # preserve links and don't follow them
    testing(_remote_1 / "file1_link", _remote_2 / "file1_link")
    assert transport.is_symlink(_remote_2 / "file1_link")
    transport.getfile(_remote_2 / "file1_link", _for_download / "file1_link")
    assert Path(_for_download / "file1_link").read_text() == "file1"
