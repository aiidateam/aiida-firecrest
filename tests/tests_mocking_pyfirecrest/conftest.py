from __future__ import annotations

from pathlib import Path
import os
import stat
import hashlib

from _pytest.terminal import TerminalReporter
import firecrest.path
import pytest
import firecrest 
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



class MockFirecrest:
    def __init__(self, firecrest_url, *args, **kwargs):
        self._firecrest_url = firecrest_url
        self.args = args
        self.kwargs = kwargs
        self.whoami = whomai
        self.list_files = list_files
        self.stat = stat_
        self.mkdir = mkdir
        self.simple_delete = simple_delete
        self.parameters = parameters
        self.symlink = symlink
        self.checksum = checksum
        self.simple_download = simple_download
        self.simple_upload = simple_upload
        self.compress = compress
        self.extract = extract
        self.copy = copy
        self.submit = submit
        # self.poll_active = poll_active


class MockClientCredentialsAuth:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

@pytest.fixture(scope="function")
def myfirecrest(
    pytestconfig: pytest.Config,
    monkeypatch,
):

    monkeypatch.setattr(firecrest, "Firecrest", MockFirecrest)
    monkeypatch.setattr(firecrest, "ClientCredentialsAuth", MockClientCredentialsAuth)

# def poll_active(machine: str, job_id: str):
def submit(machine: str, script_str: str = None, script_remote_path: str = None, script_local_path: str = None):
    pass

def whomai(machine: str):
    assert machine == "MACHINE_NAME"
    return "test_user"

def list_files(
    machine: str, target_path: str, recursive: bool = False, show_hidden: bool = False):
    # this is mimiking the expected behaviour from the firecrest code.

    content_list = []
    for root, dirs, files in os.walk(target_path):
        if not recursive and root != target_path:
            continue
        for name in dirs + files:
            full_path = os.path.join(root, name)
            relative_path = Path(os.path.relpath(root, target_path)).joinpath(name).as_posix()
            if os.path.islink(full_path):
                content_type = 'l'
                link_target = os.readlink(full_path) if os.path.islink(full_path) else None
            elif os.path.isfile(full_path):
                content_type = '-'
                link_target = None
            elif os.path.isdir(full_path):
                content_type = 'd'
                link_target = None
            else:
                content_type = 'NON'
                link_target = None
            permissions = stat.filemode(Path(full_path).lstat().st_mode)[1:] 
            if name.startswith('.') and not show_hidden:
                continue
            content_list.append({
                'name': relative_path,
                'type': content_type,
                'link_target': link_target,
                'permissions': permissions,
            })

    return content_list

def stat_(machine:str, targetpath: firecrest.path, dereference=True):
    stats = os.stat(targetpath, follow_symlinks= True if dereference else False)
    return {
        "ino": stats.st_ino,
        "dev": stats.st_dev,
        "nlink": stats.st_nlink,
        "uid": stats.st_uid,
        "gid": stats.st_gid,
        "size": stats.st_size,
        "atime": stats.st_atime,
        "mtime": stats.st_mtime,
        "ctime": stats.st_ctime,
    }

def mkdir(machine: str, target_path: str, p: bool = False):
    if p:
        os.makedirs(target_path)
    else:
        os.mkdir(target_path)

def simple_delete(machine: str, target_path: str):
    if not Path(target_path).exists():
        raise FileNotFoundError(f"File or folder {target_path} does not exist")
    if os.path.isdir(target_path):
        os.rmdir(target_path)
    else:
        os.remove(target_path)

def symlink(machine: str, target_path: str, link_path: str):
    # this is how firecrest does it
    os.system(f"ln -s {target_path}  {link_path}")

def simple_download(machine: str, remote_path: str, local_path: str):
    # this procedure is complecated in firecrest, but I am simplifying it here
    # we don't care about the details of the download, we just want to make sure
    # that the aiida-firecrest code is calling the right functions at right time
    if Path(remote_path).is_dir():
        raise IsADirectoryError(f"{remote_path} is a directory")
    if not Path(remote_path).exists():
        raise FileNotFoundError(f"{remote_path} does not exist")
    os.system(f"cp {remote_path} {local_path}")

def simple_upload(machine: str, local_path: str, remote_path: str, file_name: str = None):
    # this procedure is complecated in firecrest, but I am simplifying it here
    # we don't care about the details of the upload, we just want to make sure
    # that the aiida-firecrest code is calling the right functions at right time
    if Path(local_path).is_dir():
        raise IsADirectoryError(f"{local_path} is a directory")
    if not Path(local_path).exists():
        raise FileNotFoundError(f"{local_path} does not exist")
    if file_name:
        remote_path = os.path.join(remote_path, file_name)
    os.system(f"cp {local_path} {remote_path}")    

def copy(machine: str, source_path: str, target_path: str):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/utilities/utilities.py#L451C1-L452C1    
    os.system(f"cp --force -dR --preserve=all -- '{source_path}' '{target_path}'")

def compress(machine: str, source_path: str, target_path: str, dereference: bool = True):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/utilities/utilities.py#L460
    basedir = os.path.dirname(source_path)
    file_path = os.path.basename(source_path)
    deref = "--dereference" if dereference else ""
    os.system(f"tar {deref} -czf '{target_path}' -C '{basedir}' '{file_path}'")

def extract(machine: str, source_path: str, target_path: str):
    # this is how firecrest does it
    # https://github.com/eth-cscs/firecrest/blob/db6ba4ba273c11a79ecbe940872f19d5cb19ac5e/src/common/cscs_api_common.py#L1110C18-L1110C65
    os.system(f"tar -xf '{source_path}' -C '{target_path}'")

def checksum(machine: str, remote_path: str) -> int:
    if not remote_path.exists():
        return False
    # Firecrest uses sha256
    sha256_hash = hashlib.sha256()
    with open(remote_path,"rb") as f:
        for byte_block in iter(lambda: f.read(4096),b""):
            sha256_hash.update(byte_block)

    return sha256_hash.hexdigest()

def parameters():
    # note: I took this from https://firecrest-tds.cscs.ch/ or https://firecrest.cscs.ch/
    # if code is not working but test passes, it means you need to update this dictionary 
    # with the latest FirecREST parameters
    return {
        "compute": [
        {
            "description": "Type of resource and workload manager used in compute microservice",
            "name": "WORKLOAD_MANAGER",
            "unit": "",
            "value": "Slurm"
        }
        ],
        "storage": [
        {
            "description": "Type of object storage, like `swift`, `s3v2` or `s3v4`.",
            "name": "OBJECT_STORAGE",
            "unit": "",
            "value": "s3v4"
        },
        {
            "description": "Expiration time for temp URLs.",
            "name": "STORAGE_TEMPURL_EXP_TIME",
            "unit": "seconds",
            "value": "86400"
        },
        {
            "description": "Maximum file size for temp URLs.",
            "name": "STORAGE_MAX_FILE_SIZE",
            "unit": "MB",
            "value": "5120"
        },
        {
            "description": "Available filesystems through the API.",
            "name": "FILESYSTEMS",
            "unit": "",
            "value": [
            {
                "mounted": [
                "/project",
                "/store",
                "/scratch/snx3000tds"
                ],
                "system": "dom"
            },
            {
                "mounted": [
                "/project",
                "/store",
                "/capstor/scratch/cscs"
                ],
                "system": "pilatus"
            }
            ]
        }
        ],
        "utilities": [
        {
            "description": "The maximum allowable file size for various operations of the utilities microservice",
            "name": "UTILITIES_MAX_FILE_SIZE",
            "unit": "MB",
            "value": "69"
        },
        {
            "description": "Maximum time duration for executing the commands in the cluster for the utilities microservice.",
            "name": "UTILITIES_TIMEOUT",
            "unit": "seconds",
            "value": "5"
        }
        ]
    }