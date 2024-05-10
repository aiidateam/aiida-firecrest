from __future__ import annotations

from pathlib import Path
import os

from _pytest.terminal import TerminalReporter
import firecrest.path
import pytest
import firecrest 


class MockFirecrest:
    def __init__(self, firecrest_url, *args, **kwargs):
        self._firecrest_url = firecrest_url
        self.args = args
        self.kwargs = kwargs
        self.whoami = mock_whomai
        self.list_files = list_files
        self.stat = stat_
        self.mkdir = mkdir
        self.simple_delete = simple_delete
        self.parameters = parameters
        self.symlink = symlink

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
    # monkeypatch.setattr(firecrest.path, "_ls_to_st_mode", _ls_to_st_mode)



def mock_whomai(machine: str):
    assert machine == "MACHINE_NAME"
    return "test_user"


import stat

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
            # breakpoint()
            print(relative_path)
            if os.path.isdir(full_path):
                content_type = 'd'
            elif os.path.isfile(full_path):
                content_type = '-'
            elif os.path.islink(full_path):
                content_type = 'l'
            else:
                content_type = 'NON'
            link_target = os.readlink(full_path) if os.path.islink(full_path) else None
            # permissions = stat.S_IMODE(os.lstat(full_path).st_mode)
            permissions = stat.filemode(Path(full_path).lstat().st_mode)[1:] 
            # stat.S_ISREG(permissions)
            if name.startswith('.') and not show_hidden:
                continue
            content_list.append({
                'name': relative_path,
                'type': content_type,
                'link_target': link_target,
                'permissions': permissions,
            })

    return content_list

# def _ls_to_st_mode(ftype: str, permissions: str) -> int:
#     return int(permissions)

def stat_(machine:str, targetpath: firecrest.path, dereference=True):
    stats = os.stat(targetpath)
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
    os.symlink(target_path, link_path)


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