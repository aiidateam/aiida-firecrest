###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Transport interface."""
from aiida.transports import Transport


class FirecrestTransport(Transport):
    """Transport interface for FirecREST."""

    def open(self):  # noqa: A003
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def chdir(self, path):
        raise NotImplementedError

    def chmod(self, path, mode):
        raise NotImplementedError

    def chown(self, path, uid, gid):
        raise NotImplementedError

    def copy(self, remotesource, remotedestination, dereference=False, recursive=True):
        raise NotImplementedError

    def copyfile(self, remotesource, remotedestination, dereference=False):
        raise NotImplementedError

    def copytree(self, remotesource, remotedestination, dereference=False):
        raise NotImplementedError

    def _exec_command_internal(self, command, **kwargs):
        raise NotImplementedError

    def exec_command_wait_bytes(self, command, stdin=None, **kwargs):
        raise NotImplementedError

    def get(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def gettree(self, remotepath, localpath, *args, **kwargs):
        raise NotImplementedError

    def getcwd(self):
        raise NotImplementedError

    def get_attribute(self, path):
        raise NotImplementedError

    def isdir(self, path):
        raise NotImplementedError

    def isfile(self, path):
        raise NotImplementedError

    def listdir(self, path=".", pattern=None):
        raise NotImplementedError

    def makedirs(self, path, ignore_existing=False):
        raise NotImplementedError

    def mkdir(self, path, ignore_existing=False):
        raise NotImplementedError

    def put(self, localpath, remotepath, *args, **kwargs):
        raise NotImplementedError

    def putfile(self, localpath, remotepath, *args, **kwargs):
        raise NotImplementedError

    def puttree(self, localpath, remotepath, *args, **kwargs):
        raise NotImplementedError

    def remove(self, path):
        raise NotImplementedError

    def rename(self, oldpath, newpath):
        raise NotImplementedError

    def rmdir(self, path):
        raise NotImplementedError

    def rmtree(self, path):
        raise NotImplementedError

    def gotocomputer_command(self, remotedir):
        raise NotImplementedError

    def symlink(self, remotesource, remotedestination):
        raise NotImplementedError

    def path_exists(self, path):
        raise NotImplementedError
