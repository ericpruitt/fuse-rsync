#!/usr/bin/env python3

# Copyright (c) 2014 Jonas Zaddach
# Licensed under the MIT License (https://github.com/zaddach/fuse-rsync/blob/master/LICENSE)

import os
import sys
import errno
import logging
import subprocess
import stat
import datetime
import time
import re
import fuse
from tempfile import mkstemp
from threading import Lock

fuse.fuse_python_api = (0, 2)
log = logging.getLogger('fuse_rsync')

class RsyncModule():
    """
        This class implements access to an Rsync module.
    """
    def __init__(self, host, module, user=None, password=None):
        self._environment = os.environ.copy()
        self._environment["TZ"] = "Etc/UTC"
        self._remote_url = "rsync://"
        if user is not None:
            self._remote_url += user + "@"

        self._remote_url += host + "/" + module

        if password is not None:
            self._environment['RSYNC_PASSWORD'] = password
        self._attr_cache = {}

    def _parse_attrs(self, attrs):
        """
            Parse the textual representation of file attributes to binary representation.
        """
        result = 0
        if attrs[0] == 'd':
            result |= stat.S_IFDIR
        elif attrs[0] == 'l':
            result |= stat.S_IFLNK
        elif attrs[0] == '-':
            result |= stat.S_IFREG
        else:
           assert False

        for i in range(0, 3):
            val = 0
            if 'r' in attrs[1 + 3 * i: 4 + 3 * i]:
                val |= 4
            if 'w' in attrs[1 + 3 * i: 4 + 3 * i]:
                val |= 2
            if 'x' in attrs[1 + 3 * i: 4 + 3 * i]:
                val |= 1
            result |= val << ((2 - i) * 3)

        return result

    def list(self, path='/'):
        """
            List files contained in directory __path__.
            Returns a list of dictionaries with keys *attrs* (numerical attribute
            representation), *size* (file size), *timestamp* (File's atime timestamp
            in a datetime object) and *filename* (The file's name).
        """
        remote_url = self._remote_url + path
        try:
            cmdline = ["rsync", "--list-only", remote_url]
            log.debug("executing %s", " ".join(cmdline))
            output = subprocess.check_output(cmdline, env=self._environment, text=True)

            listing = []
            for line in output.splitlines():
                attrs, size_str, date_str, time_str, filename = line.split(None, 4)

                try:
                    size = int(size_str.replace(',', ''))
                    dt = datetime.datetime.strptime(
                        f"{date_str} {time_str} +0000", "%Y/%m/%d %H:%M:%S %z"
                    )

                    listing.append({
                        "attrs": self._parse_attrs(attrs),
                        "size": size,
                        "timestamp": dt.timestamp(),
                        "filename": filename
                    })
                except (ValueError, AssertionError):
                    log.warn("Unable to parse line: %r", line)
                    continue

            return listing
        except subprocess.CalledProcessError as err:
            if err.returncode == 23:
                return []
            raise err

    def copy(self, remotepath='/', localpath=None):
        """
            Copy a file from the remote rsync module to the local filesystem.
            If no local destination is specified in __localpath__, a temporary
            file is created and its filename returned. The temporary file has
            to be deleted by the caller.
        """
        remote_url = self._remote_url + remotepath
        if localpath is None:
            fd, localpath = mkstemp()
            os.close(fd)
        cmdline = ["rsync", "--copy-links", remote_url, localpath]
        log.debug("executing %s", " ".join(cmdline))
        subprocess.check_call(cmdline, env=self._environment)

        return localpath

class FuseRsyncFileInfo(fuse.FuseFileInfo):
    """
        Encapsulates the file handle for an opened file.
    """
    def __init__(self, handle, **kw):
        super().__init__(**kw)
        self.keep = True
        self.handle = handle

class FuseRsync(fuse.Fuse):
    """
        The implementation of the FUSE filesystem.
    """
    def __init__(self, *args, **kw):
        self.host = None
        self.module = None
        self.user = None
        self.password = None
        self.path = "/"

        self._attr_cache = {}
        self._file_cache = {}
        self._file_cache_lock = Lock()

        super().__init__(*args, **kw)

        self.parser.add_option(mountopt='user', default=None, help="Rsync user on the remote host")
        self.parser.add_option(mountopt='password', type=str, default=None, help="Rsync password on the remote host")
        self.parser.add_option(mountopt='host', type=str, help="Rsync remote host")
        self.parser.add_option(mountopt='module', type=str, help="Rsync module on remote host")
        self.parser.add_option(mountopt='path', type=str, default="/", help="Rsync path in module on remote host that is supposed to be the root point")

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.path, partial)
        return path

    def init(self):
        options = self.cmdline[0]
        log.debug("Invoked fsinit() with host=%s, module=%s, user=%s, password=%s", options.host, options.module, options.user, options.password)
        self._rsync = RsyncModule(options.host, options.module, options.user, options.password)

    def getattr(self, path, fh=None):
        try:
            log.debug("Invoked getattr('%s')", path)

            path = self._full_path(path)

            st = fuse.Stat()

            if path == "/":
                st.st_atime = int(time.time())
                st.st_ctime = int(time.time())
                st.st_mode  = stat.S_IFDIR | 0o555
                st.st_mtime = int(time.time())
                st.st_nlink = 2
                st.st_uid = os.geteuid()
                st.st_gid = os.getegid()
                return st

            if path in self._attr_cache:
                info = self._attr_cache[path]
            else:
                listing = self._rsync.list(path)
                if len(listing) != 1:
                    log.warning("Found none or several files for path")
                    return -errno.ENOENT
                info = listing[0]
                self._attr_cache[path] = info

            timestamp = info["timestamp"]
            st.st_atime = timestamp  # TODO: consider maintaining in-memory atimes.
            st.st_ctime = timestamp
            st.st_uid = os.geteuid()
            st.st_gid = os.getegid()
            if info["attrs"] & stat.S_IFDIR:
                st.st_mode  = stat.S_IFDIR | 0o555
            else:
                st.st_mode = stat.S_IFREG | 0o444
            st.st_mtime = timestamp
            st.st_nlink = 1
            st.st_size = info["size"]

            return st
        except Exception:
            log.exception("while doing getattr")
            return -errno.ENOENT

    def readdir(self, path, offset):
        try:
            if not path.endswith("/"):
                path += "/"
            log.debug("Invoked readdir('%s')", path)

            full_path = self._full_path(path)

            yield fuse.Direntry('.')
            yield fuse.Direntry('..')

            for dirent in self._rsync.list(full_path):
                if dirent["filename"] == ".":
                    continue
                self._attr_cache[path + dirent["filename"]] = dirent
                yield fuse.Direntry(str(dirent["filename"]))
        except Exception:
            log.exception("While doing readdir")

    def open(self, path, flags):
        log.debug("invoking open(%s, %d)", path, flags)

        full_path = self._full_path(path)
        if (flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)) != os.O_RDONLY:
            return -errno.EACCES

        with self._file_cache_lock:
            if path not in self._file_cache:
                localfile = self._rsync.copy(full_path)
                self._file_cache[path] = {"refcount": 1, "localpath": localfile}
            else:
                self._file_cache[path]["refcount"] += 1
                localfile = self._file_cache[path]["localpath"]

        handle = os.open(localfile, os.O_RDONLY)
        log.debug("Created file handle %d", handle)
        return FuseRsyncFileInfo(handle)

    def read(self, path, length, offset, fh):
        log.debug("invoking read(%s, %d, %d, %d)", path, length, offset, fh.handle)
        os.lseek(fh.handle, offset, os.SEEK_SET)
        return os.read(fh.handle, length)

    def release(self, path, dummy, fh):
        log.debug("invoking release(%s, %d, %d)", path, dummy, fh.handle)
        os.close(fh.handle)

        with self._file_cache_lock:
            self._file_cache[path]["refcount"] -= 1
            if self._file_cache[path]["refcount"] <= 0:
                localfile = self._file_cache[path]["localpath"]
                del self._file_cache[path]
                os.unlink(localfile)

if __name__ == '__main__':
    fs = FuseRsync()
    fs.parse(errex=1)
    if '-d' in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
    fs.init()
    fs.main()
