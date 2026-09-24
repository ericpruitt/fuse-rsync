#!/usr/bin/env python3

# Copyright (c) 2014 Jonas Zaddach
# Licensed under the MIT License (https://github.com/zaddach/fuse-rsync/blob/master/LICENSE)

import collections
import threading
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

fuse.fuse_python_api = (0, 2)
log = logging.getLogger('fuse_rsync')

EXIT_PARTIAL_TRANSFER_DUE_TO_ERROR = 23

FILE_MODE_RE = re.compile("^.([-r][-w][-xsS]){2}([-r][-w][-xtT])$", re.ASCII)

class TTLLRUMapping:
    """
    A class for defining a mapping with a finite number of members that have a
    user-defined validity period.
    """
    _sentinel = object()

    def __init__(self, ttl, maxsize=128, *, data=None):
        """
        Arguments:
        - ttl: Lifetime in seconds of mapping members.
        - maxsize: The maximum number of values the mapping can hold before the
          oldest items are evicted.
        - data: The initial set of members of the mapping. This can be any
          value accepted by the `dict` built-in.
        """
        self._dict = collections.OrderedDict(data or ())
        self._maxsize = maxsize
        self._ttl = ttl
        self._lock = threading.RLock()

        if data:
            for key, value in dict(data).items():
                self.set(key, value)

    def get(self, key, default=_sentinel):
        try:
            with self._lock:
                value, expiration = self._dict.pop(key)

                if time.monotonic() >= expiration:
                    raise KeyError

                now = time.monotonic()
                self._dict[key] = (value, now + self._ttl)
                return value
        except KeyError:
            if default is not self._sentinel:
                return default

        raise KeyError(key)

    def set(self, key, value):
        with self._lock:
            now = time.monotonic()

            try:
                self._dict.pop(key)
            except KeyError:
                if len(self._dict) >= self._maxsize:
                    # While eliminating entries to reduce the amount of values
                    # stored, we also prune any expired values even if it's not
                    # necessary to get the dictionary below capacity.
                    while self._dict:
                        _, expiration = self._dict.popitem(last=False)

                        if now < expiration:
                            break

            self._dict[key] = (value, now + self._ttl)


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

        self._file_cache = {}
        self._file_cache_lock = threading.Lock()

        super().__init__(*args, **kw)

        self.parser.add_option(mountopt='user', default=None, help="Rsync user on the remote host")
        self.parser.add_option(mountopt='password', type=str, default=None, help="Rsync password on the remote host")
        self.parser.add_option(mountopt='host', type=str, help="Rsync remote host")
        self.parser.add_option(mountopt='module', type=str, help="Rsync module on remote host")
        self.parser.add_option(mountopt='path', type=str, default="/", help="Rsync path in module on remote host that is supposed to be the root point")

        self.parser.add_option("-t", "--cache-ttl",
            default=300,
            type="int",
            help="number of seconds file metadata is cached in memory"
        )
        self.parser.add_option("-c", "--cache-size",
            default=300,
            type="int",
            help="maximum number of file metadata entries cached in memory"
        )

    def _text_to_mode(self, attrs):
        """
        Convert textural representation of a file's mode to its numeric
        representation.

        Arguments:
        - attrs: String containing the file type and permissions. The format
          rsync uses is the same as `ls -l`.

        Return: A numeric value representing the reconstructed st_mode.
        """
        if not FILE_MODE_RE.match(attrs):
            log.error("Unsupported permission/mode string: %r", attrs)
            return 0

        if attrs[0] == 'd':
            mode = stat.S_IFDIR
        elif attrs[0] == 'l':
            mode = stat.S_IFLNK
        elif attrs[0] == '-':
            mode = stat.S_IFREG
        else:
            mode = 0
            log.error("Unable to determine file type from %r", attrs)

        for i in range(3):
            val = 0
            perms = attrs[1 + 3 * i: 4 + 3 * i]

            if "r" in perms:
                val |= 4

            if "w" in perms:
                val |= 2

            if "x" in perms or "s" in perms or "t" in perms:
                val |= 1

            if "s" in perms or "S" in perms:
                if i == 0:  # User
                    mode |= stat.S_ISUID
                elif i == 1:  # Group
                    mode |= stat.S_ISGID
            elif "t" in perms or "T" in perms:
                if i == 2:  # Other
                    mode |= stat.S_ISVTX

            mode |= val << ((2 - i) * 3)

        return mode

    def list(self, path):
        """
            List files contained in directory __path__.
            Returns a list of dictionaries with keys *attrs* (numerical attribute
            representation), *size* (file size), *timestamp* (File's atime timestamp
            in a datetime object) and *filename* (The file's name).
        """
        remote_url = self._remote_url + path
        isdir = path.endswith("/")
        listing = self._attr_cache.get(remote_url, [])

        if not listing:
            cmdline = ["rsync", "--list-only", remote_url]
            log.debug("executing %s", " ".join(cmdline))

            try:
                output = subprocess.check_output(
                    cmdline, env=self._environment, text=True
                )
            except subprocess.CalledProcessError as err:
                if err.returncode != EXIT_PARTIAL_TRANSFER_DUE_TO_ERROR:
                    raise err

                return listing

            if isdir:
                self._attr_cache.set(remote_url, listing)

            for line in output.splitlines():
                try:
                    attrs, size_str, date, time, filename = line.split(None, 4)

                    size = int(size_str.replace(',', ''))
                    dt = datetime.datetime.strptime(
                        f"{date} {time} +0000", "%Y/%m/%d %H:%M:%S %z"
                    )
                except ValueError:
                    log.warn("Unable to parse line: %r", line)
                else:
                    entry = {
                        "st_mode": self._text_to_mode(attrs),
                        "size": size,
                        "timestamp": dt.timestamp(),
                        "filename": filename
                    }
                    listing.append(entry)
                    self._attr_cache.set(
                        remote_url + filename if isdir else remote_url, [entry]
                    )

        return listing

    def copy(self, remotepath):
        """
            Copy a file from the remote rsync module to the local filesystem.
            If no local destination is specified in __localpath__, a temporary
            file is created and its filename returned. The temporary file has
            to be deleted by the caller.
        """
        remote_url = self._remote_url + remotepath
        fd, localpath = mkstemp()
        os.close(fd)

        argv = ["rsync", "--copy-links", "--inplace", remote_url, localpath]
        log.critical("executing %s", " ".join(argv))
        process = subprocess.Popen(argv, env=self._environment)
        return (process, localpath)


    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.path, partial)
        return path

    def init(self):
        options = self.cmdline[0]
        log.debug("Invoked fsinit() with host=%s, module=%s, user=%s, password=%s", options.host, options.module, options.user, options.password)

        self._environment = os.environ.copy()
        self._environment["TZ"] = "Etc/UTC"
        self._remote_url = "rsync://"
        if options.user:
            self._remote_url += options.user + "@"

        self._remote_url += options.host + "/" + options.module

        if options.password:
            self._environment['RSYNC_PASSWORD'] = options.password
        self._attr_cache = TTLLRUMapping(ttl=options.cache_ttl, maxsize=options.cache_size)

    def getattr(self, path, fh=None):
        log.debug("getattr(%r)", path)
        path = self._full_path(path)

        try:
            listing = self.list(path)
        except Exception:
            log.expcetion("list(%r): exception raised", path)
            return -errno.EIO

        if not listing:
            log.warning("%s: file not found or rsync return invalid output", path)
            return -errno.ENOENT

        if path.endswith("/"):
            listing = [x for x in listing if x["filename"] == "."]

        if len(listing) == 0:
            return -errno.ENOENT
        elif len(listing) > 1:
            return -errno.EIO

        metadata = listing[0]
        timestamp = metadata["timestamp"]

        st = fuse.Stat()
        st.st_atime = timestamp  # TODO: consider maintaining in-memory atimes.
        st.st_ctime = timestamp
        st.st_mtime = timestamp

        st.st_uid = os.geteuid()
        st.st_gid = os.getegid()

        st.st_nlink = 2 if path.endswith("/") else 1
        st.st_size = metadata["size"]
        st.st_mode = 0o777 & metadata["st_mode"]

        if metadata["st_mode"] & stat.S_IFDIR:
            st.st_mode |= stat.S_IFDIR
        else:
            st.st_mode |= stat.S_IFREG

        return st

    def readdir(self, path, offset):
        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        if not path.endswith("/"):
            path += "/"

        for dirent in self.list(self._full_path(path)):
            if dirent["filename"] != ".":
                yield fuse.Direntry(dirent["filename"])

    def open(self, path, flags):
        log.debug("open(%r, %d)", path, flags)

        if (flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)) != os.O_RDONLY:
            return -errno.EACCES

        full_path = self._full_path(path)

        with self._file_cache_lock:
            if path in self._file_cache:
                self._file_cache[path]["refcount"] += 1
                _, localfile = self._file_cache[path]["proc_file"]
            else:
                proc_file = _, localfile = self.copy(full_path)
                self._file_cache[path] = {
                    "refcount": 1,
                    "proc_file": proc_file,
                }

        handle = os.open(localfile, os.O_RDONLY)
        log.debug("Created file handle %d", handle)
        return FuseRsyncFileInfo(handle)

    def read(self, path, length, offset, fh):
        log.critical("read(%r, %d, %d, %d)", path, length, offset, fh.handle)
        minimum_size_required = length + offset
        process, localfile = self._file_cache[path]["proc_file"]

        while process.returncode is None:
            try:
                st = os.fstat(fh.handle)
            except Exception:
                log.exception("os.fstat(%r (%r))", fh.handle, localfile)
                return -errno.EIO

            if st.st_size >= minimum_size_required:
                break

            time.sleep(0.100)

        if process.returncode:
            log.error("%s: non-zero rsync exit code %s", path, process.returncode)

            # Even if rsync failed, we will only report a problem if the user
            # is trying to read past any data that was already downloaded.
            st = os.fstat(fh.handle)

            if st.st_size < minimum_size_required:
                return -errno.EIO

        os.lseek(fh.handle, offset, os.SEEK_SET)
        return os.read(fh.handle, length)

    def release(self, path, dummy, fh):
        log.debug("release(%r, %d, %d)", path, dummy, fh.handle)
        os.close(fh.handle)

        with self._file_cache_lock:
            self._file_cache[path]["refcount"] -= 1
            if self._file_cache[path]["refcount"] <= 0:
                process, localfile = self._file_cache[path]["proc_file"]
                del self._file_cache[path]
                os.unlink(localfile)

                # It's important that we use terminate here because rsync may
                # fork into multiple processes, and using kill will result in
                # the children lingering.
                process.terminate()
                process.wait()


if __name__ == '__main__':
    fs = FuseRsync()
    fs.parse(errex=1)
    if '-d' in sys.argv:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.ERROR)
    fs.init()
    fs.main()
