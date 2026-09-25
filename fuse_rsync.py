#!/usr/bin/env python3
import collections
import datetime
import errno
import logging
import os
import re
import stat
import subprocess
import sys
import tempfile
import threading
import time

import fuse

fuse.fuse_python_api = (0, 2)
log = logging.getLogger("fuse_rsync")

RSYNC_EXIT_PARTIAL_TRANSFER_DUE_TO_ERROR = 23

FILE_MODE_RE = re.compile("^.([-r][-w][-xsS]){2}([-r][-w][-xtT])$", re.ASCII)
RSYNC_ESCAPE_RE = re.compile(br"\\#([0-3][0-7][0-7])", re.ASCII)

EXIT_BAD_USAGE = 2


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
        """
        Return the value for key if key is in the mapping, else default. If no
        default value is specified, a KeyError is raised.

        Arguments:
        - key: Mapping key.
        - default: The default value return if the key is not in the mapping.

        Raises:
        - KeyError: No default value was specified, and the key does not exist
          in the mapping.

        Return: The value associated with the key in the mapping.
        """
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
        """
        Set the mapping entry for the specified key to the specified value.

        Arguments:
        - key: Mapping key.
        - value: Mapped value.
        """
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
    Information about an opened rsync file.
    """
    def __init__(self, handle, **kw):
        """
        Arguments:
        - handle: File's file descriptor number.
        """
        super().__init__(**kw)
        self.keep = True
        self.handle = handle

    def __repr__(self):
        """
        Return the canonical string representation of the object.
        """
        return f"{self.__class__.__name__}(handle={self.handle!r})"


class FuseRsync(fuse.Fuse):
    """
    Implementation of a FUSE filesystem for rsync protocol servers.
    """
    def main(self, argv):
        self.parser.add_option(
            mountopt="user",
            default=None,
            help="Rsync user on the remote host"
        )
        self.parser.add_option(
            mountopt="password",
            type=str,
            default=None,
            help="Rsync password on the remote host"
        )
        self.parser.add_option(
            mountopt="host",
            type=str,
            help="Remote rsync host"
        )
        self.parser.add_option(
            mountopt="module",
            type=str,
            help="Rsync module on remote host"
        )
        self.parser.add_option(
            mountopt="path",
            type=str,
            default="/",
            help="Path under the module that acts as the mountpoint root"
        )
        self.parser.add_option(
            "-t", "--metadata-cache-ttl",
            default=300,
            type="int",
            help="Number of seconds file metadata is cached in memory"
        )
        self.parser.add_option(
            "-c", "--metadata-cache-size",
            default=8192,
            type="int",
            help="Maximum number of file metadata entries cached in memory"
        )

        try:
            super().parse(argv)
        except fuse.OptParseError:
            return EXIT_BAD_USAGE  # A message will already have been shown.
        except Exception as exc:
            error = str(exc)
        else:
            if "debug" in self.fuse_args.optlist:
                logging.basicConfig(level=logging.DEBUG)
            else:
                logging.basicConfig(level=logging.ERROR)

            options, parameters = self.cmdline

            if len(parameters) > 1:
                error = "Too many non-option arguments"
            elif not parameters:
                error = "Mountpoint not specified"
            elif options.metadata_cache_ttl < 0:
                error = "Cache TTL must be at least 0"
            elif options.metadata_cache_size < 1:
                error = "Cache size must be greater than or equal to 1"
            else:
                error = None

        # The logic for displaying the FUSE documentation and version
        # information does not get executed until the fuse.Fuse.main method
        # gets called, so we have to detect whether the associated options were
        # used to determine if we should actually initialize the objects needed
        # to host the filesystem.
        if (not self.fuse_args.getmod("showhelp") and
            not self.fuse_args.getmod("showversion")):

            if error:
                self.parser.print_usage()
                print(error, file=sys.stderr)
                return EXIT_BAD_USAGE

            self._file_cache = {}
            self._file_cache_lock = threading.Lock()

            self._environment = os.environ.copy()
            self._environment["TZ"] = "Etc/UTC"
            self._environment["LC_ALL"] = "C"

            self._remote_url = "rsync://"

            if options.user:
                self._remote_url += options.user + "@"

            self._remote_url += options.host + "/" + options.module

            if options.path:
                options.path = "/" + options.path.lstrip("/")
                self._remote_url = os.path.join(
                    self._remote_url, os.path.relpath(options.path, "/")
                )

            if options.password:
                self._environment['RSYNC_PASSWORD'] = options.password

            try:
                # Perform a smoke test to verify that the remote URL is valid.
                subprocess.check_call(
                    ["rsync", "--list-only", self._remote_url],
                    env=self._environment,
                    stdout=subprocess.DEVNULL,
                )
            except subprocess.CalledProcessError as error:
                return error.returncode

            self._attr_cache = TTLLRUMapping(
                ttl=options.metadata_cache_ttl,
                maxsize=options.metadata_cache_size,
            )

        super().main()

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
        Get metadata for the specified path. If the path ends with a "/", it is
        treated as a directory and the metadata of the contents is returned.
        Otherwise, metadata for the individual file is returned. Regardless of
        whether the input path is a file or a directory, the returned value is
        always a list.

        Arguments:
        - path: FUSE file path with "/" representing the root of the FUSE
          mount.

        Return: A list of dictionaries. Each dictionary will have the following
        keys:
        - st_mode: A reconstruction of the file's mode i.e. `stat.st_mode`.
        - size: An integer representing the size of the file in bytes.
        - timestamp: A UNIX timestamp representing the file's modification
          time.
        - filename: The file's basename.
        """
        remote_url = self._remote_url + path
        isdir = path.endswith("/")
        listing = self._attr_cache.get(remote_url, [])

        if not listing:
            cmdline = ["rsync", "--8-bit-output", "--list-only", remote_url]
            log.debug("executing %s", " ".join(cmdline))

            try:
                output = subprocess.check_output(
                    cmdline, env=self._environment, text=True
                )
            except subprocess.CalledProcessError as err:
                if err.returncode != RSYNC_EXIT_PARTIAL_TRANSFER_DUE_TO_ERROR:
                    raise err

                return listing

            if isdir:
                self._attr_cache.set(remote_url, listing)

            for line in output.splitlines():
                try:
                    attrs, size_str, date, time, filename = line.split(None, 4)
                    filename = rsync_unescape(filename)

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

    def fetch(self, remotepath):
        """
        Launch an rsync process to copy a remote file to the local system. The
        download is done in a non-blocking manner, and the returned subprocess
        object can be used to check on its status.

        Return: A tuple consisting of the rsync subprocess (a subprocess.Popen
        instance) and the name of the local file.
        """
        remote_url = self._remote_url + remotepath
        fd, localpath = tempfile.mkstemp()
        os.close(fd)

        argv = ["rsync", "--copy-links", "--inplace", remote_url, localpath]
        log.critical("executing %s", " ".join(argv))
        process = subprocess.Popen(argv, env=self._environment)
        return (process, localpath)

    def getattr(self, path, fh=None):
        """
        Get a file's status.

        Arguments:
        - path: Path of the file in FUSE filesystem.

        Return: A populated instance of fuse.Stat or, if the file does not
        exist, `-errno.ENOENT`.
        """
        log.debug("getattr(%r)", path)

        try:
            listing = self.list(path)
        except Exception:
            log.expcetion("list(%r): exception raised", path)
            return -errno.EIO

        if not listing:
            log.warning("%s: file not found or rsync output was invalid", path)
            return -errno.ENOENT

        if path.endswith("/"):
            listing = [x for x in listing if x["filename"] == "."]

        if len(listing) == 0:
            return -errno.ENOENT
        elif len(listing) > 1:
            return -errno.EIO

        metadata = listing[0]
        timestamp = metadata["timestamp"]

        st = fuse.Stat(
            st_atime=timestamp,  # TODO: consider maintaining in-memory atimes.
            st_ctime=timestamp,
            st_mtime=timestamp,
            st_uid=os.geteuid(),
            st_gid=os.getegid(),
            st_nlink=2 if path.endswith("/") else 1,
            st_size=metadata["size"],
            st_mode=0o777 & metadata["st_mode"],
        )

        if metadata["st_mode"] & stat.S_IFDIR:
            st.st_mode |= stat.S_IFDIR
        else:
            st.st_mode |= stat.S_IFREG

        return st

    def readdir(self, path, offset):
        """
        Yield directory entries for the FUSE path.

        Arguments:
        - path: Path of the directory in FUSE filesystem.
        - offset: This value is unused.

        Yield: Instances of fuse.Direntry. Entries for "." and ".." will always
        be yielded.
        """
        log.debug("readdir(%r, %s)", path, offset)

        yield fuse.Direntry('.')
        yield fuse.Direntry('..')

        if not path.endswith("/"):
            path += "/"

        for dirent in self.list(path):
            if dirent["filename"] != ".":
                yield fuse.Direntry(dirent["filename"])

    def open(self, path, flags):
        """
        Open a file for reading. If the file is not already open, an
        asynchronous operation to download it is started in the background.

        - path: Path of the file in FUSE filesystem.
        - flags: Flags that determine the behavior of the file. Only flags that
          determine if the file is readable and/or writeable are used, and a
          flags that requests write access will result in this function
          returning `-errno.EACCES` since the FUSE filesystem is read-only.

        Return: None if the operating succeeds. If the operation fails,
        `-errno.EIO` or `-errno.ENOENT` is returned.
        """
        log.debug("open(%r, 0x%x)", path, flags)

        if (flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)) != os.O_RDONLY:
            log.debug("open(%r, 0x%x) -> EACCESS", path, flags)
            return -errno.EACCES

        with self._file_cache_lock:
            if path in self._file_cache:
                self._file_cache[path]["refcount"] += 1
                _, localfile = self._file_cache[path]["proc_file"]
            else:
                proc_file = _, localfile = self.fetch(path)
                self._file_cache[path] = {
                    "refcount": 1,
                    "proc_file": proc_file,
                }

        handle = os.open(localfile, os.O_RDONLY)
        log.debug("open(%r, 0x%x) -> %d", path, flags, handle)
        return FuseRsyncFileInfo(handle)

    def read(self, path, length, offset, fh):
        """
        Read data from the specified file.

        - path: Path of the file in FUSE filesystem.
        - size: The maximum number of bytes to read.
        - offset: The offset within the file from which the read should begin.
        - fh: FUSE file handle for the opened file.

        Return: The requested data if the operation succeeds which may be an
        empty string of bytes if the end of the file has been reached. If this
        function fails, `-errno.EIO` is returned.
        """
        log.critical("read(%r, %d, %d, %r)", path, length, offset, fh)

        minimum_size_required = length + offset
        process, localfile = self._file_cache[path]["proc_file"]

        while process.poll() is None:
            try:
                st = os.fstat(fh.handle)
            except Exception:
                log.exception("os.fstat(%r (%r))", fh.handle, localfile)
                return -errno.EIO

            if st.st_size >= minimum_size_required:
                break

            time.sleep(0.100)

        if process.poll():
            log.error("%s: rsync returned code %s", path, process.returncode)

            # Even if rsync failed, we will only report a problem if the user
            # is trying to read past any data that was already downloaded.
            st = os.fstat(fh.handle)

            if st.st_size < minimum_size_required:
                return -errno.EIO

        return os.pread(fh.handle, length, offset)

    def release(self, path, flags, fh):
        """
        Release resources allocated to a file. If a file has been opened more
        than once, this function will only decrement the file's reference count
        until it reaches 0 at which point the associated resources will
        actually be deleted.

        Arguments:
        - path: Path of the file in FUSE filesystem.
        - flags: This value is unused.
        - fh: FUSE file handle for the opened file.
        """
        log.debug("release(%r, %d, %d)", path, flags, fh.handle)

        with self._file_cache_lock:
            os.close(fh.handle)

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


def rsync_unescape(text):
    """
    Translate any rsync escape sequences in the text.

    Arguments:
    - text: Text containing rsync escape sequences.

    Return: Unescaped/canonical text.
    """
    if "\\#" in text:
        data = text.encode("UTF-8", "surrogateescape")
        unescaped_data = RSYNC_ESCAPE_RE.sub(
            lambda x: bytes([int(x.group(1), 8)]), data
        )
        text = unescaped_data.decode("UTF-8", "surrogateescape")

    return text


if __name__ == '__main__':
    sys.exit(FuseRsync().main(sys.argv))
