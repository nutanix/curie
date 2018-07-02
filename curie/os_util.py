#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging
import os
import tempfile
import threading

log = logging.getLogger(__name__)


class OsUtil(object):
  @staticmethod
  def write_and_rename(pathname, data, tmp_suffix=".tmp"):
    """
    Create a temporary file with contents 'data', then rename the file to
    'pathname'. If 'tmp_suffix' is set, then the temporary file path will
    be 'pathname' with the suffix 'tmp_suffix' appended to it, else it will
    be a unique temporary file in the directory where 'pathname' resides.

    Note that, on Unix, if pathname exists and is a file, it will be replaced
    silently if the user has permission.
    """
    if tmp_suffix:
      tmp = open(pathname + tmp_suffix, "w")
    else:
      tmp_dir = os.path.dirname(pathname)
      tmp = tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False)
    tmp.write(data)
    tmp.flush()
    os.fsync(tmp.fileno())
    tmp.close()
    os.rename(tmp.name, pathname)


class FileLock(object):
  """
  Lock acquired/released by creating/unlinking a file.
  """
  @classmethod
  def _validate_path_and_fd(cls, path, fd):
    """
    Validates that 'fd' and 'path' are the same file, and 'path' is unique.

    NB: Caller is responsible for any necessary locking.

    Returns:
      True on success, else False.
    """
    if not path and fd:
      return False
    path_inode = os.stat(path).st_ino
    fd_stat = os.fstat(fd)
    if path_inode != fd_stat.st_ino:
      return False
    return fd_stat.st_nlink == 1

  def __init__(self, path):
    """
    Args:
      path (str): Absolute path to the file to open when acquiring the lock.
    """
    self._path = path
    # Lock protecting the below field.
    self._lock = threading.Lock()
    # File descriptor. Set by 'acquire'.
    self._fd = None

  def acquire(self):
    """
    Acquires lock by creating a file.

    NB: Per manpage for 'open', this is subject to race conditions on NFS if
    either NFS versions < 3 or linux kerenel version < 2.6.

    Returns:
      True on success, else False
    """
    with self._lock:
      if self._fd is not None:
        return False
      try:
        self._fd = os.open(self._path, os.O_CREAT | os.O_EXCL)
      except OSError:
        log.exception("Unable to create lockfile at '%s'", self._path)
        return False
      os.fsync(self._fd)
    return True

  def release(self):
    """
    Releases lock by closing associated fd and removing associated file.

    Raises:
      AssertionError on failure.
    """
    with self._lock:
      # Sanity check, confirm 'fd' has a unique hard link which matches the
      # configured file path.
      assert self._validate_path_and_fd(self._path, self._fd)
      os.unlink(self._path)
      os.fsync(self._fd)
      os.close(self._fd)
      self._fd = None
