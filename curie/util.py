#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import base64
import errno
import logging
import os
import re
import signal
import socket
import subprocess
import tempfile
import time
import weakref
from collections import OrderedDict
from functools import wraps
from itertools import islice

import werkzeug._internal
from Crypto import Random
from Crypto.Cipher import AES

from curie import curie_extensions_pb2
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.log import CHECK_EQ, patch_trace

log = logging.getLogger(__name__)
patch_trace()

# Protobuf 'encrypted' field option.
ENCRYPTED_FIELD_OPT = curie_extensions_pb2.encrypted


class CurieUtil(object):
  # Regex to match IPv4 addresses. Looks for 4 consecutive occurrences, each
  # having 1-3 digits followed by a period or the end of the string. Final
  # lookbehind checks that the last digits end the string and do not have a
  # trailing period.
  # (i.e. ensures that "1.1.1.1." won't match while "1.1.1.1" will)
  IPV4_RE = re.compile(r"^(\d{1,3}(\.|$)){4}(?<!\.)")

  # Regex to match UUIDs. 26528a6c-c9a6-4d17-8598-20c8e90bd990
  UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$")

  VALID_STR_SLICE_CHUNK_RE = re.compile(r"[ :0-9()/*+-]+")

  @staticmethod
  def wait_for(func, msg, timeout_secs, poll_secs=None):
    """
    Invoke 'func' every 'poll_secs' seconds until the boolean-ness of the
    returned value is True, in which case we return that value. Otherwise, or
    if 'timeout_secs' seconds have elapsed, we return None.

    Args:
      func (callable): Callable to poll.
      msg (str): Readable description of the action.
      timeout_secs (int): Maximum amount of time to wait for func to complete.
      poll_secs (int): Polling interval in seconds. None uses default polling
        interval.
    """
    ret = CurieUtil.wait_for_any([func], msg, timeout_secs,
                                  poll_secs=poll_secs)
    if ret is not None:
      return ret[0]
    else:
      return None

  @staticmethod
  def wait_for_any(callables, msg, timeout_secs, poll_secs=None):
    """
    Invoke each item in 'callables' every 'poll_secs' seconds until the
    boolean-ness of any of the returned values are True, in which case we
    return that value. Otherwise, or if 'timeout_secs' seconds have elapsed,
    we return None.

    Args:
      callables (sequence of callable): Callables to poll.
      msg (str): Readable description of the action.
      timeout_secs (int): Maximum amount of time to wait for func to complete.
      poll_secs (int): Polling interval in seconds. None uses default polling
        interval.
    """
    if poll_secs is None:
      poll_secs = 1
    start_time_secs = time.time()
    while True:
      return_values = []
      for func in callables:
        return_values.append(func())
      duration_secs = time.time() - start_time_secs
      if any(return_values):
        if duration_secs > timeout_secs:
          log.warning("Waiting for %s succeeded, but exceeded the timeout "
                      "(duration was %d secs, timeout was %d secs)",
                      msg, duration_secs, timeout_secs)
        return return_values
      if duration_secs > timeout_secs:
        log.error("Timed out waiting for %s", msg)
        return None
      log.info("Waiting for %s (%d secs elapsed, timeout %d secs)",
               msg, duration_secs, timeout_secs)
      time.sleep(poll_secs)

  @staticmethod
  def monkey_patch_werkzeug_logger():
    """
    Override 'werkzeug._internal._log' with a method that discards all output
    for log type 'info'.
    """
    _WERKZEUG_LOG = werkzeug._internal._log
    def suppressed_log(typ, msg, *args, **kwargs):
      """
      Replacement for the werkzeug internal logger which will drop all
      messages of type 'info'.
      """
      # Suppress the undesired 'info' level logs.
      if typ.lower() == "info":
        return
      # Otherwise, log as normal.
      _WERKZEUG_LOG(typ, msg, *args, **kwargs)

    werkzeug._internal._log = suppressed_log

  @staticmethod
  def monkey_patch_dummy_process():
    """
    Fix a known issue with 'multiprocessing.dummy.DummyProcess':
      See https://bugs.python.org/issue14881.
    """
    from multiprocessing.dummy import DummyProcess
    class PatchedDummyProcess(DummyProcess):
      def start(self):
        if not hasattr(self._parent, "_children"):
          setattr(self._parent, "_children", weakref.WeakKeyDictionary())
        DummyProcess.start(self)
    import multiprocessing
    setattr(multiprocessing.dummy, "Process", PatchedDummyProcess)
    setattr(multiprocessing.pool.ThreadPool, "Process", PatchedDummyProcess)

  @staticmethod
  def timed_command(cmd, timeout_secs, formatted_cmd=None, interval_secs=0.1):
    """
    Execute 'cmd' with a timeout of 'timeout_secs'. On a timeout, the
    underlying command being executed is killed.

    Args:
      cmd (str): Command to run.
      timeout_secs (int): Maximum number of seconds to wait for the command
        to finish.
      formatted_cmd (str): If not None, it will be used in place of 'cmd' when
        writing human-readable logs.
      interval_secs (float): Command polling interval.

    Returns:
      (rv, stdout, stderr) if command is executed within 'timeout_secs'.
      (-1, None, None) on timeout or other error.
    """
    if formatted_cmd is None:
      formatted_cmd = cmd

    t1 = time.time()
    stdout_file = tempfile.NamedTemporaryFile()
    stderr_file = tempfile.NamedTemporaryFile()
    proc = subprocess.Popen(cmd,
                            shell=True,
                            stdout=stdout_file,
                            stderr=stderr_file,
                            close_fds=True)
    execution_failure = False
    while True:
      wait_pid, wait_status = os.waitpid(proc.pid, os.WNOHANG)
      if wait_pid == proc.pid:
        # 'cmd' has finished. Check whether it succeeded or failed.
        stdout_file.flush()
        stderr_file.flush()
        stdout = open(stdout_file.name).read()
        stderr = open(stderr_file.name).read()
        if wait_status == 0:
          log.debug("%s exited successfully", formatted_cmd)
        else:
          if os.WIFEXITED(wait_status):
            exit_status = os.WEXITSTATUS(wait_status)
            log.error("%s failed, exit_status %d, %s, %s",
                      formatted_cmd, exit_status, stdout, stderr)
          else:
            log.error("%s exited abnormally, wait_status = %d, %s, %s",
                      formatted_cmd, wait_status, stdout, stderr)
          execution_failure = True
        break
      t2 = time.time()
      elapsed_secs = t2 - t1
      if elapsed_secs > timeout_secs:
        # If the command timed out, kill the command and return False.
        log.error("%s timed out (%d secs elapsed)",
                  formatted_cmd, elapsed_secs)
        try:
          os.kill(proc.pid, signal.SIGKILL)
        except OSError, ex:
          CHECK_EQ(ex.errno, errno.ESRCH, msg=str(ex))
        execution_failure = True
        break
      time.sleep(interval_secs)
    if execution_failure:
      return -1, None, None
    return os.WEXITSTATUS(wait_status), stdout, stderr

  @staticmethod
  def log_duration(wrapped, log_func=log.debug):
    """Decorator to log the duration of the wrapped function.

    Logs the total execution time in milliseconds.

    Args:
      wrapped: Function being profiled.
      log_func: Optional log function to be called (defaults to log.debug)

    Returns:
      New function that wraps 'wrapped'.
    """
    @wraps(wrapped)
    def wrapper(*args, **kwargs):
      start_time_secs = time.time()
      ret = wrapped(*args, **kwargs)
      log_func("%s exited (%d ms)",
               wrapped.__name__, (time.time() - start_time_secs) * 1000)
      return ret
    return wrapper

  @staticmethod
  def encrypt_aes256_cfb(plaintext, key):
    """
    Encrypts 'plaintext' using 'key'.

    Args:
      plaintext (str): Text to encrypt.
      key (str): Key to use for encryption, assumed to be a SHA-256 digest.

    Returns:
      (str) Base64 encoded ciphertext.
    """
    iv = Random.new().read(AES.block_size)
    return base64.b64encode(
        iv + AES.new(key, AES.MODE_CFB, iv).encrypt(plaintext))

  @staticmethod
  def decrypt_aes256_cfb(ciphertext, key):
    """
    Decrypts Base64 encoded 'ciphertext' using 'key'.

    Args:
      ciphertext (str): Base64 encoded ciphertext to decrypt.
      key (str): Key to use for decryption, assumed to be a SHA-256 digest.

    Returns:
      (str) Decoded plaintext.
    """
    ciphertext = base64.b64decode(ciphertext)
    iv = ciphertext[:AES.block_size]
    return AES.new(key, AES.MODE_CFB, iv).decrypt(ciphertext[AES.block_size:])

  @staticmethod
  def is_ipv4_address(string):
    """
    Args:
      string (str): Potential IPv4 address to test.
    Returns:
      True if 'string' appears to be a valid IPv4 address, else False.
    """
    if not isinstance(string, basestring):
      log.warning("Received non-string argument to 'is_ipv4_address'")
      return False
    return CurieUtil.IPV4_RE.match(string.strip()) is not None

  @staticmethod
  def is_uuid(string):
    """
    Args:
      string (str): Potential UUID to test.
    Returns:
      True if 'string' appears to be a UUID, else False.
    """
    if not isinstance(string, basestring):
      log.warning("Received non-string argument to 'is_uuid'")
      return False
    return CurieUtil.UUID_RE.match(string.strip()) is not None

  @staticmethod
  def ipv4_to_ipv6(ipv4_address):
    """
    Converts an IPv4 address to the IPv4-mapped IPv6 address.

    Args:
      ipv4_address (str): IPv4 address to convert.

    Returns:
      (str) v4-mapped v6 address
    """
    chunks = [int(c) for c in ipv4_address.strip().split(".")]
    return "::FFFF:%s" % ":".join(["%x" % ((chunks[ii] << 8) + chunks[ii + 1])
                                   for ii in [0, 2]])

  @staticmethod
  def ping_ip(ip):
    """
    Attempts to send a single ECHO_REQUEST packet to 'ip' and confirm receipt
    of the corresponding reply.

    Returns:
      (bool) True on success, else False.
    """
    rv, stdout, stderr = CurieUtil.timed_command("ping -c1 -q %s" % ip, 10)
    if rv != 0:
      log.warning("Unable to ping host at '%s'", ip)
      return False
    return True

  @staticmethod
  def resolve_hostname(hostname, timeout=1):
    """
    Args:
      (str) hostname: Hostname whose IP to resolve.
      timeout (number): Timeout in seconds.

    Returns:
      (str) Resolved IP address of 'hostname'.

    Raises:
      CurieException on error.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
      sock.connect((hostname, 53))
      return sock.getpeername()[0]
    except (TypeError, socket.error) as exc:
      msg = "Unable to resolve IP address for host '%s': %s" % (hostname, exc)
      log.exception(msg)
      raise CurieException(CurieError.kInternalError, msg)
    finally:
      sock.close()

  # TODO (ryan.hardin/jklein): Determine desired behavior for overlapping
  # slices, e.g.: some_list["1, 2:5, 4, 3:6"].
  @staticmethod
  def slice_with_string(sequence, slice_str):
    """Perform fancy slicing as described by a string.

    Also performs the task of converting negative indices to positive. This is
    done so that the consumer of this method can write meaningful log messages
    and annotations. For example, if a test is supposed to power off the last
    node in the cluster (i.e. slice_str is "-1") with a sequence size of 4, the
    index returned is 4 so an annotation can read "Powering off node 4" instead
    of "Powering off node -1", which would look suspicious or incorrect to a
    user.

    Args:
      sequence (list or other sequence): Thing to slice
      slice_str (str): Comma-separated slices, such as "0", "1:", "0, 1, 2",
        "0:2, 3", "0:2, 3:4, :-1", etc.

    Returns (OrderedDict): Map of indices to items in the sequence.
    """
    ret = OrderedDict()
    slice_str = str(slice_str).replace("all", ":").replace(
        "n", "%d" % len(sequence))
    for slice_chunk in slice_str.split(","):
      match = CurieUtil.VALID_STR_SLICE_CHUNK_RE.match(slice_chunk)
      if match is None or not (
          match.start() == 0 and match.end() == len(slice_chunk)):
        raise ValueError("Invalid slice string passed: '%s'" % slice_chunk)
      elts = [eval(elt) if elt else None for elt in slice_chunk.split(":")]
      if len(elts) > 3:
        raise ValueError("Invalid slice '%s'" % slice_chunk)
      elif len(elts) > 1:
        s = slice(*elts)
      else:
        index = elts[0] + len(sequence) if elts[0] < 0 else elts[0]
        if index < 0 or index >= len(sequence):
          raise IndexError("Slice %r is out of range for sequence %r" %
                           (index, sequence))
        s = slice(index, index + 1)

      ret.update(zip(range(*s.indices(len(sequence))), sequence[s]))
    return ret


class ReleaseLock(object):
  """
  Context manager which releases lock on entry and reacquires on exit.

  If 'recursive' is True, repeatedly releases lock until it is not owned, and
  subsequently reacquires it an appropriate number of times.
  """
  def __init__(self, lock, recursive=False):
    self.__lock = lock
    self.__recursive = recursive
    self.__count = 0

  def __enter__(self):
    log.trace("Releasing lock on context enter")
    if self.__recursive:
      while self.__lock._is_owned():
        self.__lock.release()
        self.__count += 1
    else:
      self.__lock.release()
      self.__count += 1

  def __exit__(self, exc_type, exc_val, tb):
    log.trace("Acquiring lock on context exit")
    for ii in range(self.__count):
      self.__lock.acquire()


def get_optional_vim_attr(vim_object, attr_name):
  """
  Returns either the attribute value for the attribute 'attr_name' on the
  object 'vim_object', else None if the attribute isn't set.
  """
  try:
    return getattr(vim_object, attr_name)
  except IndexError:
    # IndexError is raised if an optional, non-array "May not be present" (see
    # vSphere API documentation) attribute is accessed. For optional, array
    # attributes, it appears the attribute is always present although the array
    # could consist of zero elements.
    return None


def get_cluster_class(cluster_metadata):
  """
  Returns the subclass of Cluster corresponding to the hypervisor
  type specified in 'cluster_metadata'.

  Returns:
    (Subclass of Cluster)
  """
  from curie.acropolis_cluster import AcropolisCluster
  from curie.generic_vsphere_cluster import GenericVsphereCluster
  from curie.hyperv_cluster import HyperVCluster
  from curie.nutanix_hyperv_cluster import NutanixHypervCluster
  from curie.nutanix_vsphere_cluster import NutanixVsphereCluster

  if cluster_metadata.cluster_hypervisor_info.HasField("esx_info"):
    if cluster_metadata.cluster_software_info.HasField("nutanix_info"):
      return NutanixVsphereCluster
    else:
      return GenericVsphereCluster
  elif cluster_metadata.cluster_hypervisor_info.HasField("hyperv_info"):
    if cluster_metadata.cluster_software_info.HasField("nutanix_info"):
      return NutanixHypervCluster
    else:
      return HyperVCluster
  elif cluster_metadata.cluster_hypervisor_info.HasField("ahv_info"):
    return AcropolisCluster
  else:
    raise ValueError("Unknown hypervisor type, metadata %s" %
                     cluster_metadata)


def chunk_iter(sequence, n):
  """
  Split a sequence into chunks no larger than n.

  If the sequence is not evenly divisible by n, the final chunk which is
  smaller than n will not be padded.

  Yields:
    sequence: Chunk with length less than or equal to n.
  """
  start = 0
  while True:
    end = start + n
    chunk = sequence[start:end]
    if chunk:
      yield chunk
      start += n
    else:
      break
