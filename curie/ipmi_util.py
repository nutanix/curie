#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
"""
Provides utils related to IPMI, and wrapper around ipmitool calls.
"""
import json
import logging
import re
import time

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.log import CHECK_EQ
from curie.oob_management_util import OobManagementUtil
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class Flag(object):
  def __init__(self, switch, value):
    self._switch = switch
    self._value = value

  def __iter__(self):
    return iter([self._switch, str(self._value)])

  def __str__(self):
    return " ".join(self)

  def to_unredacted(self):
    return self


class RedactedFlag(Flag):
  def __iter__(self):
    return iter([self._switch, "<REDACTED>"])

  def to_unredacted(self):
    return Flag(self._switch, self._value)


class RepeatedFlag(Flag):
  def __iter__(self):
    return iter([self._switch] * self._value)


class SystemEvent(object):
  FIELDS = ["event_id", "timestamp", "sensor", "description", "direction"]
  TIME_FORMAT = "%m/%d/%Y %H:%M:%S"

  def __init__(self, raw_ipmi_output):
    ipmi_info = map(lambda col: col.strip(), raw_ipmi_output.split("|"))
    # Combine date and time into one field, shift rest of array.
    ipmi_info[1] = "%s %s" % (ipmi_info[1], ipmi_info.pop(2))

    # NOTE: Setting these fields explicitly, rather than iterating to aid
    # with autocomplete and pylint.
    self.event_id = int(ipmi_info[0], base=16)
    # Event times only have resolution in seconds.
    self.timestamp = int(time.mktime(time.strptime(ipmi_info[1],
                                                   self.TIME_FORMAT)))
    self.sensor = ipmi_info[2]
    self.description = ipmi_info[3]
    self.direction = ipmi_info[4]

  def __str__(self):
    return json.dumps(
      dict(map(lambda f: (f, getattr(self, f)), self.FIELDS)),
      indent=2, sort_keys=True)


class IpmiUtil(OobManagementUtil):
  """
  Wraps calls to ipmitool.

  Generated commands will be of the form:
    <IPMITOOL_ABSPATH> <GENERATED_FLAGS> <COMMAND> [SUB_COMMAND] [CMD_ARGS]
  """
  VALID_BOOT_DEVICES = [
    "none", "pxe", "disk", "safe", "diag", "cdrom", "bios", "floppy"]

  # Upper limit on exponential backoff applied to interval between retries.
  _MAX_RETRY_INTERVAL_SECS = 16

  def __init__(self, ip, username, password, interface="lanplus",
               verbosity=0, ipmitool_abspath="/usr/bin/ipmitool"):
    self.host = ip
    self.__ipmitool_abspath = ipmitool_abspath
    self.__flags = {
      "host": Flag("-H", ip),
      "user": RedactedFlag("-U", username),
      "password": RedactedFlag("-P", password),
      "interface": Flag("-I", interface),
      "verbosity": RepeatedFlag("-v", verbosity)
      }

  def get_chassis_status(self):
    """
    Returns:
      (dict): Map of IPMI chassis status data.

    Raises:
      CurieException on error.
    """
    stdout, stderr = self.__execute_command_with_retries(["chassis", "status"])

    output_map = {}
    for line in stdout.splitlines():
      key, value = line.split(":")
      output_map[key.strip()] = value.strip()
    return output_map

  def power_cycle(self, async=False):
    """
    Power cycles the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, making blocking calls to 'power_off'
      and then 'power_on'.

      If True and node is powered off, performs async 'power_on' call,
      otherwise issues the (async) 'power cycle' command.

    Returns:
      (bool): True on success, else False.
    """
    if not self.is_powered_on():
      log.warning("power_cycle requested for powered-down node via IPMI at "
                  "'%s'", self.__flags["host"])
      return self.power_on(async=async)

    if async:
      try:
        self.__execute_command_with_retries(["power", "cycle"])
        return True
      except CurieException:
        log.warning("Failed to power cycle", exc_info=True)
        return False

    # Synchronous: perform blocking calls to power off then power on.
    if not self.power_off(async=False):
      return False
    return self.power_on(async=False)

  def power_on(self, async=False):
    """
    Powers on the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, block until power state is on.

    Returns:
      (bool): True on success, else False.
    """
    # NB: IPMI power ops are not necessarily idempotent, so it's necessary to
    # check the power state before issuing the command. This still does not
    # guarantee that we're safe from a potential race.
    if self.is_powered_on():
      return True

    try:
      self.__execute_command_with_retries(["power", "on"])
    except CurieException:
      log.exception("Exception in __execute_command_with_retries")
      return False

    if async:
      return True
    return CurieUtil.wait_for(
      self.is_powered_on,
      "node '%s' to power on" % self.__flags["host"]._value,
      timeout_secs=600, poll_secs=5)

  def power_off(self, async=False):
    """
    Powers off the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, block until power state is off.

    Returns:
      (bool): True on success, else False.
    """
    # NB: IPMI power ops are not necessarily idempotent, so it's necessary to
    # check the power state before issuing the command. This still does not
    # guarantee that we're safe from a potential race.
    if not self.is_powered_on():
      return True

    try:
      self.__execute_command_with_retries(["power", "off"])
    except CurieException:
      log.exception("Exception in __execute_command_with_retries")
      return False

    if async:
      return True

    return CurieUtil.wait_for(
      lambda: not self.is_powered_on(),
      "node '%s' to power off" % self.__flags["host"]._value,
      timeout_secs=600, poll_secs=5)

  def is_powered_on(self):
    """
    Checks whether chassis power state is 'on'.

    Returns:
      (bool) True if powered on, else False.
    """
    status = self.get_chassis_status().get("System Power", "")
    return status.strip().lower() == "on"

  def get_fru_info(self):
    """
    Dump FRU info.

    Returns:
      (list<dict>): List of FRU info dicts.
    """
    fru_list = []
    # TODO: See if there's a guaranteed upper bound on the number of valid
    # fru_ids.
    for ii in range(256):
      try:
        # Get byte-length of FRU data at index 'ii'.
        stdout, stderr = self.__execute_command_with_retries(
          ["raw", "0x0a", "0x10", hex(ii)])
      except CurieException:
        # Index 0 should exist. Raise exception if we fail to find data.
        # Otherwise, assume we've found the last FRU index.
        if ii == 0:
          raise
        break

      try:
        curr_fru_byte_length = int("0x%s" % "".join(stdout.split()), 16)
      except (TypeError, ValueError):
        # Unable to parse output as a hex string, consider index as invalid.
        curr_fru_byte_length = 0

      # Index 0 should not have length 0, otherwise, assume we've found the
      # last FRU index.
      if curr_fru_byte_length == 0:
        if ii == 0:
          raise CurieException(
            CurieError.kInternalError,
            "Failed to parse length for fru_id 0:\n\tstdout=%s\n\tstderr=%s" %
            (stdout, stderr))
        break

      # FRU index 'ii' reports non-zero length, attempt to dump contents.
      try:
        stdout, stderr = self.__execute_command_with_retries(
          ["fru", "print", str(ii)])
      except CurieException:
        log.warning("Unable to read data from fru_id '%s'", exc_info=True)
        time.sleep(1)
        continue

      # Possible FRU data is invalid (in particular, that reading from the
      # index will return a block of zeros). This may result in stdout
      # reporting an error, or containing no data.
      lines = []
      if stdout is not None:
        # Filter out section headers.
        lines = filter(lambda l: l.count(":") > 0, stdout.splitlines())
        # Skip empty data.
        if lines:
          fru_list.append(dict(map(
            lambda l: map(str.strip, l.split(":", 1)), lines)))
      if not lines:
        log.warning("Skipping invalid data from fru_id '%s':\n"
                    "\tstdout=%s\n"
                    "\tstderr=%s", ii, stdout, stderr)

      # Avoid flooding IPMI with requests.
      time.sleep(1)

    return fru_list

  def get_lan_config(self):
    """
    Get IP, MAC address, etc. for the node's IPMI.

    Returns:
      (dict): Map of IPMI lan cofiguration data.
    """
    stdout, stderr = self.__execute_command_with_retries(["lan", "print"])

    # Various keys have multiline data.
    stdout = re.sub(r"\n\s+:", ";:;", stdout)

    # Key/Val pairs.
    lan_map_pairs = map(
      lambda tupl: map(str.strip, tupl),
      map(lambda line: line.split(":", 1), stdout.splitlines()))
    # Some values may themselves be space separated lists, or lists of colon
    # delimited pairs.
    sub_objs = filter(lambda pair: ";:;" in pair[1], lan_map_pairs)
    sub_objs = map(lambda pair: (pair[0], pair[1].split(";:;")), sub_objs)
    sub_lists = filter(lambda pair: ":" not in pair[1], sub_objs)
    sub_dicts = filter(lambda pair: ":" in pair[1], sub_objs)

    sub_lists = map(lambda pair: (pair[0], map(str.strip, pair[1])), sub_lists)
    sub_lists = dict(sub_lists)
    sub_dicts = map(lambda pair: (
      pair[0], dict(map(str.strip, pair[1].split(":")))), sub_dicts)
    sub_dicts = dict(sub_dicts)

    lan_map = dict(lan_map_pairs)
    lan_map.update(sub_lists)
    lan_map.update(sub_dicts)

    return lan_map

  def get_mac_addrs_supermicro(self):
    """
    Get MAC addresses for the node's network cards.

    NB: Returns only two addresses, second address is autogenerated.

    Returns:
      (list<str>): MAC Addresses.
    """
    stdout, stderr = self.__execute_command_with_retries(
      ["raw", "0x30", "0x21"])

    byte_list = stdout.split()
    CHECK_EQ(len(byte_list), 10, "Failed to retrieve MAC addresses from BMC")

    macs = ["-".join(byte_list[-6:])]
    byte_list[-1] = "%02x" % (int(byte_list[-1], base=16) + 1)
    macs.append("-".join(byte_list[-6:]))

    return macs

  def get_event_log(self):
    """
    Dump System Event Log.

    Returns:
      (list<dict>): List of events represented as dict/JSON.
    """
    stdout, stderr = self.__execute_command_with_retries(["sel", "list"])
    return [SystemEvent(line) for line in stdout.splitlines()]

  def set_bootdev_pxe(self):
    """
    Forces PXE on next boot for the node associated with
    'self.__flags["host"]'.

    Returns:
      (bool): True on success, else False.
    """
    try:
      self.set_bootdev("pxe")
      return True
    except Exception:
      log.exception("Exception in set_bootdev")
      return False

  def set_bootdev(self, device):
    """
    Sets first bootdevice to `device`.on next boot for the node associated
    with 'self.__flags["host"]'.

    Returns:
      (bool): True on success, else False.
    """
    if device not in self.VALID_BOOT_DEVICES:
      raise CurieException(CurieError.kInvalidParameter,
                            "Invalid boot device '%s'" % device)
    try:
      stdout, stderr = self.__execute_command_with_retries(
        ["chassis", "bootdev", device])
    except CurieException:
      log.exception("Exception in 'chassis bootdev %s'", device)
      return False
    return True

  def __execute_command(self, cmd):
    """
    Executes 'cmd' via ipmitool 'self.__ipmitool_abspath' using '__flags'.

    Returns:
      (tuple): (rv, stdout, stderr)

    Raises:
      CurieException on bad input or failure to execute the command.
    """
    if isinstance(cmd, basestring):
      cmd = cmd.split(" ")

    if not isinstance(cmd, list):
      raise CurieException(
        CurieError.kInternalError,
        "'cmd' must be of type list or str, not '%s'" % cmd.__class__.__name__)

    generated_cmd = []
    map(generated_cmd.extend,
        [[self.__ipmitool_abspath], self.__get_flag_list(redacted=False), cmd])

    redacted_cmd = []
    map(redacted_cmd.extend,
        [[self.__ipmitool_abspath], self.__get_flag_list(redacted=True), cmd])

    log.info("Executing IPMI command:\n\t'%s'", " ".join(redacted_cmd))
    rv, stdout, stderr = CurieUtil.timed_command(
      " ".join(generated_cmd), timeout_secs=60,
      formatted_cmd=" ".join(redacted_cmd))

    cmd_output = "rv=%s\nstdout=%s\nstderr=%s" % (rv, stdout, stderr)
    if rv < 0:
      raise CurieException(
        CurieError.kInternalError,
        "Failed to execute command: '%s': %s" % (redacted_cmd, cmd_output))
    log.debug(cmd_output)
    return (rv, stdout, stderr)

  def __execute_command_with_retries(self, cmd, max_retries=5):
    """
    Executes 'cmd', retrying on error up to 'max_retries' times.

    Interval between calls has exponential backoff applied up to a cap of
    '_MAX_RETRY_INTERVAL_SECS'.

    Returns:
      (tuple): (stdout, stderr)

    Raises:
      CurieException if 'cmd' does not succeed within 'max_retries' + 1 calls.
    """
    curr_retry_interval_secs = 1
    for ii in range(max_retries + 1):
      try:
        rv, stdout, stderr = self.__execute_command(cmd)
        if rv == 0:
          return stdout, stderr
        error_msg = (
          "Error executing '%s':\n\trv=%s\n\tstdout=%s\n\tstderr=%s" %
          (cmd, rv, stdout, stderr))
      except CurieException as exc:
        error_msg = "'%s' failed: '%s'" % (cmd, exc)

      if ii < max_retries:
        log.error(error_msg)
        log.info("Retrying (%d of %d retries)", ii + 1, max_retries)
        curr_retry_interval_secs = min(self._MAX_RETRY_INTERVAL_SECS,
                                       2 * curr_retry_interval_secs)
        time.sleep(curr_retry_interval_secs)
      else:
        raise CurieException(CurieError.kInternalError, error_msg)

  def __get_flag_list(self, redacted=True):
    """
    Generates list of command line switch/values to pass with the command.

    Args:
      redacted (bool): Optional. If False, RedactedFlags are returned unmasked.

    Returns:
      (list<Flag>): List of flags (corresponds to splitting serialized flags on
        whitespace).
    """
    flags = []
    if not redacted:
      map(flags.extend, [f.to_unredacted() for f in self.__flags.values()])
    else:
      map(flags.extend, self.__flags.values())
    return flags
