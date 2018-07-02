#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
"""
Util for iDRAC XML requests.

The idracadm command-line util provided by Dell essentially wraps a command
in an <EXEC> tag and issues a POST request to the iDRAC host. As an alternative
to wrapping calls to the iracadm binary, this util accepts racadm commands,
generates and sends the POST request, and parses the returned data.

This allows for both caching sessions (overhead for authentication averages
roughly 2-4 seconds), as well as eliminating the need for additional
dependencies on vendor binaries.

IdracUtil may be registered as an OoB Management handler
for OobManagementUtil, as it implements the required API.

Example (note it attempts to cache and reuse session credentials):

  iDRAC commandline:

    idracadm -r neutrino07-i1 -u root -p nutanix/4u getsysinfo
    idracadm -r neutrino07-i1 -u root -p nutanix/4u serveraction poweron

  Util usage:

    idrac = IdracUtil("neutrino07-i1", "root", "nutanix/4u")
    idrac.send_racadm_command("getsysinfo")
    idrac.send_racadm_command("serveraction poweron")
"""

import logging
import re
import time
from threading import Lock

import requests
from lxml import etree

from curie.exception import CurieError, CurieException
from curie.oob_management_util import OobManagementUtil
from curie.util import CurieUtil
from curie.xml_util import XmlData, XmlElementDescriptor, XmlMeta

log = logging.getLogger(__name__)


class RacAdmResponse(dict):
  def __init__(self, resp_root):
    super(RacAdmResponse, self).__init__()
    for node in resp_root:
      self[node.tag] = node.text.strip()

    rc = self.get("RC")
    cmdrc = self.get("CMDRC")

    self.CMDOUTPUT = self.get("CMDOUTPUT", "").strip()
    self.CMDRC = int(cmdrc, 16) if cmdrc else None
    self.RC = int(rc, 16) if rc else None
    self.SID = self.get("SID")


class RacAdmRequest(XmlData):
  """
  Base class for any iDRAC request objects.

  Manages DOM root for building and serializing a message.

  For login requests (handled internally as necessary):
  <LOGIN>
    <REQ>
      <USERNAME>USERNAME</USERNAME>
      <PASSWORD>PASSWORD</PASSWORD>
    </REQ>
  </LOGIN>

  For commands:

  <EXEC>
    <REQ>
      <CMDINPUT>SOME COMMAND STRING</CMDINPUT>
      <MAXOUTPUTLEN>0x0fff</MAXOUTPUTLEN>
    </REQ>
  </EXEC>
  """
  __metaclass__ = XmlMeta
  _DESCRIPTOR = XmlElementDescriptor("REQ")

  HEADERS = {
    "User-Agent": "racadm"
    }

  # URL template for iDRAC endpoint. 'cmd_type' is "exec" or "login".
  URL = "https://{idrac_ip}/cgi-bin/{cmd_type}"

  def __init__(self, host, method_name, cookie=None):
    # iDRAC IP Address.
    self._host = host

    # Name of method to invoke.
    self._method_name = method_name

    # iDRAC session cookie for 'host'.
    self._cookie = cookie

    # Headers for this request.
    self.__headers = super(RacAdmRequest, self).headers()
    self.__headers.update(self.HEADERS)
    if cookie:
      self.__headers.update(cookie)

    # Store a reference to the unwrapped node to use in appending parameters.
    self.__node = self._DESCRIPTOR.clone()

    # Wrap node in command type (LOGIN or EXEC)
    self.__wrapped_node = etree.Element(method_name)
    self.__wrapped_node.append(self.__node)
    super(RacAdmRequest, self).__init__(self.__wrapped_node)

  def headers(self):
    return self.__headers

  def add_param(self, name, value):
    """
    Appends a request parameter 'name' with value 'value'.

    Appends a parameter to the <REQ> node as:
    <$name>
      $value
    </$name>
    """
    param = etree.Element(name.upper())
    param.text = value
    # Append parameters relative to the inner node, not the command wrapper.
    self.__node.append(param)

  def send(self):
    """
    Issues command represented by this instance to iDRAC at 'self._host'.

    Returns:
      (dict) parsed XML response.

    Raises:
      CurieException<kInvalidParameter> on error.
    """
    cmd_type = "exec" if self._cookie else "login"
    path = self.URL.format(idrac_ip=self._host, cmd_type=cmd_type)
    # Don't log raw login command as credentials are in plaintext.
    if cmd_type != "login":
      log.trace("Sending XML-HTTP request to: %s\n"
                "\tHeaders: %s\n"
                "\tBody: %s", path, self.headers(), self.xml())
    raw_resp = requests.post(path, data=self.xml(), headers=self.headers(),
                             verify=False)
    # pylint takes issue with the requests.status_codes.codes LookupDict.
    # pylint: disable=no-member
    if raw_resp.status_code != requests.status_codes.codes.OK:
      raise CurieException(
        CurieError.kInternalError,
        "Error sending XML-HTTP request to iDRAC: %s %s" %
        (raw_resp.status_code, raw_resp.reason))
    log.trace("Received raw response:\n"
              "Headers: %s\n"
              "Body: %s", raw_resp.headers, raw_resp.content)
    return self.__parse_response(raw_resp)

  def __parse_response(self, raw_resp):
    """
    Parse 'raw_resp' whose content is expected to be an XML-formatted response.

    Args:
      raw_resp (requests.Response): response to parse.

    Returns:
      (dict) parsed response.

    Raises:
      CurieException<kInvalidParameter> on error.
    """
    error = ""
    try:
      root = etree.fromstring(raw_resp.content)
      assert root.tag == self.node().getroottree().getroot().tag, (
        "Invalid response, root tags do not match between request and "
        "response. (req: %s, resp: %s)" %
        (self.__node.getroottree().getroot().tag, root.tag))
      resp_node = root.getchildren()
    except (AssertionError, etree.Error) as exc:
      error = str(exc)
    if len(resp_node) == 0:
      error = "Response content missing <RESP> body"
    elif len(resp_node) > 1:
      error = "Invalid response content returned: '%s'" % raw_resp.content
    elif resp_node[0].tag != "RESP":
      error = ("Invalid response. First child is not a <RESP> tag. (found %s)"
               % resp_node[0].tag)
    if error:
      raise CurieException(CurieError.kInternalError, error)

    return RacAdmResponse(resp_node[0])


class IdracUtil(OobManagementUtil):
  """
  Util class for managing XML-HTTP iDRAC calls.
  """
  # TODO (jklein): Replace this global lock with more granular locking.
  # Lock for accessing HOST_SESSION_ID_MAP so that concurrent calls may make
  # use of a single session per iDRAC host.
  LOCK = Lock()

  # Map of iDRAC host IPs to cached session IDs.
  HOST_SESSION_ID_MAP = {}

  # Regex to parse the output from 'getsysinfo'.
  # Either captures 1 and 2 will be nonempty, or capture 3 will be nonempty.
  # If capture 1 is present, it's the key name for subdict from capture 2.
  _INFO_RE = re.compile(
    r"(?:(^[^:]+):$((?:[^\n]|\n(?!\n))+))|(^$(?:[^\n]|\n(?!\n))+\n^$)",
    re.DOTALL | re.MULTILINE)

  # Upper limit on exponential backoff applied to interval between retries.
  _MAX_RETRY_INTERVAL_SECS = 16

  def __init__(self, ip, username, password):
    # iDRAC host IP.
    self.__host = ip

    # iDRAC username.
    self.__username = username

    # iDRAC password.
    self.__password = password

  def get_chassis_status(self):
    """
    Returns:
      (dict) map of system info.

    Raises:
      CurieException<kInternalError> on failure.
    """
    resp = self.send_racadm_command("getsysinfo")
    status_map = {}

    def parse_group(text):
      return dict(
        [map(str.strip, line.split("=", 1)) for line in
         filter(None, map(str.strip, text.splitlines()))])

    for captures in self._INFO_RE.findall(resp.CMDOUTPUT):
      if captures[2]:
        status_map.update(parse_group(captures[2]))
      else:
        status_map[captures[0].strip()] = parse_group(captures[1])

    return status_map

  def power_cycle(self, async=False):
    """
    Power cycles node associated with iDRAC at 'self.__host'.

    Args:
      async (bool): Optional. If False, issue blocking calls to 'power_off'
      then 'power_on'.

    Returns:
      (bool) True on success, else False.
    """
    try:
      if not self.is_powered_on():
        return self.power_on(async=async)
      if async:
        self.send_racadm_command("serveraction powercycle")
        return True
    except CurieException:
      log.exception("Power cycling failed")
      return False

    # Synchronous: Make blocking calls to 'power_off', 'power_on'.
    if not self.power_off(async=False):
      return False
    return self.power_on(async=False)

  def power_on(self, async=False):
    """
    Powers on node associated with iDRAC at 'self.__host'.

    Args:
      async (bool): Optional. If True, return immediately after command
        succeeds, don't block until power state has changed to on.

    Returns:
      (bool) True on success, else False.
    """
    success = True
    try:
      self.send_racadm_command("serveraction powerup")
    except CurieException:
      log.exception("Power On failed")
      success = False

    if async or not success:
      return success

    return CurieUtil.wait_for(
      self.is_powered_on,
      "IPMI at '%s' to report node powered on" % self.__host,
      timeout_secs=600,
      poll_secs=5)

  def power_off(self, async=False):
    """
    Powers off node associated with iDRAC at 'self.__host'.

    Args:
      async (bool): Optional. If True, return immediately after command
        succeeds, don't block until power state has changed to off.

    Returns:
      (bool) True on success, else False.
    """
    success = True
    try:
      self.send_racadm_command("serveraction powerdown")
    except CurieException:
      log.exception("Power Off failed")
      success = False

    if async or not success:
      return success

    return CurieUtil.wait_for(
      lambda: not self.is_powered_on(),
      "IPMI at '%s' to report node powered off" % self.__host,
      timeout_secs=600,
      poll_secs=5)

  def is_powered_on(self):
    """
    Returns:
      (bool) True if powered on, else False.
    """
    resp = self.send_racadm_command("serveraction powerstatus")
    return resp.CMDOUTPUT.upper().endswith("ON")

  def send_racadm_command(self, cmd, max_retries=5):
    """
    Issues 'cmd' to iDRAC at 'self.__host', retrying up to 'max_retries' times.

    Automatically reauthenticates if necessary.

    Args:
      cmd (str): Command to execute.
      max_retries (int): Optional. Maximum number of retry attempts on failure.

    Returns:
      (dict) parsed XML response.
    """
    return self.__send_racadm_command_with_retries(cmd,
                                                   max_retries=max_retries)

  def __authenticate(self):
    """
    Send LOGIN request to iDRAC at 'self.__host'.

    Returns:
      (str) Session ID

    Raises:
      CurieException on error .
    """
    req = RacAdmRequest(self.__host, "LOGIN")
    req.add_param("USERNAME", self.__username)
    req.add_param("PASSWORD", self.__password)

    # Allow exception in send to propagate up.
    resp = req.send()
    if not (resp.SID and int(resp.SID)):
      raise CurieException(CurieError.kOobAuthenticationError,
                            "Failed to get valid session ID")
    return resp.SID

  def __get_session_cookie(self, cached_ok=True, max_retries=5):
    """
    Initialize new session if necessary, and return a session cookie.

    Args:
      cached_ok (bool): Optional. If True, use cached session if present.
      max_retries (int): Maximum number of retries on failure.

    Returns:
      (dict) Map "Cookie" -> <formatted session cookie>
    """
    with self.LOCK:
      if cached_ok:
        session_id = self.HOST_SESSION_ID_MAP.get(self.__host)
        if session_id:
          return {"Cookie": "sid=%s; path=/cgi-bin/" % session_id}
      for ii in range(max_retries):
        try:
          session_id = self.__authenticate()
          break
        except CurieException as exc:
          log.debug("Failed to establish session: %s (%d of %d attempts)",
                    exc, ii + 1, max_retries)
          time.sleep(5)
      else:
        err_msg = ("Failed to establish valid iDRAC session within %d attempts"
                   % max_retries)
        log.error(err_msg)
        raise CurieException(CurieError.kInternalError, err_msg)
      self.HOST_SESSION_ID_MAP[self.__host] = session_id
      return {"Cookie": "sid=%s; path=/cgi-bin/" % session_id}

  def __send_racadm_command(self, cmd):
    """
    Sends 'cmd' to iDRAC at 'self.__host'.

    Args:
      cmd (str): Command to execute.

    Returns:
      (dict) parsed XML response

    Raises:
      CurieException on error.
    """
    log.trace("Issuing iDRAC command: '%s'", cmd)
    req = RacAdmRequest(
      self.__host, "EXEC", cookie=self.__get_session_cookie())
    req.add_param("CMDINPUT", "racadm %s" % cmd)
    req.add_param("MAXOUTPUTLEN", "0x0fff")
    # Allow errors here to propagate up.
    resp = req.send()

    err_msg = ""
    err_code = CurieError.kInvalidParameter
    if resp.RC != 0:
      err_msg = "Error issuing RacAdm command: %s (RC=%s)" % (resp, resp.RC)
    elif resp.CMDRC != 0:
      cmd_output = resp.get("CMDOUTPUT", "").strip().lower()
      if (resp.CMDRC == 1 and
          resp.CMDOUTPUT.lower().find("session is not valid") > -1):
        err_code = CurieError.kOobAuthenticationError
        err_msg = "RacAdm authentication failed: %s" % resp
      else:
        err_msg = "RacAdm command failed: %s (CMDRC=%s)" % (resp, resp.CMDRC)

    if err_msg:
      raise CurieException(err_code, err_msg)
    return resp

  def __send_racadm_command_with_retries(self, cmd, max_retries=5):
    """
    Issue 'cmd', retrying on failure up to 'max_retries' times.

    Interval between calls has exponential backoff applied up to a cap of
    '_MAX_RETRY_INTERVAL_SECS'.

    Args:
      cmd (str): Command to execute.
      max_retries (int): Optional. Maximum number of retry attempts.

    Returns:
      (dict) parsed XML response

    Raises:
      CurieException on error.
    """
    curr_retry_interval_secs = 1
    for ii in range(max_retries + 1):
      try:
        return self.__send_racadm_command(cmd)
      except CurieException as exc:
        log.exception("'%s' failed", cmd)
        if ii < max_retries:
          log.info("Retrying (%d of %d attempts)", ii + 1, max_retries)
          if exc.error_code == CurieError.kOobAuthenticationError:
            log.debug("Possible session expiration, reauthenticating")
            self.__get_session_cookie(cached_ok=False)
          curr_retry_interval_secs = min(
            self._MAX_RETRY_INTERVAL_SECS, 2 * curr_retry_interval_secs)
          time.sleep(curr_retry_interval_secs)
    raise CurieException(
      CurieError.kInternalError,
      "Failed to execute '%s' after %d retries" % (cmd, max_retries))
