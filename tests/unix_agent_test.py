#!/usr/bin/env python
#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
#
import cStringIO
import httplib
import inspect
import logging
import os
import pwd
import shutil
import socket
import subprocess
import sys
import time
import unittest
import urllib2

import gflags

g_top_dir = "%s/.." % \
  os.path.abspath(os.path.dirname(os.path.abspath(globals()["__file__"])))
sys.path.append("%s/py" % g_top_dir)

from curie import charon_agent_interface_pb2
from curie import log as curie_log
from curie.agent_rpc_client import  AgentRpcClient
from curie.charon_agent_interface_pb2 import CmdStatus
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.rpc_util import CURIE_CLIENT_DEFAULT_RETRY_TIMEOUT_CAP_SECS

log = logging.getLogger(__name__)

FLAGS = gflags.FLAGS

gflags.DEFINE_string("curie_unix_agent_test_dir",
                     "%s/pytest/output/curie_unix_agent_test" % g_top_dir,
                     "Test output directory.",
                     short_name="o")

#==============================================================================

def wait_for_curie_unix_agent():
  url = "http://127.0.0.1:5001"
  t1 = time.time()
  while True:
    try:
      urllib2.urlopen(url).read()
      break
    except (httplib.HTTPException, socket.error, urllib2.URLError):
      pass
    t2 = time.time()
    elapsed_secs = t2 - t1
    if elapsed_secs > 15:
      assert 0, "Timeout waiting for curie_unix_agent to accept requests"
    time.sleep(0.1)

def start_curie_unix_agent(test_dir):
  cmd_buf = cStringIO.StringIO()
  cmd_buf.write("env NUTANIX_LOG_DIR=%s " % test_dir)
  cmd_buf.write("python %s/bin/curie_unix_agent " % g_top_dir)
  cmd_buf.write("--curie_agent_dir=%s/curie_unix_agent &" % test_dir)
  cmd = cmd_buf.getvalue()
  rv = os.system(cmd)
  assert rv == 0, cmd
  wait_for_curie_unix_agent()

def stop_curie_unix_agent():
  cmd = "fuser -n tcp 5001"
  proc = subprocess.Popen(
    cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  stdout, _ = proc.communicate()
  rv = proc.wait()
  if rv != 0:
    log.error("%s RV is %d", cmd, rv)
    assert rv == 1, cmd
    # No process listening on TCP port 5001.
    return
  pid = int(stdout.strip())
  cmdline = open("/proc/%d/cmdline" % pid).read()
  if "curie_unix_agent" not in cmdline:
    assert 0, "Non-curie_unix_agent process %d listening on TCP port 5001" % \
      pid
  cmd = "kill -9 %d" % pid
  rv = os.system(cmd)
  assert rv == 0, cmd

#==============================================================================

class CurieUnixAgentApiTest(unittest.TestCase):
  @staticmethod
  def output_dir_name():
    """
    Return a directory name under --curie_unix_agent_test_dir for the test's
    output.
    """
    raise NotImplementedError()

  def setup_curie_unix_agent(self):
    stop_curie_unix_agent()
    log_dir = os.path.join(FLAGS.curie_unix_agent_test_dir,
                           self.output_dir_name())
    if os.path.exists(log_dir):
      shutil.rmtree(log_dir)
    os.mkdir(log_dir)
    start_curie_unix_agent(log_dir)

  def teardown_curie_unix_agent(self):
    stop_curie_unix_agent()

  def run_hostname(self):
    user = pwd.getpwuid(os.getuid()).pw_name
    proc = subprocess.Popen("hostname",
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True)
    hostname_stdout, hostname_stderr = proc.communicate()
    rv = proc.wait()
    self.assertEqual(rv, 0)
    return user, hostname_stdout

  def send_rpc(self,
               cmd_name,
               arg,
               expected_rpc_error_codes=()):
    """
    Send RPC 'cmd_name' via RpcClient. Returns the deserialized protobuf
    response from the server.

    If 'expected_rpc_error_codes' is not empty, then we expect the RPC to
    fail with the given RPC error codes.
    """
    arg_data = arg.SerializeToString()
    rpc_client = AgentRpcClient("127.0.0.1")
    ret, err = rpc_client.send_rpc(cmd_name, arg)
    if expected_rpc_error_codes:
      if err is None:
        self.fail("RPC '%s' should have failed with error codes '%s'" % (
          cmd_name, ", ".join(map(str, expected_rpc_error_codes))))
      actual_rpc_error_codes = err.error_codes
      actual_rpc_error_codes_set = set(actual_rpc_error_codes)
      expected_rpc_error_codes_set = set(expected_rpc_error_codes)
      self.assertEqual(actual_rpc_error_codes_set,
                       expected_rpc_error_codes_set)
    else:
      self.assert_(ret is not None)
    return ret

  def cmd_status(self, cmd_id, include_output=False):
    arg = charon_agent_interface_pb2.CmdStatusArg()
    arg.cmd_id = cmd_id
    arg.include_output = include_output
    ret = self.send_rpc("CmdStatus", arg)
    return ret

  def cmd_stop(self, cmd_id):
    arg = charon_agent_interface_pb2.CmdStopArg()
    arg.cmd_id = cmd_id
    ret = self.send_rpc("CmdStop", arg)
    return ret

  def cmd_remove(self, cmd_id):
    arg = charon_agent_interface_pb2.CmdRemoveArg()
    arg.cmd_id = cmd_id
    ret = self.send_rpc("CmdRemove", arg)
    return ret

  def cmd_list(self):
    arg = charon_agent_interface_pb2.CmdListArg()
    ret = self.send_rpc("CmdList", arg)
    return ret

  def wait_for_cmd(self, cmd_id, include_output=False):
    """
    Wait for the command with ID 'cmd_id' to reach a terminal state. Returns a
    CmdStatus response once the command has reached a terminal state.
    """
    while True:
      ret = self.cmd_status(cmd_id, include_output=include_output)
      if ret.cmd_status.state != CmdStatus.kRunning:
        return ret
      log.info("Waiting for command %s to finish", cmd_id)
      time.sleep(1)

#==============================================================================

class CurieUnixAgentExecuteTest(CurieUnixAgentApiTest):
  @staticmethod
  def output_dir_name():
    return "api_execute_test"

  def test(self):
    log.info("Running %s", self.__class__.__name__)
    user, hostname_stdout = self.run_hostname()
    try:
      self.setup_curie_unix_agent()
      self._test_body(user, hostname_stdout)
    finally:
      self.teardown_curie_unix_agent()

  def _test_body(self, user, hostname_stdout):
    # Test CmdExecute.
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.user = user
    arg.cmd_id = str(int(time.time() * 1e6))
    arg.cmd = "hostname"
    self.send_rpc("CmdExecute", arg)
    self.send_rpc("CmdExecute", arg)  # Idempotent so this shouldn't fail.
    # Test CmdStatus while waiting for the command to finish and CmdStop.
    ret = self.wait_for_cmd(arg.cmd_id, include_output=True)
    self.cmd_stop(arg.cmd_id)
    self.cmd_stop(arg.cmd_id)  # Idempotent so this shouldn't fail.
    self.assert_(ret.cmd_status.HasField("exit_status"))
    self.assertEqual(ret.cmd_status.exit_status, 0)
    self.assert_(ret.HasField("stdout"))
    self.assert_(ret.HasField("stderr"))
    self.assertEqual(ret.stdout, hostname_stdout)
    self.assertEqual(ret.stderr, "")
    # Test CmdList.
    ret = self.cmd_list()
    self.assertEqual(len(ret.cmd_summary_list), 1)
    self.assertEqual(ret.cmd_summary_list[0].cmd_id, arg.cmd_id)
    self.assertEqual(ret.cmd_summary_list[0].cmd, "hostname")
    self.assertEqual(ret.cmd_summary_list[0].state, CmdStatus.kSucceeded)
    # Test CmdRemove.
    self.cmd_remove(arg.cmd_id)
    ret = self.cmd_list()
    self.assertEqual(len(ret.cmd_summary_list), 0)
    self.cmd_remove(arg.cmd_id)  # Idempotent so this shouldn't fail.
    ret = self.cmd_list()
    self.assertEqual(len(ret.cmd_summary_list), 0)
    # Test method for simulating synchronous commands.
    rpc_client = AgentRpcClient("127.0.0.1")
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.user = user
    arg.cmd_id = str(int(time.time() * 1e6))
    arg.cmd = "hostname"
    exit_status, stdout, stderr = \
        rpc_client.cmd_execute_sync(arg, 10, include_output=True)
    self.assertEqual(exit_status, 0)
    self.assertEqual(stdout, hostname_stdout)
    self.assertEqual(stderr, "")
    ret = self.cmd_list()
    self.assertEqual(len(ret.cmd_summary_list), 0)  # Should be removed.
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.user = user
    arg.cmd_id = str(int(time.time() * 1e6))
    arg.cmd = "hostname"
    exit_status, stdout, stderr = \
        rpc_client.cmd_execute_sync(arg, 10, include_output=False)
    self.assertEqual(exit_status, 0)
    self.assert_(stdout is None)
    self.assert_(stderr is None)
    ret = self.cmd_list()
    self.assertEqual(len(ret.cmd_summary_list), 0)  # Should be removed.

#==============================================================================

class CurieUnixAgentRpcTimeoutTest(CurieUnixAgentApiTest):

  @staticmethod
  def output_dir_name():
    return "rpc_timeout_test"

  def test(self):
    """
    This test runs a modified version of the CurieUnixAgent which will
    artificially introduce timeouts, patches the '_send_rpc_sync' method
    to enforce a check on the total (simulated) duration of each call with
    retries, and then runs the test as in 'CurieUnixAgentExecuteTest.test'.
    """
    # Original _send_rpc_sync method to patch and restore after test.
    orig_send_rpc_sync = AgentRpcClient._send_rpc_sync

    class RpcWithTimeout(object):
      TIMEOUT_COUNT = 16
      TIMEOUT_DURATION_SECS = 1

      @classmethod
      def get_timeout_with_backoff(cls, retry_num):
        self.assert_(retry_num >= 0, "'retry_num' cannot be negative")
        timeout_cap_secs = max(
          cls.TIMEOUT_DURATION_SECS,
          CURIE_CLIENT_DEFAULT_RETRY_TIMEOUT_CAP_SECS)
        return min(2**retry_num * cls.TIMEOUT_DURATION_SECS, timeout_cap_secs)

      def __init__(self):
        self.__curr_count = 0

      def __call__(self, method_name, arg, rpc_timeout_secs):
        expected_timeout = self.get_timeout_with_backoff(self.__curr_count)
        assert expected_timeout == rpc_timeout_secs, (
          "Timeout mismatch: Expected %s\tActual %s" %
          (expected_timeout, rpc_timeout_secs))
        self.__curr_count += 1
        if self.__curr_count > self.TIMEOUT_COUNT:
          self.__curr_count = 0
          this = inspect.getouterframes(
            inspect.currentframe())[1][0].f_locals["self"]
          return orig_send_rpc_sync(this, method_name, arg, rpc_timeout_secs)
        else:
          raise CurieException(CurieError.kTimeout,
                                "RPC '%s' timed out after %f seconds" % (
                                  method_name, rpc_timeout_secs))

    log.info("Running %s", self.__class__.__name__)
    user, hostname_stdout = self.run_hostname()
    try:
      # Patch '_send_rpc_sync'
      AgentRpcClient._send_rpc_sync = RpcWithTimeout()
      self.setup_curie_unix_agent()
      wait_for_curie_unix_agent()
      CurieUnixAgentExecuteTest._test_body.__get__(self)(user,
                                                          hostname_stdout)
    finally:
      # Restore '_send_rpc_sync'
      AgentRpcClient._send_rpc_sync = orig_send_rpc_sync
      self.teardown_curie_unix_agent()

#==============================================================================

class CurieUnixAgentFileTest(CurieUnixAgentApiTest):
  @staticmethod
  def output_dir_name():
    return "api_file_test"

  def test(self):
    log.info("Running %s", self.__class__.__name__)
    try:
      self.setup_curie_unix_agent()
      data = open("/etc/services").read()
      # Get entire file.
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = "/etc/services"
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data)
      # Get various ranges of the file with default offset, custom length.
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = "/etc/services"
      arg.length = 0
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == "")
      arg.length = 1
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data[0:1])
      arg.length = 20
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data[0:20])
      # Get various ranges of the file with custom offset, default length.
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = "/etc/services"
      arg.offset = 1
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data[1:])
      arg.offset = len(data)
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == "")
      arg.offset = len(data) + 12345
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == "")
      # Get various ranges of the file with custom offset, custom length.
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = "/etc/services"
      arg.offset = 1
      arg.length = 20
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data[1:21])
      arg.offset = 40
      arg.length = 99999999
      ret = self.send_rpc("FileGet", arg)
      self.assert_(ret.data == data[40:])
      # Get non-existent file.
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = "/foo/bar/baz"
      expected_rpc_error_codes = [CurieError.kInvalidParameter]
      self.send_rpc("FileGet",
                    arg,
                    expected_rpc_error_codes=expected_rpc_error_codes)
    finally:
      self.teardown_curie_unix_agent()

#==============================================================================

if __name__ == "__main__":
  try:
    argv = FLAGS(sys.argv)
    curie_log.initialize()
  except gflags.FlagsError, ex:
    sys.stderr.write("Error %s\nUsage: %s [FLAGS]\n" % (ex, sys.argv[0]))
    sys.exit(3)
  if not os.path.exists(FLAGS.curie_unix_agent_test_dir):
    os.makedirs(FLAGS.curie_unix_agent_test_dir)
  sys.argv = argv
  unittest.main()
