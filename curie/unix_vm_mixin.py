#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import logging
import time

import requests

from curie import charon_agent_interface_pb2
from curie.agent_rpc_client import AgentRpcClient
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class CurieUnixVmMixin(object):
  """
  Mixin providing functionality for Unix VMs hosting CurieUnixAgent.
  """

  def is_accessible(self):
    "See Vm.is_accessible documentation."
    if not self._vm_ip:
      log.debug("VM '%s' (node_id: %r), IP address not set (got: %r)",
                self, self._node_id, self._vm_ip)
      return False
    try:
      # Access a URL served by the curie_unix_agent that the VM should
      # eventually be running.
      url = "http://%s:5001" % self._vm_ip
      resp = requests.head(url, timeout=10)
      # Check that the response code is not an error.
      resp.raise_for_status()
      return True
    except requests.exceptions.RequestException:
      return False

  def path_exists(self, path):
    exit_status, stdout, stderr = self.execute_sync(str(time.time()),
                                                    "ls %s" % path,
                                                    120)
    if exit_status == 0:
      return True
    else:
      return False

  def transfer_to(self,
                  local_path,
                  remote_path,
                  timeout_secs,
                  recursive=False):
    """See Vm.transfer_to documentation.
    """
    result = self._scp_client.transfer_to(local_path,
                                          remote_path,
                                          timeout_secs,
                                          recursive=recursive)
    if result:
      log.debug("Transferred %s to %s:%s", local_path, self, remote_path)
    else:
      log.error("Failed to transfer %s to %s:%s",
                local_path, self, remote_path)
    return result

  def transfer_from(self,
                    remote_path,
                    local_path,
                    timeout_secs,
                    recursive=False):
    """See Vm.transfer_from documentation.
    """
    if not recursive:
      # If we're fetching a single file, use a FileGet RPC which is much faster
      # than forking and running scp.
      rpc_client = AgentRpcClient(self._vm_ip)
      arg = charon_agent_interface_pb2.FileGetArg()
      arg.path = remote_path
      try:
        ret, err = rpc_client.FileGet(arg)
      except CurieException as ex:
        log.debug("Caught CurieException during FileGet")
        ret, err = None, ex.error_msg
      if err is not None:
        log.exception("Error fetching file %s from %s",
                      remote_path, self._vm_ip)
        result = False
      else:
        open(local_path, "w").write(ret.data)
        result = True
    else:
      result = self._scp_client.transfer_from(remote_path,
                                              local_path,
                                              timeout_secs,
                                              recursive=recursive)
    if result:
      log.debug("Transferred %s from %s:%s", local_path, self, remote_path)
    else:
      log.error("Failed to transfer %s from %s:%s",
                local_path, self, remote_path)
    return result

  def execute_async(self, cmd_id, cmd, user="nutanix"):
    "See Vm.execute_async documentation."
    rpc_client = AgentRpcClient(self._vm_ip)
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.user = user
    arg.cmd_id = cmd_id
    arg.cmd = cmd
    log.debug("%s executing async command: %s", self, cmd)
    ret, err = rpc_client.CmdExecute(arg)
    if err is not None:
      raise CurieException(err.error_codes[0],
                            "Command %s (%s) failed, err %s" %
                            (cmd, cmd_id, err))

  def execute_sync(self,
                   cmd_id,
                   cmd,
                   timeout_secs,
                   user="nutanix",
                   include_output=False):
    "See Vm.execute_sync documentation."
    rpc_client = AgentRpcClient(self._vm_ip)
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.user = user
    arg.cmd_id = cmd_id
    arg.cmd = cmd
    log.debug("%s executing synchronous command: %s", self, cmd)
    exit_status, stdout, stderr = \
      rpc_client.cmd_execute_sync(arg,
                                  timeout_secs,
                                  include_output=include_output)
    if exit_status != 0:
      raise CurieException(CurieError.kInternalError,
                            "Command %s (%s) failed, exit_status %d, %s, %s" %
                            (cmd, cmd_id, exit_status, stdout, stderr))
    return (exit_status, stdout, stderr)

  def stop_cmd(self, cmd_id):
    """Stop a command running on this VM.

    The command specified by cmd_id must already be running.

    Args:
      cmd_id: (str) ID of the command to stop.

    Raises:
      CurieException: If stopping the command fails.
    """
    rpc_client = AgentRpcClient(self._vm_ip)
    arg = charon_agent_interface_pb2.CmdStopArg()
    arg.cmd_id = cmd_id
    log.debug("Stopping command with id: %s", cmd_id)
    ret, err = rpc_client.CmdStop(arg)
    if err is not None:
      raise CurieException(CurieError.kInternalError,
                            "Stopping command with id '%s' failed: %s" %
                            (cmd_id, err))
