#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging
import time

from curie import charon_agent_interface_pb2
from curie.charon_agent_interface_pb2 import CurieAgentRpcSvc_Stub
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.log import CHECK
from curie.rpc_util import RpcClientUtil, RpcClientMeta

log = logging.getLogger(__name__)


class AgentRpcClient(RpcClientUtil):
  """
  Client to issue CurieAgentRpcSvc RPCs to server at a given 'ip', 'port',
  and endpoint 'path'.
  """

  __metaclass__ = RpcClientMeta(CurieAgentRpcSvc_Stub)

  def __init__(self, ip, port=5001, path="/rpc"):
    super(AgentRpcClient, self).__init__(ip, port, path)

  # NB: These stubs are not necessary, but help avoid spurious errors in
  #     static analysis tools.
  # 'arg' is intentionally unused in these stubs.
  # pylint: disable=unused-argument
  def CmdExecute(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  def CmdStatus(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  def CmdStop(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  def CmdRemove(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  def CmdList(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  def FileGet(self, arg):
    raise NotImplementedError("RPC util failed to set handler")

  # pylint: enable=unused-argument

  def cmd_execute_sync(self, arg, timeout_secs, include_output=False):
    """
    Given the CmdExecute request 'arg', simulate synchronous execution by
    sending a CmdExecute RPC to start the command, polling until the command
    reaches a terminal state, then returning an (exit_status, stdout, stderr)
    tuple for the command. 'include_output' specifies whether stdout and stderr
    should be returned or not (both are None if this is set to False).
    """
    curie_ex = None
    try:
      t1 = time.time()
      # Send a CmdExecute RPC to start the command.
      execute_ret, execute_err = self.CmdExecute(arg)
      if execute_err is not None:
        raise CurieException(execute_err.error_codes[0],
                              execute_err.error_msgs[0])
      status_arg = charon_agent_interface_pb2.CmdStatusArg()
      status_arg.cmd_id = arg.cmd_id
      status_arg.include_output = include_output
      status_ret = None
      # Poll until the command reaches a terminal state or it times out.
      while True:
        status_ret, status_err = self.CmdStatus(status_arg)
        if status_err is not None:
          raise CurieException(status_err.error_codes[0],
                                status_err.error_msgs[0])
        if (status_ret.cmd_status.state !=
            charon_agent_interface_pb2.CmdStatus.kRunning):
          # Command is in a terminal state.
          break
        t2 = time.time()
        if (t2 - t1) > timeout_secs:
          raise CurieException(CurieError.kTimeout,
                                "Timeout waiting for command %s" % arg.cmd_id)
        time.sleep(1)
      # Check that we have the exit status for the command.
      CHECK(status_ret is not None)
      if not status_ret.cmd_status.HasField("exit_status"):
        raise CurieException(
          CurieError.kInternalError,
          "Missing exit status for command %s" % arg.cmd_id)
      # Return an (exit_status, stdout, stderr) tuple for the command.
      exit_status = status_ret.cmd_status.exit_status
      stdout = status_ret.stdout if status_ret.HasField("stdout") else None
      stderr = status_ret.stderr if status_ret.HasField("stderr") else None
      return (exit_status, stdout, stderr)
    except CurieException, ex:
      curie_ex = ex
      raise
    finally:
      # On either success or failure, attempt to remove the command.
      remove_arg = charon_agent_interface_pb2.CmdRemoveArg()
      remove_arg.cmd_id = arg.cmd_id
      remove_ret, remove_err = self.CmdRemove(remove_arg)
      if curie_ex is None:
        if remove_err is not None:
          raise CurieException(remove_err.error_codes[0],
                                remove_err.error_msgs[0])
      else:
        if remove_err is not None:
          log.error("Error removing command %s: %s", arg.cmd_id, remove_err)
        raise

  def fetch_cmd_status(self, cmd_id):
    """Fetch the status for cmd_id.

    Returns:
      (ret, err): where
      ret: CmdStatusRet filled out by rpc call to agent.
      err: None if no error or CurieError if there was an error.

    Raises:
      CurieException may be raised by rpc_client.CmdStatus().
    """
    arg = charon_agent_interface_pb2.CmdStatusArg()
    arg.cmd_id = cmd_id
    ret, err = self.CmdStatus(arg)
    CHECK(ret.cmd_status)
    return ret, err
