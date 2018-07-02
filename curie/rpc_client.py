#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging
import time

from curie.curie_error_pb2 import CurieError
from curie.curie_interface_pb2 import CurieRpcSvc_Stub, ServerStatusGetArg
from curie.curie_server_state_pb2 import ServerStatus
from curie.exception import CurieException
from curie.rpc_util import RpcClientMeta, RpcClientUtil
from curie.rpc_util import RpcServer

log = logging.getLogger(__name__)


class RpcClient(RpcClientUtil):
  """
  Client to issue CurieRpcSvc RPCs to server at a given 'ip', 'port',
  and endpoint 'path'.
  """

  __metaclass__ = RpcClientMeta(CurieRpcSvc_Stub)

  def __init__(self, ip, port=5000, path="/rpc"):
    super(RpcClient, self).__init__(ip, port, path)

  # NB: These stubs are not necessary, but help avoid spurious errors in
  #     static analysis tools.
  # 'arg' is intentionally unused in these stubs.
  # pylint: disable=unused-argument
  def ServerStatusGet(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def TestCatalog(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def TestStatus(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def SetEncryptionKey(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def DiscoverClusters(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def DiscoverNodes(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def DiscoverClustersV2(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def DiscoverNodesV2(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def UpdateAndValidateCluster(self, arg):
    NotImplementedError("RPC util failed to set handler")

  def initialize_server(self, encryption_key, timeout_secs=60):
    """
    Convenience method which handles waiting for server to be ready.

    Handles:
      -- Polling server state during initialization.
      -- Setting encryption key.
      -- Blocking until recovery completes.

    Args:
      encryption_key (str): Encryption key to set, encoded as hex string.
      timeout_secs (number): Optional. Maximum time in seconds to allow for
        server to become ready. Default 60.

    Raises:
      CurieException<kAuthenticationError> On failure to set encryption key.
      CurieException<kTimeout> If server recovery does not otherwise complete
        within 'timeout_secs'.
      CurieException<kInternalError> On encountering an unexpected server
        state.
    """
    arg = ServerStatusGetArg()
    end_time_secs = time.time() + timeout_secs
    while True:
      ret, err = None, None
      try:
        ret, err = self.send_rpc("ServerStatusGet", arg)
      except CurieException:
        log.warning("Unable to query server status", exc_info=True)

      if not ret:
        if err:
          log.error("Encountered error querying server status: %s", str(err))
        # Err and ret will be None in e.g. the case where an exception was
        # encountered above. In that case, do nothing here, and allow flow
        # to proceed to checking the timeout and sleeping.
      elif ret.status.status == ServerStatus.kAwaitingEncryptionKey:
        auth_arg = SetEncryptionKeyArg()
        auth_arg.encryption_key = encryption_key

        log.info("Server initialization complete, setting encryption key")
        ret, err = self.send_rpc("SetEncryptionKey", auth_arg)
        if not ret:
          raise CurieException(CurieError.kAuthenticationError,
                                "Failed to set encryption key: %s" % str(err))
      elif ret.status.status in RpcServer.__PRE_AUTH_STATUS_CODES__:
        log.info("Server initialization in progress. Server is not ready "
                 "to accept encryption key")
      elif ret.status.status in RpcServer.__PRE_RECOVERED_STATUS_CODES__:
        log.info("Server recovery still in progress")
      elif not ret.status.is_ready:
        raise CurieException(
            CurieError.kInternalError,
            "Server reports 'is_ready = False' with unexpected status '%s'" %
            ServerStatus.Status.Name(ret.status.status))
      else:
        log.info("Curie server initialized")
        return

      if time.time() > end_time_secs:
        log.error("Timed out after seconds %s", timeout_secs)
        raise CurieException(CurieError.kTimeout,
                              "Timed out attempting to initialize server")
      time.sleep(1)
