#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
import unittest

import mock

from curie import agent_rpc_client
from curie import charon_agent_interface_pb2


class TestCurieAgentRpcCLient(unittest.TestCase):

  @mock.patch.object(agent_rpc_client.AgentRpcClient, "CmdStatus")
  def test_fetch_cmd_status(self, cmd_status_mock):
    mock_ret = charon_agent_interface_pb2.CmdStatusRet()
    mock_ret.cmd_status.state = mock_ret.cmd_status.kSucceeded
    cmd_status_mock.return_value = (mock_ret, None)
    rpc_agent = agent_rpc_client.AgentRpcClient("vm_ip")
    ret, err = rpc_agent.fetch_cmd_status("foo")
    self.assertEqual(err, None)
    self.assertEqual(ret, mock_ret)
