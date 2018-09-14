#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

from mock import mock

from curie.exception import CurieTestException
from curie.steps import check
from curie.scenario import Scenario
from curie.testing import environment
from curie.testing.util import mock_cluster
from curie.vm import Vm


class TestStepsCheck(unittest.TestCase):
  def setUp(self):
    self.cluster = mock_cluster()
    vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    for index, vm in enumerate(vms):
      vm.vm_name.return_value = "fake_vm_%d" % index
    self.cluster.vms.return_value = vms
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  @mock.patch("curie.steps.check.CurieUtil.ping_ip")
  def test_NodePowerManagementConnectivity_no_connectivity(self, m_ping_ip):
    m_ping_ip.return_value = False
    step = check.NodePowerManagementConnectivity(self.scenario, 0)
    with self.assertRaises(CurieTestException) as ar:
      step()
    self.assertEqual(
      str(ar.exception),
      "Cause: Out-of-band power management for node '0' with address "
      "'127.0.0.0' did not respond to a network ping.\n\n"
      "Impact: There is no network connectivity to '127.0.0.0'.\n\n"
      "Corrective Action: Please troubleshoot network connectivity to the "
      "power management controller with address '127.0.0.0'.\n\n"
      "Traceback: None")

  @mock.patch("curie.steps.check.CurieUtil.ping_ip")
  def test_NodePowerManagementConnectivity_auth_fail(self, m_ping_ip):
    m_ping_ip.return_value = True
    step = check.NodePowerManagementConnectivity(self.scenario, 0)
    node_0 = self.cluster.nodes.return_value[0]
    node_0.power_management_util = mock.Mock()
    node_0.power_management_util.get_chassis_status.side_effect = IOError("Oh noes")
    with self.assertRaises(CurieTestException) as ar:
      step()
    self.assertIn(
      "Cause: Command failure occurred on the out-of-band power management "
      "for node '0' with address '127.0.0.0'.\n\n"
      "Impact: There is network connectivity to '127.0.0.0', but power "
      "management commands are failing.\n\n"
      "Corrective Action: Please check the configured username and password "
      "for the power management controller with address '127.0.0.0'. Please "
      "also ensure that 'IPMI over LAN' is enabled for the power management "
      "controller, and that network connections to '127.0.0.0' on UDP port "
      "623 are allowed.\n\n"
      "Traceback (most recent call last):",
      str(ar.exception))
    self.assertIn("IOError: Oh noes", ar.exception.traceback)
