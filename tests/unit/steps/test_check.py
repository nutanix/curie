#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

from mock import mock

from curie.exception import CurieTestException
from curie.nutanix_vsphere_cluster import NutanixVsphereCluster
from curie.scenario import Scenario
from curie.steps import check
from curie.testing import environment
from curie.testing.util import mock_cluster
from curie.vm import Vm
from curie.vsphere_cluster import VsphereCluster


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

  def test_VSphereDatastoreVisible_nodeid_is_hostname(self):
    self.cluster = mock_cluster(spec=VsphereCluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    for index, cluster_node in enumerate(self.cluster.nodes()):
      cluster_node.node_id.return_value = "fake_node_%d.rtp.nutanix.com" % index
    step = check.VSphereDatastoreVisible(self.scenario, "fake_datastore_name", 0)
    step()
    self.scenario.cluster.datastore_visible.assert_called_once_with("fake_datastore_name", host_name="fake_node_0.rtp.nutanix.com")

  @mock.patch("curie.steps.check.NutanixRestApiClient")
  def test_PrismHostsInSameCluster_ok(self, m_NutanixRestApiClient):
    self.cluster = mock_cluster(spec=NutanixVsphereCluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

    m_client = mock.Mock()
    m_client.host = "fake_prism_host"
    m_client.hosts_get.return_value = {
      "entities": [
        {"hypervisorAddress": "169.254.0.0"},
        {"hypervisorAddress": "169.254.0.1"},
        {"hypervisorAddress": "169.254.0.2"},
        {"hypervisorAddress": "169.254.0.3"},
      ]
    }
    m_NutanixRestApiClient.from_proto.return_value = m_client

    step = check.PrismHostInSameCluster(self.scenario, 2)
    step()

  @mock.patch("curie.steps.check.NutanixRestApiClient")
  def test_PrismHostsInSameCluster_missing(self, m_NutanixRestApiClient):
    self.cluster = mock_cluster(spec=NutanixVsphereCluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

    m_client = mock.Mock()
    m_client.host = "fake_prism_host"
    m_client.hosts_get.return_value = {
      "entities": [
        {"hypervisorAddress": "169.254.0.0"},
        {"hypervisorAddress": "169.254.0.1"},
        # {"hypervisorAddress": "169.254.0.2"},
        {"hypervisorAddress": "169.254.0.3"},
      ]
    }
    m_NutanixRestApiClient.from_proto.return_value = m_client

    step = check.PrismHostInSameCluster(self.scenario, 2)
    with self.assertRaises(CurieTestException) as ar:
      step()
    self.assertEqual(
      "Cause: Node '2' with hypervisor address '169.254.0.2' is not a member "
      "of the Nutanix cluster managed at 'fake_prism_host'.\n\n"
      "Impact: The configured nodes belong to multiple Nutanix clusters, "
      "which is not supported.\n\n"
      "Corrective Action: Please choose a set of nodes that belong to a "
      "single Nutanix cluster. If the target is configured for metro "
      "availability, please choose nodes that all belong to a single site.\n\n"
      "Traceback: None",
      str(ar.exception))
