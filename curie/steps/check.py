#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import logging

from pyVmomi import vim

from _base_step import BaseStep
from curie.acropolis_cluster import AcropolisCluster
from curie.exception import CurieTestException
from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.scenario_util import ScenarioUtil
from curie.util import CurieUtil
from curie.vsphere_cluster import VsphereCluster

log = logging.getLogger(__name__)


class VCenterConnectivity(BaseStep):
  """
  Verify login to the vCenter Server.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, annotate=True):
    super(VCenterConnectivity, self).__init__(scenario, annotate=annotate)
    self.description = "vCenter server connectivity"

  def verify(self):
    if not isinstance(self.scenario.cluster, VsphereCluster):
      raise CurieTestException(
        cause=
        "Step %s is only compatible with clusters managed by vSphere." %
        self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    vcenter_host = self.scenario.cluster._vcenter_info.vcenter_host
    try:
      with self.scenario.cluster._open_vcenter_connection() as vcenter:
        assert vcenter
    except vim.fault.InvalidLogin:
      raise CurieTestException(
        cause=
        "Authentication error while connecting to vCenter '%s'." %
        vcenter_host,
        impact=
        "The username and password is incorrect. Other steps may not be able "
        "to proceed without being able to log in to vCenter.",
        corrective_action=
        "Please double-check and re-enter the username and password used to "
        "connect to vCenter '%s'." % vcenter_host
      )
    except Exception:
      raise CurieTestException(
        cause=
        "vCenter '%s' is unreachable." % vcenter_host,
        impact=
        "There is no network connectivity to vCenter '%s'. This may also "
        "indicate a more widespread network connectivity issue to other "
        "hosts." % vcenter_host,
        corrective_action=
        "Please check the network connection to vCenter '%s'." % vcenter_host
      )


class VSphereDatastoreVisible(BaseStep):
  """
  Verify that the datastore is visible by a given node in vSphere.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    datastore_name (str): Name of the datastore to check.
    node_index (int): Index of the ESXi node to check.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, datastore_name, node_index, annotate=True):
    super(VSphereDatastoreVisible, self).__init__(scenario, annotate=annotate)
    self.datastore_name = datastore_name
    self.node_index = node_index
    self.description = "Verifying datastore '%s' is visible on node '%s'" % \
                       (self.datastore_name, self.node_index)

  def verify(self):
    if not isinstance(self.scenario.cluster, VsphereCluster):
      raise CurieTestException(
        cause=
        "Step %s is only compatible with clusters managed by vSphere." %
        self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    if not self.scenario.cluster.datastore_visible(self.datastore_name,
                                                   host_name=node.node_id()):
      raise CurieTestException(
        cause=
        "Datastore '%s' is not visible on vSphere node '%s'." %
        (self.datastore_name, node),
        impact=
        "VMs being deployed to node '%s' can not be placed on the chosen "
        "datastore." % node,
        corrective_action=
        "Please ensure that the datastore '%s' is mounted to node '%s' in "
        "vSphere, or choose a datastore that is mounted on all nodes." %
        (self.datastore_name, node)
      )


class PrismDatastoreVisible(BaseStep):
  """
  Verify that the datastore is visible by a given node in Prism.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    datastore_name (str): Name of the datastore to check.
    node_index (int): Index of the ESXi node to check.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, datastore_name, node_index, annotate=True):
    super(PrismDatastoreVisible, self).__init__(scenario, annotate=annotate)
    self.datastore_name = datastore_name
    self.node_index = node_index
    self.description = "Verifying datastore '%s' is visible by Prism on " \
                       "node '%s'" % (self.datastore_name, self.node_index)

  def verify(self):
    if not (isinstance(self.scenario.cluster, VsphereCluster) and
            isinstance(self.scenario.cluster, NutanixClusterDPMixin)):
      raise CurieTestException(
        cause=
        "Step %s is only compatible with Nutanix clusters managed by "
        "vSphere." %
        self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    cluster_software_info = self.scenario.cluster.metadata().cluster_software_info
    # On a Nutanix cluster, check that the datastore is also visible on all
    # nodes in Prism.
    client = NutanixRestApiClient.from_proto(
      cluster_software_info.nutanix_info)
    for item in client.datastores_get():
      if item["hostIpAddress"] == node.node_ip():
        if item["datastoreName"] == self.datastore_name:
          break  # Success
    else:
      raise CurieTestException(
        cause=
        "Datastore '%s' not mounted to ESXi host '%s' with address '%s'" %
        (self.datastore_name, node.node_id(), node.node_ip()),
        impact=
        "The local path to the datastore is unavailable.",
        corrective_action=
        "Please choose a datastore that is mapped to all hosts, including the "
        "ESXi host '%s' with address '%s'" % (node.node_id(), node.node_ip()),
      )


class PrismConnectivity(BaseStep):
  """
  Verify login to Prism.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, annotate=True):
    super(PrismConnectivity, self).__init__(scenario, annotate=annotate)
    self.description = "Prism connectivity"

  def verify(self):
    if not (isinstance(self.scenario.cluster, AcropolisCluster) or
            isinstance(self.scenario.cluster, NutanixClusterDPMixin)):
      raise CurieTestException(
        cause=
        "Step %s is only compatible with Nutanix clusters." %
        self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    prism_host = self.scenario.cluster._prism_client.host
    try:
      self.scenario.cluster._prism_client.clusters_get()
    except Exception:
      raise CurieTestException(
        cause=
        "Prism '%s' is unreachable." % prism_host,
        impact=
        "There is no network connectivity to Prism '%s'. This may also "
        "indicate a more widespread network connectivity issue to other "
        "hosts." % prism_host,
        corrective_action=
        "Please check the network connection to Prism '%s'." % prism_host
      )


class HypervisorConnectivity(BaseStep):
  """
  Verify connectivity to the power management controller on a given node.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    node_index (int): Index of the node to check.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, node_index, annotate=True):
    super(HypervisorConnectivity, self).__init__(scenario, annotate=annotate)
    self.description = "Hypervisor connectivity for node %d" % node_index
    self.node_index = node_index

  def _run(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    if not CurieUtil.ping_ip(node.node_ip()):
      raise CurieTestException(
        cause=
        "Hypervisor for node '%s' with address %s did not respond to a "
        "network ping." % (node, node.node_ip()),
        impact=
        "There is no network connectivity to %s." % node.node_ip(),
        corrective_action=
        "Please troubleshoot network connectivity to the hypervisor for node "
        "'%s' with address %s." % (node, node.node_ip())
      )


class NodePowerManagementConnectivity(BaseStep):
  """
  Verify connectivity to the power management controller on a given node.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    node_index (int): Index of the node to check.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, node_index, annotate=True):
    super(NodePowerManagementConnectivity, self).__init__(scenario, annotate=annotate)
    self.description = "Power management connectivity for node %d" % node_index
    self.node_index = node_index

  def verify(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    if not node.power_management_is_configured():
      raise CurieTestException(
        cause=
        "Step %s is only compatible with nodes configured with out-of-band "
        "power management." % self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    address = node.metadata().node_out_of_band_management_info.ip_address
    if not CurieUtil.ping_ip(address):
      raise CurieTestException(
        cause=
        "Out-of-band power management for node '%d' with address '%s' did not "
        "respond to a network ping." % (node.node_index(), address),
        impact=
        "There is no network connectivity to '%s'." % address,
        corrective_action=
        "Please troubleshoot network connectivity to the power management "
        "controller with address '%s'." % address
      )
    try:
      node.power_management_util.get_chassis_status()
    except Exception:
      raise CurieTestException(
        cause=
        "Command failure occurred on the out-of-band power management for "
        "node '%d' with address '%s'." % (node.node_index(), address),
        impact=
        "There is network connectivity to '%s', but power management commands "
        "are failing." % address,
        corrective_action=
        "Please check the configured username and password for the power "
        "management controller with address '%s'. Please also ensure that "
        "'IPMI over LAN' is enabled for the power management controller, and "
        "that network connections to '%s' on UDP port 623 are allowed." %
        (address, address)
      )


class ClustersMatch(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(ClustersMatch, self).__init__(scenario, annotate=annotate)
    self.description = ("Checking that clusters defined by management "
                        "software and clustering software match")

  def _run(self):
    ScenarioUtil.prereq_runtime_storage_cluster_mgmt_cluster_match(
      self.scenario.cluster)


class PrismHostInSameCluster(BaseStep):
  """
  Verify that the node is a member of the Prism cluster.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    node_index (int): Index of the node to check.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, node_index, annotate=True):
    super(PrismHostInSameCluster, self).__init__(scenario, annotate=annotate)
    self.node_index = node_index
    self.description = ("Verifying node '%s' is a member of the Prism "
                        "cluster" % self.node_index)

  def verify(self):
    if not isinstance(self.scenario.cluster, NutanixClusterDPMixin):
      raise CurieTestException(
        cause=
        "Step %s is only compatible with Nutanix clusters." %
        self.name,
        impact=
        "This Scenario is incompatible with this cluster.",
        corrective_action=
        "Please choose a different cluster, or different Scenario."
      )

  def _run(self):
    node = self.scenario.cluster.nodes()[self.node_index]
    client = NutanixRestApiClient.from_proto(
      self.scenario.cluster.metadata().cluster_software_info.nutanix_info)
    for item in client.hosts_get()["entities"]:
      if item["hypervisorAddress"] == node.node_ip():
        break  # Success
    else:
      raise CurieTestException(
        cause=
        "Node '%s' with hypervisor address '%s' is not a member of the "
        "Nutanix cluster managed at '%s'." %
        (self.node_index, node.node_id(), client.host),
        impact=
        "The configured nodes belong to multiple Nutanix clusters, which is "
        "not supported.",
        corrective_action=
        "Please choose a set of nodes that belong to a single Nutanix "
        "cluster. If the target is configured for metro availability, please "
        "choose nodes that all belong to a single site."
      )


class ClusterReady(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(ClusterReady, self).__init__(scenario, annotate=annotate)
    self.description = "Ensuring cluster is ready"

  def _run(self):
    if not ScenarioUtil.prereq_runtime_cluster_is_ready(self.scenario.cluster):
      ScenarioUtil.prereq_runtime_cluster_is_ready_fix(self.scenario.cluster)


class OobConfigured(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(OobConfigured, self).__init__(scenario, annotate=annotate)
    self.description = "Checking that out-of-band management is configured"

  def _run(self):
    ScenarioUtil.prereq_metadata_can_run_failure_scenario(
      self.scenario.cluster.metadata())
