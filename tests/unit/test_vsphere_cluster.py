# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

import mock
from pyVmomi import vim

from curie.curie_server_state_pb2 import CurieSettings
from curie.exception import CurieTestException
from curie.node import Node
from curie.oob_management_util import OobInterfaceType
from curie.proto_util import proto_patch_encryption_support
from curie.vsphere_cluster import VsphereCluster


class TestVsphereCluster(unittest.TestCase):

  def setUp(self):
    self.cluster_metadata = proto_patch_encryption_support(CurieSettings.Cluster)()
    self.cluster_metadata.cluster_name = "Fake Cluster"
    self.cluster_metadata.cluster_hypervisor_info.esx_info.SetInParent()
    self.cluster_metadata.cluster_software_info.generic_info.SetInParent()
    cluster_nodes_count = 4
    nodes = [mock.Mock(spec=Node) for _ in xrange(cluster_nodes_count)]
    for id, node in enumerate(nodes):
      node.node_id.return_value = "fake_node_%d" % id
      node.node_index.return_value = id
      curr_node = self.cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_node_%d" % id
      curr_node.node_out_of_band_management_info.interface_type = OobInterfaceType.kNone
    vcenter_info = self.cluster_metadata.cluster_management_server_info.vcenter_info
    vcenter_info.vcenter_host = "fake_vmm_server_address"
    vcenter_info.vcenter_user = "fake_vmm_username"
    vcenter_info.vcenter_password = "fake_vmm_password"
    vcenter_info.vcenter_datacenter_name = "fake_datacenter"
    vcenter_info.vcenter_cluster_name = "fake_cluster"
    vcenter_info.vcenter_datastore_name = "fake_share_name"
    vcenter_info.vcenter_network_name = "fake_network"

    self.m_vim_cluster = mock.MagicMock(spec=vim.ClusterComputeResource)
    self.m_vim_cluster.name = "fake_cluster"
    m_vim_hosts = []
    for index in xrange(4):
      m_vim_host = mock.MagicMock(spec=vim.HostSystem)
      m_vim_host.name = "fake_node_%d" % index
      m_vim_hosts.append(m_vim_host)
      self.m_vim_cluster.host = m_vim_hosts

  def tearDown(self):
    pass

  @mock.patch("curie.vsphere_cluster.VsphereVcenter")
  def test_update_metadata(self, m_VsphereVcenter):
    cluster = VsphereCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "_lookup_vim_cluster") as m_lvc:
      m_lvc.return_value = self.m_vim_cluster
      cluster.update_metadata(False)

  @mock.patch("curie.vsphere_cluster.VsphereVcenter")
  def test_update_metadata_if_cluster_contains_extra_nodes(
      self, m_VsphereVcenter):
    curr_node = self.cluster_metadata.cluster_nodes.add()
    curr_node.id = "fake_node_extra"
    cluster = VsphereCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "_lookup_vim_cluster") as m_lvc:
      m_lvc.return_value = self.m_vim_cluster
      cluster.update_metadata(False)

  @mock.patch("curie.vsphere_cluster.VsphereVcenter")
  def test_update_metadata_if_cluster_contains_fewer_nodes(
      self, m_VsphereVcenter):
    del self.cluster_metadata.cluster_nodes[-1]  # Remove the last item.
    cluster = VsphereCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "_lookup_vim_cluster") as m_lvc:
      m_lvc.return_value = self.m_vim_cluster
      cluster.update_metadata(False)

  @mock.patch("curie.vsphere_node.CurieUtil.resolve_hostname")
  @mock.patch("curie.vsphere_cluster.VsphereVcenter")
  def test_nodes_if_cluster_contains_extra_nodes(
      self, m_VsphereVcenter, m_resolve_hostname):
    m_resolve_hostname.return_value = "1.1.1.1"
    curr_node = self.cluster_metadata.cluster_nodes.add()
    curr_node.id = "fake_node_extra"
    cluster = VsphereCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "_lookup_vim_cluster") as m_lvc:
      m_lvc.return_value = self.m_vim_cluster
      with self.assertRaises(CurieTestException) as ar:
        cluster.nodes()
    self.assertEqual(
      "Cause: Node with ID 'fake_node_extra' is in the Curie cluster "
      "metadata, but not found in vSphere cluster 'fake_cluster'.\n"
      "\n"
      "Impact: The cluster configuration is invalid.\n"
      "\n"
      "Corrective Action: Please check that all of the nodes in the Curie "
      "cluster metadata are part of the vSphere cluster. For example, if the "
      "cluster configuration has four nodes, please check that all four nodes "
      "are present in the vSphere cluster. If the nodes are managed in "
      "vSphere by FQDN, please check that the nodes were also added by their "
      "FQDN to the Curie cluster metadata.\n"
      "\n"
      "Traceback: None", str(ar.exception))

  @mock.patch("curie.vsphere_node.CurieUtil.resolve_hostname")
  @mock.patch("curie.vsphere_cluster.VsphereVcenter")
  def test_nodes_if_cluster_contains_fewer_nodes(
      self, m_VsphereVcenter, m_resolve_hostname):
    m_resolve_hostname.return_value = "1.1.1.1"
    del self.cluster_metadata.cluster_nodes[-1]  # Remove the last item.
    cluster = VsphereCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "_lookup_vim_cluster") as m_lvc:
      m_lvc.return_value = self.m_vim_cluster
      self.assertEqual(3, len(cluster.nodes()))
