#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

import mock
from pyVmomi import vim

from curie.curie_server_state_pb2 import CurieSettings
from curie.exception import CurieTestException
from curie.proto_util import proto_patch_encryption_support
from curie.vsphere_vcenter import VsphereVcenter


class TestVsphereVcenter(unittest.TestCase):
  def setUp(self):
    self.m_vim_cluster = mock.Mock(spec=vim.ComputeResource)
    m_vim_datastore = mock.Mock(spec=vim.Datastore)
    m_vim_datastore.name = "fake_datastore"
    self.m_vim_cluster.datastore = [m_vim_datastore]
    m_vim_hosts = []
    for index in range(4):
      m_vim_host = mock.Mock(spec=vim.HostSystem)
      m_vim_host.name = "fake_host_%d" % index
      m_vim_host.datastore = [m_vim_datastore]
      m_vim_host.hardware.cpuInfo.numCpuPackages = 2
      m_vim_host.hardware.cpuInfo.numCpuCores = 32
      m_vim_host.hardware.cpuInfo.numCpuThreads = 64
      m_vim_host.hardware.cpuInfo.hz = int(3e9)
      m_vim_host.hardware.memorySize = int(32e9)
      m_vim_hosts.append(m_vim_host)
    self.m_vim_cluster.host = m_vim_hosts

  def test_fill_cluster_metadata_include_reporting_fields(self):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")

    cluster_metadata = proto_patch_encryption_support(CurieSettings.Cluster)()
    cluster_metadata.cluster_name = "Fake Cluster"
    cluster_metadata.cluster_hypervisor_info.esx_info.SetInParent()
    for index in range(4):
      curr_node = cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_host_%d" % index
    vcenter_info = cluster_metadata.cluster_management_server_info.vcenter_info
    vcenter_info.vcenter_host = "fake_vmm_server_address"
    vcenter_info.vcenter_user = "fake_vmm_username"
    vcenter_info.vcenter_password = "fake_vmm_password"
    vcenter_info.vcenter_datacenter_name = "fake_datacenter"
    vcenter_info.vcenter_cluster_name = "fake_cluster"
    vcenter_info.vcenter_datastore_name = "fake_share_name"
    vcenter_info.vcenter_network_name = "fake_network"

    for index, cluster_node in enumerate(cluster_metadata.cluster_nodes):
      self.assertEqual("fake_host_%d" % index, cluster_node.id)
      self.assertEqual(False, cluster_node.HasField("node_hardware"))

    vsphere.fill_cluster_metadata(self.m_vim_cluster, cluster_metadata, True)

    for index, cluster_node in enumerate(cluster_metadata.cluster_nodes):
      self.assertEqual("fake_host_%d" % index, cluster_node.id)
      self.assertEqual(True, cluster_node.HasField("node_hardware"))
      self.assertEqual(2, cluster_node.node_hardware.num_cpu_packages)
      self.assertEqual(32, cluster_node.node_hardware.num_cpu_cores)
      self.assertEqual(64, cluster_node.node_hardware.num_cpu_threads)
      self.assertEqual(int(3e9), cluster_node.node_hardware.cpu_hz)
      self.assertEqual(int(32e9), cluster_node.node_hardware.memory_size)

  def test_fill_cluster_metadata_do_not_include_reporting_fields(self):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")

    cluster_metadata = proto_patch_encryption_support(CurieSettings.Cluster)()
    cluster_metadata.cluster_name = "Fake Cluster"
    cluster_metadata.cluster_hypervisor_info.esx_info.SetInParent()
    for index in range(4):
      curr_node = cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_host_%d" % index
    vcenter_info = cluster_metadata.cluster_management_server_info.vcenter_info
    vcenter_info.vcenter_host = "fake_vmm_server_address"
    vcenter_info.vcenter_user = "fake_vmm_username"
    vcenter_info.vcenter_password = "fake_vmm_password"
    vcenter_info.vcenter_datacenter_name = "fake_datacenter"
    vcenter_info.vcenter_cluster_name = "fake_cluster"
    vcenter_info.vcenter_datastore_name = "fake_share_name"
    vcenter_info.vcenter_network_name = "fake_network"

    for index, cluster_node in enumerate(cluster_metadata.cluster_nodes):
      self.assertEqual("fake_host_%d" % index, cluster_node.id)
      self.assertEqual(False, cluster_node.HasField("node_hardware"))

    vsphere.fill_cluster_metadata(self.m_vim_cluster, cluster_metadata, False)

    for index, cluster_node in enumerate(cluster_metadata.cluster_nodes):
      self.assertEqual("fake_host_%d" % index, cluster_node.id)
      self.assertEqual(False, cluster_node.HasField("node_hardware"))

  def test_fill_cluster_metadata_nodes_do_not_match(self):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")

    cluster_metadata = proto_patch_encryption_support(CurieSettings.Cluster)()
    cluster_metadata.cluster_name = "Fake Cluster"
    cluster_metadata.cluster_hypervisor_info.esx_info.SetInParent()
    for index in range(3):  # Only 3 nodes in the metadata.
      curr_node = cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_host_%d" % index
    vcenter_info = cluster_metadata.cluster_management_server_info.vcenter_info
    vcenter_info.vcenter_host = "fake_vmm_server_address"
    vcenter_info.vcenter_user = "fake_vmm_username"
    vcenter_info.vcenter_password = "fake_vmm_password"
    vcenter_info.vcenter_datacenter_name = "fake_datacenter"
    vcenter_info.vcenter_cluster_name = "fake_cluster"
    vcenter_info.vcenter_datastore_name = "fake_share_name"
    vcenter_info.vcenter_network_name = "fake_network"

    self.assertEqual(3, len(cluster_metadata.cluster_nodes))

    vsphere.fill_cluster_metadata(self.m_vim_cluster, cluster_metadata, True)

    self.assertEqual(3, len(cluster_metadata.cluster_nodes))
    for index, cluster_node in enumerate(cluster_metadata.cluster_nodes):
      self.assertEqual("fake_host_%d" % index, cluster_node.id)
      self.assertEqual(True, cluster_node.HasField("node_hardware"))

  @mock.patch("curie.vsphere_vcenter.SmartConnectNoSSL")
  def test_lookup_datastore_mounted_on_all_hosts(self, m_SmartConnectNoSSL):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")
    with vsphere:
      datastore = vsphere.lookup_datastore(self.m_vim_cluster,
                                           "fake_datastore")
    self.assertIsInstance(datastore, vim.Datastore)
    self.assertEqual(datastore.name, "fake_datastore")

  @mock.patch("curie.vsphere_vcenter.SmartConnectNoSSL")
  def test_lookup_datastore_not_on_all_hosts(self, m_SmartConnectNoSSL):
    m_some_other_datastore = mock.Mock(spec=vim.Datastore)
    m_some_other_datastore.name = "some_other_datastore"
    self.m_vim_cluster.host[0].datastore = [m_some_other_datastore]
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")
    with vsphere:
      datastore = vsphere.lookup_datastore(self.m_vim_cluster,
                                           "fake_datastore")
    self.assertIsNone(datastore)

  @mock.patch("curie.vsphere_vcenter.SmartConnectNoSSL")
  def test_lookup_datastore_on_one_host(self, m_SmartConnectNoSSL):
    m_some_other_datastore = mock.Mock(spec=vim.Datastore)
    m_some_other_datastore.name = "some_other_datastore"
    self.m_vim_cluster.host[0].datastore = [m_some_other_datastore]
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")
    with vsphere:
      datastore = vsphere.lookup_datastore(self.m_vim_cluster,
                                           "fake_datastore", "fake_host_1")
    self.assertIsInstance(datastore, vim.Datastore)
    self.assertEqual(datastore.name, "fake_datastore")

  @mock.patch("curie.vsphere_vcenter.SmartConnectNoSSL")
  def test_lookup_datastore_host_not_found(self, m_SmartConnectNoSSL):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")
    with vsphere:
      with self.assertRaises(CurieTestException) as ar:
        vsphere.lookup_datastore(self.m_vim_cluster,
                                 "fake_datastore",
                                 "this_is_a_non_existent_host")
    self.assertEqual("Host 'this_is_a_non_existent_host' not found",
                     str(ar.exception))

  @mock.patch("curie.vsphere_vcenter.SmartConnectNoSSL")
  def test_lookup_datastore_does_not_exist(self, m_SmartConnectNoSSL):
    vsphere = VsphereVcenter(vcenter_host="fake_vcenter",
                             vcenter_user="fake_user",
                             vcenter_password="fake_password")
    with vsphere:
      datastore = vsphere.lookup_datastore(self.m_vim_cluster,
                                           "this_is_a_non_existent_datastore")
    self.assertIsNone(datastore)

