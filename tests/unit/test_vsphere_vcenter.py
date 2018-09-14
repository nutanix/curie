#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

import mock
from pyVmomi import vim

from curie.exception import CurieTestException
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
      m_vim_hosts.append(m_vim_host)
    self.m_vim_cluster.host = m_vim_hosts

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

