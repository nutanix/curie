#
#  Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest

import gflags

from curie.acropolis_cluster import AcropolisCluster
from curie.exception import CurieTestException
from curie.name_util import NameUtil
from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.scenario import Scenario
from curie.steps import check
from curie.testing import environment, util
from curie.vsphere_cluster import VsphereCluster


class TestIntegrationStepsCheck(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_VCenterConnectivity(self):
    if isinstance(self.cluster, VsphereCluster):
      self.assertIsNone(check.VCenterConnectivity(self.scenario)())
    else:
      with self.assertRaises(CurieTestException) as ar:
        self.assertIsNone(check.VCenterConnectivity(self.scenario)())
      self.assertEqual("Step check.VCenterConnectivity is only compatible "
                       "with clusters managed by vSphere.", ar.exception.cause)

  def test_VSphereDatastoreVisible(self):
    if isinstance(self.cluster, VsphereCluster):
      vcenter_info = self.cluster.metadata().cluster_management_server_info.vcenter_info
      datastore_name = vcenter_info.vcenter_datastore_name
      for node in self.cluster.nodes():
        self.assertIsNone(check.VSphereDatastoreVisible(
          self, datastore_name=datastore_name, node_index=node.node_index())())
    else:
      with self.assertRaises(CurieTestException) as ar:
        self.assertIsNone(check.VSphereDatastoreVisible(
          self.scenario, "fake_datastore", 0)())
      self.assertEqual("Step check.VSphereDatastoreVisible is only compatible "
                       "with clusters managed by vSphere.", ar.exception.cause)

  def test_PrismDatastoreVisible(self):
    if (isinstance(self.cluster, VsphereCluster) and
        isinstance(self.scenario.cluster, NutanixClusterDPMixin)):
      vcenter_info = self.cluster.metadata().cluster_management_server_info.vcenter_info
      datastore_name = vcenter_info.vcenter_datastore_name
      for node in self.cluster.nodes():
        self.assertIsNone(check.PrismDatastoreVisible(
          self, datastore_name=datastore_name, node_index=node.node_index())())
    else:
      with self.assertRaises(CurieTestException) as ar:
        self.assertIsNone(check.PrismDatastoreVisible(
          self.scenario, "fake_datastore", 0)())
      self.assertEqual("Step check.PrismDatastoreVisible is only compatible "
                       "with Nutanix clusters managed by vSphere.",
                       ar.exception.cause)

  def test_PrismConnectivity(self):
    if (isinstance(self.cluster, AcropolisCluster) or
        isinstance(self.scenario.cluster, NutanixClusterDPMixin)):
      self.assertIsNone(check.PrismConnectivity(self.scenario)())
    else:
      with self.assertRaises(CurieTestException) as ar:
        self.assertIsNone(check.PrismConnectivity(self.scenario)())
      self.assertEqual("Step check.PrismConnectivity is only compatible with "
                       "Nutanix clusters.", ar.exception.cause)

  def test_HypervisorConnectivity(self):
    for node in self.cluster.nodes():
      self.assertIsNone(check.HypervisorConnectivity(
        self.scenario, node_index=node.node_index())())

  def test_NodePowerManagementConnectivity(self):
    for node in self.cluster.nodes():
      self.assertIsNone(check.NodePowerManagementConnectivity(
        self.scenario, node_index=node.node_index())())

  def test_clusters_match(self):
    self.assertIsNone(check.ClustersMatch(self.scenario)())

  def test_cluster_ready(self):
    self.assertIsNone(check.ClusterReady(self.scenario)())

  def test_oob_configured(self):
    self.assertIsNone(check.OobConfigured(self.scenario)())
