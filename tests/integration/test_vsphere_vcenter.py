#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import unittest

import gflags
from pyVmomi import vim

from curie.testing import util
from curie.vsphere_cluster import VsphereCluster

log = logging.getLogger(__name__)


class TestVsphereVcenter(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    assert isinstance(self.cluster, VsphereCluster), \
           "This test must run on a vCenter cluster"
    self.vcenter = self.cluster._open_vcenter_connection()

  def test_walk(self):
    count = 0
    expected_types = [vim.Folder, vim.Datacenter, vim.ClusterComputeResource, vim.ComputeResource]
    with self.vcenter:
      for thing in self.vcenter.walk():
        self.assertIn(type(thing), expected_types)
        log.debug("%-60s%-40s", thing, thing.name)
        count += 1
    self.assertGreater(count, 0, "vcenter.walk must yield at least one thing")

  def test_lookup_datacenter(self):
    assert isinstance(self.cluster, VsphereCluster), \
           "This test must run on a vCenter cluster"
    dc_name = self.cluster._vcenter_info.vcenter_datacenter_name
    with self.vcenter:
      found_dc = self.vcenter.lookup_datacenter(dc_name)
      self.assertIsInstance(found_dc, vim.Datacenter)
      self.assertEqual(found_dc.name, dc_name)

  def test_lookup_cluster(self):
    assert isinstance(self.cluster, VsphereCluster), \
           "This test must run on a vCenter cluster"
    dc_name = self.cluster._vcenter_info.vcenter_datacenter_name
    cluster_name = self.cluster._vcenter_info.vcenter_cluster_name
    with self.vcenter:
      found_dc = self.vcenter.lookup_datacenter(dc_name)
      found_cluster = self.vcenter.lookup_cluster(found_dc, cluster_name)
      self.assertIsInstance(found_cluster, vim.ClusterComputeResource)
      self.assertEqual(found_cluster.name, cluster_name)
