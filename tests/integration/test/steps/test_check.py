#
#  Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest

import gflags

from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.test.steps import check
from curie.testing import environment, util


class TestIntegrationStepsCheck(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_clusters_match(self):
    self.assertIsNone(check.ClustersMatch(self.scenario)())

  def test_cluster_ready(self):
    self.assertIsNone(check.ClusterReady(self.scenario)())

  def test_vm_storage_available(self):
    self.assertIsNone(check.VMStorageAvailable(self.scenario)())

  def test_oob_configured(self):
    self.assertIsNone(check.OobConfigured(self.scenario)())
