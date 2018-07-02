#
#  Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#

import time
import unittest

import gflags

from curie.name_util import NameUtil
from curie.scenario import Scenario, Status
from curie.test import steps
from curie.testing import environment, util


class TestIntegrationStepsTest(unittest.TestCase):
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

  def test_wait(self):
    duration_secs = 5
    start_secs = time.time()
    slept_secs = steps.test.Wait(self.scenario, duration_secs)()
    total_secs = time.time() - start_secs
    self.assertTrue(duration_secs <= slept_secs)
    self.assertTrue(slept_secs <= total_secs)

  def test_wait_stop_test(self):
    self.scenario._status = Status.kStopping
    duration_secs = 5
    start_secs = time.time()
    slept_secs = steps.test.Wait(self.scenario, duration_secs)()
    total_secs = time.time() - start_secs
    self.assertTrue(total_secs < duration_secs)
    self.assertTrue(slept_secs < total_secs)
