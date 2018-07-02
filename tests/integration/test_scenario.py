#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import os
import unittest

import gflags

from curie import scenario_parser
from curie.scenario import Status
from curie.testing import environment, util


class TestCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)

  def test_four_corners(self):
    scenario_directory = os.path.join(
      environment.top, "curie", "test", "yaml", "four_corners_microbenchmark")
    scenario = scenario_parser.from_path(
      scenario_directory,
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

    scenario.start()
    scenario.join()
    self.assertEqual(Status.kSucceeded, scenario.status())

  def test_vdi_low_count(self):
    scenario_directory = os.path.join(
      environment.top, "curie", "test", "yaml", "vdi_simulator_task_100")
    scenario = scenario_parser.from_path(
      scenario_directory,
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir,
      vars={"vms_per_node": 1, "runtime_secs": 60}
    )

    scenario.start()
    scenario.join()
    self.assertEqual(Status.kSucceeded, scenario.status())
