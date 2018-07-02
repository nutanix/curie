#
# Copyright (c) 2015, 2016 Nutanix Inc. All rights reserved.
#
#
# Unit tests for cluster operations.
#
# Disable pylint checks that are too strict for unit test TestCases.
# pylint: disable=C0111,R0904
#  C0111: missing function docstring
#  R0904: too many public methods
import logging
import time
import unittest

import gflags

from curie.metrics_util import MetricsUtil
from curie.test.scenario_util import ScenarioUtil
from curie.testing import environment, util

log = logging.getLogger(__name__)


class TestCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)

  def tearDown(self):
    pass

  def test_collect_dump_read_stats(self):
    results_map = self.cluster.collect_performance_stats()
    dir_name = environment.test_output_dir(self)
    ScenarioUtil.append_cluster_stats(results_map, dir_name)
    read_results_map = ScenarioUtil.read_cluster_stats(dir_name)
    self.assertEqual(results_map.keys(), read_results_map.keys())
    for node_id in results_map:
      self.assertEqual(len(results_map[node_id]),
                       len(read_results_map[node_id]))
      for expected_metric, metric in zip(results_map[node_id],
                                         read_results_map[node_id]):
        self.assertEqual(MetricsUtil.metric_name(metric),
                         MetricsUtil.metric_name(expected_metric))
        self.assertEqual(metric.instance, expected_metric.instance)
        self.assertEqual(metric.timestamps, expected_metric.timestamps)
        self.assertEqual(metric.values, expected_metric.values)

  def test_collect_stats_time_bounds(self):
    max_retries = 20
    retry_interval_secs = 12
    iteration = 0
    # If this test is scheduled immediately after a failure test, the uptime of
    # one or more nodes may be very short. This can cause counters to be
    # missing in the result. To avoid failing the test based on test
    # scheduling, retry until the results validate cleanly.
    while True:
      epoch_time = int(time.time())
      results = self.cluster.collect_performance_stats(
        start_time_secs=(epoch_time - 600),
        end_time_secs=(epoch_time - 300))
      try:
        self.__verify_results(results)
        break
      except AssertionError:
        iteration += 1
        if iteration > max_retries:
          log.error("test_collect_stats_time_bounds exceeded the maximum "
                    "number of retries.")
          raise
        else:
          log.debug("test_collect_stats_time_bounds iteration %d of %d "
                    "failed, and will be retried.",
                    iteration, max_retries, exc_info=True)
          time.sleep(retry_interval_secs)

  def __verify_results(self, results):
    for node in results:
      # Verify the number of counters.
      self.assertTrue(len(results[node]) >= len(self.cluster.metrics()))
      for metric in results[node]:
        # Verify the number of samples.
        self.assertTrue(len(metric.timestamps) > 0)
        self.assertTrue(len(metric.values) > 0)
        self.assertTrue(len(metric.timestamps) == len(metric.values))
