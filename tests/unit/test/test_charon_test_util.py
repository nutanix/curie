#
# Copyright (c) 2015, 2016 Nutanix Inc. All rights reserved.
#
#
# Unit tests for Curie test utilities.
#
# Disable pylint checks that are too strict for unit test TestCases.
# pylint: disable=C0111,R0904
#  C0111: missing function docstring
#  R0904: too many public methods

import os
import shutil
import unittest

from curie.curie_metrics_pb2 import CurieMetric
from curie.metrics_util import MetricsUtil
from curie.test.scenario_util import ScenarioUtil
from curie.testing import environment


class TestCurieTestUtil(unittest.TestCase):
  def setUp(self):
    self.counter_template = CurieMetric(name=CurieMetric.kCpuUsage,
                                         description="foo",
                                         instance="Aggregated",
                                         type=CurieMetric.kGauge,
                                         consolidation=CurieMetric.kAvg,
                                         unit=CurieMetric.kPercent)

  def _counter_template_name(self):
    return MetricsUtil.metric_name(self.counter_template)

  def test_append_read_cluster_stats_simple(self):
    output_dir = os.path.join(environment.test_output_dir(self),
                              "test_append_read_cluster_stats_simple")
    if os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092320, 1454092321])
    metric_to_append.values.extend([1, 2])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], metric_to_append)

  def test_append_read_cluster_stats_empty(self):
    output_dir = os.path.join(environment.test_output_dir(self),
                              "test_append_read_cluster_stats_empty")
    if os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
    empty_metric = CurieMetric()
    empty_metric.CopyFrom(self.counter_template)
    del empty_metric.timestamps[:]
    del empty_metric.values[:]
    self.assertEqual(empty_metric.timestamps, [])
    self.assertEqual(empty_metric.values, [])
    # Write empty.
    new_results = {"node_0": [empty_metric]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], empty_metric)
    # Append empty.
    new_results = {"node_0": [empty_metric]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], empty_metric)
    # Append non-empty.
    non_empty_metric = CurieMetric()
    non_empty_metric.CopyFrom(self.counter_template)
    non_empty_metric.timestamps.extend([1454092320, 1454092321])
    non_empty_metric.values.extend([1, 2])
    new_results = {"node_0": [non_empty_metric]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], non_empty_metric)
    # Append empty again.
    new_results = {"node_0": [empty_metric]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], non_empty_metric)

  def test_append_cluster_stats_corrupt(self):
    output_dir = os.path.join(environment.test_output_dir(self),
                              "test_append_cluster_stats_corrupt")
    if os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092320, 1454092321])
    metric_to_append.values.extend([1, 2])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    # Corrupt the file.
    filename = ("%s_%s" % (self._counter_template_name(),
                           self.counter_template.instance)).replace(".", "_")
    bin_path = os.path.join(output_dir, "node_0", filename + ".bin")
    assert (os.path.isfile(bin_path))
    with open(bin_path, "w") as f:
      f.write("Cela ne veut pas un protobuf.")
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092322, 1454092323])
    metric_to_append.values.extend([3, 4])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    expected_metric = CurieMetric()
    expected_metric.CopyFrom(self.counter_template)
    expected_metric.timestamps.extend([1454092322, 1454092323])
    expected_metric.values.extend([3, 4])
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], expected_metric)

  def test_append_cluster_stats_no_duplicates(self):
    output_dir = os.path.join(environment.test_output_dir(self),
                              "test_append_cluster_stats_no_duplicates")
    if os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092320, 1454092321])
    metric_to_append.values.extend([1, 2])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092322, 1454092323])
    metric_to_append.values.extend([3, 4])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    expected_metric = CurieMetric()
    expected_metric.CopyFrom(self.counter_template)
    expected_metric.timestamps.extend([1454092320,
                                       1454092321,
                                       1454092322,
                                       1454092323])
    expected_metric.values.extend([1,
                                   2,
                                   3,
                                   4])
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], expected_metric)

  def test_append_cluster_stats_duplicates(self):
    output_dir = os.path.join(environment.test_output_dir(self),
                              "test_append_cluster_stats_duplicates")
    if os.path.isdir(output_dir):
      shutil.rmtree(output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092320,
                                        1454092321,
                                        1454092322])
    metric_to_append.values.extend([1,
                                    2,
                                    3])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    metric_to_append = CurieMetric()
    metric_to_append.CopyFrom(self.counter_template)
    metric_to_append.timestamps.extend([1454092322,
                                        1454092323])
    metric_to_append.values.extend([3,
                                    4])
    new_results = {"node_0": [metric_to_append]}
    ScenarioUtil.append_cluster_stats(new_results, output_dir)
    expected_metric = CurieMetric()
    expected_metric.CopyFrom(self.counter_template)
    expected_metric.timestamps.extend([1454092320,
                                       1454092321,
                                       1454092322,
                                       1454092323])
    expected_metric.values.extend([1,
                                   2,
                                   3,
                                   4])
    results = ScenarioUtil.read_cluster_stats(output_dir)
    self.assertEqual(results.keys(), ["node_0"])
    self.assertEqual(len(results["node_0"]), 1)
    self.assertEqual(results["node_0"][0], expected_metric)

  def test_to_csv(self):
    metric = CurieMetric()
    metric.CopyFrom(self.counter_template)
    metric.timestamps.extend([1454092320, 1454092321])
    metric.values.extend([1, 2])
    new_results = {"node_0": [metric]}
    csv = ScenarioUtil.results_map_to_csv(new_results)
    self.assertEqual(csv,
                     "timestamp,node_id,metric_name,instance,value\n" +
                     "1454092320,node_0,CpuUsage.Avg.Percent,Aggregated,1\n" +
                     "1454092321,node_0,CpuUsage.Avg.Percent,Aggregated,2\n")

  def test_to_csv_no_header(self):
    metric = CurieMetric()
    metric.CopyFrom(self.counter_template)
    metric.timestamps.extend([1454092320, 1454092321])
    metric.values.extend([1, 2])
    new_results = {"node_0": [metric]}
    csv = ScenarioUtil.results_map_to_csv(new_results, header=False)
    self.assertEqual(csv,
                     "1454092320,node_0,CpuUsage.Avg.Percent,Aggregated,1\n" +
                     "1454092321,node_0,CpuUsage.Avg.Percent,Aggregated,2\n")

  def test_to_csv_newline(self):
    metric = CurieMetric()
    metric.CopyFrom(self.counter_template)
    metric.timestamps.extend([1454092320, 1454092321])
    metric.values.extend([1, 2])
    new_results = {"node_0": [metric]}
    csv = ScenarioUtil.results_map_to_csv(new_results, newline="\r\n")
    self.assertEqual(
      csv,
      "timestamp,node_id,metric_name,instance,value\r\n" +
      "1454092320,node_0,CpuUsage.Avg.Percent,Aggregated,1\r\n" +
      "1454092321,node_0,CpuUsage.Avg.Percent,Aggregated,2\r\n")

  def test_to_csv_rate(self):
    metric = CurieMetric()
    metric.CopyFrom(self.counter_template)
    metric.timestamps.extend([1454092320, 1454092321])
    metric.values.extend([1, 2])
    metric.name = CurieMetric.kNetTransmitted
    metric.unit = CurieMetric.kKilobytes
    metric.rate = CurieMetric.kPerSecond
    new_results = {"node_0": [metric]}
    csv = ScenarioUtil.results_map_to_csv(new_results, newline="\r\n")
    self.assertEqual(
      csv,
      "timestamp,node_id,metric_name,instance,value\r\n" +
      "1454092320,node_0,NetTransmitted.Avg.KilobytesPerSecond,Aggregated,1\r\n" +
      "1454092321,node_0,NetTransmitted.Avg.KilobytesPerSecond,Aggregated,2\r\n")
