#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import cPickle as pickle
import logging
import unittest

import mock
import pandas

from curie.curie_metrics_pb2 import CurieMetric
from curie.curie_test_pb2 import CurieTestResult
from curie.exception import CurieTestException
from curie.scenario import Scenario, Phase
from curie.result.cluster_result import ClusterResult
from curie.scenario_util import ScenarioUtil
from curie.testing import environment
from curie.testing.util import mock_cluster

log = logging.getLogger(__name__)


class TestClusterResult(unittest.TestCase):
  def setUp(self):
    self.cluster = mock_cluster()
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.scenario.phase = Phase.RUN

  def test_get_metric_cpu_average_megahertz(self):
    metric = ClusterResult.get_metric("CpuUsage.Avg.Megahertz")
    self.assertEqual(CurieMetric.kCpuUsage, metric.name)
    self.assertEqual(CurieMetric.kAvg, metric.consolidation)
    self.assertEqual(CurieMetric.kMegahertz, metric.unit)

  def test_get_metric_not_exist(self):
    with self.assertRaises(CurieTestException) as ar:
      ClusterResult.get_metric("not_a_real_metric")
    self.assertEqual("'not_a_real_metric' is not supported - is not collected "
                     "or does not exist", str(ar.exception))

  def test_pickle_unpickle(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond")
    result.metric.description = "Why would one modify the description anyway?"
    unpickled_result = pickle.loads(pickle.dumps(result))
    self.assertEqual(result.scenario, unpickled_result.scenario)
    self.assertEqual(result.name, unpickled_result.name)
    self.assertEqual(result.aggregate, unpickled_result.aggregate)
    self.assertEqual(result.report_metrics, unpickled_result.report_metrics)
    self.assertEqual(result.report_group, unpickled_result.report_group)
    self.assertEqual(result.metric_name, unpickled_result.metric_name)
    self.assertEqual(result.metric, unpickled_result.metric)
    self.assertEqual(result.kwargs, unpickled_result.kwargs)

  def test_get_result_pbs(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond")
    metric = CurieMetric(
      name=CurieMetric.kNetReceived,
      description="Average network data received across all "
                  "interfaces.",
      instance="Aggregated",
      type=CurieMetric.kGauge,
      consolidation=CurieMetric.kAvg,
      unit=CurieMetric.kKilobytes,
      rate=CurieMetric.kPerSecond,
      experimental=True)
    ScenarioUtil.append_cluster_stats(
      {"0": [metric],
       "1": [metric],
       "2": [metric],
       "3": [metric],
       },
      self.scenario.cluster_stats_dir())
    pbs = result.get_result_pbs()
    self.assertEqual(4, len(pbs))
    for pb in pbs:
      self.assertIsInstance(pb, CurieTestResult)

  def test_get_result_pbs_partial(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond")
    metric = CurieMetric(
      name=CurieMetric.kNetReceived,
      description="Average network data received across all "
                  "interfaces.",
      instance="Aggregated",
      type=CurieMetric.kGauge,
      consolidation=CurieMetric.kAvg,
      unit=CurieMetric.kKilobytes,
      rate=CurieMetric.kPerSecond,
      experimental=True)
    ScenarioUtil.append_cluster_stats(
      {"0": [metric],
       "1": [metric],
       "2": [],
       "3": [],
       },
      self.scenario.cluster_stats_dir())
    pbs = result.get_result_pbs()
    self.assertEqual(2, len(pbs))
    self.assertIsInstance(pbs[0], CurieTestResult)
    self.assertIsInstance(pbs[1], CurieTestResult)

  def test_get_result_pbs_before_append(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond")
    pbs = result.get_result_pbs()
    self.assertEqual(0, len(pbs))

  def test_series_list_to_result_pbs_bandwidth_default(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for node_index, result_pb in enumerate(result_pbs):
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kBytesPerSecond)
      self.assertIn("(Node %d)" % node_index, result_pb.name)

  def test_series_list_to_result_pbs_percent_default(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "CpuUsage.Avg.Percent")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for node_index, result_pb in enumerate(result_pbs):
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kPercent)
      self.assertIn("(Node %d)" % node_index, result_pb.name)

  def test_series_list_to_result_pbs_megahertz_default(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "CpuUsage.Avg.Megahertz")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for node_index, result_pb in enumerate(result_pbs):
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kHertz)
      self.assertIn("(Node %d)" % node_index, result_pb.name)

  def test_series_list_to_result_pbs_iops_bandwidth_aggregate(self):
    result = ClusterResult(self.scenario, "fake_result",
                           "NetReceived.Avg.KilobytesPerSecond",
                           aggregate="sum")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 1)
    for result_pb in result_pbs:
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kBytesPerSecond)
      self.assertNotIn("(Node", result_pb.name)

  def test_construct_invalid_metric_name(self):
    with self.assertRaises(CurieTestException):
      ClusterResult(self.scenario, "fake_result", "invalid_metric_name")

  def test_construct_unsupported_metric(self):
    with self.assertRaises(CurieTestException):
      ClusterResult(self.scenario, "fake_result",
                    "DatastoreWrite.Avg.KilobytesPerSecond")

  @mock.patch("curie.cluster.Cluster.metrics")
  def test_parse_normal_metric(self, mock_metrics):
    mock_metrics.return_value = [
      CurieMetric(name=CurieMetric.kCpuUsage,
                   description="This is a fake non-experimental metric",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kMegahertz)]
    result = ClusterResult.parse(self.scenario, "Experimental Metric",
                                 {"metric": "CpuUsage.Avg.Megahertz",
                                  "aggregate": "sum"})
    self.assertIsInstance(result, ClusterResult)
    self.assertEqual(result.metric_name, "CpuUsage.Avg.Megahertz")
    self.scenario.enable_experimental_metrics = True
    result = ClusterResult.parse(self.scenario, "Experimental Metric",
                                 {"metric": "CpuUsage.Avg.Megahertz",
                                  "aggregate": "sum"})
    self.assertIsInstance(result, ClusterResult)
    self.assertEqual(result.metric_name, "CpuUsage.Avg.Megahertz")

  @mock.patch("curie.cluster.Cluster.metrics")
  def test_parse_experimental_metric(self, mock_metrics):
    mock_metrics.return_value = [
      CurieMetric(name=CurieMetric.kCpuUsage,
                   description="This is a fake experimental metric",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kMegahertz,
                   experimental=True)]
    result = ClusterResult.parse(self.scenario, "Experimental Metric",
                                 {"metric": "CpuUsage.Avg.Megahertz",
                                  "aggregate": "sum"})
    self.assertIsNone(result)
    self.scenario.enable_experimental_metrics = True
    result = ClusterResult.parse(self.scenario, "Experimental Metric",
                                 {"metric": "CpuUsage.Avg.Megahertz",
                                  "aggregate": "sum"})
    self.assertIsInstance(result, ClusterResult)
    self.assertEqual(result.metric_name, "CpuUsage.Avg.Megahertz")

  def __fake_timestamps_and_series_list(self):
    series_list = []
    timestamps = [1471355105, 1471355106, 1471355107, 1471355108, 1471355109,
                  1471355110]
    for _ in xrange(4):
      fake_values = [0, 1, 2, 3, 4, 5]
      series_list.append(pandas.Series(fake_values,
                                       index=pandas.to_datetime(timestamps,
                                                                unit="s")))
    return timestamps, series_list
