#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

import mock
import pandas
from pandas.util.testing import assert_series_equal

from curie import curie_test_pb2
from curie.curie_test_pb2 import CurieTestResult
from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.iogen.iogen import IOGen
from curie.scenario import Scenario
from curie.test.result.iogen_result import IogenResult
from curie.test.vm_group import VMGroup
from curie.test.workload import Workload
from curie.testing import environment


class TestIogenResult(unittest.TestCase):
  def setUp(self):
    self.iogen = mock.Mock(spec=IOGen)
    self.iogen.reporting_interval = 1
    self.workload = mock.Mock(spec=Workload)
    self.workload.iogen.return_value = self.iogen
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.get_vms.return_value = []
    self.cluster = mock.Mock(spec=Cluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  def test_series_list_to_result_pbs_iops_default(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="iops")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for result_pb in result_pbs:
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kIOPS)

  def test_series_list_to_result_pbs_errors_default(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="errors")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for result_pb in result_pbs:
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kCount)

  def test_series_list_to_result_pbs_active_default(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="active")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 4)
    for result_pb in result_pbs:
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kBoolean)

  def test_series_list_to_result_pbs_invalid_type(self):
    with self.assertRaises(CurieTestException):
      IogenResult(self.scenario, "fake_result", self.workload,
                  result_type="not_a_type")

  def test_series_list_to_result_pbs_aggregate_sum(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="iops", aggregate="sum")
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    result_pbs = result.series_list_to_result_pbs(series_list)
    self.assertEqual(len(result_pbs), 1)
    for result_pb in result_pbs:
      self.assertEqual(type(result_pb), CurieTestResult)
      self.assertEqual(result_pb.data_2d.x_unit_type,
                       CurieTestResult.Data2D.kUnixTimestamp)
      self.assertEqual(result_pb.data_2d.y_unit_type,
                       CurieTestResult.Data2D.kIOPS)

  def test_series_list_to_result_pbs_aggregate_empty_list(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="iops", aggregate="sum")
    result_pbs = result.series_list_to_result_pbs([])
    self.assertEqual(len(result_pbs), 0)

  def test_series_list_to_result_pbs_aggregate_empty_series(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="iops", aggregate="sum")
    result_pbs = result.series_list_to_result_pbs([pandas.Series()])
    self.assertEqual(len(result_pbs), 0)

  def test_series_list_to_result_pbs_aggregate_invalid(self):
    with self.assertRaises(CurieTestException):
      IogenResult(self.scenario, "fake_result", self.workload,
                  result_type="iops", aggregate="not_a_valid_input")

  def test_combine_results_sum(self):
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    combined = IogenResult._combine_results(
      series_list, "sum",
      resample_secs=self.iogen.reporting_interval)
    assert_series_equal(combined,
                        pandas.Series([0, 4, 8, 12, 16, 20],
                                      index=pandas.to_datetime(timestamps,
                                                               unit="ms")))

  def test_combine_results_mean(self):
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    combined = IogenResult._combine_results(
      series_list, "mean",
      resample_secs=self.iogen.reporting_interval)
    assert_series_equal(combined,
                        pandas.Series([0.0, 1.0, 2.0, 3.0, 4.0, 5.0],
                                      index=pandas.to_datetime(timestamps,
                                                               unit="ms")))

  def test_combine_results_empty_list(self):
    self.assertTrue(IogenResult._combine_results(
      [], "mean",
      resample_secs=self.iogen.reporting_interval) is None)

  def test_combine_results_partially_empty_series(self):
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    series_list[-1] = series_list[-1].head(3)
    combined = IogenResult._combine_results(
      series_list, "sum",
      resample_secs=self.iogen.reporting_interval)
    assert_series_equal(combined,
                        pandas.Series([0.0, 4.0, 8.0, 9.0, 12.0, 15.0],
                                      index=pandas.to_datetime(timestamps,
                                                               unit="ms")))

  def test_combine_results_empty_series(self):
    timestamps, series_list = self.__fake_timestamps_and_series_list()
    series_list.append(pandas.Series(index=pandas.to_datetime([], unit="ms")))
    combined = IogenResult._combine_results(
      series_list, "mean",
      resample_secs=self.iogen.reporting_interval)
    assert_series_equal(combined,
                        pandas.Series(index=pandas.to_datetime([],
                                                               unit="ms")))

  def test_report_metrics(self):
    result = IogenResult(self.scenario, "fake_result", self.workload,
                         result_type="iops",
                         report_metrics=["Mean", "Variability"])

    Data2D = curie_test_pb2.CurieTestResult.Data2D
    self.assertEqual([Data2D.kMean, Data2D.kVariability],
                     result.report_metrics)

  def test_report_metrics_exception(self):
    with mock.patch("curie.test.result.base_result.curie_test_pb2."
                    "CurieTestResult.Data2D.ReportMetric.Value") as mock_value:
      mock_value.side_effect = ValueError
      with self.assertRaises(CurieTestException):
        IogenResult(self.scenario, "fake_result", self.workload,
                    result_type="iops",
                    report_metrics=["Mean", "Variability"])

  def __fake_timestamps_and_series_list(self):
    series_list = []
    timestamps = [1471355105000, 1471355106000, 1471355107000, 1471355108000,
                  1471355109000, 1471355110000]
    for _ in xrange(4):
      fake_values = [0, 1, 2, 3, 4, 5]
      series_list.append(pandas.Series(fake_values,
                                       index=pandas.to_datetime(timestamps,
                                                                unit="ms")))
    return timestamps, series_list
