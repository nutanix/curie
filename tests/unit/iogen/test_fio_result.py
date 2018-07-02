#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import os
import unittest

import mock

from curie.curie_test_pb2 import CurieTestResult
from curie.exception import CurieTestException
from curie.iogen import fio_result
from curie.iogen.fio_result import FioWorkloadResult
from curie.testing import environment


class TestFioWorkloadResult(unittest.TestCase):
  def setUp(self):
    example_results_path = os.path.join(environment.resource_dir(),
                                        "test_fio_util",
                                        "example_results.out")
    self.fio_result = FioWorkloadResult(example_results_path)

  def test_get_results_data_default(self):
    with mock.patch.object(fio_result.log,
                           "warning",
                           wraps=fio_result.log.warning) as mock_warning:
      self.fio_result.get_results_data()
      self.assertEqual(mock_warning.call_count, 0)

  @mock.patch("curie.iogen.fio_result.json")
  def test_get_results_data_ValueError(self, mock_json):
    mock_json.loads.side_effect = ValueError
    with mock.patch.object(fio_result.log,
                           "warning",
                           wraps=fio_result.log.warning) as mock_warning:
      self.fio_result.get_results_data()
      self.assertGreater(mock_warning.call_count, 0)

  @mock.patch("curie.iogen.fio_result.json")
  def test_get_results_data_TypeError(self, mock_json):
    mock_json.loads.side_effect = TypeError
    with mock.patch.object(fio_result.log,
                           "warning",
                           wraps=fio_result.log.warning) as mock_warning:
      self.fio_result.get_results_data()
      self.assertGreater(mock_warning.call_count, 0)

  def test_clear_cache(self):
    self.fio_result.get_results_data()
    self.assertTrue(self.fio_result._results_samples is not None)
    self.fio_result.clear_cache()
    self.assertTrue(self.fio_result._results_samples is None)

  def test_get_series_unexpected_results_type(self):
    with self.assertRaises(CurieTestException):
      self.fio_result.get_series("not_a_type")

  def test_get_series_active(self):
    for value in self.fio_result.get_series("active").values:
      self.assertIn(value, [0.0, 1.0])

  def test_get_series_errors(self):
    self.assertEqual(list(self.fio_result.get_series("errors").values),
                     [0] * 11)

  def test_get_series_iops(self):
    self.assertEqual(list(self.fio_result.get_series("iops").values),
                     [4220.6, 4231.0])

  def test_get_series_bandwidth(self):
    self.assertEqual(list(self.fio_result.get_series("bandwidth").values),
                     [49266688.0, 49120870.4])

  def test_get_iops_result_pb_default(self):
    pb = self.fio_result.get_iops_result_pb()
    self.assertIsInstance(pb.data_2d.pickled_2d_data,
                          CurieTestResult.Data2D.Pickled2DData)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.x_vals_pickled, str)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.y_vals_pickled, str)

  def test_get_errors_result_pb_default(self):
    pb = self.fio_result.get_errors_result_pb()
    self.assertIsInstance(pb.data_2d.pickled_2d_data,
                          CurieTestResult.Data2D.Pickled2DData)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.x_vals_pickled, str)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.y_vals_pickled, str)

  def test_get_active_result_pb_default(self):
    pb = self.fio_result.get_active_result_pb()
    self.assertIsInstance(pb.data_2d.pickled_2d_data,
                          CurieTestResult.Data2D.Pickled2DData)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.x_vals_pickled, str)
    self.assertIsInstance(pb.data_2d.pickled_2d_data.y_vals_pickled, str)
