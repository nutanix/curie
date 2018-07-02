#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import os
import pickle
import unittest
from difflib import unified_diff

import numpy

from curie.iogen.fio_config import FioConfiguration
from curie.iogen.fio_result import FioWorkloadResult
from curie.testing import environment


class TestParameterFile(unittest.TestCase):
  def setUp(self):
    self.valid_fio_path = os.path.join(environment.resource_dir(),
                                       "test_fio_util",
                                       "example_valid.fio")
    self.valid_expected_contents = (
      "[global]\n"
      "ioengine=libaio\n"
      "direct=1\n"
      "runtime=3600\n"
      "time_based\n"
      "norandommap\n"
      "ba=4k\n"
      "continue_on_error=all\n"
      "\n"
      "[db-oltp1]\n"
      "bssplit=8k/90:32k/10,8k/90:32k/10\n"
      "size=28G\n"
      "filename=/dev/sdd\n"
      "rw=randrw\n"
      "iodepth=128\n"
      "rate_iops=650,650\n"
      "\n"
      "[db-oltp2]\n"
      "bssplit=8k/90:32k/10,8k/90:32k/10\n"
      "size=28G\n"
      "filename=/dev/sde\n"
      "rw=randrw\n"
      "iodepth=128\n"
      "rate_iops=650,650\n"
      "\n")

    self.valid_expected_prefill_contents = (
      "[global]\n"
      "ioengine=libaio\n"
      "direct=1\n"
      "bs=1m\n"
      "iodepth=128\n"
      "rw=write\n"
      "\n"
      "[db-oltp1]\n"
      "filename=/dev/sdd\n"
      "size=28G\n"
      "\n"
      "[db-oltp2]\n"
      "filename=/dev/sde\n"
      "size=28G\n"
      "\n")

  def tearDown(self):
    pass

  def test_append_parameter_in_order(self):
    parameter_file = FioConfiguration()
    parameter_file.set("global", "ioengine", "libaio")
    parameter_file.set("global", "direct", "1")
    parameter_file.set("global", "time_based")
    parameter_file.set("db-oltp1", "size", "28G")
    parameter_file.set("db-oltp1", "rw", "randrw")
    expected_str = ("[global]\n"
                    "ioengine=libaio\n"
                    "direct=1\n"
                    "time_based\n"
                    "\n"
                    "[db-oltp1]\n"
                    "size=28G\n"
                    "rw=randrw\n"
                    "\n")
    value = FioConfiguration.dumps(parameter_file)
    expected = expected_str
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_append_parameter_out_of_order(self):
    parameter_file = FioConfiguration()
    parameter_file.set("global", "ioengine", "libaio")
    parameter_file.set("db-oltp1", "size", "28G")
    parameter_file.set("global", "direct", "1")
    parameter_file.set("db-oltp1", "rw", "randrw")
    parameter_file.set("global", "time_based")
    expected_str = ("[global]\n"
                    "ioengine=libaio\n"
                    "direct=1\n"
                    "time_based\n"
                    "\n"
                    "[db-oltp1]\n"
                    "size=28G\n"
                    "rw=randrw\n"
                    "\n")
    value = FioConfiguration.dumps(parameter_file)
    expected = expected_str
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_load_compared_to_dumps(self):
    parameter_file_from_file = FioConfiguration.load(open(self.valid_fio_path))
    value = FioConfiguration.dumps(parameter_file_from_file)
    expected = self.valid_expected_contents
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_load_compared_to_loads(self):
    parameter_file_from_file = FioConfiguration.load(open(self.valid_fio_path))
    parameter_file_from_str = FioConfiguration.loads(
      self.valid_expected_contents)
    self.assertEquals(parameter_file_from_file,
                      parameter_file_from_str)

  def test_convert_prefill(self):
    param_file = FioConfiguration.load(open(self.valid_fio_path))
    prefill_params = param_file.convert_prefill()
    expected_prefill_param = FioConfiguration.loads(
      self.valid_expected_prefill_contents)
    self.assertEquals(prefill_params, expected_prefill_param)


class TestFIOResults(unittest.TestCase):
  def setUp(self):
    self.valid_fio_results_path = os.path.join(environment.resource_dir(),
                                               "test_fio_util",
                                               "example_results.out")
    self.valid_fio_empty_results_path = os.path.join(
      environment.resource_dir(),
      "test_fio_util",
      "example_results_empty.out")
    self.valid_fio_single_results_path = os.path.join(
      environment.resource_dir(),
      "test_fio_util",
      "example_results_single.out")
    self.fio_result = FioWorkloadResult(self.valid_fio_results_path)

  def test_get_parsed_data(self):
    data = self.fio_result.get_results_data()
    self.assertTrue(len(data) > 0, "No data was included in parsed results.")

  def test_get_iops_result(self):
    result = self.fio_result.get_iops_result_pb()
    x_vals = pickle.loads(result.data_2d.pickled_2d_data.x_vals_pickled)
    y_vals = pickle.loads(result.data_2d.pickled_2d_data.y_vals_pickled)
    self.assertEquals(len(x_vals), len(y_vals))
    self.assertIsInstance(x_vals, list)
    self.assertIsInstance(y_vals, list)
    self.assertTrue(type(y_vals) == list,
                    "Unpicked y_vals are  type %s" % str(type(y_vals)))
    self.assertIsInstance(x_vals[0], numpy.integer)
    self.assertIsInstance(y_vals[0], int)

  def test_get_parsed_data_empty(self):
    empty_fio_result = FioWorkloadResult(self.valid_fio_empty_results_path)
    data = empty_fio_result.get_results_data()
    self.assertTrue(len(data) == 0)

  def test_get_iops_result_empty(self):
    empty_fio_result = FioWorkloadResult(self.valid_fio_empty_results_path)
    result = empty_fio_result.get_iops_result_pb()
    x_vals = pickle.loads(result.data_2d.pickled_2d_data.x_vals_pickled)
    y_vals = pickle.loads(result.data_2d.pickled_2d_data.y_vals_pickled)
    self.assertEquals(len(x_vals), len(y_vals), 0)

  def test_get_parsed_data_single(self):
    single_fio_result = FioWorkloadResult(self.valid_fio_single_results_path)
    data = single_fio_result.get_results_data()
    self.assertTrue(len(data) == 1)

  def test_get_iops_result_single(self):
    single_fio_result = FioWorkloadResult(self.valid_fio_single_results_path)
    result = single_fio_result.get_iops_result_pb()
    x_vals = pickle.loads(result.data_2d.pickled_2d_data.x_vals_pickled)
    y_vals = pickle.loads(result.data_2d.pickled_2d_data.y_vals_pickled)
    self.assertEquals(len(x_vals), len(y_vals), 1)
