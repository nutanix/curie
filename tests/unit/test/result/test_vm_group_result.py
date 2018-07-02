#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
#
import cPickle as pickle
import unittest

import mock
import pandas

from curie import curie_test_pb2, prometheus
from curie.curie_test_pb2 import CurieTestResult
from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.iogen.iogen import IOGen
from curie.metrics_util import MetricsUtil
from curie.scenario import Phase, Scenario
from curie.test.result.vm_group_result import VMGroupResult
from curie.test.vm_group import VMGroup
from curie.test.workload import Workload
from curie.testing import environment


class TestVMGroupResult(unittest.TestCase):
  def setUp(self):
    self.vm_group = mock.Mock(spec=VMGroup)
    self.vm_group.get_vms.return_value = []
    self.cluster = mock.Mock(spec=Cluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  def test_init_iops(self):
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="iops")
    self.assertEqual(curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp,
                     result.x_unit)
    self.assertEqual(curie_test_pb2.CurieTestResult.Data2D.kIOPS,
                     result.y_unit)

  def test_init_latency(self):
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="latency")
    self.assertEqual(curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp,
                     result.x_unit)
    self.assertEqual(curie_test_pb2.CurieTestResult.Data2D.kMicroseconds,
                     result.y_unit)

  def test_init_invalid_type(self):
    with self.assertRaises(CurieTestException) as ar:
      VMGroupResult(self.scenario, "fake_result", self.vm_group,
                    result_type="not_a_type")
    self.assertEqual("Unexpected results type 'not_a_type'", str(ar.exception))

  def test_parse_missing_vm_group_in_definition(self):
    definition = {"workload_mask": "workload_0"}
    with self.assertRaises(KeyError) as ar:
      VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertEqual("\"Key 'vm_group' not defined in result definition\"",
                     str(ar.exception))

  def test_parse_missing_vm_group_in_scenario(self):
    self.scenario.vm_groups = dict()
    definition = {"vm_group": "group_0"}
    with self.assertRaises(KeyError) as ar:
      VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertEqual("\"No VM group named 'group_0' found\"",
                     str(ar.exception))

  def test_parse_missing_result_type(self):
    self.scenario.vm_groups = {"group_0": self.vm_group}
    self.scenario.workloads = {}
    definition = {"vm_group": "group_0"}
    with self.assertRaises(KeyError) as ar:
      VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertEqual("'result_type is required'", str(ar.exception))

  def test_parse_missing_workload_mask_default(self):
    self.scenario.vm_groups = {"group_0": self.vm_group}
    self.scenario.workloads = {}
    definition = {"vm_group": "group_0",
                  "result_type": "iops"}
    result = VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertIsInstance(result, VMGroupResult)
    self.assertEqual("fake_name", result.name)
    self.assertEqual(self.vm_group, result.vm_group)
    self.assertEqual("iops", result.result_type)
    self.assertEqual(None, result.workload_mask)

  def test_parse_step(self):
    self.scenario.vm_groups = {"group_0": self.vm_group}
    self.scenario.workloads = {}
    definition = {"vm_group": "group_0",
                  "result_type": "iops",
                  "step": "1s"}
    result = VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertEqual("1s", result.step)

  def test_parse_workload_mask_without_workload_definition(self):
    self.scenario.vm_groups = {"group_0": self.vm_group}
    self.scenario.workloads = {}
    definition = {"vm_group": "group_0",
                  "result_type": "iops",
                  "workload_mask": "nonexistent_workload"}
    with self.assertRaises(KeyError) as ar:
      VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertEqual("\"workload_mask 'nonexistent_workload' defined, but "
                     "workload 'nonexistent_workload' not found\"",
                     str(ar.exception))

  def test_parse_workload_mask(self):
    mock_workload = mock.Mock(spec=Workload)
    self.scenario.vm_groups = {"group_0": self.vm_group}
    self.scenario.workloads = {"workload_0": mock_workload}
    definition = {"vm_group": "group_0",
                  "result_type": "iops",
                  "workload_mask": "workload_0"}
    result = VMGroupResult.parse(self.scenario, "fake_name", definition)
    self.assertIsInstance(result, VMGroupResult)
    self.assertEqual("fake_name", result.name)
    self.assertEqual(self.vm_group, result.vm_group)
    self.assertEqual("iops", result.result_type)
    self.assertEqual(mock_workload, result.workload_mask)

  @mock.patch(
    "curie.test.result.vm_group_result.prometheus.PrometheusAdapter")
  def test_get_result_pbs_no_prometheus_address(self, mock_prom_adapter):
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="iops")
    self.scenario.prometheus_address = None
    with mock.patch.object(result, "series_list_to_result_pbs") as mock_to_pbs:
      self.assertEqual([], result.get_result_pbs())
      mock_prom_adapter.assert_not_called()
      mock_prom_adapter.get_disk_ops.assert_not_called()
      mock_to_pbs.assert_not_called()

  @mock.patch(
    "curie.test.result.vm_group_result.prometheus.PrometheusAdapter")
  def test_get_result_pbs_no_run_start(self, mock_prom_adapter):
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="iops")
    self.scenario.prometheus_address = "fake_address:1234"
    with mock.patch.object(result, "series_list_to_result_pbs") as mock_to_pbs:
      self.assertEqual([], result.get_result_pbs())
      mock_prom_adapter.assert_called_once_with(host="fake_address",
                                                port="1234")
      mock_prom_adapter.return_value.get_disk_ops.assert_not_called()
      mock_to_pbs.assert_not_called()

  @mock.patch(
    "curie.test.result.vm_group_result.prometheus.PrometheusAdapter")
  def test_get_result_workload_mask(self, mock_prom_adapter):
    data = {"result": []}
    mock_prom_adapter.return_value.get_disk_ops.return_value = data
    mock_workload = mock.Mock(spec=Workload)
    mock_iogen = mock.Mock(spec=IOGen)
    mock_workload.iogen.return_value = mock_iogen
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           "iops", workload_mask=mock_workload, step="1s")
    self.scenario.prometheus_address = "fake_address:1234"
    self.scenario.phase = Phase.kRun
    mock_iogen.get_workload_start_time.return_value = 123456
    mock_iogen.get_workload_end_time.return_value = 234567
    result.get_result_pbs()
    mock_prom_adapter.assert_called_once_with(host="fake_address",
                                              port="1234")
    mock_prom_adapter.return_value.get_disk_ops.assert_called_once_with(
      self.vm_group, 123456, 234567, agg_func=None, step="1s")

  @mock.patch(
    "curie.test.result.vm_group_result.prometheus.PrometheusAdapter")
  def test_get_result_pbs_iops(self, mock_prom_adapter):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"fake_vm_0"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        }
      ]
    }
    mock_prom_adapter.return_value.get_disk_ops.return_value = data
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="iops")
    self.scenario.prometheus_address = "fake_address:1234"
    self.scenario.phase = Phase.kRun
    self.scenario._phase_start_time_secs[Phase.kRun] = 123456
    self.scenario._phase_start_time_secs[Phase.kTeardown] = 234567
    ret = result.get_result_pbs()
    mock_prom_adapter.assert_called_once_with(host="fake_address",
                                              port="1234")
    mock_prom_adapter.return_value.get_disk_ops.assert_called_once_with(
      self.vm_group, 123456, 234567, agg_func=None, step=None)
    self.assertIsInstance(ret, list)
    for pb in ret:
      self.assertIsInstance(pb, CurieTestResult)
      self.assertEqual(CurieTestResult.Data2D.kUnixTimestamp,
                       pb.data_2d.x_unit_type)
      self.assertEqual(CurieTestResult.Data2D.kIOPS,
                       pb.data_2d.y_unit_type)
      self.assertEqual(
        5, len(pickle.loads(pb.data_2d.pickled_2d_data.x_vals_pickled)))
      self.assertEqual(
        5, len(pickle.loads(pb.data_2d.pickled_2d_data.y_vals_pickled)))

  @mock.patch(
    "curie.test.result.vm_group_result.prometheus.PrometheusAdapter")
  def test_get_result_pbs_bandwidth(self, mock_prom_adapter):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"fake_vm_0"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        }
      ]
    }
    mock_prom_adapter.return_value.get_disk_octets.return_value = data
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="bandwidth")
    self.scenario.prometheus_address = "fake_address:1234"
    self.scenario.phase = Phase.kRun
    self.scenario._phase_start_time_secs[Phase.kRun] = 123456
    self.scenario._phase_start_time_secs[Phase.kTeardown] = 234567
    ret = result.get_result_pbs()
    mock_prom_adapter.assert_called_once_with(host="fake_address",
                                              port="1234")
    mock_prom_adapter.return_value.get_disk_octets.assert_called_once_with(
      self.vm_group, 123456, 234567, agg_func=None, step=None)
    self.assertIsInstance(ret, list)
    for pb in ret:
      self.assertIsInstance(pb, CurieTestResult)
      self.assertEqual(CurieTestResult.Data2D.kUnixTimestamp,
                       pb.data_2d.x_unit_type)
      self.assertEqual(CurieTestResult.Data2D.kBytesPerSecond,
                       pb.data_2d.y_unit_type)
      self.assertEqual(
        5, len(pickle.loads(pb.data_2d.pickled_2d_data.x_vals_pickled)))
      self.assertEqual(
        5, len(pickle.loads(pb.data_2d.pickled_2d_data.y_vals_pickled)))

  def test_series_list_to_result_pbs_order_by_instance(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"fake_vm_1"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        },
        {
          u"metric": {u"instance": u"fake_vm_0"},
          u"values": [[1509653823, u"2.123"],
                      [1509653828, u"3.123"],
                      [1509653833, u"4.123"],
                      [1509653843, u"6.123"]]
        }
      ]
    }
    series_list = prometheus.to_series_list(data, dropna=False)
    result = VMGroupResult(self.scenario, "fake_result", self.vm_group,
                           result_type="bandwidth")
    ret = result.series_list_to_result_pbs(series_list)

    self.assertEqual("fake_result (VM 0)", ret[0].name)
    unpickled_x = pickle.loads(
      ret[0].data_2d.pickled_2d_data.x_vals_pickled)
    unpickled_y = pickle.loads(
      ret[0].data_2d.pickled_2d_data.y_vals_pickled)
    self.assertEqual([1509653823,
                      1509653828,
                      1509653833,
                      1509653838,
                      1509653843],
                     unpickled_x)
    for expected, y in zip([2.123, 3.123, 4.123, None, 6.123], unpickled_y):
      self.assertAlmostEqual(y, expected)

    self.assertEqual("fake_result (VM 1)", ret[1].name)
    unpickled_x = pickle.loads(
      ret[1].data_2d.pickled_2d_data.x_vals_pickled)
    unpickled_y = pickle.loads(
      ret[1].data_2d.pickled_2d_data.y_vals_pickled)
    self.assertEqual([1509653822,
                      1509653827,
                      1509653832,
                      1509653837,
                      1509653842],
                     unpickled_x)
    for expected, y in zip([1.123, 2.123, 3.123, None, 5.123], unpickled_y):
      self.assertAlmostEqual(y, expected)
