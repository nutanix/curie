#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest
from datetime import datetime

import mock
import pandas
import pytz
import requests

from curie import prometheus
from curie.scenario import Scenario
from curie.vm_group import VMGroup
from curie.testing.util import mock_cluster
from curie.vm import Vm


class TestPrometheusAdapter(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_init_defaults(self):
    p = prometheus.PrometheusAdapter()
    self.assertEqual("localhost", p.host)
    self.assertEqual(9090, p.port)
    self.assertEqual("http://localhost:9090/api/v1/", p.base_url)

  def test_init_override(self):
    p = prometheus.PrometheusAdapter(host="fake_host", port=12345,
                                     protocol="https")
    self.assertEqual("https://fake_host:12345/api/v1/", p.base_url)

  @mock.patch("curie.prometheus.requests")
  def test_query_range_start_end_integer(self, mock_requests):
    data_return_value = {"some_data"}
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": data_return_value}
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    data = p.query_range("this_is_the_query", 1510076387, 1510076487)
    self.assertEqual(data_return_value, data)
    mock_requests.get.assert_called_once_with(
      "http://localhost:9090/api/v1/query_range",
      params={"query": "this_is_the_query",
              "start": "2017-11-07T17:39:47Z",
              "end": "2017-11-07T17:41:27Z",
              "step": "5s"})

  @mock.patch("curie.prometheus.requests")
  def test_query_range_start_end_float_floor(self, mock_requests):
    data_return_value = {"some_data"}
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": data_return_value}
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    p.query_range("this_is_the_query", 1510076387.9, 1510076487.9)
    mock_requests.get.assert_called_once_with(
      "http://localhost:9090/api/v1/query_range",
      params={"query": "this_is_the_query",
              "start": "2017-11-07T17:39:47Z",
              "end": "2017-11-07T17:41:27Z",
              "step": "5s"})

  @mock.patch("curie.prometheus.requests")
  def test_query_range_start_end_naive_datetime(self, mock_requests):
    data_return_value = {"some_data"}
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": data_return_value}
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    p.query_range("this_is_the_query",
                  datetime(2017, 11, 07, 17, 39, 47),
                  datetime(2017, 11, 07, 17, 41, 27))
    mock_requests.get.assert_called_once_with(
      "http://localhost:9090/api/v1/query_range",
      params={"query": "this_is_the_query",
              "start": "2017-11-07T17:39:47Z",
              "end": "2017-11-07T17:41:27Z",
              "step": "5s"})

  @mock.patch("curie.prometheus.requests")
  def test_query_range_start_end_tz_datetime(self, mock_requests):
    data_return_value = {"some_data"}
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": data_return_value}
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    eastern = pytz.timezone("US/Eastern")
    p.query_range(
      "this_is_the_query",
      eastern.localize(datetime(2017, 11, 07, 12, 39, 47)),
      eastern.localize(datetime(2017, 11, 07, 12, 41, 27)))
    mock_requests.get.assert_called_once_with(
      "http://localhost:9090/api/v1/query_range",
      params={"query": "this_is_the_query",
              "start": "2017-11-07T17:39:47Z",
              "end": "2017-11-07T17:41:27Z",
              "step": "5s"})

  @mock.patch("curie.prometheus.datetime")
  @mock.patch("curie.prometheus.requests")
  def test_query_range_start_end_tz_None(self, mock_requests, mock_datetime):
    data_return_value = {"some_data"}
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": data_return_value}
    mock_requests.get.return_value = mock_response
    now = datetime(2017, 11, 07, 17, 39, 47)
    mock_datetime.utcnow.return_value = now
    p = prometheus.PrometheusAdapter()
    p.query_range("this_is_the_query", None, None)
    mock_requests.get.assert_called_once_with(
      "http://localhost:9090/api/v1/query_range",
      params={"query": "this_is_the_query",
              "start": "2017-11-07T17:39:47Z",
              "end": "2017-11-07T17:39:47Z",
              "step": "5s"})

  @mock.patch("curie.prometheus.requests")
  def test_query_range_error_400_json_valid(self, mock_requests):
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.json.return_value = {"error": "OMGWTFBBQ"}
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    with self.assertRaises(prometheus.PrometheusAPIError) as ar:
      p.query_range("this_is_the_query", 1510076387, 1510076487)
    self.assertEqual("Failed to query_range: 'OMGWTFBBQ'", str(ar.exception))

  @mock.patch("curie.prometheus.requests")
  def test_query_range_error_400_json_invalid(self, mock_requests):
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.json.side_effect = ValueError("This ain't JSON, fella.")
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    with self.assertRaises(prometheus.PrometheusAPIError) as ar:
      p.query_range("this_is_the_query", 1510076387, 1510076487)
    self.assertEqual("Failed to query_range: %r" % mock_response,
                     str(ar.exception))

  @mock.patch("curie.prometheus.requests")
  def test_query_range_error_200_json_invalid(self, mock_requests):
    mock_response = mock.Mock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.side_effect = ValueError("This ain't JSON, fella.")
    mock_response.text = "{\"I hope I didn't miss a closing brace somewhere\""
    mock_requests.get.return_value = mock_response
    p = prometheus.PrometheusAdapter()
    with self.assertRaises(prometheus.PrometheusAPIError) as ar:
      p.query_range("this_is_the_query", 1510076387, 1510076487)
    self.assertEqual("Received status code 200, but JSON could not be "
                     "decoded: '{\"I hope I didn\\\'t miss a closing brace "
                     "somewhere\"'", str(ar.exception))

  def test_get_disk_iops(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "sum("
      "irate(node_disk_reads_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_writes_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_ops(vm_group, start=1510076387, end=1510076487)
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_iops_aggregate_sum(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "sum("
      "sum("
      "irate(node_disk_reads_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_writes_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_ops(vm_group, start=1510076387, end=1510076487,
                            agg_func="sum")
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_iops_aggregate_mean(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "avg("
      "sum("
      "irate(node_disk_reads_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_writes_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_ops(vm_group, start=1510076387, end=1510076487,
                            agg_func="mean")
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_iops_aggregate_unsupported(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      with self.assertRaises(ValueError) as ar:
        p.get_disk_ops(vm_group, start=1510076387, end=1510076487,
                       agg_func="meaner")
      self.assertEqual("Unsupported aggregation function 'meaner'",
                       str(ar.exception))
      mock_query_range.assert_not_called()

  def test_get_disk_octets(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "sum("
      "irate(node_disk_read_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_written_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_octets(vm_group, start=1510076387, end=1510076487)
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_octets_aggregate_sum(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "sum("
      "sum("
      "irate(node_disk_read_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_written_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_octets(vm_group, start=1510076387, end=1510076487,
                               agg_func="sum")
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_octets_aggregate_mean(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "avg("
      "sum("
      "irate(node_disk_read_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_written_bytes_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group)")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_disk_octets(vm_group, start=1510076387, end=1510076487,
                               agg_func="mean")
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_disk_octets_aggregate_unsupported(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      with self.assertRaises(ValueError) as ar:
        p.get_disk_octets(vm_group, start=1510076387, end=1510076487,
                          agg_func="meaner")
      self.assertEqual("Unsupported aggregation function 'meaner'",
                       str(ar.exception))
      mock_query_range.assert_not_called()

  def test_get_avg_latency(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "("
      "sum("
      "irate(node_disk_read_time_seconds_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_write_time_seconds_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance) / "
      "("
      "sum("
      "irate(node_disk_reads_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_writes_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance) > 5"
      ")"
      ") * 1000000.0")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_avg_disk_latency(vm_group, start=1510076387, end=1510076487)
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_avg_latency_aggregate_mean(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "("
      "sum("
      "sum("
      "irate(node_disk_read_time_seconds_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_write_time_seconds_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group) / "
      "("
      "sum("
      "sum("
      "irate(node_disk_reads_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s]) + "
      "irate(node_disk_writes_completed_total{"
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\"}[30s])) "
      "by(instance)) "
      "by(vm_group) > 5"
      ")"
      ") * 1000000.0")
    p = prometheus.PrometheusAdapter()
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_avg_disk_latency(vm_group, start=1510076387, end=1510076487,
                                    agg_func="mean")
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_get_generic(self):
    vm_group = mock.Mock(spec=VMGroup)
    vm_group.name.return_value = "Fake VM Group"
    vm_group._scenario = mock.Mock(spec=Scenario)
    vm_group._scenario.id = "854982789152076068"
    expected_query_str = (
      "irate("
      "node_network_receive_bytes_total{"
      "device=\"eth0\", "
      "scenario_id=\"854982789152076068\", "
      "vm_group=\"Fake VM Group\""
      "}"
      "[30s])"
    )
    p = prometheus.PrometheusAdapter()
    query_input = (
      "irate("
      "node_network_receive_bytes_total{"
      "device=\"eth0\", "
      "__curie_filter_scenario__, "
      "__curie_filter_vm_group__"
      "}"
      "[30s])"
    )
    with mock.patch.object(p, "query_range") as mock_query_range:
      mock_query_range.return_value = "this_is_fake_data"
      data = p.get_generic(vm_group, start=1510076387, end=1510076487,
                           query=query_input)
      self.assertEqual("this_is_fake_data", data)
      mock_query_range.assert_called_once_with(
        expected_query_str, 1510076387, 1510076487, step=None)

  def test_to_series_default(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        }
      ]
    }
    result = data["result"][0]
    s = prometheus.to_series(result)
    self.assertEqual("mock_vm_0", s.name)
    self.assertEqual(pandas.np.float64, s.dtype)
    for expected, actual in zip([1509653822,
                                 1509653827,
                                 1509653832,
                                 1509653837,
                                 1509653842], s.index):
      self.assertEqual(expected, actual)
    for expected, actual in zip([1.123,
                                 2.123,
                                 3.123,
                                 pandas.np.nan,
                                 5.123], s.values):
      if pandas.np.isnan(expected):
        self.assertTrue(pandas.np.isnan(actual))
      else:
        self.assertAlmostEqual(expected, actual)

  def test_to_series_dropna_false(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        }
      ]
    }
    result = data["result"][0]
    s = prometheus.to_series(result, dropna=False)
    # 5 instead of 4 due to gap in the index.
    self.assertEqual(5, len(s.index))
    self.assertEqual(5, len(s.values))
    self.assertTrue(pandas.np.isnan(s.values[3]))

  def test_to_series_metric_empty(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"4.123"]]
        }
      ]
    }
    result = data["result"][0]
    s = prometheus.to_series(result, dropna=False)
    self.assertEqual(None, s.name)

  def test_to_series_list_default(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_1"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        },
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.234"],
                      [1509653827, u"2.234"],
                      [1509653837, u"4.234"],
                      [1509653842, u"5.234"]]
        },
      ]
    }
    s_list = prometheus.to_series_list(data)
    self.assertTrue(prometheus.to_series(data["result"][0]).equals(s_list[0]))
    self.assertEqual("mock_vm_1", s_list[0].name)
    self.assertTrue(pandas.np.isnan(s_list[0].values[3]))
    self.assertTrue(prometheus.to_series(data["result"][1]).equals(s_list[1]))
    self.assertEqual("mock_vm_0", s_list[1].name)
    self.assertTrue(pandas.np.isnan(s_list[1].values[2]))

  def test_to_series_list_dropna_true(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_1"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        },
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.234"],
                      [1509653827, u"2.234"],
                      [1509653837, u"4.234"],
                      [1509653842, u"5.234"]]
        },
      ]
    }
    s_list = prometheus.to_series_list(data, dropna=True)
    self.assertTrue(
      prometheus.to_series(data["result"][0], dropna=True).equals(s_list[0]))
    self.assertEqual("mock_vm_1", s_list[0].name)
    self.assertFalse(pandas.np.isnan(s_list[0].values[3]))
    self.assertTrue(
      prometheus.to_series(data["result"][1], dropna=True).equals(s_list[1]))
    self.assertEqual("mock_vm_0", s_list[1].name)
    self.assertFalse(pandas.np.isnan(s_list[1].values[2]))

  def test_to_series_list_dropna_false(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_1"},
          u"values": [[1509653822, u"1.123"],
                      [1509653827, u"2.123"],
                      [1509653832, u"3.123"],
                      [1509653842, u"5.123"]]
        },
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.234"],
                      [1509653827, u"2.234"],
                      [1509653837, u"4.234"],
                      [1509653842, u"5.234"]]
        },
      ]
    }
    s_list = prometheus.to_series_list(data, dropna=False)
    self.assertTrue(
      prometheus.to_series(data["result"][0], dropna=False).equals(s_list[0]))
    self.assertEqual("mock_vm_1", s_list[0].name)
    self.assertTrue(pandas.np.isnan(s_list[0].values[3]))
    self.assertTrue(
      prometheus.to_series(data["result"][1], dropna=False).equals(s_list[1]))
    self.assertEqual("mock_vm_0", s_list[1].name)
    self.assertTrue(pandas.np.isnan(s_list[1].values[2]))

  def test_to_series_values_empty(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": []
        }
      ]
    }
    result = data["result"][0]
    s = prometheus.to_series(result)
    self.assertTrue(s.empty)

  def test_to_series_values_length_one(self):
    data = {
      u"resultType": u"matrix",
      u"result": [
        {
          u"metric": {u"instance": u"mock_vm_0"},
          u"values": [[1509653822, u"1.234"]]
        }
      ]
    }
    result = data["result"][0]
    s = prometheus.to_series(result)
    self.assertEqual(len(s), 1)

  def test_scenario_target_config(self):
    scenario = Scenario(name="Fake Scenario")
    scenario.cluster = mock_cluster()
    scenario.cluster.name.return_value = "fake_cluster"
    vm_group = VMGroup(scenario, "nasty\nvg?nm /\t#$\\")
    mock_vms = [mock.Mock(spec=Vm) for _ in xrange(3)]
    mock_vms[0].vm_name.return_value = "mock_vm_0"
    mock_vms[1].vm_name.return_value = "mock_vm_1"
    mock_vms[2].vm_name.return_value = "mock_vm_2"
    mock_vms[0].vm_ip.return_value = "fake_addr_0"
    mock_vms[1].vm_ip.return_value = "fake_addr_1"
    mock_vms[2].vm_ip.return_value = None
    scenario.vm_groups = {"fake_vm_group": vm_group}
    with mock.patch.object(vm_group, "get_vms") as mock_get_vms:
      mock_get_vms.return_value = mock_vms
      ret = prometheus.scenario_target_config(scenario)
    expected = [
      {
        "labels": {
          "cluster_name": "fake_cluster",
          "instance": "mock_vm_0",
          "job": "xray",
          "scenario_display_name": "Fake Scenario",
          "scenario_id": str(scenario.id),
          "scenario_name": "Fake Scenario",
          "vm_group": "nasty\nvg?nm /\t#$\\",
        },
        "targets": [
          "fake_addr_0:9100",
        ],
      },
      {
        "labels": {
          "cluster_name": "fake_cluster",
          "instance": "mock_vm_1",
          "job": "xray",
          "scenario_display_name": "Fake Scenario",
          "scenario_id": str(scenario.id),
          "scenario_name": "Fake Scenario",
          "vm_group": "nasty\nvg?nm /\t#$\\",
        },
        "targets": [
          "fake_addr_1:9100",
        ],
      }
    ]
    self.assertEqual(ret, expected)

  def test_aggregate(self):
    self.assertEqual("sum(this_is_the_query)",
                     prometheus._aggregate("this_is_the_query", "sum"))
    self.assertEqual("sum(this_is_the_query) by(var1, var2)",
                     prometheus._aggregate("this_is_the_query", "sum",
                                           by=["var1", "var2"]))
    self.assertEqual("avg(this_is_the_query) by(var1, var2)",
                     prometheus._aggregate("this_is_the_query", "avg",
                                           by=["var1", "var2"]))
    self.assertEqual("avg(this_is_the_query) by(var1, var2)",
                     prometheus._aggregate("this_is_the_query", "mean",
                                           by=["var1", "var2"]))
    self.assertEqual("min(this_is_the_query) by(var1, var2)",
                     prometheus._aggregate("this_is_the_query", "min",
                                           by=["var1", "var2"]))
    self.assertEqual("max(this_is_the_query) by(var1, var2)",
                     prometheus._aggregate("this_is_the_query", "max",
                                           by=["var1", "var2"]))
    with self.assertRaises(ValueError) as ar:
      prometheus._aggregate("this_is_the_query", "potato")
    self.assertEqual("Unsupported aggregation function 'potato'",
                     str(ar.exception))
