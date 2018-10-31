#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import logging

import pandas

from curie import curie_test_pb2, prometheus
from curie.exception import CurieTestException
from curie.metrics_util import MetricsUtil
from curie.scenario import Phase
from curie.result.base_result import BaseResult

log = logging.getLogger(__name__)


class VMGroupResult(BaseResult):
  """
  Manages information about VMGroup activity.
  """

  def __init__(self, scenario, name, vm_group, result_type, aggregate=None,
               workload_mask=None, step=None, **kwargs):
    super(VMGroupResult, self).__init__(scenario, name, aggregate, **kwargs)
    self.vm_group = vm_group
    self.result_type = result_type.lower()
    self.workload_mask = workload_mask
    self.step = step
    self.kwargs = kwargs
    if self.result_type == "iops":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kIOPS
    elif self.result_type == "bandwidth":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kBytesPerSecond
    elif self.result_type == "latency":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kMicroseconds
    elif self.result_type == "generic":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kCount
    else:
      raise CurieTestException("Unexpected results type '%s'" %
                               self.result_type)

  @staticmethod
  def parse(scenario, name, definition):
    vm_group_name = definition.get("vm_group", None)
    if not vm_group_name:
      raise KeyError("Key 'vm_group' not defined in result definition")
    vm_group = scenario.vm_groups.get(vm_group_name, None)
    if not vm_group:
      raise KeyError("No VM group named %r found" % vm_group_name)
    result_type = definition.get("result_type", None)
    if not result_type:
      raise KeyError("result_type is required")
    workload_mask_name = definition.get("workload_mask", None)
    if workload_mask_name:
      workload = scenario.workloads.get(workload_mask_name)
      if not workload:
        raise KeyError("workload_mask %r defined, but workload %r not found" %
                       (workload_mask_name, workload_mask_name))
      else:
        definition["workload_mask"] = workload
    del definition["vm_group"]
    del definition["result_type"]
    return VMGroupResult(scenario, name, vm_group, result_type, **definition)

  def get_result_pbs(self):
    """
    Produce the results for a given VM Group.
    """
    if not self.scenario.prometheus_address:
      log.debug("Unable to get protobufs from VMGroupResult: "
                "scenario.prometheus_address not configured")
      return []
    host, port = self.scenario.prometheus_address.split(":")
    prometheus_adapter = prometheus.PrometheusAdapter(host=host, port=port)
    if self.workload_mask:
      run_start = self.workload_mask.iogen().get_workload_start_time()
      run_end = self.workload_mask.iogen().get_workload_end_time()
    else:
      run_start = self.scenario.phase_start_time_secs(Phase.RUN)
      run_end = self.scenario.phase_start_time_secs(Phase.TEARDOWN)
    if not run_start:
      return []
    if self.result_type == "iops":
      data = prometheus_adapter.get_disk_ops(
        self.vm_group, run_start, run_end, agg_func=self.aggregate,
        step=self.step)
    elif self.result_type == "bandwidth":
      data = prometheus_adapter.get_disk_octets(
        self.vm_group, run_start, run_end, agg_func=self.aggregate,
        step=self.step)
    elif self.result_type == "latency":
      data = prometheus_adapter.get_avg_disk_latency(
        self.vm_group, run_start, run_end, agg_func=self.aggregate,
        step=self.step)
    elif self.result_type == "generic":
      data = prometheus_adapter.get_generic(
        self.vm_group, run_start, run_end, query=self.kwargs["query"],
        agg_func=self.aggregate, step=self.step)
    else:
      raise ValueError("Unexpected result_type %r" % self.result_type)
    return self.series_list_to_result_pbs(prometheus.to_series_list(data))

  def series_list_to_result_pbs(self, series_list):
    """
    Convert a list of series objects into a list of result protobufs.

    Args:
      series_list (list of pandas.Series): List of objects returned from one
        of the curie.prometheus helper methods, e.g. to_series_list.

    Returns:
      List of curie_test_pb2.CurieTestResult.

    Raises:
      CurieTestException:
        If this result's result_type is invalid.
    """
    result_pbs = []
    for result_num, series in enumerate(sorted(series_list,
                                               key=lambda s: s.name)):
      test_result = curie_test_pb2.CurieTestResult()
      # Replace NaN with None.
      series = series.where(pandas.notnull(series), None)
      vals = series.values.tolist()
      timestamps = series.index.astype(pandas.np.int64).tolist()
      MetricsUtil.pickle_xy_data2d(timestamps, vals, test_result.data_2d)
      test_result.data_2d.x_unit_type = self.x_unit
      test_result.data_2d.y_unit_type = self.y_unit
      test_result.name = self.kwargs.get("title", self.name)
      if len(series_list) > 1:
        test_result.name += " (VM %d)" % result_num
      test_result.description = self.kwargs.get("description", "")
      test_result.group = "%s_%s" % (
        self.__class__.__name__,
        self.kwargs.get("group", self.vm_group.name()))
      test_result.result_id = ("%s_%s_%d" %
                               (self.scenario.name, self.name, result_num))
      test_result.data_2d.x_label = self.kwargs.get("x_label", "")
      test_result.data_2d.y_label = self.kwargs.get("y_label", "")
      test_result.data_2d.report_metrics.extend(self.report_metrics)
      self._add_expected_value_details(test_result)
      if self.report_group:
        test_result.data_2d.report_group = self.report_group
      result_pbs.append(test_result)
      test_result.result_hint = self.kwargs.get("result_hint", "")
    return result_pbs
