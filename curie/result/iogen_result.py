#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
from curie import curie_test_pb2
from curie.exception import CurieTestException
from curie.iogen.fio_result import FioWorkloadResult
from curie.result.base_result import BaseResult


class IogenResult(BaseResult):
  """Manages information about a workloads results.

  """
  def __init__(self, scenario, name, workload, result_type="", aggregate=None,
               **kwargs):
    super(IogenResult, self).__init__(scenario, name, aggregate, **kwargs)
    self.workload = workload
    self.result_type = result_type.lower()
    self.kwargs = kwargs
    if self.result_type == "iops":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kIOPS
    elif self.result_type == "bandwidth":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kBytesPerSecond
    elif self.result_type == "errors":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kCount
    elif self.result_type == "active":
      self.x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      self.y_unit = curie_test_pb2.CurieTestResult.Data2D.kBoolean
    else:
      raise CurieTestException("Unexpected results type '%s'" %
                               self.result_type)

  @staticmethod
  def parse(scenario, name, definition):
    workload_name = definition.get("workload_name", None)
    if not workload_name:
      raise KeyError("Key 'workload_name' not defined in result definition")
    workload = scenario.workloads[definition.get("workload_name", None)]
    results = IogenResult(scenario, name, workload, **definition)
    return results

  def get_result_pbs(self):
    """Produces the results for a given workload with the specified design.

    Depending on what the result_type is, generate the appropriate result
    from the iogen. Then, apply various items to the resulting protobuf
    result. Additional parameters may be provided as kwargs in configuration.
    """
    iogen = self.workload.iogen()
    vms = self.workload.vm_group().get_vms()
    results = iogen.fetch_remote_results(vms)
    series_list = []
    for result in results:
      if result.data is not None:
        series_list.append(result.data.get_series(self.result_type))
        result.data.clear_cache()
    return self.series_list_to_result_pbs(series_list)

  def series_list_to_result_pbs(self, series_list):
    """Convert a list of series objects into a list of result protobufs.

    Args:
      series_list (list of pandas.Series): List of objects returned from one
        of the FioWorkloadResult helper methods, e.g. get_series.

    Returns:
      List of curie_test_pb2.CurieTestResult.

    Raises:
      CurieTestException:
        If this result's result_type is invalid.
    """
    iogen = self.workload.iogen()
    result_pbs = []
    if self.aggregate:
      series = self._combine_results(series_list, how=self.aggregate,
                                     resample_secs=iogen.reporting_interval)
      if series is None or series.empty:
        series_list = []
      else:
        series_list = [series]
    for result_num, series in enumerate(series_list):
      test_result = FioWorkloadResult.series_to_result_pb(series)
      test_result.data_2d.x_unit_type = self.x_unit
      test_result.data_2d.y_unit_type = self.y_unit
      test_result.name = self.kwargs.get("title", self.name)
      if len(series_list) > 1:
        test_result.name += " (VM %d)" % result_num
      test_result.description = self.kwargs.get("description", "")
      test_result.group = "%s_%s" % (
        self.__class__.__name__,
        self.kwargs.get("group", self.workload.name()))
      test_result.result_id = ("%s_%s_%d" %
                               (self.scenario.name, self.name, result_num))
      test_result.data_2d.x_label = self.kwargs.get("x_label", "")
      test_result.data_2d.y_label = self.kwargs.get("y_label", "")
      test_result.data_2d.report_metrics.extend(self.report_metrics)
      self._add_expected_value_details(test_result)
      if self.report_group:
        test_result.data_2d.report_group = self.report_group
      test_result.result_hint = self.kwargs.get("result_hint", "")
      result_pbs.append(test_result)
    return result_pbs
