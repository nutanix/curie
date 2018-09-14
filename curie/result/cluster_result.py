#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging

import pandas as pd

from curie import curie_test_pb2
from curie.curie_metrics_pb2 import CurieMetric
from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.iogen.fio_result import FioWorkloadResult
from curie.metrics_util import MetricsUtil
from curie.name_util import NameUtil
from curie.result.base_result import BaseResult
from curie.scenario_util import ScenarioUtil

log = logging.getLogger(__name__)


class ClusterResult(BaseResult):
  """Manages information about a workloads results.

  """

  def __init__(self, scenario, name, metric_name, aggregate=None, **kwargs):
    super(ClusterResult, self).__init__(scenario, name, aggregate, **kwargs)
    self.metric_name = metric_name
    self.metric = self.get_metric(self.metric_name)
    self.kwargs = kwargs

  def __getstate__(self):
    state = self.__dict__.copy()
    state["metric"] = state["metric"].SerializeToString()
    return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    metric_str = self.metric
    self.metric = self.get_metric(self.metric_name)
    self.metric.ParseFromString(metric_str)

  @staticmethod
  def get_metric(metric_name):
    # Only generic metrics are supported here.
    for metric in Cluster.metrics():
      if MetricsUtil.metric_name(metric) == metric_name:
        if metric.instance == "Aggregated":
          return metric
        else:
          raise CurieTestException("'%s' is not supported - requested metric "
                                    "must be aggregated type" % metric_name)
    else:
      raise CurieTestException("'%s' is not supported - is not collected or "
                                "does not exist" % metric_name)

  @staticmethod
  def parse(scenario, name, definition):
    metric_name = definition.get("metric", None)
    cluster_result = ClusterResult(scenario, name, metric_name, **definition)
    if cluster_result.metric.experimental:
      try:
        display_experimental = scenario.enable_experimental_metrics
      except AttributeError:
        display_experimental = False
      if display_experimental:
        log.info("Including experimental metric '%s'",
                 cluster_result.metric_name)
        return cluster_result
      else:
        log.debug("Skipping experimental metric '%s'",
                  cluster_result.metric_name)
        return None
    else:
      return cluster_result

  def get_result_pbs(self):
    """Produces the results for a given workload with the specified design.

    Depending on what the result_type is, generate the appropriate result
    from the iogen. Then, apply various items to the resulting protobuf
    result. Additional parameters may be provided as kwargs in configuration.
    """
    cluster_stats_dir = self.scenario.cluster_stats_dir()
    if cluster_stats_dir is None:
      log.debug("Cluster results not read because the output directory is not "
                "set")
      return []
    try:
      results_map = ScenarioUtil.read_cluster_stats(cluster_stats_dir)
    except (IOError, OSError):
      log.debug("Skipping reporting cluster stats to GUI since they have not "
                "been created yet")
      return []
    if not results_map:
      return []
    results_map = MetricsUtil.filter_results_map(
      results_map, self.metric_name, "Aggregated")
    series_list = []
    for node in self.scenario.cluster.nodes():
      for metric in results_map.get(str(node.node_id()), []):
        series_list.append(MetricsUtil.get_series(metric))
    return self.series_list_to_result_pbs(series_list)

  def series_list_to_result_pbs(self, series_list):
    result_pbs = []
    if self.aggregate:
      series = self._combine_results(series_list, how=self.aggregate)
      if series is None or series.empty:
        series_list = []
      else:
        series_list = [series]
    run_phase_start = pd.to_datetime(
      self.scenario.phase_start_time_secs(self.scenario.phase.RUN), unit="s")
    for result_num, series in enumerate(series_list):
      x_unit = curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
      y_unit = None
      if self.metric.rate == CurieMetric.kPerSecond:
        if self.metric.unit == CurieMetric.kOperations:
          y_unit = curie_test_pb2.CurieTestResult.Data2D.kIOPS
        elif self.metric.unit == CurieMetric.kKilobytes:
          # Convert from KB/s to B/s
          series *= 1024
          y_unit = curie_test_pb2.CurieTestResult.Data2D.kBytesPerSecond
        else:
          y_unit = curie_test_pb2.CurieTestResult.Data2D.kCount
          log.warning("Unexpected unit %s, defaulting to %s",
                      CurieMetric.Unit.Name(self.metric.unit), y_unit)
      elif self.metric.unit == CurieMetric.kPercent:
        y_unit = curie_test_pb2.CurieTestResult.Data2D.kPercent
      elif self.metric.unit == CurieMetric.kMegahertz:
        # Convert from megahertz to hertz
        series *= 1e6
        y_unit = curie_test_pb2.CurieTestResult.Data2D.kHertz
      else:
        y_unit = curie_test_pb2.CurieTestResult.Data2D.kCount
        log.warning("Unexpected rate %s, defaulting to %s",
                    CurieMetric.Rate.Name(self.metric.rate), y_unit)
      assert y_unit is not None
      test_result = FioWorkloadResult.series_to_result_pb(
        series[run_phase_start:])
      test_result.data_2d.x_unit_type = x_unit
      test_result.data_2d.y_unit_type = y_unit
      test_result.name = self.kwargs.get("title", self.name)
      if len(series_list) > 1:
        test_result.name += " (Node %d)" % result_num
      test_result.description = self.kwargs.get("description",
                                                self.metric.description)
      test_result.group = self.__class__.__name__
      test_result.result_id = NameUtil.sanitize_filename(test_result.name)
      test_result.data_2d.report_metrics.extend(self.report_metrics)
      self._add_expected_value_details(test_result)
      if self.report_group:
        test_result.data_2d.report_group = self.report_group
      test_result.result_hint = self.kwargs.get("result_hint", "")
      result_pbs.append(test_result)
    return result_pbs
