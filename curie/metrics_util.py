#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import cPickle as pickle
import re
from collections import OrderedDict

import pandas as pd

from curie.curie_metrics_pb2 import CurieMetric


class MetricsUtil(object):
  @staticmethod
  def metric_name(curie_metric):
    """Get the pretty, dot-delimited name of a CurieMetric.

    Args:
      curie_metric (curie_metrics_pb2.CurieMetric): Metric to convert to
        string.

    Returns:
      (str) Name of the CurieMetric.
    """
    # Slice the "k" from the start of various enum values.
    name = CurieMetric.Name.Name(
      curie_metric.name)[1:]
    consolidation = CurieMetric.Consolidation.Name(
      curie_metric.consolidation)[1:]
    unit = CurieMetric.Unit.Name(
      curie_metric.unit)[1:]
    # If it has a rate, append that to the unit (e.g. KilobytesPerSecond).
    if curie_metric.rate != CurieMetric.kInstantaneous:
      unit += CurieMetric.Rate.Name(curie_metric.rate)[1:]
    return ".".join([name, consolidation, unit])

  @staticmethod
  def sorted_results_map(results_map):
    """Return a new sorted results map.

    Args:
      results_map (dict): Results map to sort.

    Returns:
      (dict) Sorted results map.
    """
    sorted_results_map = OrderedDict()
    for node_id in sorted(results_map):
      if not results_map[node_id]:
        # Error message already logged in collect_cluster_performance_stats.
        continue
      # Sort primarily by metric name, and secondarily by instance.
      sorted_results_map[node_id] = sorted(results_map[node_id],
                                           key=lambda result: result.instance)
      sorted_results_map[node_id] = sorted(sorted_results_map[node_id],
                                           key=MetricsUtil.metric_name)
    return sorted_results_map

  @staticmethod
  def filter_results_map(results_map, metric_name_regex, instance_regex=".*"):
    """Return a new filtered results map.

    Args:
      results_map (dict): Results map to filter.
      metric_name_regex (str): Regex pattern to match against metric names.
      instance_regex (str): Regex pattern to match against instance names.

    Returns:
      (dict) Filtered results map.
    """
    filtered_results_map = OrderedDict()
    for node_id in results_map:
      filtered_results_map[node_id] = []
      for metric in results_map[node_id]:
        if (re.match(metric_name_regex,
                     MetricsUtil.metric_name(metric)) and
            re.match(instance_regex, str(metric.instance))):
          filtered_results_map[node_id].append(metric)
    return filtered_results_map

  @staticmethod
  def get_series(metric):
    """Convert a CurieMetric to pandas.Series.

    Args:
      metric (CurieMetric): Metric to convert.

    Returns:
      (pandas.Series)
    """
    return pd.Series(list(metric.values),
                     index=pd.to_datetime(list(metric.timestamps), unit="s"))

  @staticmethod
  def pickle_xy_data2d(x_series, y_series, data_2d):
    """
    Takes an x_series (most likely timestamps) and a y_series, pickles the
    result and adds it to the data_2d protobuf.

    Args:
      x_series: (list) List of x values.
      y_series: (list) List of y values.
      data_2d: (curie_test_pb2.CurieTestResult.Data2D.Pickled2DData)
        Protobuf in which to insert the series data.
    """
    data_2d.pickled_2d_data.x_vals_pickled = pickle.dumps(x_series)
    data_2d.pickled_2d_data.y_vals_pickled = pickle.dumps(y_series)
