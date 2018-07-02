#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import logging
import sys
import time

import pandas as pd

from curie import curie_test_pb2
from curie.exception import CurieTestException

log = logging.getLogger(__name__)

# Convenient access.
Data2D = curie_test_pb2.CurieTestResult.Data2D


class BaseResult(object):
  """Manages information about a workloads results.

  """

  def __init__(self, scenario, name, aggregate=None, report_metrics=None,
               report_group=None, **kwargs):
    if not report_metrics:
      report_metrics = ["NoScore"]
    self.scenario = scenario
    self.name = name
    self.aggregate = aggregate
    self.report_metrics = []
    self.report_group = report_group

    ReportMetric = Data2D.ReportMetric
    try:
      self.report_metrics = [ReportMetric.Value("k" + s)
                             for s in report_metrics]
    except ValueError as exc:
      # Preserve the traceback of the original exception during the re-raise.
      raise CurieTestException, exc, sys.exc_info()[2]

    if self.aggregate:
      if self.aggregate not in ["sum", "min", "mean", "median", "max"]:
        raise CurieTestException("Unexpected 'aggregate' value '%s'" %
                                  self.aggregate)
    self.kwargs = kwargs

  def __str__(self):
    return "%s %s" % (self.__class__.__name__, self.name)

  @staticmethod
  def parse(scenario, name, definition):
    # Local imports prevent circular import.
    from curie.test.result.vm_group_result import VMGroupResult
    from curie.test.result.iogen_result import IogenResult
    from curie.test.result.cluster_result import ClusterResult
    for cls in [VMGroupResult, IogenResult, ClusterResult]:
      try:
        return cls.parse(scenario, name, definition)
      except Exception:
        log.debug("Failed to parse %s as %r", scenario, cls, exc_info=True)
        continue
    else:
      raise CurieTestException("Failed to parse '%s' in '%s'" %
                                (name, scenario.name))

  @staticmethod
  def _combine_results(series_list, how, resample_secs=None):
    """Combine multiple results Series objects into one.

    Args:
      series_list (list of pandas.Series): List of objects returned from one
        of the FioWorkloadResult helper methods, e.g. get_series.
      how (str): Name of function that corresponds to one of the members of
        pandas.Series used to aggregate the data (e.g. pandas.Series.sum,
        pandas.Series.mean).
      resample_secs (int): Optionally resample the data before combining it.
        This allows for samples that are off by a small amount to be "aligned"
        to a sampling frequency before they are combined.

    Returns:
      pandas.Series.
    """
    if len(series_list) == 0:
      log.debug("Cannot combine 0 dataframes; empty data returned")
      return None
    start_time = time.time()
    resampled_series_list = []
    total_elements = 0
    for series in series_list:
      if series.empty:
        log.debug("Encountered empty dataframe; Returning empty dataframe")
        return pd.Series(index=pd.to_datetime([], unit="ms"))
      else:
        total_elements += series.size
        if resample_secs is not None:
          resampled_series_list.append(series.resample(
            "%ds" % resample_secs, how="mean", fill_method="ffill"))
        else:
          resampled_series_list.append(series)
    iops_df = pd.concat(resampled_series_list, axis=1)
    # Get the aggregation callable from the df, e.g. 'iops_df.sum'
    aggregation_function = getattr(iops_df, how)
    series = pd.Series(aggregation_function(axis=1)).dropna(how="any")
    log.debug("Combined %d data points from %d entities into %d results using "
              "'%s' in %d ms", total_elements, len(resampled_series_list),
              series.size, how, (time.time() - start_time) * 1000)
    return series
