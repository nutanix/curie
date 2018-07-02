#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import json
import logging
import re
import sys

import pandas

from curie import curie_test_pb2
from curie.exception import CurieTestException
from curie.metrics_util import MetricsUtil
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class FioWorkloadResult(object):
  """
  An object to represent the results acquired from an FIO Workload using
  minimal (terse) reporting.
  """

  def __init__(self, results_file):
    self.__results_file = results_file
    self._results_samples = None

  def clear_cache(self):
    self._results_samples = None

  def get_series(self, result_type):
    if result_type == "active":
      return self.get_active_series()
    elif result_type == "errors":
      return self.get_errors_series()
    elif result_type == "iops":
      return self.get_iops_series()
    elif result_type == "bandwidth":
      return self.get_bytes_per_second_series()
    else:
      raise CurieTestException("Unexpected results type %s" % result_type)

  def get_results_data(self):
    """
    Provides the results data.

    Parses raw file if necessary.

    Returns: (list) A list of parsed JSON blocks (dicts).

    """
    if not self._results_samples:
      self.__parse_results_file()
    return self._results_samples

  def get_active_series(self):
    """
    Provides a series that shows when the IO is active.

    Returns: (pandas.Series) 1 where IO is active, 0 otherwise.

    """
    return self.get_iops_series().clip(lower=0, upper=1)

  def get_errors_series(self):
    """
    Provides a total IOPS series.

    Returns: (pandas.Series) Total errors.

    """
    error_series = self.get_io_field_series(job=0, field="total_err")
    if not error_series.empty:
      error_series = error_series.resample("s", fill_method="ffill")
    return error_series

  def get_iops_series(self):
    """
    Provides an IOPS series.

    Returns: (pandas.Series) IOPS
    """
    read_ios = self.get_io_field_series(job=0, field="read",
                                        subfield="total_ios")
    write_ios = self.get_io_field_series(job=0, field="write",
                                         subfield="total_ios")
    read_iops = self.__series_rate(read_ios)
    write_iops = self.__series_rate(write_ios)
    total_iops = read_iops + write_iops
    total_iops.dropna(inplace=True, how="all")
    return total_iops

  def get_bytes_per_second_series(self):
    """
    Provides a bandwidth (bytes per second) series.

    Returns: (pandas.Series) Bytes per second
    """
    read_kb = self.get_io_field_series(job=0, field="read",
                                       subfield="io_bytes")
    write_kb = self.get_io_field_series(job=0, field="write",
                                        subfield="io_bytes")
    read_kb_per_sec = self.__series_rate(read_kb)
    write_kb_per_sec = self.__series_rate(write_kb)
    total_kb_per_sec = read_kb_per_sec + write_kb_per_sec
    total_kb_per_sec.dropna(inplace=True, how="all")
    return total_kb_per_sec * 1024

  def get_io_field_series(self, job, field, subfield=None):
    """
    Acquires a field of data from all samples for a specific job and IO
    field.

    Note: This relies upon the availability of timestamp_ms in the FIO JSON
      output which is available in FIO releases > 2.10.

    Args:
      job: (str) FIO job index (typically 0 if group_reporting is used with
        only one group)
      field: (str) "read", "write", "total_err", "trim", etc.
      subfield: (str) Sub-field name to extract: e.g. "total_ios"

    Returns: (pandas.Series) Series indexed by timestamp of the requested field.

    """
    results = self.get_results_data()
    field_vals = []
    timestamps = []
    if len(results) > 0:
      for sample in results:
        # Timestamp is a local time value.
        timestamps.append(int(sample["timestamp_ms"]))
        if subfield is None:
          field_vals.append(sample["jobs"][job][field])
        else:
          field_vals.append(sample["jobs"][job][field][subfield])
    field_series = pandas.Series(
      field_vals,
      index=pandas.to_datetime(timestamps, unit="ms"))
    # Drop any rows with non-unique indices.
    field_series = field_series.groupby(field_series.index).first()
    return field_series

  def get_iops_result_pb(self):
    """
    Produces a total IOPS timeseries result object.

    Returns: test_result (CurieTestResult proto)
    """
    return FioWorkloadResult.series_to_result_pb(self.get_iops_series())

  def get_errors_result_pb(self):
    """
    Produces a cumulative error count timeseries result object.

    Returns: test_result (CurieTestResult proto)
    """
    return FioWorkloadResult.series_to_result_pb(self.get_errors_series())

  def get_active_result_pb(self):
    """
    Produces a time series representing when a workload was performing work.

    Returns: test_result (CurieTestResult proto)
    """

    return FioWorkloadResult.series_to_result_pb(self.get_active_series())

  @staticmethod
  def series_to_result_pb(series):
    """
    Converts a Pandas series of data indexed by timestamps to a results
    protobuf.

    Args:
      series: (pandas.Series) Series to convert.

    Returns: (curie_test_pb2.CurieTestResult) test_result

    """
    test_result = curie_test_pb2.CurieTestResult()
    vals = series.values.astype(pandas.np.int64).tolist()
    log.debug("Packing values into protobuf (head: %r, tail: %r, length: %r)",
              vals[:5], vals[-5:], len(vals))
    # Convert the datetime objects to timestamps, converting ns to s. Note that
    # 10**9 is used instead of 1e9 to prevent implicit cast to float.
    timestamps = (series.index.astype(pandas.np.int64) // 10**9).tolist()
    MetricsUtil.pickle_xy_data2d(timestamps, vals, test_result.data_2d)
    return test_result

  @CurieUtil.log_duration
  def __parse_results_file(self):
    with open(self.__results_file, "r") as fp:
      results = []
      last_timestamp_ms = 0
      num_skipped_samples = 0
      min_skipped_ms = sys.maxint
      total_skipped_ms = 0
      # FIO outputs samples in blocks of JSON, where the blocks are
      # well-formed, however, the entire file is not a valid JSON block.
      # Split on curly bracket open "{" and close "\n}" since we know
      # this is how each block is enclosed.
      pattern = re.compile(r'{.*?\n}', re.DOTALL)

      for block in re.findall(pattern, fp.read()):
        try:
          sample = json.loads(block)
        except (ValueError, TypeError):
          # Couldn't parse json block, this is ok.
          log.warning("JSON block from results was not parseable.")
          continue
        else:
          timestamp_ms = sample["timestamp_ms"]
          diff_ms = timestamp_ms - last_timestamp_ms
          if diff_ms < 500:
            # TODO (ryan.hardin): Skip samples that are too close together.
            # Should be removed after FIO issue is fixed:
            # https://github.com/axboe/fio/issues/233
            num_skipped_samples += 1
            min_skipped_ms = min(min_skipped_ms, diff_ms)
            total_skipped_ms += diff_ms
          else:
            last_timestamp_ms = timestamp_ms
            results.append(sample)
      if num_skipped_samples > 0:
        log.debug("Skipped %d malformed sample(s). Min diff: %d ms, avg "
                  "skipped diff: %d ms", num_skipped_samples, min_skipped_ms,
                  total_skipped_ms / num_skipped_samples)
      self._results_samples = results

  def __series_rate(self, series):
    diffed_series = series.diff()
    sample_interval = pandas.Series(
      series.index.astype(pandas.np.int64),
      index=series.index).diff() / 1e9  # Convert ns to s.
    rate = diffed_series / sample_interval
    if not rate.empty:
      rate = rate.resample("s")
    return rate
