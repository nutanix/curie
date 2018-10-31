#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import logging
import numbers
from datetime import datetime

import pandas
import pytz
import requests

from curie.util import CurieUtil

log = logging.getLogger(__name__)


class PrometheusAPIError(Exception):
  pass


class PrometheusAdapter(object):
  """
  Interface to a Prometheus server.
  """
  DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

  def __init__(self, host="localhost", port=9090, protocol="http"):
    self.host = host
    self.port = port
    self.base_url = "{protocol}://{host}:{port}/api/v1/".format(
      host=host, port=port, protocol=protocol)

  def query_range(self, query, start, end, step=None):
    """
    Low-level interface to Prometheus' query_range API.

    Args:
      query (str): The Prometheus-flavored query to perform.
      start (int, float, or datetime): Start of the date range. If int or
        float, will be interpreted as a UTC epoch timestamp.
      end (int, float, or datetime): End of the date range. If int or float,
        will be interpreted as a UTC epoch timestamp.
      step (str): The query_range "step" parameter, which is equivalent to the
        desired sampling interval.

    Returns:
      dict: The response body's "data" field.

    Raises:
      PrometheusAPIError: If the query fails.
    """
    if step is None:
      step = "5s"
    if start is None:
      start = datetime.utcnow()
    else:
      if isinstance(start, numbers.Number):
        start = datetime.utcfromtimestamp(start)
      elif start.tzinfo:
        start = start.astimezone(tz=pytz.utc)
    if end is None:
      end = datetime.utcnow()
    else:
      if isinstance(end, numbers.Number):
        end = datetime.utcfromtimestamp(end)
      elif end.tzinfo:
        end = end.astimezone(tz=pytz.utc)
    params = {"query": query,
              "start": start.strftime(self.DATE_FORMAT),
              "end": end.strftime(self.DATE_FORMAT),
              "step": step}
    resp = requests.get(self.base_url + "query_range", params=params)
    try:
      json_data = resp.json()
    except:
      log.debug("Failed to decode JSON from query_range: %r", resp,
                exc_info=True)
      if resp.status_code != 200:
        raise PrometheusAPIError("Failed to query_range: %r" % resp)
      else:
        raise PrometheusAPIError("Received status code 200, but JSON could "
                                 "not be decoded: %r" % resp.text)
    if resp.status_code != 200:
      raise PrometheusAPIError("Failed to query_range: %r" %
                               json_data.get("error"))
    return json_data["data"]

  @CurieUtil.log_duration
  def get_disk_ops(self, vm_group, start, end,
                   step=None, agg_func=None, lookback="30s"):
    """
    Convenience for querying the disk ops per second for a VM Group.

    Args:
      vm_group (curie.vm_group.VMGroup): VM Group of interest.
      start (int, float, or datetime): Start of the date range. See
        `query_range` for more details.
      end (int, float, or datetime): End of the date range. See `query_range`
        for more details.
      step (str): The query_range "step" parameter. See `query_range` for more
        details.
      agg_func (str): Function to use while aggregating the results. See
        `_aggregate` for more details.
      lookback (str): Amount of time into the past to look for the two most
        recent data points.

    Returns:
      dict: Returned value from PrometheusAdapter.query_range.

    Raises:
      PrometheusAPIError: Passed through from PrometheusAdapter.query_range.
    """
    filter_list = ["scenario_id=\"%s\"" % vm_group._scenario.id,
                   "vm_group=\"%s\"" % vm_group.name()]
    filters = "{%s}" % ", ".join(filter_list)
    read_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_reads_completed_total",
      filters=filters, lookback=lookback)
    write_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_writes_completed_total",
      filters=filters, lookback=lookback)
    # This 'sum' aggregates across all of the disks on the worker.
    query = "sum({read_query} + {write_query}) by(instance)".format(
      read_query=read_query, write_query=write_query)
    if agg_func:
      query = _aggregate(query, agg_func, by=["vm_group"])
    return self.query_range(query, start, end, step=step)

  @CurieUtil.log_duration
  def get_disk_octets(self, vm_group, start, end,
                      step=None, agg_func=None, lookback="30s"):
    """
    Convenience for querying the disk bytes per second for a VM Group.

    Args:
      vm_group (curie.vm_group.VMGroup): VM Group of interest.
      start (int, float, or datetime): Start of the date range. See
        `query_range` for more details.
      end (int, float, or datetime): End of the date range. See `query_range`
        for more details.
      step (str): The query_range "step" parameter. See `query_range` for more
        details.
      agg_func (str): Function to use while aggregating the results. See
        `_aggregate` for more details.
      lookback (str): Amount of time into the past to look for the two most
        recent data points.

    Returns:
      dict: Returned value from PrometheusAdapter.query_range.

    Raises:
      PrometheusAPIError: Passed through from PrometheusAdapter.query_range.
    """
    filter_list = ["scenario_id=\"%s\"" % vm_group._scenario.id,
                   "vm_group=\"%s\"" % vm_group.name()]
    filters = "{%s}" % ", ".join(filter_list)
    read_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_read_bytes_total",
      filters=filters, lookback=lookback)
    write_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_written_bytes_total",
      filters=filters, lookback=lookback)
    # This 'sum' aggregates across all of the disks on the worker.
    query = "sum({read_query} + {write_query}) by(instance)".format(
      read_query=read_query, write_query=write_query)
    if agg_func:
      query = _aggregate(query, agg_func, by=["vm_group"])
    return self.query_range(query, start, end, step=step)

  @CurieUtil.log_duration
  def get_avg_disk_latency(self, vm_group, start, end,
                           step=None, agg_func=None, lookback="30s",
                           iops_floor=5):
    """
    Convenience for querying the average disk latency for a VM Group.

    The value reported is in microseconds.

    Args:
      vm_group (curie.vm_group.VMGroup): VM Group of interest.
      start (int, float, or datetime): Start of the date range. See
        `query_range` for more details.
      end (int, float, or datetime): End of the date range. See `query_range`
        for more details.
      step (str): The query_range "step" parameter. See `query_range` for more
        details.
      agg_func (str): Function to use while aggregating the results. See
        `_aggregate` for more details.
      lookback (str): Amount of time into the past to look for the two most
        recent data points.
      iops_floor (int): Latency will be reported as NaN when samples are below
        iops_floor. This filters extreme latency values when the divisor is too
        small.

    Returns:
      dict: Returned value from PrometheusAdapter.query_range.

    Raises:
      PrometheusAPIError: Passed through from PrometheusAdapter.query_range.
    """
    filter_list = ["scenario_id=\"%s\"" % vm_group._scenario.id,
                   "vm_group=\"%s\"" % vm_group.name()]
    filters = "{%s}" % ", ".join(filter_list)
    read_ops_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_reads_completed_total",
      filters=filters, lookback=lookback)
    read_time_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_read_time_seconds_total",
      filters=filters, lookback=lookback)
    write_ops_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_writes_completed_total",
      filters=filters, lookback=lookback)
    write_time_query = "irate({metric}{filters}[{lookback}])".format(
      metric="node_disk_write_time_seconds_total",
      filters=filters, lookback=lookback)
    # These 'sum' aggregate across all of the disks on the worker.
    total_time_query = "sum(" \
                       "{read_time_query} + {write_time_query}" \
                       ") by(instance)".format(
      read_time_query=read_time_query, write_time_query=write_time_query)
    total_ops_query = "sum(" \
                      "{read_ops_query} + {write_ops_query}" \
                      ") by(instance)".format(
      read_ops_query=read_ops_query, write_ops_query=write_ops_query)
    # Aggregate before dividing to ensure the average is weighted correctly.
    if agg_func:
      if agg_func not in ["avg", "mean", "sum"]:
        raise ValueError("Unsupported aggregation function %r" % agg_func)
      # Average and sum do exactly the same thing since n is the same for both
      # the numerator and denominator, so use sum only to allow use of
      # iops_floor.
      total_time_query = _aggregate(total_time_query, "sum", by=["vm_group"])
      total_ops_query = _aggregate(total_ops_query, "sum", by=["vm_group"])
    query = "({total_time_query} / ({total_ops_query} > {iops_floor})) * " \
            "1000000.0".format(
      total_time_query=total_time_query, total_ops_query=total_ops_query,
      iops_floor=iops_floor)
    return self.query_range(query, start, end, step=step)

  @CurieUtil.log_duration
  def get_generic(self, vm_group, start, end, query,
                  step=None, agg_func=None):
    """
    Convenience for querying generic metrics for a VM Group.

    The string `__curie_filter_scenario__` will be replaced by a filter
    string for the current scenario, and `__curie_filter_vm_group__`
    will be replaced by a filter string for the VM group. If both of
    these strings are not present, the query result may not be specific
    to the expected set of VMs.

    Args:
      vm_group (curie.vm_group.VMGroup): VM Group of interest.
      start (int, float, or datetime): Start of the date range. See
        `query_range` for more details.
      end (int, float, or datetime): End of the date range. See `query_range`
        for more details.
      query (str): The query string.
      step (str): The query_range "step" parameter. See `query_range` for more
        details.
      agg_func (str): Function to use while aggregating the results. See
        `_aggregate` for more details.

    Returns:
      dict: Returned value from PrometheusAdapter.query_range.

    Raises:
      PrometheusAPIError: Passed through from PrometheusAdapter.query_range.
    """
    query = query.replace("__curie_filter_scenario__",
                          "scenario_id=\"%s\"" % vm_group._scenario.id)
    query = query.replace("__curie_filter_vm_group__",
                          "vm_group=\"%s\"" % vm_group.name())
    if agg_func:
      query = _aggregate(query, agg_func, by=["vm_group"])
    return self.query_range(query, start, end, step=step)


def to_series(result, dropna=None):
  """
  Convert a result to a pandas.Series.

  Args:
    result (dict): Result from a query data object.
    dropna (bool): If true, drop N/A values from the Series.

  Returns:
    pandas.Series: Data expressed as a Series.
  """
  if dropna is None:
    dropna = False
  try:
    series_name = result["metric"].values()[0]
  except IndexError:
    series_name = None
  series = pandas.to_numeric(
    pandas.Series(dict(result["values"]), name=series_name), errors="coerce")
  if dropna:
    series.dropna(inplace=True)
  elif len(series) > 1:
    # Infer the frequency, filling gaps with NaN values.
    freq = int(min(series.index.to_series().diff().dropna()))
    new_index = pandas.np.arange(min(series.index), max(series.index) + 1,
                                 freq)
    series = series.reindex(new_index)
  return series


def to_series_iter(data, dropna=None):
  """
  Yield pandas.Series objects from data from a query.

  The series objects will be yielded in the same order as the results inside
  the query data object.

  Args:
    data (dict): Data from a query.
    dropna (bool): If true, drop rows containing N/A values.

  Yields:
    pandas.Series
  """
  for result in data["result"]:
    yield to_series(result, dropna=dropna)


def to_series_list(data, dropna=None):
  """
  Convert data from a query into a list of pandas.Series.

  The list of series objects will be in the same order as the results inside
  the query data object.

  Args:
    data (dict): Data from a query.
    dropna (bool): If true, drop rows containing N/A values.

  Returns:
    list of pandas.Series
  """
  return [series for series in to_series_iter(data, dropna=dropna)]


def target_config(targets, **labels):
  """
  Create a Prometheus target configuration.

  A target configuration describes a collection endpoint. The labels will be
  applied to data from the target (endpoint) on ingest.

  Args:
    targets (list of str): IP address and port of each target.
    **labels: Key/value pairs used as labels for data from the target.

  Returns:
    dict: Target configuration that may be written to a file for Prometheus to
      parse.
  """
  return {"targets": targets, "labels": labels}


def scenario_target_config(scenario):
  """
  Generate a Prometheus target configuration for a Curie Scenario.

  Args:
    scenario (curie.scenario.Scenario): Scenario of choice.
    worker_exporter_port (int): Port number of exporter on the worker VMs.

  Returns:
    dict: The target configuration to be passed to Prometheus.
  """
  configs = []
  for vm_group in scenario.vm_groups.values():
    for vm in vm_group.get_vms():
      ip = vm.vm_ip()
      if not ip:
        continue
      for worker_exporter_port in vm_group.exporter_ports:
        config = target_config(
          ["%s:%s" % (ip, worker_exporter_port)],
          job="xray",
          instance=vm.vm_name(),
          scenario_id=str(scenario.id),
          scenario_display_name=scenario.display_name,
          scenario_name=scenario.name,
          cluster_name=scenario.cluster.name(),
          vm_group=vm_group.name())
        configs.append(config)
  return configs


def _aggregate(query, func, by=None):
  """
  Wrap a query in an aggregation clause.

  Use this convenience function if the aggregation parameters are coming from
  user input so that they can be validated.

  Args:
    query (str): Query string to wrap.
    func (str): Aggregation function of choice. Valid choices are 'avg'/'mean',
      'min', 'max', 'sum'.
    by (list of str): Optional list of variables by which to perform the
      aggregation.

  Returns:
    str: New query string.
  """
  if func == "mean":
    func = "avg"
  if func not in ["avg", "min", "max", "sum"]:
    raise ValueError("Unsupported aggregation function %r" % func)
  query = "{func}({query})".format(func=func, query=query)
  if by:
    query += " by({by_variables})".format(by_variables=", ".join(by))
  return query
