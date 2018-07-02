#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import cStringIO
import calendar
import collections
import datetime
import itertools
import logging
import os
import pickle
import re
import time

from curie import agent_rpc_client
from curie import charon_agent_interface_pb2 as agent_ifc_pb2
from curie.exception import CurieTestException
from curie.log import CHECK, CHECK_GT, CHECK_LE, CHECK_EQ
from curie.test.steps import _util

log = logging.getLogger(__name__)

# File that can be used to stop vdbench before it would normally stop due to
# time or file limit.
# write "end_rd" to terminate the current run and continue with any remaining
# runs or "end_vdbench" to terminate the current run, skipping any remaining
# runs.
# For this to work the monitor parameter will need to be set in the vdb file.
# For example:
# monitor=/home/nutanix/vdbench_monitor
VDBENCH_MONITOR_PATH = "/home/nutanix/vdbench_monitor"

# Text to go in VDBENCH_MONITOR_PATH to stop current run.
VDBENCH_MONITOR_STOP_RUN = "end_rd"

# Text to go in VDBENCH_MONITOR_PATH to stop current run and skip any remaining
# runs.
VDBENCH_MONITOR_STOP_ALL = "end_vdbench"

# Maximum number of jobs to run simultanously per node.
MAX_JOBS_PER_NODE = 10

# A list of column names and types (for values we return) for vdbench HTML
# logfile lines that contain the actual vdbench output results. Clients calling
# VdbenchUtil.parse_vdbench_logfile can specify a subset of these names to
# obtain the corresponding subset of the results for each time interval.
VDBENCH_OUTPUT_KEY_TYPES = [("timestamp", int), # Seconds since the Epoch.
                            ("interval", int),
                            ("iops", float),
                            ("mbps", float),
                            ("num_bytes", int),
                            ("read_pct", float),
                            ("lat_msecs", float),
                            ("read_lat_msecs", float),
                            ("write_lat_msecs", float),
                            ("lat_max_msecs", float),
                            ("lat_std_msecs", float),
                            ("avg_qdepth", float),
                            ("cpu_sys_user_pct", float),
                            ("cpu_sys_pct", float)]
VDBENCH_OUTPUT_KEYS = set([_key_type[0] for _key_type
                           in VDBENCH_OUTPUT_KEY_TYPES])

# Regular expression to match (1) the day in the header line that precedes the
# logfile lines that contain the actual vdbench output results and (2)
# everything else in a vdbench HTML logfile. The key part of the header line
# matching is the day (e.g., "Sep 18, 2015") when vdbench was started.
VDBENCH_OUTPUT_FILE_RE = \
  re.compile("(\S+) (\d+), (\d+)\s+interval(.*)", re.S)

# Regular expression to match vdbench HTML logfile lines that contain the
# actual vdbench output results. The comment for each column's portion of the
# regular expression indicates the column name we use internally in this class
# and the column text emitted by vdbench in its HTML logfiles.
#
# The following is based on the output columns in vdbench 50403.
#
# Reference: the "Report file examples" section in the vdbench manual.
VDBENCH_OUTPUT_RESULT_LINE_RE = \
  re.compile("(\d+:\d+:\d+\.\d+)\s+" # timestamp ().
             "(\d+)\s+"              # interval (interval).
             "(\d+\.\d+)\s+"         # iops (i/o rate).
             "(\d+\.\d+)\s+"         # mbps (MB/sec 1024**2).
             "(\d+)\s+"              # num_bytes (bytes i/o).
             "(\d+\.\d+)\s+"         # read_pct (read pct).
             "(\d+\.\d+)\s+"         # lat_msecs (resp time).
             "(\d+\.\d+)\s+"         # read_lat_msecs (read resp).
             "(\d+\.\d+)\s+"         # write_lat_msecs (write resp).
             "(\d+\.\d+)\s+"         # lat_max_msecs (resp max).
             "(\d+\.\d+)\s+"         # lat_std_msecs (resp stddev).
             "(\d+\.\d+)\s+"         # avg_qdepth (queue depth).
             "(\d+\.\d+)\s+"         # cpu_sys_user_pct (cpu% sys+u).
             "(\d+\.\d+)$"           # cpu_sys_pct (cpu% sys).
             )

# Regular expression to match vdbench HTML logfile lines that contain IO
# error information.  The following is based on lines like:
#   12:57:57.092 localhost-3: 12:57:57.049 op: write  lun: /dev/sdd
#     lba:  15229894656 0x38BC5C000 xfer:     8192 errno: EIO: 'I/O error'
VDBENCH_OUTPUT_ERROR_LINE_RE = \
  re.compile("(\d+:\d+:\d+\.\d+)\s+"          # Error message timestamp.
             "(\S+):\s+"                      # Thread name.
             "(\d+:\d+:\d+\.\d+)\s+"          # Error timestamp.
             "op:\s+(\S+)\s+"                 # Operation type.
             "lun:\s+(\/\S+\/\S+)\s+"         # LUN name.
             "lba:\s+(\d+)\s+0x[0-9A-F]+\s+"  # LBA.
             "xfer:\s+(\d+)\s+"               # Operation size.
             "errno:\s+EIO:\s+'I/O error'\s*")

VDBENCH_ERROR_KEY_TYPES = [("timestamp", int), # Seconds since the Epoch.
                           ("thread", str),
                           ("error_timestamp", int),
                           ("op_type", str),
                           ("lun_name", str),
                           ("lba", int),
                           ("op_size", int)]

#===============================================================================

class VdbenchJob(object):
  """Class to hold Vdbench job descriptions and status."""

  def __init__(self, vm, cmd, cmd_description, timeout_secs):
    """Create a VdbenchJob

    Pass a list of these object to VdbenchUtil.execute_vdbench_jobs() which will
    run the jobs without overloading the system.

    When it is finished, it will fill in cmd_id, start_secs, complete_secs, and
      cmd_status.

    Args:
      vm: Vm to run job on.
      cmd: str command for running job.
      cmd_description: str to be used when constructing cmd_id. Cmd_id will
        be "%d_%s" % (int(time.time() *1e6), cmd_description). The description
        should have no spaces.
      timeout_secs: int number of seconds to wait for job to complete.
    """
    self.vm = vm
    self.cmd = cmd
    self.cmd_description = cmd_description
    self.timeout_secs = timeout_secs
    self.cmd_id = None
    self.start_secs = -1
    self.complete_secs = -1
    self.rpc_client = None
    self.cmd_status = None

#===============================================================================

class VdbenchUtil(object):

  @staticmethod
  def build_vdbench_prefill_cmd(test,
                                workload_path,
                                interval_secs=5):
    """
    Create and return a Vdbench prefill command.

    Args:
      test (BaseTest): Test that is orchestrating the prefill.
      workload_path (str): path to workload file.
      interval_secs (int): Number of seconds between each reporting interval.

    Returns:
      Vdbench command for given parameters.

    Note:
      Vdbench has an automatic prefill when it detects that a file is empty, but
      it acts differently depending on whether or not it is working with a file
      or a block device. This command will work for block devices. All of the
      current scenarios use block devices only. If any new ones are added that
      require interacting with the file system, this method will have to be
      modified. The for a filesystem, the workaround is to do the same workload
      with a short run time (interval_secs). The reporting interval also has
      to be adjusted so that elapsed time (runtime) >= 2 * reporting interval.
    """
    output_dir = VdbenchUtil.vdbench_prefill_output_directory()
    cmd_buf = cStringIO.StringIO()
    cmd_buf.write("/home/nutanix/vdbench/vdbench ")
    cmd_buf.write("-f %s " % workload_path)
    cmd_buf.write("-o %s " % output_dir)
    cmd_buf.write("-i %d " % interval_secs)
    return cmd_buf.getvalue().rstrip()

  @staticmethod
  def build_vdbench_cmd(test,
                        workload_path,
                        output_dir=None,
                        elapsed_secs=None,
                        interval_secs=None,
                        validate_data=False):
    """
    Return the full command line to run vdbench using the workload parameter
    file with path 'workload_path' and have test output go to 'output_dir'. If
    'elapsed_secs' or 'interval_secs' are specified, they override the elapsed
    and interval values, respectively in the workload parameter file. If
    'validate_data' is specified, data validation is activated.
    """
    if output_dir is None:
      output_dir = VdbenchUtil.vdbench_test_output_directory()
    cmd_buf = cStringIO.StringIO()
    cmd_buf.write("/home/nutanix/vdbench/vdbench ")
    # Reference: the "Execution parameter overview" section in the vdbench
    # manual.
    cmd_buf.write("-f %s " % workload_path)
    cmd_buf.write("-o %s " % output_dir)
    if elapsed_secs is not None:
      cmd_buf.write("-e %d " % elapsed_secs)
    if interval_secs is not None:
      cmd_buf.write("-i %d " % interval_secs)
    if validate_data:
      cmd_buf.write("-v ")
    return cmd_buf.getvalue().rstrip()

  @staticmethod
  def vdbench_stop_all_async(vm, monitor_path=VDBENCH_MONITOR_PATH,
                             user="nutanix"):
    """Initiate stop vdbench using its monitor feature.

    The stop all is initiated by writing "vdbench_end" to the monitor file.
    When vdbench reads the file and comp lets it reporting period, it will shut
    down. It is up to the caller to wait for the vdbench process to complete.

    For this to work the currently running vdbench process must have
    monitor='monitor_path' in its configuration file.

    Args:
      vm: A instance of CurieVM.
      monitor_path: A str which is the path to the monitor file on 'vm'.
      user: A str which is the user which will be used to execute the command
        on 'vm'.
    Returns:
      str: command_id

    Raises:
      Does not catch exceptions raised by 'vm.execute_async()'.
    """
    command_id = "vdbench_stop_all_%d" % int(time.time() * 1e6)
    command = r"echo %s \> %s" % (VDBENCH_MONITOR_STOP_ALL, monitor_path)
    vm.execute_async(command_id, command, user=user)
    return command_id

  @staticmethod
  def parse_vdbench_logfile(logfile_path, keys=["timestamp", "iops"]):
    """
    Parse the vdbench HTML logfile 'logfile_path' and return a list of
    dictionaries, one per time interval from the vdbench output. Each
    dictionary will contain key/val pairs for the specified keys which must be
    values from VDBENCH_OUTPUT_KEYS.

    Assumption: the number of time intervals in the vdbench HTML logfile isn't
    so large that we can't comfortably return the parsed result in memory.

    Args:
      logfile_path: Path to a vdbench logfile.html file.
      keys: List of keys from VDBENCH_OUTPUT_KEYS that should be returned in
        'datapoints'.

    Returns: (datapoints, ioerrors)
      datapoints: list of dictionaries, one per time interval from the
        vdbench output.
      ioerrors: list of dictionaries, one per ioerror encountered in the
        vdbench output.
    """
    results = []
    errors = []
    CHECK_GT(len(keys), 0)
    for key in keys:
      CHECK(key in VDBENCH_OUTPUT_KEYS, msg=key)
    start_date = None
    last_datetime = None
    # Match the vdbench start date in the header line.
    logfile_data = open(logfile_path).read()
    output_file_match = VDBENCH_OUTPUT_FILE_RE.search(logfile_data)
    if not output_file_match:
      log.warning("No header line in logfile %s", logfile_path)
      return results, errors
    start_year = int(output_file_match.group(3))
    month_str = output_file_match.group(1)
    start_month = datetime.datetime.strptime(month_str, "%b").month
    start_day = int(output_file_match.group(2))
    start_date = datetime.datetime(year=start_year,
                                   month=start_month,
                                   day=start_day)


    def get_updated_datetime_timestamp(timestamp_str, start_date,
                                       last_datetime):
      """Parse a timestamp string such as "14:57:34.149" and produce a
      complete datetime.

      Args:
        timestamp_str: a timestamp in the form of "14:57:34.149".
        start_date (datetime): the initial date encountered by the parser.
        last_datetime (datetime): the last complete datetime parsed.

      Returns: (result_timegm, last_datetime)
        result_time (calendar.timegm): the parsed timestamp.
        last_datetime (datetime): the datetime object.
      """
      # We only want the HH:MM::SS part of this.
      hh_mm_ss, _ = timestamp_str.split(".")
      hour, minute, second = [int(field) for field
                              in hh_mm_ss.split(":")]
      if last_datetime is None:
        result_datetime = datetime.datetime(
          start_date.year, start_date.month, start_date.day,
          hour, minute, second)
      else:
        result_datetime = datetime.datetime(
          last_datetime.year, last_datetime.month, last_datetime.day,
          hour, minute, second)
        # Check if we crossed midnight. We assume that there is never a
        # time gap between output result lines that is > 24 hours.
        if last_datetime.hour == 23 and hour == 0:
          result_datetime += datetime.timedelta(days=1)
      last_datetime = result_datetime
      # The timestamp value is returned in seconds since the Epoch.
      last_datetime_str = "%d-%d-%d %d:%d:%d" % (last_datetime.year,
                                                 last_datetime.month,
                                                 last_datetime.day,
                                                 last_datetime.hour,
                                                 last_datetime.minute,
                                                 last_datetime.second)
      result_timegm = calendar.timegm(time.strptime(last_datetime_str,
                                                    "%Y-%m-%d %H:%M:%S"))
      return result_timegm, last_datetime

    # Match the output result and error lines.
    for line in output_file_match.group(4).split("\n"):
      # Match on the actual results lines.
      result_line_match = VDBENCH_OUTPUT_RESULT_LINE_RE.match(line)
      if result_line_match:
        result = {}
        for xx, (key, val_type) in enumerate(VDBENCH_OUTPUT_KEY_TYPES):
          if key not in keys:
            # Caller not interested in this particular key.
            continue
          val = result_line_match.group(xx + 1)
          if key == "timestamp":
            result_timegm, last_datetime = get_updated_datetime_timestamp(
              val, start_date, last_datetime)
            result[key] = val_type(result_timegm)
          else:
            result[key] = val_type(val)
        results.append(result)
        continue
      # Match on lines containing IO errors.
      error_line_match = VDBENCH_OUTPUT_ERROR_LINE_RE.match(line)
      if error_line_match:
        error = {}
        for xx, (key, val_type) in enumerate(VDBENCH_ERROR_KEY_TYPES):
          val = error_line_match.group(xx + 1)
          if key == "timestamp" or key == "error_timestamp":
            result_timegm, last_datetime = get_updated_datetime_timestamp(
              val, start_date, last_datetime)
            error[key] = val_type(result_timegm)
          else:
            error[key] = val_type(val)
        errors.append(error)
        continue
    return results, errors

  @staticmethod
  def pickle_xy_data2d(x_series, y_series, data_2d):
    """
    Takes an x_series (most likely timestamps) and a y_series, pickles the
    result and adds it to the data_2d protobuf.

    Args:
      x_series: list of values (most likely time)
      y_series: list of values
      data_2d: data_2d protobuf
    """
    data_2d.pickled_2d_data.x_vals_pickled = pickle.dumps(x_series)
    data_2d.pickled_2d_data.y_vals_pickled = pickle.dumps(y_series)

  @staticmethod
  def encode_vdbench_results_data(results, data_2d):
    """
    Encode the data for the vdbench results 'results' in 'data_2d'.

    Args:
      results (dict): input parsed vdbench logfile output returned from
      VdbenchUtil.parse_vdbench_logfile.
      data_2d (CurieTestResult.Data2D): output protobuf to write the encoded
      vdbench data into.
    """
    timestamp_list = [int(result["timestamp"]) for result in results]
    iops_list = [int(result["iops"]) for result in results]
    VdbenchUtil.pickle_xy_data2d(timestamp_list, iops_list, data_2d)

  @staticmethod
  def encode_vdbench_active_results_data(results, data_2d):
    """
    Encode the data for the vdbench results 'results' in 'data_2d'. This
    encoding process translates the IOPS into "active" (1) or "inactive" (0)
    to illustrate that at least some IO is occurring during a specific time
    period.

    Args:
      results (dict): input parsed vdbench logfile output returned from
        VdbenchUtil.parse_vdbench_logfile.
      data_2d (CurieTestResult.Data2D): output protobuf to write the encoded
        and translated vdbench data into.
    """
    timestamp_list = []
    is_active_list = []
    for result in results:
      timestamp_list.append(int(result["timestamp"]))
      is_active = 1 if int(result["iops"]) > 0 else 0
      is_active_list.append(is_active)
    VdbenchUtil.pickle_xy_data2d(timestamp_list, is_active_list, data_2d)

  @staticmethod
  def encode_vdbench_incremental_error_data(ioerrors, data_2d, start_timestamp,
                                            end_timestamp, resample_secs=10):
    """
    Encode the 'ioerrors' observed in the vdbench results as an increasing count
    over time in 'data_2d'.
    Args:
      ioerrors (list of dicts): input ioerrors parsed from vdbench logfile
        returned from VdbenchUtil.parse_vdbench_logfile.
      data_2d: (CurieTestResult.Data2D): output protobuf to write the
        encoded data into.
      start_timestamp (int): the initial reference timestamp to start the series
      resample_secs (int): the interval at which to resample the sequence
    """
    # Prime the timestamp and ioerror lists with the start timestamp and 0 so
    # there is at least one value in the series.
    timestamp_list = [start_timestamp]
    total_ioerrors_list = [0]
    total_ioerrors = 0
    last_timestamp = start_timestamp
    for ioerror in ioerrors:
      # Fill in the sequence at the resample_secs interval
      while (last_timestamp + resample_secs) < ioerror["error_timestamp"]:
        last_timestamp += resample_secs
        timestamp_list.append(last_timestamp)
        total_ioerrors_list.append(total_ioerrors)
      last_timestamp = ioerror["error_timestamp"]
      total_ioerrors += 1
      timestamp_list.append(last_timestamp)
      total_ioerrors_list.append(total_ioerrors)
    while last_timestamp <= end_timestamp:
      timestamp_list.append(last_timestamp)
      total_ioerrors_list.append(total_ioerrors)
      last_timestamp += resample_secs
    VdbenchUtil.pickle_xy_data2d(timestamp_list, total_ioerrors_list, data_2d)

  @staticmethod
  def write_prefill_parameters(input_fp, output_fp, threads=20):
    """Generate prefill parameters for a vdbench configuration.

    Given a vdbench parameter file-like object 'input_fp', write a new vdbench
    parameter file-like object 'output_fp' that would prefill the workload
    described by the input.

    Args:
      input_fp: File-like object that describes the workload.
      output_fp: File-like object to which the prefill parameters are written.
      threads (int): Number of threads (I/O depth) to use for each disk.
    Returns:
      A ParameterFile instance representing the prefill parameters.
    """
    input_parameter_file = VdbenchParameterFile.load(input_fp)
    output_parameter_file = VdbenchParameterFile()
    # Transfer any general parameters, like compratio, etc.
    output_parameter_file["general"] = input_parameter_file["general"]
    # Iterate over storage definitions,and create prefill definitions for each.
    wd_pattern = "wd%s"
    for idx, sd in enumerate(input_parameter_file['sd'].itervalues()):
      # Add the sd.
      output_parameter_file.append([("sd", sd["sd"]),
                                    ("lun", sd["lun"]),
                                    ("size", sd.get("size", None)),
                                    ("range", sd.get("range", None)),
                                    ("openflags", "o_direct"),
                                    ("threads", threads)])
      # Add the wd.
      wd_name = wd_pattern % idx
      output_parameter_file.append([("wd", wd_name),
                                    ("sd", sd["sd"]),
                                    ("xfersize", "1m"),
                                    ("rdpct", 0),
                                    ("seekpct", "eof")])
    # Add the rd.
    output_parameter_file.append([("rd", "rd0"),
                                  ("wd", wd_pattern % "*"),
                                  ("interval", 5),  # Seconds.
                                  ("elapsed", 24 * 60 * 60),  # Secs in a day.
                                  ("iorate", "max")])
    output_fp.write(str(output_parameter_file))
    return output_parameter_file

  @staticmethod
  def create_prefill_parameter_file(input_path, output_path):
    """Generate vdbench prefill parameters and write them to a file.

    Given a path to a vdbench parameter file, 'input_path', write a new vdbench
    parameter file to a new file located at 'output_path' that would prefill
    the workload described by the input. If a file at 'output_path' exists, it
    is overwritten.

    Args:
      input_path: Path to the vdbench file that describes the workload.
      output_path: Path to the file to which the prefill parameters are
        written.

    Returns:
      A ParameterFile instance representing the prefill parameters.
    """
    with open(input_path, "r") as input_fp:
      with open(output_path, "w") as output_fp:
        return VdbenchUtil.write_prefill_parameters(input_fp, output_fp)

  @staticmethod
  def execute_vdbench_jobs(vdbench_jobs,
                           max_jobs_per_node=MAX_JOBS_PER_NODE):
    """Execute at most 'max_jobs_per_node' per node simultaneously.

    For tests with large number of vdisks that need to be filled, attempting to
    fill hundreds of vdisks simultanously can push a system past its ability to
    handle concurrent work. This method will run at most
    'max_jobs_per_node' on each node and will attempt to keep up to
    that limit running at all times.

    The jobs will be started in the order that they are given for each node. As
    the jobs complete, their status will be set.

    Args:
      vdbench_jobs: List of tuples (vm, command, timeout_secs), where command
        is the vdbench command suitable for passing to CurieVM.execute_async()
      max_jobs_per_node: int, number of jobs per node. The default is
        based on the assumption that each VM has one or two vdisks to be filled.
        Lower this limit when using VMs with larger number of vdisks.

    Returns:
      List of VdbenchJob in the order they were detected as completed.

    Raises:
      CurieTestException if there are any errors running a job or if the
      run time of a job exceed es its timeout.
    """
    node_id_jobs_map = {}
    running_jobs = set()
    waiting_jobs = set()
    completed_jobs = []
    for job in vdbench_jobs:
      node_id = job.vm.node_id()
      if node_id not in node_id_jobs_map:
        node_id_jobs_map[node_id] = collections.deque()
      node_id_jobs_map[node_id].append(job)
      waiting_jobs.add(job)
    node_id_started_map = dict((node_id, set())
                               for node_id in node_id_jobs_map.keys())
    while len(running_jobs) > 0 or len(waiting_jobs) > 0:
      # Figure out how many jobs need to and can be started for each node.
      node_id_jobs_to_start_map = {}
      for node_id, pending_jobs in node_id_jobs_map.iteritems():
        node_id_jobs_to_start_map[node_id] = []
        num_started = len(node_id_started_map[node_id])
        while ((num_started + len(node_id_jobs_to_start_map[node_id])) <
               max_jobs_per_node and len(pending_jobs) > 0):
          node_id_jobs_to_start_map[node_id].append(pending_jobs.popleft())
        CHECK_LE(len(node_id_jobs_to_start_map[node_id]), max_jobs_per_node)
        if len(node_id_jobs_to_start_map[node_id]) > 0:
          log.info("Need to start %d jobs on node: %s",
                   len(node_id_jobs_to_start_map[node_id]), node_id)
      # Start jobs one node at a time to keep the load balanced.
      for jobs in itertools.izip_longest(*node_id_jobs_to_start_map.values()):
        for job in jobs:
          if job is None:
            continue
          job.cmd_id = "%d_%s" % (int(time.time() * 1e6), job.cmd_description)
          log.info("Starting job on node: %s VM: %s; command id: %s",
                   job.vm.node_id(), job.vm.vm_name(), job.cmd_id)
          job.vm.execute_async(job.cmd_id, job.cmd, user="root")
          job.rpc_client = agent_rpc_client.AgentRpcClient(
            job.vm.vm_ip())
          waiting_jobs.remove(job)
          job.start_secs = int(time.time())
          running_jobs.add(job)
          node_id_started_map[job.vm.node_id()].add(job)
          CHECK_LE(len(node_id_started_map[job.vm.node_id()]),
                   max_jobs_per_node)
      # See if any jobs have completed.
      jobs_to_cleanup = set()
      for job in running_jobs:
        ret, err = job.rpc_client.fetch_cmd_status(job.cmd_id)
        if err:
          message = ("Unable to get status from VM: %s running %s; "
                     "cmd_id %s; err: %s" %
                     (job.vm.vm_name(), job.cmd, job.cmd_id, err))
          log.error(message)
          raise CurieTestException(message)
        job.cmd_status = ret.cmd_status
        now_secs = int(time.time())
        if job.cmd_status.state == agent_ifc_pb2.CmdStatus.kRunning:
          if now_secs - job.start_secs > job.timeout_secs:
            raise CurieTestException(
              "Failed to complete job on %s in %ds" % (
                job.vm.vm_name(), job.timeout_secs))
        elif job.cmd_status.state == agent_ifc_pb2.CmdStatus.kSucceeded:
          log.info("Completed job on node: %s VM: %s command id: %s",
                   job.vm.node_id(), job.vm.vm_name(), job.cmd_id)
          job.complete_secs = int(time.time())
          completed_jobs.append(job)
          node_id_started_map[job.vm.node_id()].remove(job)
          jobs_to_cleanup.add(job)
        else:
          message = ("Job failed on node: %s VM: %s command id: %s "
                     "cmd_status: %s " %
                     (job.vm.node_id(), job.vm.vm_name(), job.cmd_id,
                      job.cmd_status))
          log.error(message)
          raise CurieTestException(message)
      # Remove completed jobs from started jobs.
      running_jobs -= jobs_to_cleanup
      time.sleep(5)
    CHECK_EQ(0, len(waiting_jobs))
    CHECK_EQ(len(completed_jobs), len(vdbench_jobs))
    return completed_jobs

  @staticmethod
  def vdbench_prefill_output_directory():
    return os.path.join(_util.remote_output_directory(),
                        "vdbench_prefill")

  @staticmethod
  def vdbench_test_output_directory():
    return os.path.join(_util.remote_output_directory(),
                        "vdbench_test")

  @staticmethod
  def vdbench_warmup_output_directory():
    return os.path.join(_util.remote_output_directory(),
                        "vdbench_warmup")

  @staticmethod
  def vdbench_test_output_path():
    return os.path.join(VdbenchUtil.vdbench_test_output_directory(),
                        "logfile.html")


class VdbenchParameter(collections.OrderedDict):
  """OrderedDict-like object representing one line in a vdbench parameter file.

  This subclass of OrderedDict can be used to convert vdbench parameter strings
  into Python objects, and vice-versa. A common interface into this class is
  the loads() static method:

  >> p = vdbench_util.VdbenchParameter.loads("wd=wd1,sd=sd1,xfersize=32768\n")
  >> p
  VdbenchParameter([('wd', 'wd1'), ('sd', 'sd1'), ('xfersize', '32768')])
  >> p["xfersize"] = 16384
  >> p
  VdbenchParameter([('wd', 'wd1'), ('sd', 'sd1'), ('xfersize', '16384')])
  >> str(p)
  'wd=wd1,sd=sd1,xfersize=16384'
  """
  def __str__(self):
    # e.g. [('wd', 'wd1'), ('sd', 'sd1'), ('xfersize', '32768'), ('rdpct',
    # '0')]
    tuples = [(str(key).strip(), str(val).strip())
              for key, val in self.iteritems() if val is not None]
    # e.g. ['wd=wd1', 'sd=sd1', 'xfersize=32768', 'rdpct=0']
    key_vals = ["=".join(tup) for tup in tuples]
    # e.g. 'wd=wd1,sd=sd1,xfersize=32768,rdpct=0'
    return ",".join(key_vals)

  @property
  def type(self):
    """Return a string describing the type of the VdbenchParameter.

    Possible return values are:
    "general"
    "hd": Host definition
    "rg": Replay group
    "sd": Storage definition
    "fsd": File system definition
    "wd": Workload definition
    "fwd": File system workload definition
    "rd": Run definition

    The type is determined by the first piece of the first item. If the value
    is not in the list of expected types (VdbenchParameterFile.line_order), the
    type is returned as "general".

    For example, if the parameter is "wd=wd1,sd=sd1,xfersize=32768", the first
    piece of the first item is "wd". Because "wd" is in
    VdbenchParameterFile.line_order, the type returned is "wd". If the
    parameter is "compratio=20", the first piece of the first item is
    "compratio". Because "compratio" is not in VdbenchParameterFile.line_order,
    the type returned is "general".

    Returns:
      str describing the type of the VdbenchParameter.
    """
    parameter_type = self.items()[0][0]
    if parameter_type in VdbenchParameterFile.line_order:
      return parameter_type
    else:
      return "general"

  @property
  def name(self):
    """Return a string representing the name of the VdbenchParameter.

    General VdbenchParameter are named after the first piece of the first item.
    For example, given a parameter "compratio=20",
    VdbenchParameter([("compratio", 20)]).name() returns "compratio".

    Non-general VdbenchParameter are named after the second piece of the first
    item. For example, given a parameter "wd=wd1,sd=sd1",
    VdbenchParameter([("wd", "wd1"), ("sd", "sd1")]).name() returns "wd1".

    Returns:
      str representation of the of the VdbenchParameter.
    """
    if self.type == "general":
      return self.items()[0][0]
    else:
      return self.items()[0][1]

  def format(self, *args, **kwargs):
    """Return a new VdbenchParameter with applied string formatting.

    The input parameters to this method are passed through to str.format.

    Returns:
      A new VdbenchParameter.
    """
    return VdbenchParameter.loads(str(self).format(*args, **kwargs))

  @staticmethod
  def loads(string):
    """Convert a string in vdbench format to a VdbenchParameter instance.

    Args:
      string: str in vdbench format, e.g. "wd=wd1,sd=sd1,xfersize=32768\n".

    Returns:
      VdbenchParameter.
    """
    # Split the input string by commas, as long as they are not wrapped in
    # parentheses. e.g. convert "sd=sd1,xfersize=(32768,10,8192,90)" to
    # ['sd=sd1', 'xfersize=(32768,10,8192,90)']
    r = re.compile(r'(?:[^,(]|\([^)]*\))+')
    subparameters = r.findall(string)
    # Convert the subparameters list into a list of lists by the = character,
    # e.g. [['sd', 'sd1'], ['xfersize', '(32768,10,8192,90)']]
    key_vals = [key_val.split("=") for key_val in subparameters]
    return VdbenchParameter(key_vals)


class VdbenchParameterFile(collections.OrderedDict):
  """OrderedDict-like object representing a vdbench parameter file.

  This subclass of OrderedDict can be used to convert vdbench parameter files
  into Python objects, and vice-versa. A common interface into this class is
  the load() and loads() static methods:

  >> with open("vdi.vdb", "r") as input_fp:
  ..   parameter_file = vdbench_util.VdbenchParameterFile.load(input_fp)

  >> str(parameter_file)

  'hd=default,vdbench=/home/nutanix/vdbench,shell=ssh,user=root\n
   sd=sd1,lun=/dev/sdb,range=(0,8g),openflags=o_direct\n
   sd=sd2,lun=/dev/sdb,range=(8g,10g),openffersize=(32768,10,8192,90),
   rdpct=100,iorate=10,priority=1,seekpct=80\n
   wd=wd2,sd=sd2,xfersize=32768,rdpct=0,seekpct=20,iorate=10,priority=2\n
   rd=run1,wd=*,elapsed=10,iorate=max\n'

  >> parameter_file["hd"]["default"]
  VdbenchParameter([('hd', 'default'),
                    ('vdbench', '/home/nutanix/vdbench'),
                    ('shell', 'ssh'),
                    ('user', 'root\n')])

  >> parameter_file["hd"]["default"]["shell"] = "csh"

  >> parameter_file["hd"]["default"]
  VdbenchParameter([('hd', 'default'),
                    ('vdbench', '/home/nutanix/vdbench'),
                    ('shell', 'csh'),
                    ('user', 'root\n')])

  >> str(parameter_file["hd"]["default"])
  'hd=default,vdbench=/home/nutanix/vdbench,shell=csh,user=root'

  This class has been written to match the spec here:
  http://download.oracle.com/otn/utilities_drivers/vdbench/vdbench-50403.pdf

  Attributes:
    line_order: Expected order of vdbench parameters, according to the spec.
  """
  line_order = ["general",
                "hd",  # Host definition.
                "rg",  # Replay group.
                "sd",  # Storage definition.
                "fsd",  # File system definition.
                "wd",  # Workload definition.
                "fwd",  # File system workload definition.
                "rd",  # Run definition.
                ]

  def __init__(self, newline="\n", *args, **kwargs):
    super(VdbenchParameterFile, self).__init__(*args, **kwargs)
    self.newline = newline
    # Initialize the parameters containers.
    for parameter_type in VdbenchParameterFile.line_order:
      self[parameter_type] = collections.OrderedDict()

  def __str__(self):
    buf = ""
    for parameter in self.__iter_parameters():
      buf += str(parameter) + self.newline
    return buf

  def __iter_parameters(self):
    """Iterator to yield VdbenchParameters in the correct order.

    Yields:
      VdbenchParameters in the correct order.
    """
    for parameter_type in VdbenchParameterFile.line_order:
      for parameter in self[parameter_type].itervalues():
        yield parameter

  def append(self, parameter):
    """Add a new parameter to this VdbenchParameterFile.

    Because VdbenchParameterFile is OrderedDict-like, the insertion order of
    the parameters is preserved (hence "append").

    Args:
      parameter: str, unicode, or VdbenchParameter to append.

    Returns:
      str matching the vdbench parameter file format.
    """
    # Cast, if necessary.
    if isinstance(parameter, basestring):
      parameter = VdbenchParameter.loads(parameter)
    elif not isinstance(parameter, VdbenchParameter):
      parameter = VdbenchParameter(parameter)
    # Append it.
    self[parameter.type][parameter.name] = parameter
    # Also make general parameters available in the root.
    if parameter.type == "general":
      self[parameter.name] = parameter

  def format(self, *args, **kwargs):
    """Return a new VdbenchParameterFile with applied string formatting.

    Return a new VdbenchParameterFile whose parameters are formatted using the
    provided arguments. The input arguments to this method are passed through
    to str.format.

    Returns:
      A new VdbenchParameterFile.
    """
    parameter_file = VdbenchParameterFile()
    for parameter in self.__iter_parameters():
      parameter_file.append(parameter.format(*args, **kwargs))
    return parameter_file

  @staticmethod
  def load(fp):
    """Convert a file-like object containing parameters to an object.

    Args:
      fp: File-like object from which the vdbench data are read.

    Returns:
      VdbenchParameterFile.
    """
    # Consume one line at a time.
    parameter_file = VdbenchParameterFile()
    for line in fp:
      # Skip comments and blank lines.
      try:
        if line[0] in ("/", "#", "*", " ", "\n", "\r"):  # Comment or empty.
          continue
      except IndexError:  # Line is not terminated by a newline.
        continue
      # The line is not a comment and it's not blank, so assume it's a
      # parameter.
      parameter = VdbenchParameter.loads(line)
      parameter_file.append(parameter)
    return parameter_file

  @staticmethod
  def loads(string):
    """Convert a string containing parameters to an object.

    Args:
      string: str or unicode from which the vdbench data are read.

    Returns:
      VdbenchParameterFile.
    """
    # Wrap the string with StringIO so we can use the existing load() function.
    fp = cStringIO.StringIO(string)
    return VdbenchParameterFile.load(fp)
