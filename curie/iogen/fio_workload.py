#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import cStringIO
import logging
import os
from collections import namedtuple

from curie.exception import CurieException
from curie.iogen.fio_result import FioWorkloadResult
from curie.iogen.iogen import IOGen
from curie.name_util import NameUtil
from curie.vsphere_unix_vm import VsphereUnixVm

log = logging.getLogger(__name__)


ResultResponse = namedtuple("ResultResponse", ["success", "data"])


class FioWorkload(IOGen):
  """
  Implements a workload object for FIO.

  Notes:
    1. Terse output is used.
    2. Group reporting is currently required.
  """
  FIO_DEFAULT_REPORTING_INTERVAL = 5
  # Expected defaults to be set so that results parsing works properly.
  FIO_DEFAULT_GLOBAL_CONFIGURATION_PARAMS = {
    "group_reporting": None,
    "disk_util": 0,
    "continue_on_error": "all"
  }

  def __init__(self, name, scenario, configuration,
               reporting_interval=FIO_DEFAULT_REPORTING_INTERVAL):
    super(FioWorkload, self).__init__(name, scenario, configuration,
                                      short_name="fio")
    self.reporting_interval = reporting_interval
    self.__remote_results_file = os.path.join(self._remote_results_path,
                                            "fio.out")

  def _cmd(self, randseed=None):
    """Construct the FIO command to generate the given workload.

    Returns:
      str: FIO command string.

    """
    iops_log_path = os.path.join(self._remote_results_path,
                                 NameUtil.sanitize_filename(self._name))
    lat_log_path = os.path.join(self._remote_results_path,
                                NameUtil.sanitize_filename(self._name))
    cmd = cStringIO.StringIO()
    cmd.write("/usr/bin/fio "
              "--write_iops_log=%s "
              "--write_lat_log=%s "
              "--log_avg_msec=%d "
              "--output-format=json "
              "--status-interval=%d " % (iops_log_path,
                                         lat_log_path,
                                         1000,
                                         self.reporting_interval))
    if self._workload_duration_secs:
      cmd.write("--runtime=%d " % self._workload_duration_secs)
    if randseed:
      cmd.write("--randseed=%d " % randseed)
    else:
      cmd.write("--randrepeat=0 ")
    cmd.write("--output=%s %s" % (self.__remote_results_file,
                                  self._remote_config_path))
    cmd_str = cmd.getvalue()
    log.debug("FIO command: %s", cmd_str)
    return cmd_str

  def _fetch_remote_results(self, vms, timeout=120):
    """Return a list of result objects for each VM.

    Args:
      vms: (list of curie.vm.Vm) VMs from which to get results.
      timeout: (int) Maximum number of seconds to wait for the transfer to
        complete.

    Returns:
      list of ResultResponse tuples: List order matches the input list.
    """
    success_results_tuples = []
    for vm in vms:
      local_results_path = os.path.join(self.get_local_results_path(vm),
                                        "fio.out")
      if not vm.vm_ip() or not vm.is_accessible():
        log.info("VM: %s is inaccessible", vm.vm_name())
        success = False
      else:
        try:
          success = vm.transfer_from(self.__remote_results_file,
                                     local_results_path,
                                     timeout)
        except CurieException:
          success = False
      if os.path.isfile(local_results_path):
        if not success:
          log.warning("Failed to fetch new FIO data for VM %s", vm.vm_name())
        success_results_tuples.append(
          ResultResponse(success=success,
                         data=FioWorkloadResult(local_results_path)))
      else:
        if success:
          log.warning("vm.transfer_from returned success, but %s does not "
                      "exist", local_results_path)
        else:
          log.warning("No previously-collected results available for VM %s",
                      vm.vm_name())
        success_results_tuples.append(ResultResponse(success=success,
                                                     data=None))
    return success_results_tuples

  def _set_global_defaults(self, vm):
    """Set the default workload parameters for a given VM.

    Args:
      vm: (list of curie.vm.Vm) VM for which to set parameters.
    """
    log.debug("Setting global defaults for %s on %s", self._name, vm.vm_name())
    default_params = FioWorkload.FIO_DEFAULT_GLOBAL_CONFIGURATION_PARAMS
    for key, value in default_params.iteritems():
      self._configuration.set("global", key, value)
    if type(vm) == VsphereUnixVm:
      self._configuration.set("global", "ioengine", "libaio")
