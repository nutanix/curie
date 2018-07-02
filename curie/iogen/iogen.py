#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import logging
import multiprocessing
import os
import random
import time
from collections import namedtuple
from multiprocessing.pool import ThreadPool

from curie.charon_agent_interface_pb2 import CmdStatus
from curie.exception import CurieException, ScenarioStoppedError
from curie.exception import CurieTestException
from curie.name_util import NameUtil
from curie.test.scenario_util import ScenarioUtil
from curie.test.steps import _util

log = logging.getLogger(__name__)

ResultResponse = namedtuple("ResultResponse",
                            ["success", "vm", "failure_reason"])


class IOGen(object):
  """
  IOGen: Provides a basis for workload generator objects

  An IOGen object is responsible for managing a workload generator:
   1. Configuration: setting and storing
   2. Results fetching
   3. Running

  Note: Actual workload generators must implement certain functions to work
  properly, including:
   1. _cmd()
   2. _fetch_remote_results()
   3. _set_global_defaults()
  """

  def __init__(self, name, scenario, configuration, short_name):
    """
    Create an IOGen object.

    Args:
      name: (str) Name of the iogen instance, used throughout. i.e. "oltp_run"
      scenario: (Scenario) Used to acquire scenario information.
      configuration: (configuration object) Depends on the workload
        generator, but is the configuration to be used.
      short_name: (str) Used as a name for the base directory name of
        the iogen-produced files as well as an extension  to the iogen
        configuration input files.
    """
    # The name of the workload generator instance.
    self._name = name

    # The scenario object this instance is related to.
    self._scenario = scenario

    # Name of the workload generator type. (e.g. fio)
    self._short_name = short_name

    # Configuration object for workload generator.
    self._configuration = configuration

    self._workload_duration_secs = None
    self._expected_workload_finish_secs = None
    self.__workload_start_secs = None
    self.__workload_end_secs = None
    self.__prepared_vms = None

    # Base path on the remote virtual machine. (e.g. /home/nutanix/output/fio)
    self._remote_path_root = os.path.join(
      _util.remote_output_directory(), self._short_name)

    # Path to output path on remote virtual machine.
    # (e.g. /home/nutanix/output/fio/dss_prefill)
    self._remote_results_path = os.path.join(
      self._remote_path_root, NameUtil.sanitize_filename(self._name))

    # Path to configuration on the remote VM.
    self._remote_config_path = "%s.%s" % (self._remote_results_path,
                                          self._short_name)

    self._local_path_root = os.path.join(
      self._scenario.test_dir(), self._short_name)

  def prepare(self, vms):
    """
    Sets the configuration and saves locally and on each VM in 'vms'.

    Args:
      vms (list of curie.vm.Vm): List of VMs to prepare.

    Raises:
      CurieTestException
    """
    local_config_paths = []
    for vm in vms:
      local_config_path = "%s.%s" % (self.get_local_results_path(vm),
                                     self._short_name)
      self._set_global_defaults(vm)
      self._configuration.save(local_config_path)
      local_config_paths.append(local_config_path)
    # Prepare the VMs.
    self.__prepare_vms(vms, local_config_paths)
    self.__prepared_vms = set(vms)

  def get_cmd_id(self):
    """
    Get the command ID for this workload generator.

    Returns: (str) command id
    """
    return "%s_%s_%s" % (self._scenario.test_id(),
                         self.__class__.__name__,
                         self._name)

  def get_local_results_path(self, vm):
    """
    Get the local results path for this workload generator and VM.

    Args:
      vm: (curie.vm.Vm) VM associated with this results path.

    Returns: (str) command id
    """
    return os.path.join(self._local_path_root,
                        vm.vm_name(),
                        NameUtil.sanitize_filename(self._name))

  def get_workload_start_time(self):
    """
    Returns: (int) Start time of the workload generator

    """
    return self.__workload_start_secs

  def get_workload_end_time(self):
    """
    Returns: (int) End time of the workload generator

    """
    return self.__workload_end_secs

  def start(self, vms, runtime_secs=None, stagger_secs=None, seed=None):
    """
    Starts the workload generator on the VM for a given 'runtime_secs' if
    specified.

    Args:
      vms: (list of curie.vm.Vm) VMs to start workload on.
      runtime_secs: (int) how long to run the workload.
      stagger_secs: (int) Maximum amount of time in which to evenly space the
        start of the workloads.
      seed: (str) seed value to be used by the workload generator.

    Raises:
      Will raise CurieException from execute_async()
    """
    self.__workload_start_secs = int(time.time())
    self._workload_duration_secs = runtime_secs
    if stagger_secs is None:
      stagger_secs = 0
    if runtime_secs is not None:
      self._expected_workload_finish_secs = (self.__workload_start_secs +
                                             self._workload_duration_secs +
                                             stagger_secs)
    next_deadline = self.__workload_start_secs
    last_deadline = self.__workload_start_secs + stagger_secs
    if len(vms) < 2:
      interval_secs = 0
    else:
      interval_secs = stagger_secs / (len(vms) - 1)
    log.info("Starting '%s' on a total of %d VM(s)", self._name, len(vms))
    if self._expected_workload_finish_secs is not None:
      log.debug("Expecting '%s' to finish at epoch %d",
                self._name, self._expected_workload_finish_secs)
    else:
      log.debug("Expecting '%s' to continue running until it is stopped",
                self._name)
    if interval_secs > 0:
      log.info("Workload start times will be offset by %d seconds",
               interval_secs)
    for vm_index, vm in enumerate(vms):
      if self._scenario.should_stop():
        log.info("Test marked to stop; Not all workloads will be started")
        return
      log.info("Starting workload on %s with command_id %s",
               vm.vm_name(), self.get_cmd_id())
      if seed is None:
        seed = self._name
      seed_hash = hash("%s_%d" % (seed, vm_index))
      vm.execute_async(self.get_cmd_id(),
                       self._cmd(randseed=seed_hash),
                       user="root")
      next_deadline += interval_secs
      if interval_secs > 0 and next_deadline <= last_deadline:
        log.info("Waiting %d seconds before starting the next workload",
                 next_deadline - time.time())
        ScenarioUtil.wait_for_deadline(self._scenario, next_deadline,
                                       "waiting to start next workload")

  def stop(self, vms):
    """
    Stops the workload generator on the VM.

    Args:
      vms: (list of curie.vm.Vm) VMs to stop workload on.

    Returns:
      (list of booleans) True if stop succeeded for each VM.
    """
    results = []
    for vm in vms:
      if not vm.is_accessible():
        msg = ("Cannot stop workload generator on %s: VM is inaccessible" %
               vm.vm_name())
        log.warning(msg)
        results.append(ResultResponse(success=False, vm=vm,
                                      failure_reason=msg))
      else:
        try:
          vm.stop_cmd(self.get_cmd_id())
        except CurieException as exception:
          log.warning("Failed to stop workload on VM '%s'", vm.vm_name(),
                      exc_info=True)
          results.append(ResultResponse(success=False, vm=vm,
                                        failure_reason=exception.message))
        else:
          results.append(ResultResponse(success=True, vm=vm,
                                        failure_reason=None))
    self.__workload_end_secs = int(time.time())
    return results

  def wait(self, vms, timeout_secs=None):
    """
    Waits for the workload generator to complete.

    Args:
      vms: (list of curie.vm.Vm) List of VMs that have been
        started previously.
      timeout_secs: (int) How long to wait for the command. If the IOGen has a
        specified runtime, will wait timeout_secs from the time wait was
        called. If the IOGen has a specified runtime, will wait timeout_secs
        past the end of the running workload.

    Returns:
      results: (list) list of named ResultResponse of (success, vm)
    """
    if timeout_secs is None:
      timeout_secs = 300
    if self.__workload_start_secs is None:
      log.warning("VMs may not have been started before wait was issued by %s",
                  self._name)
    if self._workload_duration_secs is not None:
      seconds_left = self._expected_workload_finish_secs - int(time.time())
      timeout_secs += seconds_left
    results = []
    for vm in vms:
      if self._scenario.should_stop():
        self.stop([vm])
        results.append(ResultResponse(success=False, vm=vm,
                                      failure_reason="Test marked to stop."))
      elif not vm.is_accessible():
        msg = ("Cannot wait for workload to finish on %s: VM is inaccessible" %
               vm.vm_name())
        log.warning(msg)
        results.append(ResultResponse(success=False, vm=vm,
                                      failure_reason=msg))
      else:
        log.debug("Waiting for workload command to complete for %s on %s",
                  self._name, vm.vm_name())
        try:
          ret = self._scenario.wait_for(
            lambda: vm.check_cmd(self.get_cmd_id()),
            "IO command %s to finish" % self.get_cmd_id(),
            timeout_secs,
            poll_secs=5)
        except ScenarioStoppedError:
          self.stop([vm])
          results.append(ResultResponse(success=False, vm=vm,
                                        failure_reason="Test marked to stop."))
        else:
          success = (ret.state == CmdStatus.kSucceeded)
          reason = None if success else "Return state not success from VM."
          results.append(ResultResponse(success=success, vm=vm,
                                        failure_reason=reason))
    self.__workload_end_secs = int(time.time())
    return results

  def run_max_concurrent_workloads(self, vms, max_running, runtime_secs=None,
                                   timeout_secs=None):
    """
    Runs a workload on the VMs with only the max_running number of VMs doing
    the workload at any point.

    The list of VMs, 'vms' is scrambled to achieve a more even distribution
    of VMs performing workload across the cluster, instead of having hot nodes.

    Args:
      vms: (list of curie.vm.Vm) VMs upon which to run IO.
      max_running: (int) Maximum number of IO workloads to run at once across
        all VMs.
      runtime_secs: (int) Amount of time to run IO. If runtime_secs is not
        specified, the workload will complete when another limit is reached
        (e.g. the entire disk is filled).
      timeout_secs: (int) How long to wait for the command. If the IOGen has a
        specified runtime, will wait timeout_secs from the time wait was
        called. If the IOGen has a specified runtime, will wait timeout_secs
        past the end of the running workload.
    """

    def run_workload(vm):
      log.info("Starting workload on %s", vm.vm_name())
      self.start([vm], runtime_secs)
      log.debug("Waiting for workload on %s", vm.vm_name())
      return self.wait([vm], timeout_secs=timeout_secs)[0]

    pool = ThreadPool(max_running)
    scramble_vms = vms[:]
    random.shuffle(scramble_vms)
    return pool.map(run_workload, scramble_vms)

  def fetch_remote_results(self, vms):
    """
    Acquires the results from the remote VM from the workload generator.

    Args:
      vms: (list of curie.vm.Vm) VMs from which results are
        fetched.

    Returns: List of tuples (success, results).
    """
    if self.__workload_start_secs is None:
      log.debug("%s has not started; Skipped fetching results", self._name)
      return []
    else:
      # Prepare local directories if necessary.
      for vm in vms:
        if not os.path.exists(self.get_local_results_path(vm)):
          log.debug("Preparing local directories for workload generator.")
          os.makedirs(self.get_local_results_path(vm))
      return self._fetch_remote_results(vms)

  def _cmd(self, randseed=None):
    """
    The command that will be run to start the workload generator on the
    remote VM.

    Args:
      randseed: (int) Seed passed to the workload generator's RNG.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def _fetch_remote_results(self, vms):
    """
    Acquires the results from the remote VM from the workload generator.

    Args:
      vms: (list of curie.vm.Vm) VMs from which results are
        fetched.

    Returns: List of tuples (success, results).
    """
    raise NotImplementedError("Subclasses need to implement this")

  def _set_global_defaults(self, vm):
    """
    Used to set the global default values in the workload for a given VM.

    Args:
      vm: (curie.vm.Vm) VM for which the defaults are set.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def __prepare_vms(self, vms, local_config_paths,
                    max_concurrent_transfers=24):
    """
    Prepares the remote VMs:
     1. Creates appropriate output directories.
     2. Copies the configuration to the VM.

    Args:
      vms: (curie.vm.Vm) VMs to prepare.
      local_config_paths: (str) Paths to workload configuration files.
      max_concurrent_transfers: (int) Maximum number of concurrent file
        transfers.
    """
    ScenarioUtil.create_remote_output_dir(vms, self._remote_results_path)
    transfer_pool = ThreadPool(max_concurrent_transfers)
    result_tuples = []
    transfer_timeout_secs = 120
    for vm, local_config_path in zip(vms, local_config_paths):
      async_result = transfer_pool.apply_async(
        vm.transfer_to,
        (local_config_path, self._remote_config_path, transfer_timeout_secs))
      result_tuples.append((vm, async_result))
    for vm, async_result in result_tuples:
      try:
        result = async_result.get(transfer_timeout_secs)
      except multiprocessing.TimeoutError as err:
        raise CurieTestException(
          "Timeout waiting for response from vm.transfer_to (%s, %s)" %
          (vm, err))
      else:
        if not result:
          msg = ("Failed to prepare %s for workload generator %s" %
                 (vm.vm_name(), self._name))
          log.warning(msg)
          raise CurieTestException(msg)
    command_ids = ["sync_%d_%d" % (int(time.time()), index)
                   for index in xrange(len(vms))]
    for command_id, vm in zip(command_ids, vms):
      log.info("Syncing filesystem on %s", vm.vm_name())
      vm.execute_async(command_id, "sync", user="root")
    for command_id, vm in zip(command_ids, vms):
      vm.wait_for_cmd(command_id, poll_secs=1, timeout_secs=60)
      log.info("VM %s prepared with latest configuration for %s",
               vm.vm_name(), self._name)
