#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import os

from curie.exception import CurieTestException
from curie.test.steps._base_step import BaseStep

log = logging.getLogger(__name__)


class PrefillStart(BaseStep):
  """Begin prefilling a group of VMs using FIO.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): The name of the workload to use.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, workload_name, async=False, annotate=False):
    """
    Raises:
      CurieTestException:
        If no workload named workload_name exists.
    """
    super(PrefillStart, self).__init__(scenario, annotate=annotate)
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    self.async = async
    self.description = "%s: Starting prefill" % self.workload.name()

  def _run(self):
    """Begin prefilling a group of VMs using FIO.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If one or more matching VMs are not accessible.
        If iogen_file_path does not exist or is invalid.
    """
    _iogen_start(self.workload, prefill=True, runtime_secs=None,
                 stagger_secs=None, seed=None)
    if not self.async:
      wait_step = PrefillWait(self.scenario, self.workload.name(),
                              annotate=self._annotate)
      wait_step()


class PrefillWait(BaseStep):
  """Wait for an existing FIO prefill step to finish for a group of VMs.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): Name of workload being prefilled.
    timeout_secs (int): Maximum time (seconds) to wait for prefill to finish.
    annotate (bool): If True, annotate key points in the step in the test's
      results.

  Raises:
    CurieTestException: If the workload does not exist in the test.
  """

  def __init__(self, scenario, workload_name, timeout_secs=7200,
               annotate=False):
    """
    Raises:
      CurieTestException:
        If no workload named workload_name exists.
    """
    super(PrefillWait, self).__init__(scenario, annotate=annotate)
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    self.timeout_secs = timeout_secs
    self.description = "%s: Waiting for prefill" % self.workload.name()

  def _run(self):
    """Wait for an existing FIO prefill step to finish for a group of VMs.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If one or more matching VMs are not accessible.
        If iogen_file_path does not exist or is invalid.
    """
    return _iogen_wait(self.workload, self.timeout_secs, prefill=True)


class PrefillRun(BaseStep):
  """Run prefill operations in parallel, and wait for them to complete.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): Name of workload being prefilled.
    max_concurrent (int): Maximum number of operations to run in parallel.
    annotate (bool): If True, annotate key points in the step in the test's
      results.

  Raises:
    CurieTestException: If the workload does not exist in the test.
  """

  def __init__(self, scenario, workload_name, max_concurrent=8,
               timeout_secs=7200, annotate=False):
    super(PrefillRun, self).__init__(scenario, annotate=annotate)
    self.max_concurrent = max_concurrent
    self.timeout_secs = timeout_secs
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    self.description = "%s: Prefilling" % self.workload.name()

  def _run(self):
    """Run prefill operations in parallel, and wait for them to complete.

    Returns:
      None

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If one or more matching VMs are not accessible.
        If iogen_file_path does not exist or is invalid.
    """
    vms = self.workload.vm_group().get_vms()
    if not vms:
      raise CurieTestException("No VMs found in VM group %s" %
                                self.workload.vm_group().name())
    # Check that the VMs are accessible.
    inaccessible_names = []
    for vm in vms:
      if not vm.is_accessible():
        inaccessible_names.append(vm.vm_name())
    if inaccessible_names:
      raise CurieTestException(
        "Error starting FIO %s: VM(s) inaccessible [%s]" % (
          self.workload.name(), ", ".join(inaccessible_names)))
    iogen = self.workload.prefill_iogen(max_concurrent=self.max_concurrent)
    log.info("FIO %s: Preparing", self.workload.name())
    iogen.prepare(vms)
    results = iogen.run_max_concurrent_workloads(
      vms, self.max_concurrent, timeout_secs=self.timeout_secs)
    for result in results:
      if result.success:
        log.info("Prefill completed successfully on %s", result.vm.vm_name())
      else:
        raise CurieTestException("Prefill did not complete successfully on "
                                  "%s: '%s'" % (result.vm.vm_name(),
                                                result.failure_reason))
    log.info("FIO %s: Fetching diagnostics", self.workload.name())
    iogen.fetch_remote_results(vms)
    incomplete_names = []
    for vm in vms:
      if not os.path.exists(iogen.get_local_results_path(vm)):
        incomplete_names.append(vm.vm_name())
    if incomplete_names:
      raise CurieTestException(
        "Failed to complete FIO %s for %d VM(s) [%s]" %
        (self.workload.name(), len(incomplete_names),
         ", ".join(incomplete_names)))


class Start(BaseStep):
  """Copy results for an existing FIO workload step, and return protobufs.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): Name of the workload to start.
    runtime_secs (int): Maximum time (seconds) to tell the workload to run.
    timeout_secs (int): Maximum time (seconds) to wait for the workload to
      finish if the workload is started synchronous. If not set, the default
      behavior is waiting for runtime + 300s buffer.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
    async (bool): If False, block until workload finishes. In the synchronous
      case, Wait is called with timeout_secs. If True, step returns
      immediately, in which case Wait must be used to wait for the workload
      to finish.
    stagger_secs (int): A time span, in seconds, in which to evenly stagger the
      workload start times across the VMs in the workload's VM group. For
      example, if a workload is set to run on 4 VMs and if stagger_secs is 900,
      the start time on each of the 4 VMs will be 225 seconds apart.

  Raises: CurieTestException if the workload was not set in the test, or if
     a timeout_secs is set along with async=True.

  Notes on the combinations of runtime, timeout, and async:
  ===== ======= ======= =======================================================
  Async Runtime Timeout Effect
  ----- ------- ------- -------------------------------------------------------
  False None    None    Step blocks, with a default timeout of 300s sent
                        to Wait.
  False None    > 0     Step blocks, with the timeout set to `timeout\_secs`.
  False > 0     None    Step blocks, runtime is sent to workload and default
                        timeout is runtime + 300s.
  False > 0     > 0     Step blocks, runtime is sent to workload and timeout is
                        runtime + timeout.
  True  None    None    Step is async, a Wait step should be called
                        independently on the workload.
  True  None    > 0     Exception is raised since timeout is not applicable to
                        asynchronous execution.
  True  > 0     None    Step is async, runtime is sent to workload and a Wait
                        step should be called later. If that wait step doesn't
                        have a timeout set, the default timeout will be
                        runtime + 300s.
  True  > 0     > 0     Exception is raised since timeout is not applicable to
                        asynchronous execution.
  ===== ======= ======= =======================================================
  """

  def __init__(self, scenario, workload_name, runtime_secs=None,
               timeout_secs=None, annotate=False, async=False,
               stagger_secs=None, seed=None):
    """
    Raises:
      CurieTestException:
        If no workload named workload_name exists.
    """
    super(Start, self).__init__(scenario, annotate=annotate)
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    if async and timeout_secs is not None:
      raise CurieTestException("'timeout_secs' not applicable for "
                                "asynchronous workload start.")
    self.runtime_secs = runtime_secs
    self.timeout_secs = timeout_secs
    self.async = async
    self.stagger_secs = stagger_secs
    self.seed = seed
    if async:
      self.description = "%s: Starting workload" % self.workload.name()
    else:
      self.description = "%s: Running workload" % self.workload.name()

  def _run(self):
    """Copy results for an existing FIO workload step, and return protobufs.

    Returns:
      List of curie_test_pb2.CurieTestResult.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If one or more matching VMs are not accessible.
        If iogen_file_path does not exist or is invalid.
    """
    _iogen_start(self.workload, prefill=False, runtime_secs=self.runtime_secs,
                 stagger_secs=self.stagger_secs, seed=self.seed)
    self.create_annotation("%s: Started workload" % self.workload.name())
    if not self.async:
      wait_step = Wait(self.scenario, self.workload.name(),
                       timeout_secs=self.timeout_secs, annotate=self._annotate)
      wait_step()


class Wait(BaseStep):
  """Wait for an existing FIO workload step to finish for a group of VMs.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): Name of the workload in the test.
    timeout_secs (int): The amount of time to wait for the workload beyond any
      runtime set in a Start step. If timeout_secs is not specified, the
      default behavior is to wait for runtime +  300s buffer. If the runtime
      was None, then only the default 300s is given.
    annotate (bool): If True, annotate key points in the step in the test's
      results.

  Raises: CurieTestException if the workload was not set in the test.
  """

  def __init__(self, scenario, workload_name, timeout_secs=None,
               annotate=False):
    """
    Raises:
      CurieTestException:
        If no workload named workload_name exists.
    """
    super(Wait, self).__init__(scenario, annotate=annotate)
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    self.description = "%s: Running workload" % self.workload.name()
    self.timeout_secs = timeout_secs

  def _run(self):
    """Wait for an existing FIO workload step to finish for a group of VMs.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If iogen_file_path does not exist or is invalid.
    """
    _iogen_wait(self.workload, timeout_secs=self.timeout_secs, prefill=False)
    self.create_annotation("%s: Finished workload" % self.workload.name())


class Stop(BaseStep):
  """Interrupt an existing FIO workload step to finish for a group of VMs.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    workload_name (str): Name of the workload in the test.
    annotate (bool): If True, annotate key points in the step in the test's
      results.

  Raises: CurieTestException if the workload was not set in the test.
  """

  def __init__(self, scenario, workload_name, annotate=False):
    """
    Raises:
      CurieTestException:
        If no workload named workload_name exists.
    """
    super(Stop, self).__init__(scenario, annotate=annotate)
    try:
      self.workload = scenario.workloads[workload_name]
    except KeyError:
      raise CurieTestException("Workload name %r was not in scenario." %
                                workload_name)
    self.description = "%s: Stopping workload" % self.workload.name()

  def _run(self):
    """Interrupt an existing FIO workload step to finish for a group of VMs.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If iogen_file_path does not exist or is invalid.
    """
    vms = self.workload.vm_group().get_vms()
    if not vms:
      raise CurieTestException("No VMs found in VM group %s" %
                                self.workload.vm_group().name())
    iogen = self.workload.iogen()
    log.info("FIO %s: Stopping IO", self.workload.name())
    iogen.stop(vms)
    _iogen_fetch_remote_results(self.workload, iogen, vms)
    log.info("FIO %s: Complete", self.workload.name())
    self.create_annotation("%s: Stopped workload" % self.workload.name())


def _iogen_start(workload, prefill, runtime_secs, stagger_secs, seed):
  """Low-level function to start IOGen workload or prefill.

  Args:
    workload (Workload): The workload object to get an iogen from.
    prefill (bool): Whether or not the IOGen is used for prefill.
    runtime_secs (int or None): Desired runtime for the workload.
    stagger_secs (int or None): Desired time span in seconds in which to evenly
      stagger the workload start times.
    seed (str or None): Seed value to be used by the workload generator.

  Raises:
    CurieTestException if any VMs are inaccessible.
  """
  vms = workload.vm_group().get_vms()
  if not vms:
    raise CurieTestException("No VMs found in VM group %s" %
                              workload.vm_group().name())
  # Check that the VMs are accessible.
  inaccessible_names = []
  for vm in vms:
    if not vm.is_accessible():
      inaccessible_names.append(vm.vm_name())
  if inaccessible_names:
    raise CurieTestException(
      "Error starting FIO %s: VM(s) inaccessible [%s]" % (
        workload.name(), ", ".join(inaccessible_names)))
  if prefill:
    iogen = workload.prefill_iogen()
  else:
    iogen = workload.iogen()
  log.info("FIO %s: Preparing", workload.name())
  iogen.prepare(vms)
  log.info("FIO %s: Starting IO", workload.name())
  iogen.start(vms, runtime_secs=runtime_secs, stagger_secs=stagger_secs,
              seed=seed)
  log.info("FIO %s: IO started", workload.name())


def _iogen_wait(workload, timeout_secs=None, prefill=False,
                **iogen_wait_kwargs):
  """Low-level function to wait for an existing IOGen workload or prefill.

  Args:
    workload (Workload): The workload object to get an iogen from.
    timeout_secs (int): Maximum time to wait for the IOGen to finish.
    prefill (bool): Whether or not the IOGen is used for prefill.

  Raises:
    CurieTestException:
      If the IOGen operation does not complete on one or more VMs.
  """
  vms = workload.vm_group().get_vms()
  if not vms:
    raise CurieTestException("No VMs found in VM group %s" %
                              workload.vm_group().name())
  if prefill:
    iogen = workload.prefill_iogen()
  else:
    iogen = workload.iogen()
  log.info("FIO %s: Waiting for IO to complete", workload.name())
  iogen.wait(vms, timeout_secs=timeout_secs, **iogen_wait_kwargs)
  _iogen_fetch_remote_results(workload, iogen, vms)
  log.info("FIO %s: Complete", workload.name())


def _iogen_fetch_remote_results(workload, iogen, vms):
  log.info("FIO %s: Fetching diagnostics", workload.name())
  iogen.fetch_remote_results(vms)
  incomplete_names = []
  for vm in vms:
    if not os.path.exists(iogen.get_local_results_path(vm)):
      incomplete_names.append(vm.vm_name())
  if incomplete_names:
    raise CurieTestException(
      "Failed to complete FIO %s for %d VM(s) [%s]" %
      (workload.name(), len(incomplete_names), ", ".join(incomplete_names)))
