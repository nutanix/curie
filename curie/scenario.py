#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import cPickle as pickle
import errno
import json
import logging
import os
import shutil
import threading
import time
import warnings
from collections import OrderedDict

from enum import Enum
from pkg_resources import resource_filename

from curie import prometheus
from curie.acropolis_cluster import AcropolisCluster
from curie.name_util import NameUtil
from curie.curie_test_pb2 import CurieTestPhase, CurieTestResult
from curie.curie_test_pb2 import CurieTestState, CurieTestStatus
from curie.exception import ScenarioStoppedError, ScenarioTimeoutError
from curie.os_util import OsUtil
from curie.result.cluster_result import ClusterResult
from curie.scenario_util import ScenarioUtil
from curie.steps import check, nodes
from curie.steps._base_step import BaseStep
from curie.steps.meta import MetaStep
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class Phase(Enum):
  PRE_SETUP = 1
  SETUP = 2
  RUN = 3
  TEARDOWN = 4


class Status(Enum):
  # New states.
  NOT_STARTED = 0
  # Active states.
  EXECUTING = 1
  STOPPING = 2
  FAILING = 3
  # Terminal states.
  SUCCEEDED = 4
  FAILED = 5
  STOPPED = 6
  CANCELED = 7
  INTERNAL_ERROR = 8

  def is_new(self):
    """
    Returns True if never started, or False otherwise.
    """
    return self in [Status.NOT_STARTED]

  def is_active(self):
    """
    Returns True if in progress, or False otherwise.
    """
    return self in [Status.EXECUTING,
                    Status.STOPPING,
                    Status.FAILING]

  def is_terminal(self):
    """
    Returns True if has reached a terminal state, else False.
    """
    return not (self.is_new() or self.is_active())


class Scenario(object):
  def __init__(self, name=None, id=None, display_name=None, cluster=None,
               source_directory=None, output_directory=None, readonly=False,
               prometheus_config_directory=None, prometheus_address=None,
               goldimages_directory=None, enable_experimental_metrics=False):
    self.cluster = cluster
    self.output_directory = output_directory
    self.name = name
    self.display_name = display_name if display_name else name
    # TODO(ryan.hardin): UUID is probably more appropriate for id, but using
    # int to maintain protobuf compatibility.
    self.id = id if id else int(time.time() * 1e3)
    self.summary = None
    self.description = None
    self.estimated_runtime = None
    self.tags = None
    self.vm_groups = None
    self.workloads = None
    self.phase = None
    self.results_map = dict()
    self.source_directory = source_directory
    self.steps = {phase: [] for phase in Phase}
    self.yaml_str = None
    self.vars = {}
    # Readonly is used to tell X-Ray to not allow deletion of this scenario.
    self.readonly = readonly
    self.prometheus_config_directory = prometheus_config_directory
    self.prometheus_address = prometheus_address
    self.goldimages_directory = goldimages_directory
    self.enable_experimental_metrics = enable_experimental_metrics

    self._start_time_secs = None
    self._end_time_secs = None
    self._status = Status.NOT_STARTED
    self._annotations = []
    self._threads = []
    self._phase_start_time_secs = {phase: None for phase in Phase}
    self._lock = threading.RLock()

    self.__result_pbs = []
    self.__sleep_cv = threading.Condition()
    self.__error_message = None

  def __cmp__(self, other):
    return cmp(self.id, other.id)

  def __str__(self):
    return "%s (%s)" % (self.display_name, self._status.name)

  def __repr__(self):
    return ("<%s(name=%r, display_name=%r, id=%r, status=%r)>" %
            (self.__class__.__name__,
             self.name,
             self.display_name,
             self.id,
             self._status))

  def __getstate__(self):
    with self._lock:
      state = self.__dict__.copy()
      # Remove unpicklable entries.
      del state["cluster"]
      del state["_lock"]
      del state["_Scenario__sleep_cv"]
      return state

  def __setstate__(self, state):
    self.__dict__.update(state)
    self.cluster = None
    self._lock = threading.RLock()
    self.__sleep_cv = threading.Condition()

  @staticmethod
  def find_configuration_files(root=None, filename="test.yml"):
    """
    Return a list of file paths to scenario configuration files.

    This searches for filename matches, and does not validate the configuration
    files themselves.

    Args:
      root (basestring or None): Directory in which to recursively search.
        Setting `root` to None will search the default directory.
      filename (basestring): Filename of scenario configuration files.

    Returns:
      list of str: Absolute paths to scenario configuration files.
    """
    if root is None:
      root = resource_filename(__name__, "yaml")
    if not os.path.isdir(root):
      raise ValueError("'%s' must be a directory" % root)
    paths = []
    for dirpath, _, filenames in os.walk(root):
      if filename in filenames:
        paths.append(os.path.join(dirpath, filename))
    return paths

  @classmethod
  def load_state(cls, directory):
    """
    Load a scenario from the file system.

    Args:
      directory (basestring): Directory containing the state file. This should
        be the same as the Scenario's output_directory.
    """
    state_file_path = os.path.join(directory, "state.pickle")
    with open(state_file_path, "r") as output_fp:
      return pickle.load(output_fp)

  def save_state(self):
    """
    Save the state of the scenario to the file system.
    """
    try:
      os.makedirs(self.output_directory)
    except OSError as err:
      if err.errno != errno.EEXIST:
        raise
    state_file_path = os.path.join(self.output_directory, "state.pickle")
    with open(state_file_path, "w") as output_fp:
      pickle.dump(self, output_fp)

  def results(self):
    """
    Returns the current results of this scenario.
    """
    with self._lock:
      for result in self.__result_pbs:
        del result.data_2d.x_annotations[:]
        result.data_2d.x_annotations.extend(self._annotations)
      return self.__result_pbs

  def status(self):
    """
    Returns the current status of this scenario.
    """
    with self._lock:
      return self._status

  def error_message(self):
    """
    Returns error message if one has occurred during the scenario, else None.
    """
    with self._lock:
      return self.__error_message

  def test_id(self):
    """
    Deprecated: Returns the Scenario's id.

    Returns:
      basestring: id
    """
    # TODO(ryan.hardin): Remove deprecated method.
    warnings.warn("Use .id instead", DeprecationWarning)
    return self.id

  def test_dir(self):
    """
    Deprecated: Returns the Scenario's output directory.

    Returns:
      basestring: Absolute path to output directory.
    """
    # TODO(ryan.hardin): Remove deprecated method.
    warnings.warn("Use .output_directory instead", DeprecationWarning)
    return self.output_directory

  def cluster_stats_dir(self):
    """
    Returns a path to the Scenario's cluster results.

    If no output directory is configured, this returns None.

    Returns:
      str or None: Path to the cluster results directory.
    """
    if self.output_directory is None:
      return None
    else:
      return os.path.join(self.output_directory, "cluster_stats")

  def phase_start_time_secs(self, phase):
    """
    Return the start time of a given phase, or None if it has not started.

    Args:
      phase (Phase): Phase of interest.

    Returns:
      float or None: Seconds since epoch, or None.
    """
    with self._lock:
      return self._phase_start_time_secs.get(phase)

  def add_step(self, step, phase):
    """
    Add a step to be run as part of this scenario.

    Args:
      step (BaseStep): Step to run.
      phase (Enum): Phase into which the step will be added.

    Raises:
      ValueError:
        If the phase is invalid.
    """
    with self._lock:
      if not self._status.is_new():
        raise RuntimeError("%s: Can not add step %s because the scenario has "
                           "already been started" % (self, step))
      if phase not in self.steps:
        raise ValueError("%s: Can not add step %s because the phase %r is "
                         "invalid" % (self, step, phase))
      self.steps[phase].append(step)

  def requirements(self):
    req = set()
    for phase, step in self.itersteps():
      req.update(step.requirements())
    return req

  def itersteps(self):
    """
    Iterate this Scenario's steps in execution order.

    Yields:
      BaseStep: Each step.
    """
    for phase in Phase:
      for step in self.steps.get(phase, []):
        yield phase, step

  def completed_steps(self):
    """
    Return a list of steps that have finished.

    Returns:
      list of (Phase, BaseStep): List of completed steps and their phases.
    """
    return [(phase, step) for phase, step in self.itersteps()
            if step.status == step.status.SUCCEEDED]

  def remaining_steps(self):
    """
    Return a list of steps that have not finished.

    Returns:
      list of (Phase, BaseStep): List of remaining steps and their phases.
    """
    return [(phase, step) for phase, step in self.itersteps()
            if step.status != step.status.SUCCEEDED]

  def progress(self):
    """
    Return the completion as a float between 0.0 and 1.0.

    A value of 0 is returned if no progress has been made. A value of
    1 is returned if all steps are completed.

    Returns:
      float: Completion progress between 0.0 and 1.0.
    """
    completed = 0
    total = 0
    for phase, step in self.itersteps():
      total += 1
      if step.status.is_terminal():
        completed += 1
    if total == 0:
      return 0.0
    else:
      return completed / float(total)

  def duration_secs(self):
    """
    Return the number of seconds that this scenario has been running.

    If the scenario has not started, None will be returned. If the scenario is
    in progress, the duration is relative to the current time. If the scenario
    has finished, the total duration until the test stopped will be returned.

    Returns:
      float or None: Duration, in seconds.
    """
    if self._end_time_secs and self._start_time_secs is None:
      raise RuntimeError("%s end time set (%r), but start time is None" %
                         (self, self._end_time_secs))
    elif self._start_time_secs is None:
      return None
    elif self._end_time_secs is None:
      return time.time() - self._start_time_secs
    else:
      return self._end_time_secs - self._start_time_secs

  def should_stop(self):
    """
    Returns True if the scenario should stop cleanly as soon as possible.
    """
    return self.status() in [Status.STOPPING, Status.FAILING]

  def start(self):
    """
    Begin running the scenario.

    This method returns immediately.

    Raises:
      RuntimeError: If the scenario is not in a new state.
    """
    with self._lock:
      if not self._status.is_new():
        raise RuntimeError("%s: Can not start because it has already been "
                           "started" % self)
      if self.cluster is None:
        raise RuntimeError("%s: Can not start because cluster has not been "
                           "set (perhaps it has been pickled/unpickled?)"
                           % self)
      if self.output_directory is None:
        raise RuntimeError("%s: Can not start because output directory has "
                           "not been set" % self)
      if self.steps[Phase.PRE_SETUP]:
        raise RuntimeError("%s: Steps should not be manually added to the "
                           "PreSetup phase" % self)
      self._expand_meta_steps()
      self._initialize_presetup_phase()
      try:
        os.makedirs(self.output_directory)
      except OSError as err:
        if err.errno != errno.EEXIST:
          raise
        log.debug("%s: Output directory '%s' already exists, but will be used "
                  "anyway", self, self.output_directory)
      # Copy the original configuration files.
      if self.source_directory:
        source_directory = os.path.realpath(self.source_directory)
        for file_or_dir in os.listdir(source_directory):
          source = os.path.join(source_directory, file_or_dir)
          destination = os.path.join(self.output_directory, file_or_dir)
          if os.path.isfile(source):
            shutil.copy2(source, destination)
          elif os.path.isdir(source):
            shutil.copytree(source, destination)
          else:
            log.debug("%s: '%s' is neither a file nor a directory, and "
                      "will not be copied into the output directory",
                      self, source)
        log.debug("%s: Copied original configuration files to '%s'", self,
                  self.output_directory)
      # Write the parsed YAML with substituted variables.
      if self.yaml_str:
        yaml_path = os.path.join(self.output_directory, "test-parsed.yml")
        with open(yaml_path, "w") as f:
          f.write(self.yaml_str)
        log.debug("%s: Wrote parsed scenario definition to '%s'",
                  self, yaml_path)
      self._status = Status.EXECUTING
      self._start_time_secs = time.time()
    self._threads.append(threading.Thread(target=self._step_loop,
                                          name="step_loop"))
    self._threads.append(threading.Thread(target=self._scenario_results_loop,
                                          name="scenario_results_loop"))
    self._threads.append(threading.Thread(target=self._cluster_results_loop,
                                          name="cluster_results_loop"))
    for thread in self._threads:
      thread.start()
    log.info("%s has started", self)

  def stop(self):
    """
    Interrupt a running scenario, notifying it to cleanly stop.

    This method returns immediately. If the scenario is new, it will be set in
    a canceled state. If the scenario is active, it will be set in a stopping
    state. If the scenario is terminal or already stopping, this method is a
    no-op.
    """
    with self._lock:
      if self.status() == Status.STOPPING:
        log.info("%s is already marked to stop", self)
      elif self._status.is_terminal():
        log.info("%s can not be stopped because it is already in a terminal "
                 "state", self)
      elif self._status.is_new():
        self._status = Status.CANCELED
        log.info("%s has been canceled", self)
      else:  # Is active.
        self._status = Status.STOPPING
        log.info("%s has been marked to stop", self)

  def join(self, timeout=None):
    """
    Wait for an active scenario to reach a terminal state.

    To be more specific, this method blocks until all of the scenario's worker
    threads have joined. It may be called while the scenario is already in a
    terminal state to ensure that all worker threads are properly cleaned up.
    This method is responsible for ensuring a terminal state is set.

    Args:
      timeout (float or None): Timeout parameter to pass to the join call for
        each thread.

    Raises:
      RuntimeError: If the scenario is in a new state.
    """
    if self._status.is_new():
      raise RuntimeError("%s can not be joined because it has not been "
                         "started" % self)
    try:
      for thread in self._threads:
        if thread.is_alive():
          log.debug("Waiting for %s to terminate", thread)
          thread.join(timeout=timeout)
        else:
          log.debug("%s is not alive", thread)
    except BaseException as exc:
      with self._lock:
        self._status = Status.INTERNAL_ERROR
        log.exception("Unhandled exception occurred while joining threads")
        self.__error_message = str(exc)
    finally:
      with self._lock:
        if self._status == Status.EXECUTING:
          self._status = Status.SUCCEEDED
        elif self._status == Status.FAILING:
          self._status = Status.FAILED
        elif self._status == Status.STOPPING:
          self._status = Status.STOPPED
        else:
          log.error("%s exited thread join in an unexpected state", self)
          self._status = Status.INTERNAL_ERROR
        assert self._status.is_terminal()
        log.info("%s has finished", self)

  def verify_steps(self):
    """
    Verify that all steps will work with the current cluster configuration.

    If verification succeeds, an empty list is returned.

    Returns:
      list of Step: Steps that failed to verify.
    """
    failed_steps = []
    for _, step in self.itersteps():
      try:
        step.verify()
      except Exception as exception:
        log.error("%s: Verification of %s failed - %s",
                  self, step, exception)
        failed_steps.append(step)
    return failed_steps

  def create_annotation(self, description):
    """
    Create an XAnnotation for this scenario.

    Args:
      description (basestring): Description for the annotation.

    Returns:
      None
    """
    x_annotation = CurieTestResult.Data2D.XAnnotation()
    x_annotation.x_val = int(time.time())
    x_annotation.description = description
    with self._lock:
      self._annotations.append(x_annotation)
    log.debug("Annotation for '%s' created", description)

  def resource_path(self, relative_path):
    """
    Get an absolute path to a file associated with this scenario.

    Scenarios are usually packaged with some extra configuration files, such as
    workload configuration files (e.g. .fio files). `resource_path` can be used
    to convert a path relative to the scenario's base directory into an
    absolute path.

    Args:
      relative_path (basestring): Path relative to the scenario's YAML file.

    Returns:
      str: Absolute path to the desired resource.
    """
    if not self.source_directory:
      raise ValueError("Cannot find path to resource '%s' because "
                       "source_directory has not been set" % relative_path)
    return os.path.realpath(os.path.join(self.source_directory, relative_path))

  def to_curie_test_state_protobuf(self):
    """
    Return a Curie test state protobuf representing this Scenario.

    Returns:
      CurieTestState: Curie test state protobuf.
    """
    with self._lock:
      curie_test_state = CurieTestState()
      curie_test_state.test_id = self.id
      curie_test_state.status = CurieTestStatus.Type.Value(self._status.name)
      step_num = 0
      for phase, step in self.completed_steps():
        curie_test_state.completed_steps.add(
          phase=CurieTestPhase.Type.Value(phase.name),
          step_num=step_num,
          step_description=step.description,
          status=step.status.name,
          message=str(step.exception) if step.exception else None
        )
        step_num += 1
      for phase, step in self.remaining_steps():
        curie_test_state.remaining_steps.add(
          phase=CurieTestPhase.Type.Value(phase.name),
          step_num=step_num,
          step_description=step.description,
          status=step.status.name,
          message=str(step.exception) if step.exception else None
        )
        step_num += 1
      error_msg = self.error_message()
      if error_msg is not None:
        curie_test_state.error_msg = error_msg
    return curie_test_state

  def wait_for(self, func, msg, timeout_secs, poll_secs=None):
    """
    Invoke 'func' every 'poll_secs' seconds until the boolean-ness of the
    returned value is True, in which case we return that value. If
    'timeout_secs' seconds have elapsed, raises an exception. Otherwise, if the
    test is marked to stop, raises a ScenarioStoppedError.

    Args:
      func (callable): Callable to poll.
      msg (str): Readable description of the action.
      timeout_secs (int): Maximum amount of time to wait for func to complete.
      poll_secs (int): Polling interval in seconds. None uses default polling
        interval.

    Raises:
      CurieTestException:
        If 'func' does not evaluate to True within 'timeout_secs' seconds.
      ScenarioStoppedError:
        If the scenario is stopped before wait_for returns.
    """
    return_values = CurieUtil.wait_for_any([func, self.should_stop], msg,
                                            timeout_secs, poll_secs=poll_secs)
    if return_values is None:
      raise ScenarioTimeoutError("Timed out waiting for %s within %d seconds" %
                                 (msg, timeout_secs))
    # Return value of func.
    elif return_values[0]:
      return return_values[0]
    # Return value of self.should_stop.
    elif return_values[1]:
      raise ScenarioStoppedError("Stopped waiting for %s because %s is "
                                 "stopping" % (msg, self))
    else:
      raise RuntimeError("Expected one or more return values to evaluate as "
                         "True: %r" % return_values)

  def wait_for_all(self, funcs, msg, timeout_secs, poll_secs=None,
                   skip_succeeded=True):
    """
    Invoke the callables in 'funcs' every 'poll_secs' seconds until the
    boolean-ness of all returned values are True, in which case those values
    are returned. If 'timeout_secs' seconds have elapsed since wait_for_all is
    invoked, an exception is raised. Otherwise, if the test is marked to stop,
    None is returned.

    Args:
      funcs (iterable): List of callables to invoke.
      msg (str): Readable description of the action.
      timeout_secs (int): Maximum amount of time to wait for all funcs to
        complete.
      poll_secs (int): Polling interval in seconds. None uses default polling
        interval.
      skip_succeeded (bool): If True, do not re-invoke callables that have
        succeeded previously.

    Raises:
      CurieTestException:
        If all callables in 'funcs' do not evaluate to True within
        'timeout_secs' seconds.
    """
    # TODO(ryan.hardin): There's some overlap here with wait_for_any() in
    # util.py.
    if poll_secs is None:
      poll_secs = 1
    start_time_secs = time.time()
    return_values = OrderedDict()
    log.info("Waiting for %s (timeout %d secs)", msg, timeout_secs)
    while True:
      successes = 0
      for func in funcs:
        if not skip_succeeded or not return_values.get(func):
          return_values[func] = func()
        duration_secs = time.time() - start_time_secs
        if return_values.get(func):
          successes += 1
          if successes == len(funcs):
            log.info("Finished waiting for %s (%d seconds)",
                     msg, duration_secs)
            return return_values.values()
        if duration_secs > timeout_secs:
          raise RuntimeError("Timed out waiting for %s within %d seconds" %
                             (msg, timeout_secs))
        elif self.should_stop():
          log.warning("Stopped waiting for %s because test was stopped", msg)
          return None
      log.info("Waiting for %s (%d/%d complete)", msg, successes, len(funcs))
      time.sleep(poll_secs)

  def _initialize_presetup_phase(self):
    """
    Insert PreSetup checks based on the step requirements.

    This method will overwrite all existing steps in the PreSetup phase.
    """
    if self.steps[Phase.PRE_SETUP]:
      log.debug("Existing PreSetup steps will be overwritten: %s",
                self.steps[Phase.PRE_SETUP])
    self.steps[Phase.PRE_SETUP] = []
    if BaseStep.OOB_CONFIG in self.requirements():
      self.add_step(check.OobConfigured(self), Phase.PRE_SETUP)
    log.debug("Finished inserting presetup checks")
    self._expand_meta_steps()  # In case any new MetaSteps added above.

  def _expand_meta_steps(self):
    """
    Expand all MetaSteps into their constituent Steps.
    """
    expanded_steps = {phase: [] for phase in Phase}
    for phase, step in self.itersteps():
      if isinstance(step, MetaStep):
        for substep in step.itersteps():
          log.debug("Expanded substep '%s'" % substep)
          expanded_steps[phase].append(substep)
      else:
        expanded_steps[phase].append(step)
    self.steps = expanded_steps
    log.debug("Finished expanding MetaSteps")

  def _step_loop(self):
    """
    Iterate through each step, returning after all steps have finished.
    """
    assert self._status.is_active()
    log.debug("%s: Entered _step_loop.", self)
    # If any steps that fail to verify are returned, the test is failing.
    failed_steps = self.verify_steps()
    if failed_steps:
      self._status = Status.FAILING
      self.__error_message = (
        "%d %s failed to verify: %s" %
        (len(failed_steps),
         "step" if len(failed_steps) == 1 else "steps",
         ", ".join(str(step) for step in failed_steps)))
    try:
      for phase, step in self.itersteps():
        self._execute_step(phase, step)
    except BaseException as exc:
      with self._lock:
        self._status = Status.INTERNAL_ERROR
        log.exception("Unhandled exception occurred inside _step_loop")
        self.__error_message = str(exc)
    finally:
      with self._lock:
        self._end_time_secs = time.time()
        log.info("%s: All steps finished", self)
        log.debug("_step_loop notifying all threads to wake up")
        self.__sleep_cv.acquire()
        self.__sleep_cv.notify_all()
        self.__sleep_cv.release()
        log.debug("%s: Exiting _step_loop", self)

  def _scenario_results_loop(self, interval_secs=10):
    """
    Periodically update the scenario results while the scenario is running.
    """
    target_config_path = None
    if self.prometheus_config_directory:
      target_filename = NameUtil.sanitize_filename("targets_%s.json" % self.id)
      target_config_path = os.path.join(self.prometheus_config_directory,
                                        target_filename)
    try:
      while self._end_time_secs is None:
        with self.__sleep_cv:
          self.__sleep_cv.wait(interval_secs)
        self._scenario_results_update(target_config_path)
    except BaseException as exc:
      with self._lock:
        self._status = Status.INTERNAL_ERROR
        self.__error_message = str(exc)
        log.exception("Unhandled exception occurred inside "
                      "_scenario_results_loop")
    else:
      log.debug("_scenario_results_loop exited cleanly")
    finally:
      if target_config_path and os.path.isfile(target_config_path):
        os.remove(target_config_path)
        log.debug("%s: Removed file %r", self, target_config_path)
      log.debug("%s: Exiting _scenario_results_loop", self)

  def _cluster_results_loop(self, interval_secs=60):
    """
    Periodically update the cluster results while the scenario is running.
    """
    try:
      last_appended = self._cluster_results_update()
      while self._end_time_secs is None:
        with self.__sleep_cv:
          self.__sleep_cv.wait(interval_secs)
        append_time_secs = self._cluster_results_update(last_appended)
        if append_time_secs is not None:
          last_appended = append_time_secs
    except BaseException as exc:
      with self._lock:
        self._status = Status.INTERNAL_ERROR
        self.__error_message = str(exc)
        log.exception("Unhandled exception occurred inside "
                      "_cluster_results_loop")
    else:
      log.debug("_cluster_results_loop exited cleanly")
    finally:
      log.debug("%s: Exiting _cluster_results_loop", self)

  def _execute_step(self, phase, step):
    """
    Execute a single step.

    If an exception occurs, a failing state will be set. If the scenario is
    in a stopping or failing state, the step will be skipped (not executed).
    """
    with self._lock:
      if self._status == Status.STOPPING:
        log.info("%s marked to stop: Skipping '%s'", self, step)
        return
      elif self._status == Status.FAILING:
        log.info("%s failed: Skipping '%s'", self, step)
        return
      elif self._status == Status.INTERNAL_ERROR:
        log.info("%s entered an internal error state: Skipping '%s'",
                 self, step)
        return
      elif self._status.is_new():
        raise RuntimeError("%s executed before %s entered a running state" %
                           (step, self))
      elif self._status.is_terminal():
        raise RuntimeError("%s executed while %s was in a terminal state" %
                           (step, self))
      self.phase = phase
      if self._phase_start_time_secs.get(self.phase) is None:
        self._phase_start_time_secs[self.phase] = time.time()
    try:
      step()
    except ScenarioStoppedError as exc:
      with self._lock:
        if not self.should_stop():
          log.error("A ScenarioStoppedError was raised, but %s should_stop() "
                    "is returning False (should return True)", self)
          raise
        else:
          log.warning(str(exc))
    except Exception as exc:
      log.exception("Exception during '%s'", step)
      with self._lock:
        self._status = Status.FAILING
        self.__error_message = str(exc)

  def _scenario_results_update(self, prometheus_target_config_path=None):
    """
    Update the in-memory scenario results list.
    """
    result_pbs = []
    try:
      if prometheus_target_config_path is not None:
        self._prometheus_target_config_update(prometheus_target_config_path)
      else:
        log.debug("Skipped writing Prometheus static target configuration "
                  "files: Output file not configured")
      for result_name, result in self.results_map.iteritems():
        with self._lock:
          if self.phase != Phase.RUN:
            log.debug("Skipping scenario results update while in %s phase",
                      self.phase.name)
            return
        if isinstance(result, ClusterResult):
          if isinstance(self.cluster, AcropolisCluster) and (
            (result.metric_name == "NetReceived.Avg.KilobytesPerSecond") or
            (result.metric_name == "NetTransmitted.Avg.KilobytesPerSecond")):
            log.debug("Skipping result update since AHV API "
                      "does not return valid results for network statistics")
            continue
        result_pbs.extend(result.get_result_pbs())
    except Exception:
      log.exception("An exception occurred while updating scenario results. "
                    "This operation will be retried.")
    else:
      with self._lock:
        self.__result_pbs = result_pbs

  def _prometheus_target_config_update(self, filepath):
    """
    Update the on-disk Prometheus target configurations.

    Args:
      filepath (str): Absolute path to the target configuration file.
    """
    targets = prometheus.scenario_target_config(self)
    OsUtil.write_and_rename(filepath, json.dumps(targets, sort_keys=True))
    log.debug("Wrote Prometheus config for %d VMs to %r",
              len(targets), filepath)

  def _cluster_results_update(self, start_time_secs=None):
    """
    Update the in-memory and on-disk cluster results.

    Returns:
      int or None: Epoch time passed through from
        ScenarioUtil.append_cluster_stats, or None if no data was collected.
    """
    cluster_stats_dir = self.cluster_stats_dir()
    if cluster_stats_dir is None:
      log.warning("Cluster results not collected because the output directory "
                  "is not set")
      return None
    try:
      # If data has been collected before, get the data after the previous
      # collection. Otherwise, latest_appended_time_secs is None, and the
      # default number of the latest samples will be collected.
      results_map = self.cluster.collect_performance_stats(
        start_time_secs=start_time_secs)
    except Exception:
      log.exception("An exception occurred while updating cluster results. "
                    "This operation will be retried.")
      return None
    else:
      csv_path = os.path.join(cluster_stats_dir, "cluster_stats.csv")
      appended_time_secs = ScenarioUtil.append_cluster_stats(
        results_map, self.cluster_stats_dir())
      # Write the results in CSV format for easier analysis.
      if os.path.isfile(csv_path):
        mode = "a"
        header = False
      else:
        mode = "w"
        header = True
      with open(csv_path, mode) as csv_file:
        csv_file.write(
          ScenarioUtil.results_map_to_csv(results_map, header=header))
      return appended_time_secs
