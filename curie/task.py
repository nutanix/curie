#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import logging
import threading
import time

from pyVmomi import vmodl

from curie.acropolis_types import AcropolisTaskInfo
from curie.exception import CurieTestException
from curie.json_util import Enum
from curie.util import get_optional_vim_attr, ReleaseLock
from curie.vmm_client import VmmClientException

log = logging.getLogger(__name__)


class TaskStatus(Enum):
  kUnknown = -1
  kNotStarted = 0
  kPending = 1
  kExecuting = 2
  kTimedOut = 3
  kNotFound = 4
  kCanceled = 5
  kFailed = 6
  kSucceeded = 7
  kInternalError = 8

  @classmethod
  def is_ready(cls, status):
    return status in [cls.kNotStarted, cls.kPending]

  @classmethod
  def is_active(cls, status):
    return status == cls.kExecuting

  @classmethod
  def is_terminal(cls, status):
    return status in [cls.kTimedOut, cls.kNotFound, cls.kCanceled,
                      cls.kFailed, cls.kSucceeded, cls.kInternalError]

  @classmethod
  def cannot_succeed(cls, status):
    return status in [cls.kTimedOut, cls.kCanceled, cls.kFailed,
                      cls.kInternalError]


class TaskState(object):
  def __init__(self, status=TaskStatus.kNotStarted):
    # TaskStatus for this task.
    self.status = status

    # Task return code if available.
    self.return_code = None

    # Regular task output if any.
    self.output = None

    # Task errors if any.
    self.error = None

    # Task progress, percentage as a float 0-100.
    self.progress_pct = None

  def __str__(self):
    terminal_str = "%sTerminal" % (
      "" if TaskStatus.is_terminal(self.status) else "Non-")
    return "[{terminal_str} status: {status}]".format(
      terminal_str=terminal_str, status=self.status.name)


class TaskIdPlaceholder(object):
  """
  Placeholder for task ID on descriptors for tasks which have not been started.
  """
  __slots__ = []

  def __nonzero__(self):
    return False

  def __str__(self):
    return "ID placeholder %s" % id(self)


class TaskDescriptor(object):
  @classmethod
  def get_default_task_type(cls):
    return "Task"

  @classmethod
  def get_task_class(cls):
    return Task

  def __init__(
      self,
      task_id=None,
      task_type=None,
      pre_task_msg="",
      post_task_msg="",
      create_task_func=None,
      task_func_args=None,
      task_func_kwargs=None,
      state=None,
      entity_name="",
      desired_state=TaskStatus.kSucceeded,
      is_complete_func=None,
      *args,
      **kwargs):
    super(TaskDescriptor, self).__init__()

    # Opaque task identifier.
    self.task_id = task_id if task_id is not None else TaskIdPlaceholder()

    # Type of task
    self.task_type = (self.get_default_task_type() if task_type is None
                      else task_type)

    # Callable accepting '*args' and '**kwargs' and returning a Task.
    self.create_task_func = create_task_func

    # Varargs, made available for 'create_task_func'.
    self.task_func_args = () if task_func_args is None else task_func_args

    # Additional keyword args, made available for 'create_task_func'.
    self.task_func_kwargs = ({} if task_func_kwargs is None
                             else task_func_kwargs)

    # Log message to emit prior to starting the task.
    self.pre_task_msg = pre_task_msg

    # Log message to emit after the task finishes. If 'is_complete_func' is
    # provided, then this message will be emitted only if 'is_complete_func'
    # evalues to True.
    self.post_task_msg = post_task_msg

    # If not None, this specifies the desired terminal state for the the
    # backing task.
    self.desired_state = desired_state

    # Cached task state. Updated whenever 'update' is called.
    self.state = state if state is not None else TaskState()

    # If not None, a function which evaluates additional conditions on a
    # terminal task to determine whether or not it is to be considered
    # complete.
    self.is_complete_func = is_complete_func

    # Entity name for which task is being executed. If not provided, may also
    # be determened during 'update'.
    self.entity_name = entity_name if entity_name is None else ""

  def is_complete(self):
    if not TaskStatus.is_terminal(self.state.status):
      return False
    return self.is_complete_func() if self.is_complete_func else True

  def has_succeeded(self):
    if self.is_complete():
      if (self.desired_state is not None and
          self.state.status != self.desired_state):
        log.error("%s in terminal state %s (expected %s)",
                  self, self.state.status, self.desired_state)
      else:
        return True
    return False

  def __str__(self):
    return "{type} ({id}){on_entity}: {state}".format(
      type=self.task_type, id=self.task_id,
      on_entity="" if not self.entity_name else " on '%s'" % self.entity_name,
      state=self.state)

  def __repr__(self):
    return "<%s at 0x%x>" % (self.__str__(), hash(self))


class PrismTaskDescriptor(TaskDescriptor):
  @classmethod
  def get_default_task_type(cls):
    return "PrismTask"

  @classmethod
  def get_task_class(cls):
    return PrismTask

class HypervTaskDescriptor(TaskDescriptor):
  @classmethod
  def get_default_task_type(cls):
    return "HypervTask"

  @classmethod
  def get_task_class(cls):
    return HypervTask

class VsphereTaskDescriptor(TaskDescriptor):
  @classmethod
  def get_default_task_type(cls):
    return "vCenterTask"

  @classmethod
  def get_task_class(cls):
    return VsphereTask


class Task(object):
  @classmethod
  def supports_batch_update(cls):
    return False

  @classmethod
  def get_descriptor_class(cls):
    return TaskDescriptor

  @classmethod
  def new(cls, create_task_func, *args, **kwargs):
    return cls.from_descriptor(
      cls.get_descriptor_class()(create_task_func=create_task_func,
                                 task_func_args=args, task_func_kwargs=kwargs))

  @classmethod
  def from_descriptor(cls, descriptor):
    return descriptor.get_task_class()(descriptor=descriptor)

  def __init__(self, descriptor=None, task=None, state=None, task_id=None):
    self._rlock = threading.RLock()

    self._descriptor = descriptor or TaskDescriptor(
      task_id=task_id,
      pre_task_msg="",
      post_task_msg="",
      desired_state=TaskStatus.kSucceeded,
      is_complete_func=None,
      state=state
    )

    # Opaque task object.
    self._task = task

  @property
  def _state(self):
    """
    Convenience access to self._descriptor.state.
    """
    return self._descriptor.state

  def id(self):
    with self._rlock:
      return self._descriptor.task_id

  def get_status(self):
    with self._rlock:
      return self._state.status

  def is_ready(self):
    with self._rlock:
      return TaskStatus.is_ready(self._state.status)

  def is_active(self):
    with self._rlock:
      return TaskStatus.is_active(self._state.status)

  def is_terminal(self):
    with self._rlock:
      return TaskStatus.is_terminal(self._state.status)

  def is_cancelable(self):
    return TaskStatus.is_ready(self._state.status)

  def cancel(self):
    if self.is_cancelable():
      return self._cancel()
    return False

  def start(self):
    with self._rlock:
      if not self.is_ready():
        raise RuntimeError("Cannot start task with status '%s'" %
                           self.get_status())
      if not self._descriptor.create_task_func:
        raise RuntimeError("Cannot start task with no 'create_task_func'")

      ret = self._descriptor.create_task_func(
        *self._descriptor.task_func_args, **self._descriptor.task_func_kwargs)

      if isinstance(ret, basestring):
        self._descriptor.task_id = ret
      else:
        self._task = ret
        self._descriptor.task_id = self._task.info.key

  def _cancel(self):
    with self._rlock:
      if TaskStatus.is_ready(self._state.status):
        self._state.status = TaskStatus.kCanceled
        return True
    log.error("Post-scheduling cancelation not supported for %s",
              type(self).__name__)
    return False

  def __str__(self):
    return str(self._descriptor)


class PrismTask(Task):
  __PRISM_STATUS_CURIE_STATUS_MAP__ = {
    "aborted": TaskStatus.kCanceled,
    "failed": TaskStatus.kFailed,
    "queued": TaskStatus.kPending,
    "running": TaskStatus.kExecuting,
    "succeeded": TaskStatus.kSucceeded,
  }

  @classmethod
  def supports_batch_update(cls):
    return True

  @classmethod
  def get_descriptor_class(cls):
    return PrismTaskDescriptor

  @classmethod
  def from_task_id(cls, prism, task_id):
    desc = TaskDescriptor(
      task_id=task_id, state=TaskState(TaskStatus.kExecuting))
    return cls(prism, descriptor=desc)

  def __init__(self, prism_client,
               descriptor=None, task=None, state=None, task_id=None):
    super(PrismTask, self).__init__(descriptor=descriptor,
                                    task=task, state=state, task_id=task_id)
    self._prism_client = prism_client
    self.create_time = -1

  def update_from_json(self, task_json):
    """
    Updates state from JSON response 'task_json'.
    """
    task_info = AcropolisTaskInfo(**task_json)
    self._state.output = task_info
    self._state.progress_pct = task_info.percentage_complete
    self._state.status = self.__PRISM_STATUS_CURIE_STATUS_MAP__.get(
      task_info.progress_status.lower(), TaskStatus.kUnknown)
    if self._state.status == TaskStatus.kFailed:
      self._state.error = "%s: %s" % (task_info.meta_response.error,
                                      task_info.meta_response.error_detail)
    self.create_time = task_info.create_time

  def update(self):
    task_json = self._prism_client.tasks_get_by_id(self.id())
    self.update_from_json(task_json)

class HypervTask(Task):
  __VMM_STATUS_CURIE_STATUS_MAP__ = {
    # "aborted": TaskStatus.kCanceled,
    "failed": TaskStatus.kFailed,
    "stopped": TaskStatus.kCanceled,
    # "queued": TaskStatus.kPending,
    "running": TaskStatus.kExecuting,
    "completed": TaskStatus.kSucceeded,
  }

  @classmethod
  def get_descriptor_class(cls):
    return HypervTaskDescriptor

  def update_from_json(self, task_json):
    """
    Updates state from JSON response
    """
    self._state.progress_pct = task_json.get("progress")
    self._state.status = self.__VMM_STATUS_CURIE_STATUS_MAP__.get(
      task_json["state"].lower(), TaskStatus.kUnknown)
    if self._state.status == TaskStatus.kFailed:
      self._state.error = str(task_json.get("error"))
    self._hyperv_log_helper(task_json)

  def start(self):
    with self._rlock:
      if not self.is_ready():
        raise RuntimeError("Cannot start task with status '%s'" %
                           self.get_status())
      if not self._descriptor.create_task_func:
        raise RuntimeError("Cannot start task with no 'create_task_func'")

      response = self._descriptor.create_task_func(
        *self._descriptor.task_func_args, **self._descriptor.task_func_kwargs)
      if len(response) != 1:
        raise CurieTestException(
          "Expected exactly 1 task in response - got %r" % response)

      self._descriptor.task_id = response[0]["task_id"]
      self._descriptor.task_type = response[0]["task_type"]

  def _hyperv_log_helper(self, task_json):
    if self._state.status == TaskStatus.kFailed:
      log_func = log.error
    else:
      log_func = log.info

    msgs = ["Task: %s" % self.id()]
    if "vm_id" in task_json:
      msgs.append("(VM '%s')" % task_json["vm_id"])
    elif "vm_name" in task_json:
      msgs.append("(VM '%s')" % task_json["vm_name"])
    msgs.append(task_json["state"].lower())
    msgs.append("(%s%%)" % self._state.progress_pct)

    log_func(" ".join(msgs))


class VsphereTask(Task):
  """
  Encapsulates a vCenter task executed via pyVim.
  """
  # Map of vCenter states to the corresponding TaskStatus.
  __VIM_STATE_CURIE_STATUS_MAP__ = {
    "success": TaskStatus.kSucceeded,
    "error": TaskStatus.kFailed,
    "queued": TaskStatus.kPending,
    "running": TaskStatus.kExecuting
  }

  @classmethod
  def get_descriptor_class(cls):
    return VsphereTaskDescriptor

  @classmethod
  def from_vim_task(cls, vim_task, desc=None):
    """
    Wraps existing 'vim_task' in a 'VsphereTask'.

    Args:
      vim_task (vim.Task): Task to wrap.

    Returns:
      (VsphereTask) VsphereTask wrapping 'vim_task'.
    """
    if desc is None:
      desc = VsphereTaskDescriptor(pre_task_msg="",
                                   post_task_msg="",
                                   create_task_func=None)
    desc.task_type = vim_task.info.descriptionId
    desc.task_id = vim_task.info.key
    desc.state.entity_name = get_optional_vim_attr(vim_task.info, "entityName")

    task = cls(task=vim_task, descriptor=desc)
    task.update()
    return task

  def update(self):
    with self._rlock:
      try:
        self._state.status = self.__VIM_STATE_CURIE_STATUS_MAP__.get(
          self._task.info.state, TaskStatus.kUnknown)
        if self._state.status == TaskStatus.kUnknown:
          self._state.status = TaskStatus.kInternalError
          self._state.error = "Invalid task state %s for %s" % (
            self._task.info.state, self._task)
        else:
          progress = get_optional_vim_attr(self._task.info, "progress")
          if isinstance(progress, (basestring, int, long)):
            self._state.progress_pct = progress
        # TODO (jklein): This should maybe be copied and persisted so we
        # can safely access it later and/or return copies of it on demand.
        self._state.output = get_optional_vim_attr(self._task, "info")
        self._state.error = get_optional_vim_attr(self._task.info, "error")
        if hasattr(self._state.error, "msg"):
          self._state.error = self._state.error.msg
      except vmodl.fault.ManagedObjectNotFound as err:
        self._state.status = TaskStatus.kNotFound

  def is_cancelable(self):
    with self._rlock:
      try:
        if self.is_terminal():
          return False
        elif self._task:
          return self._task.info.cancelable and not self._task.info.cancelled
        else:
          return super(VsphereTask, self).is_cancelable()
      except vmodl.fault.ManagedObjectNotFound:
        # Accessing vim_task.info raises a ManagedObjectNotFound exception if
        # the associated entity no longer exists. In this case it cannot be
        # canceled and we can safely ignore the exception and return False.
        return False

  def _cancel(self):
    """
    Cancels backing vim task if possible.

    Returns:
      (bool) True if task was canceled (or did not need to be canceled),
        else False.
    """
    with self._rlock:
      if TaskStatus.is_ready(self._state.status):
        self._state.status = TaskStatus.kCanceled
        return True
    vim_task = self._task
    try:
      log.warning("Canceling task %s on %s",
                  vim_task.info.descriptionId, vim_task.info.entityName)
      vim_task.CancelTask()
      self._state.status = TaskStatus.kCanceled
    except vmodl.fault.ManagedObjectNotFound:
      # Accessing vim_task.info raises a ManagedObjectNotFound exception if
      # the associated entity no longer exists. In this case it does not need
      # to be canceled and we can safely ignore the exception and return True.
      log.info("vCenter task no longer exists and doesn't need to be canceled")
      self._state.status = TaskStatus.kNotFound
    except vmodl.fault.InvalidState:
      # Handle case of race with vCenter task which, e.g., completes after we
      # check status but before our CancelTask is issued.
      log.info("Task has already been canceled or completed")
    except vmodl.fault.NotSupported:
      # Handle possibility of race with task becoming uncancelable after we
      # check but before CancelTask is issued.
      log.warning("Task %s on %s is not cancelable, need to wait",
                  vim_task.info.descriptionId, vim_task.info.entityName)
      return False

    return True


class TaskPoller(object):
  """
  TaskPoller manages scheduling and polling 'Task's.
  """
  # Default minimum interval in seconds between polling active tasks.
  DEFAULT_POLL_SECS = 1

  # Default maximum number of concurrent tasks.
  DEFAULT_MAX_PARALLEL = 100

  @classmethod
  def execute_parallel_tasks(cls, task_descriptors=None, tasks=None,
                             poll_secs=DEFAULT_POLL_SECS,
                             max_parallel=DEFAULT_MAX_PARALLEL,
                             timeout_secs=None, raise_on_failure=True,
                             **kwargs):
    """
    Execute tasks specified by 'task_desc_list' in parallel.

    Tasks may be provided by TaskDescriptor or Curie task instances.

    Args:
      task_descriptors (TaskDescriptor|iterable<TaskDescriptor>):
        Descriptor or list of descriptors for tasks to execute.
      tasks (Task|iterable<Task>): Task instance or list of task
        instances to execute.
      poll_secs (number): Optional. Polling period in seconds. Default 1.
      max_parallel (number): Maximum number of concurrent tasks.
      timeout_secs (number|None): Timeout in seconds for all tasks to complete,
        or None for no timeout.
      raise_on_failure (bool): If True, raise a CurieTestException if one or
        more tasks fail. Otherwise, the task map will contain failed tasks.

    Returns:
      (dict<str, ?>) Map of task IDs to task output.

    Raises:
      CurieTestException:
        If one or more tasks fail.
        If any task does not complete within the timeout.
    """
    tid_task_map = {}

    task_descriptors = task_descriptors or []
    if isinstance(task_descriptors, TaskDescriptor):
      task_descriptors = [task_descriptors, ]
    for task_desc in task_descriptors:
      tid_task_map[task_desc.task_id] = Task.from_descriptor(task_desc)

    tasks = tasks or []
    if isinstance(tasks, Task):
      tasks = [tasks, ]
    for task in tasks:
      tid_task_map[task.id()] = task

    ret_map = {}

    if not tid_task_map:
      return ret_map

    # TODO (jklein): Fix the timeout handling that's currently being faked.
    poller = cls(max_parallel, poll_interval_secs=poll_secs, **kwargs)
    map(poller.add_task, tid_task_map.itervalues())
    poller.start()

    # TODO (jklein): Fix timeout descrepancy between AHV and vCenter.
    if not poller.wait_for_all(timeout_secs=timeout_secs):
      # AHV timeout commented out below is faked.
      # timeout_secs=timeout_secs * len(tid_task_map) if timeout_secs else None)
      log.error("Tasks timed out, attempting to cancel remaining tasks")
      num_remaining = poller.stop()
      if num_remaining > 0:
        log.warning(
          "%d tasks were unable to be cancelled, waiting for them to complete",
          num_remaining)
      # TODO (jklein): Find appropriate way to handle whether or not to
      # cancel/wait on uncancelable.
      poller.wait_for_all()
      tasks_succeeded = [task for task in tid_task_map.itervalues()
                         if task._descriptor.has_succeeded()]
      err_msg = "Tasks timed out after '%s' seconds (%d/%d succeeded)" % (
        timeout_secs, len(tasks_succeeded), len(tid_task_map))
      raise CurieTestException(err_msg)

    # TODO (jklein): Thread safety

    for tid in tid_task_map.keys():
      task = tid_task_map[tid]
      if isinstance(tid, TaskIdPlaceholder):
        ret_map[task.id()] = task._state.output
      else:
        assert tid == task.id(), "Task ID mismatch: %s %s" % (tid, task.id())
        ret_map[tid] = task._state.output

    tasks_failed = [task for task in tid_task_map.itervalues()
                    if not task._descriptor.has_succeeded()]
    if tasks_failed:
      for task in tasks_failed:
        log.error("Task failed: %s - %s", task, task._state.error)
        log.debug(task._state.output)
      if raise_on_failure:
        raise CurieTestException("%d of %d tasks failed. See log for more "
                                  "details (most recent error message: '%s')" %
                                  (len(tasks_failed), len(tid_task_map),
                                   task._state.error))

    return ret_map

  @classmethod
  def get_default_timeout_secs(cls):
    return 60 * 5

  @classmethod
  def get_deadline_secs(cls, timeout_secs):
    return time.time() + (cls.get_default_timeout_secs()
                          if timeout_secs is None else timeout_secs)

  @classmethod
  def get_timeout_secs(cls, deadline):
    return cls.get_default_timeout_secs() if deadline is None else max(
      0, deadline - time.time())

  def __init__(self, max_concurrent, poll_interval_secs=DEFAULT_POLL_SECS,
               **kwargs):
    # Maximum number of tasks which should execute concurrently.
    self._max_concurrent = max_concurrent

    # Minimum interval between polling active tasks.
    self._poll_interval_secs = poll_interval_secs

    # Lock protecting the below fields.
    self._rlock = threading.RLock()

    # CV used to block on/notify task completion.
    self._task_complete_cv = threading.Condition(self._rlock)

    # Map of task IDs to task instances, pending tasks are not included.
    self._id_task_map = {}

    # Pending tasks
    self._task_queue = []

    # Set of task IDs for currently executing tasks.
    self._active_task_id_set = set()

    # Total number of tasks (including those not in the queue as they were
    # already started prior to being added to the poller.
    self._total_task_count = 0

    # Number of tasks yet to complete.
    self._remaining_task_count = 0

    # List of completed task IDs.
    self._completed_task_id_list = []

    # Map of waiter thread IDs to their current index in the completed ID list.
    self._thread_id_completed_task_index_map = {}

    # Thread which manages scheduling and polling tasks.
    self._thread = None
    self._thread_exception = None

  def add_task(self, task):
    """
    Adds 'task' to the batch.
    """
    with self._rlock:
      if self._thread != None:
        raise Exception("Cannot add task to in-flight batch")
      self._total_task_count += 1
      if task.is_active():
        self._active_task_id_set.add(task.id())
        self._id_task_map[task.id()] = task
      else:
        self._task_queue.append(task)
      self._remaining_task_count += 1

  def wait_for(self, task_id=None, timeout_secs=None):
    """
    Waits for a task to complete.

    Args:
      task_id (None|str): If provided, it waits until the specified task is
        complete, otherwise it waits until any task is complete.
      timeout_secs (None|Number): If provided, timeout in seconds.

    Returns:
      (list<str>) List of task IDs which completed since the last time this
        thread called wait_for.  If 'task_id' is not None, this set is returned
        only if it contains 'task_id'.
      (None) Timeout, or tasks finished completed and requested 'task_id' was
        never observed.
    """
    deadline = self.get_deadline_secs(timeout_secs)
    with self._task_complete_cv:
      while self._remaining_task_count > 0:
        self._task_complete_cv.wait(timeout=self.get_timeout_secs(deadline))
        if self._thread_exception:
          raise CurieTestException(
            "Unhandled exception occurred in %s while waiting for tasks: %s" %
            (self.__class__.__name__, self._thread_exception))
        completed_tids = self._get_recently_completed_task_ids()
        if task_id is None or task_id in completed_tids:
          return completed_tids
        elif self.get_timeout_secs(deadline) == 0:
          return None

      # Handle (rare) case where all tasks are completed before the CV is first
      # acquired.
      return self._get_recently_completed_task_ids()

  def wait_for_all(self, timeout_secs=None):
    """
    Waits for all tasks to complete.

    Args:
      timeout_secs (None|Number): If provided, maximum wait time in seconds.

    Returns:
      (bool) True if tasks are complete, False if 'timeout_secs' is not None
        and tasks did not complete within the timeout.
    """
    deadline = self.get_deadline_secs(timeout_secs)
    with self._task_complete_cv:
      if self._thread == None:
        raise Exception("Cannot wait, batch not started")

      while (self._remaining_task_count > 0 and
             (deadline is None or time.time() < deadline)):
        self.wait_for(timeout_secs=self.get_timeout_secs(deadline))
      return self._remaining_task_count == 0

  def start(self):
    """
    Starts executing tasks asynchronously.
    """
    with self._rlock:
      if self._thread != None:
        raise Exception("Batch already started")
      self._thread = threading.Thread(target=self._run, name="task_poller")
      self._thread.daemon = True
      self._thread.start()

  def stop(self):
    """
    Attempts to cancel all remaining non-terminal tasks.

    Returns:
      (int) Number of remaining tasks which could not be canceled.
    """
    with self._rlock:
      for task in self._task_queue:
        if task.cancel():
          self._active_task_id_set.discard(task.id())
          self._remaining_task_count -= 1

      return self._remaining_task_count

  def _get_recently_completed_task_ids(self):
    """
    NB: Assumes the lock is held. Meant to be called from within 'wait_for'.

    Returns:
      list<str>: List of task IDs completed since the active thread's previous
        call to this method. If the active thread has not yet called this
        method, returns the list of all task IDs completed since polling
        started.
    """
    curr_thread = threading.current_thread()
    index = self._thread_id_completed_task_index_map.setdefault(curr_thread, 0)
    ret = list(self._completed_task_id_list[index:])
    self._thread_id_completed_task_index_map[curr_thread] = len(
      self._completed_task_id_list) - 1

    return ret

  def _poll_active_tasks(self):
    """
    Polls status for the currently active tasks and updates as appropriate.
    """
    with self._task_complete_cv:
      # Avoid log messages if there is nothing to poll.
      if self._remaining_task_count == 0 or not self._active_task_id_set:
        return

      # Capture timestamp to allow determining appropriate sleep based on the
      # configured polling interval.
      t0 = time.time()

      log.info("Polling %d active tasks... (%d / %d tasks remaining)",
               len(self._active_task_id_set), self._remaining_task_count,
               self._total_task_count)

      completed_task_id_set = set()
      for tid in self._active_task_id_set:
        task = self._id_task_map[tid]
        try:
          task.update()
        except BaseException:
          log.exception("Uncaught exception polling task %s", task)
          task._state.status = TaskStatus.kInternalError
        if task._descriptor.is_complete():
          log.info("Completed %s (%s%%) %s", task, task._state.progress_pct,
                   task._state.error if task._state.error else "")
          if task._descriptor.post_task_msg:
            log.info(task._descriptor.post_task_msg)
          completed_task_id_set.add(tid)
          self._remaining_task_count -= 1
      if completed_task_id_set:
        self._active_task_id_set -= completed_task_id_set
        self._completed_task_id_list.extend(completed_task_id_set)
        self._task_complete_cv.notify_all()
      # Release lock and ensure we don't poll more frequently than configured.
      # Even if no sleep is required, call sleep(0) to allow the scheduler to
      # thread switch.
      with ReleaseLock(self._task_complete_cv, recursive=True):
        time.sleep(max(0, self._poll_interval_secs - (time.time() - t0)))

  def _run(self):
    """
    Executes tasks, preserving queue ordering and enforcing max concurrency.

    Should be executed in a dedicated thread.
    """
    with self._task_complete_cv:
      try:
        for task in self._task_queue:
          while len(self._active_task_id_set) == self._max_concurrent:
            self._poll_active_tasks()

          if task.is_ready():
            task.start()
            tid = task.id()
            self._id_task_map[tid] = task
            self._active_task_id_set.add(tid)
            log.info("Started %s: %s", tid, task._descriptor.pre_task_msg or "")

        while self._active_task_id_set:
          self._poll_active_tasks()
      except BaseException as exc:
        self._thread_exception = exc
        log.exception("Caught exception in main poll thread")
        raise
      finally:
        self._task_complete_cv.notify_all()


class PrismTaskPoller(TaskPoller):
  """
  Task poller which handles simulating batched task polling.
  """

  def __init__(self, max_concurrent,
               poll_interval_secs=TaskPoller.DEFAULT_POLL_SECS,
               prism_client=None, cutoff_usecs=None, **kwargs):
    """
    Args:
      max_concurrent (number): Maximum number of tasks which should execute
        concurrently.
      poll_interval_secs (number): Minimum interval between polling active
        tasks.
      prism_client (NutanixRestApiClient): Prism client instance with which to
        poll tasks.
      cutoff_usecs (number): Cutoff timestamp in usecs. Tasks older than this
        timestamp are excluded from polling.
      **kwargs (dict): Additional kwargs, passed to 'super.__init__'.
    """
    if prism_client is None:
      raise RuntimeError("Must provide a Prism client")

    self._prism_client = prism_client
    super(PrismTaskPoller, self).__init__(
      max_concurrent, poll_interval_secs=poll_interval_secs, **kwargs)

    if cutoff_usecs is None:
      cutoff_usecs = \
        self._prism_client.get_cluster_timestamp_usecs()
    self._cutoff_usecs = cutoff_usecs

  def _fetch_tasks_since_epoch_ms(self, epoch_usecs):
    """
    Lookup all tasks created since the millisecond epoch timestamp
    'epoch_usecs'.
    """
    return dict((t["uuid"], t) for t in self._prism_client.tasks_get(
      include_completed=True, start_time_usecs=epoch_usecs)["entities"])

  def _poll_active_tasks(self):
    """
    Polls status for the currently active tasks and updates as appropriate.
    """
    with self._task_complete_cv:
      # Avoid log messages if there is nothing to poll.
      if self._remaining_task_count == 0 or not self._active_task_id_set:
        return

      # Capture timestamp to allow determining appropriate sleep based on the
      # configured polling interval.
      t0 = time.time()

      log.info("Polling %d active tasks... (%d / %d tasks remaining)",
               len(self._active_task_id_set), self._remaining_task_count,
               self._total_task_count)

      tasks_json = self._fetch_tasks_since_epoch_ms(self._cutoff_usecs)
      completed_task_id_set = set()
      for tid in self._active_task_id_set:
        if tid not in tasks_json:
          continue
        task = self._id_task_map[tid]
        task.update_from_json(tasks_json[tid])
        if task._descriptor.is_complete():
          log.info("Completed %s (%s%%) %s", task, task._state.progress_pct,
                   task._state.error if task._state.error else "")
          if task._descriptor.post_task_msg:
            log.info(task._descriptor.post_task_msg)
          completed_task_id_set.add(tid)
          self._remaining_task_count -= 1
      if completed_task_id_set:
        self._active_task_id_set -= completed_task_id_set
        self._completed_task_id_list.extend(completed_task_id_set)
        self._task_complete_cv.notify_all()
      # Release lock and ensure we don't poll more frequently than configured.
      # Even if no sleep is required, call sleep(0) to allow the scheduler to
      # thread switch.
      with ReleaseLock(self._task_complete_cv, recursive=True):
        time.sleep(max(0, self._poll_interval_secs - (time.time() - t0)))


class HypervTaskPoller(TaskPoller):
  """
  Task poller which handles simulating batched task polling.
  """

  def __init__(self, max_concurrent,
               poll_interval_secs=TaskPoller.DEFAULT_POLL_SECS,
               vmm=None):
    """
    Args:
      max_concurrent (number): Maximum number of tasks which should execute
        concurrently.
      poll_interval_secs (number): Minimum interval between polling active
        tasks.
      prism_client (NutanixRestApiClient): Prism client instance with which to
        poll tasks.
      cutoff_usecs (number): Cutoff timestamp in usecs. Tasks older than this
        timestamp are excluded from polling.
      **kwargs (dict): Additional kwargs, passed to 'super.__init__'.
    """
    if vmm is None:
      raise RuntimeError("Must provide a Vmm client")

    self._vmm = vmm
    super(HypervTaskPoller, self).__init__(
      max_concurrent, poll_interval_secs=poll_interval_secs)

  def _poll_active_tasks(self):
    """
    Polls status for the currently active tasks and updates as appropriate.
    """
    with self._task_complete_cv:
      # Avoid log messages if there is nothing to poll.
      if self._remaining_task_count == 0 or not self._active_task_id_set:
        return

      # Capture timestamp to allow determining appropriate sleep based on the
      # configured polling interval.
      t0 = time.time()

      log.info("Polling %d active tasks... (%d / %d tasks remaining)",
               len(self._active_task_id_set), self._remaining_task_count,
               self._total_task_count)

      active_task_descriptors = [self._id_task_map[task_id]._descriptor
                                 for task_id in self._active_task_id_set]
      updated_task_list = self._vmm.vm_get_job_status([
        {"task_id": descriptor.task_id, "task_type": descriptor.task_type}
        for descriptor in active_task_descriptors
      ])
      updated_tasks = {task["task_id"]: task for task in updated_task_list}

      completed_task_id_set = set()
      for tid in self._active_task_id_set:
        task = self._id_task_map[tid]
        task.update_from_json(updated_tasks[task.id()])
        if task._descriptor.is_complete():
          log.info("Completed %s (%s%%) %s", task, task._state.progress_pct,
                   task._state.error if task._state.error else "")
          if task._descriptor.post_task_msg:
            log.info(task._descriptor.post_task_msg)
          completed_task_id_set.add(tid)
          self._remaining_task_count -= 1
      if completed_task_id_set:
        self._active_task_id_set -= completed_task_id_set
        self._completed_task_id_list.extend(completed_task_id_set)
        self._task_complete_cv.notify_all()
      # Release lock and ensure we don't poll more frequently than configured.
      # Even if no sleep is required, call sleep(0) to allow the scheduler to
      # thread switch.
      with ReleaseLock(self._task_complete_cv, recursive=True):
        time.sleep(max(0, self._poll_interval_secs - (time.time() - t0)))

  def stop(self):
    """
    Attempts to cancel all remaining non-terminal tasks.

    Returns:
      (int) Number of remaining tasks which could not be canceled.
    """
    log.info("Stopping all running tasks")
    active_task_descriptors = [self._id_task_map[task_id]._descriptor
                               for task_id in self._active_task_id_set]
    remaining_tasks = self._vmm.vm_stop_job(
      [{"task_id": descriptor.task_id, "task_type": descriptor.task_type}
        for descriptor in active_task_descriptors])

    for task in remaining_tasks:
      if task["state"].lower() == "running":
        log.info("Task: %s failed to be canceled", task["task_id"])
      else:
        log.info("Task: %s is in state: %s", task["task_id"], task["state"])
        self._active_task_id_set.discard(task["task_id"])
        self._remaining_task_count -= 1

    return self._remaining_task_count
