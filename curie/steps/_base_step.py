#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import time

from enum import Enum

from curie.exception import ScenarioStoppedError

log = logging.getLogger(__name__)


class Status(Enum):
  # New states.
  NOT_STARTED = 0
  # Active states.
  EXECUTING = 1
  # Terminal states.
  SUCCEEDED = 2
  FAILED = 3
  STOPPED = 4
  SKIPPED = 5

  def is_new(self):
    """
    Returns True if never started, or False otherwise.
    """
    return self in [Status.NOT_STARTED]

  def is_active(self):
    """
    Returns True if in progress, or False otherwise.
    """
    return self in [Status.EXECUTING]

  def is_terminal(self):
    """
    Returns True if has reached a terminal state, else False.
    """
    return not (self.is_new() or self.is_active())


class BaseStep(object):
  OOB_CONFIG = 1

  @classmethod
  def requirements(cls):
    """
    Gets set of any constraints this step requires be placed on the target.
    """
    return frozenset()

  def __init__(self, scenario, annotate=False):
    self.scenario = scenario
    module_name = self.__class__.__module__.split(".")[-1]
    self.name = "%s.%s" % (module_name, self.__class__.__name__)
    self.description = None
    self.exception = None
    self.status = Status.NOT_STARTED
    self._annotate = annotate
    self.__start_time_epoch = None
    self.__finish_time_epoch = None

  def __str__(self):
    phase = self.scenario.phase
    phase_str = " (%s)" % phase.name if phase else ""
    return self.name + phase_str

  def __call__(self):
    """Perform this step's action.
    """
    log.info("%s: Starting %s", self.scenario, self)
    self.status = Status.EXECUTING
    self.__start_time_epoch = time.time()
    try:
      self.verify()
      ret = self._run()
    except ScenarioStoppedError as exc:
      self.status = Status.STOPPED
      self.exception = exc
      self.__finish_time_epoch = time.time()
      log.warning("%s was stopped after %d seconds", self.description,
                  self.elapsed_secs())
      raise
    except Exception as exc:
      self.status = Status.FAILED
      self.exception = exc
      self.__finish_time_epoch = time.time()
      log.error("%s failed after %d seconds", self.description,
                self.elapsed_secs())
      raise
    else:
      self.status = Status.SUCCEEDED
      self.__finish_time_epoch = time.time()
      log.info("%s succeeded after %d seconds", self.description,
               self.elapsed_secs())
      return ret
    finally:
      log.info("%s: Finished %s", self.scenario, self)

  def verify(self):
    """Subclasses may optionally define this to do some pre-run verification.

    Raises:
      CurieTestException:
        If verification fails.
    """
    log.debug("%s does not require any pre-run verification", self)

  def create_annotation(self, text):
    if self._annotate:
      phase = self.scenario.phase
      if phase is None or phase == phase.RUN:
        log.info("Annotation (%s): %s", self, text)
        return self.scenario.create_annotation(text)
      else:
        log.debug("Hidden setup/teardown annotation (%s): %s", self, text)
    else:
      log.debug("Disabled annotation (%s): %s", self, text)

  def elapsed_secs(self):
    if not self.status.is_new():
      if self.status.is_terminal():
        end_time = self.__finish_time_epoch
      else:
        end_time = time.time()
      return end_time - self.__start_time_epoch
    return 0

  def _run(self):
    raise NotImplementedError()


class GenericStep(BaseStep):
  def __init__(self, scenario, description, annotate=False):
    super(GenericStep, self).__init__(scenario, annotate=annotate)
    self.description = description
    self.func = None
    self.args = None
    self.kwargs = None

  def set_callable(self, func, *args, **kwargs):
    self.func = func
    self.args = args
    self.kwargs = kwargs
    return self

  def _run(self):
    if self.func is None:
      raise ValueError("set_callable() must be called before _run")
    return self.func(*self.args, **self.kwargs)
