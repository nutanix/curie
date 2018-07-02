#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import time

log = logging.getLogger(__name__)


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
    self.description = None
    self._annotate = annotate
    self.__start_time_epoch = None
    self.__finish_time_epoch = None

  def __str__(self):
    phase = self.scenario.phase
    phase_str = " (%s)" % phase.name if phase else ""
    return self.__class__.__name__ + phase_str

  def __call__(self):
    """Perform this step's action.
    """
    log.info("%s: Starting %s", self.scenario, self)
    self.__start_time_epoch = time.time()
    self.verify()
    ret = self._run()
    # Finish time is only recorded if no exceptions are raised.
    self.__finish_time_epoch = time.time()
    # Lower-case the first letter in the step's description
    log.info("%s: Finished %s in %d seconds.", self.scenario, self.description,
             self.elapsed_secs())
    return ret

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
      if phase is None or phase == phase.kRun:
        log.info("Annotation (%s): %s", self, text)
        return self.scenario.create_annotation(text)
      else:
        log.debug("Hidden setup/teardown annotation (%s): %s", self, text)
    else:
      log.debug("Disabled annotation (%s): %s", self, text)

  def elapsed_secs(self):
    if self.has_started():
      if self.has_finished():
        end_time = self.__finish_time_epoch
      else:
        end_time = time.time()
      return end_time - self.__start_time_epoch
    return None

  def has_started(self):
    return self.__start_time_epoch is not None

  def has_finished(self):
    return self.__finish_time_epoch is not None

  def is_running(self):
    return self.has_started() and not self.has_finished()

  def _run(self):
    raise NotImplementedError()
