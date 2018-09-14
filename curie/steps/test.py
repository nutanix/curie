#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import time

from curie.exception import CurieTestException
from curie.steps._base_step import BaseStep

log = logging.getLogger(__name__)


class Wait(BaseStep):
  """Suspend test execution for a given number of seconds.

  The test is not guaranteed to wait for the given number of seconds if the
  test is marked to stop.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    duration_secs (int): Number of seconds to block.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, duration_secs, annotate=False):
    """
    Raises:
      CurieTestException:
        If duration_secs is not greater than zero.
    """
    super(Wait, self).__init__(scenario, annotate=annotate)
    if duration_secs <= 0:
      raise CurieTestException(
        cause=
        "'duration_secs' must be greater than zero (got '%s')." %
        duration_secs,
        impact=
        "The scenario '%s' is not valid, and can not be started." %
        self.scenario.display_name,
        corrective_action=
        "If you are the author of the scenario, please check the syntax of "
        "the %s step; Ensure that the 'duration_secs' parameter is greater "
        "than zero." % self.name)
    self.duration_secs = duration_secs
    self.description = "Waiting for %d seconds" % self.duration_secs

  def _run(self):
    """Suspend test execution for a given number of seconds.

    Returns:
      int: Number of seconds slept.
    """
    waited_secs = 0
    self.create_annotation("Waiting for %d seconds" % self.duration_secs)
    while not self.scenario.should_stop() and waited_secs < self.duration_secs:
      time.sleep(1)
      waited_secs = self.elapsed_secs()
    if self.scenario.should_stop():
      log.warning("Test stopped before finished waiting (%d of %d seconds)",
                  waited_secs, self.duration_secs)
    else:
      self.create_annotation("Waited for %d seconds" % waited_secs)
    return waited_secs
