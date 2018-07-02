#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import logging

from curie.exception import CurieTestException
from curie.test.steps._base_step import BaseStep

log = logging.getLogger(__name__)


class CleanUp(BaseStep):
  """Pass-through to curie.cluster.Cluster.cleanup.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, annotate=False):
    super(CleanUp, self).__init__(scenario, annotate=annotate)
    self.description = "Cluster cleanup"

  def _run(self):
    """Pass-through to curie.cluster.Cluster.cleanup.
    """
    self.create_annotation("Starting cluster cleanup")
    try:
      self.scenario.cluster.cleanup()
    except Exception as exc:
      log.exception("Unhandled exception during cluster cleanup")
      raise CurieTestException(
        "Cluster cleanup on %s failed. One or more objects may require manual "
        "cleanup, such as VMs, snapshots, protection domains, etc. Cleanup "
        "will be reattempted at the beginning of the next test.\n\n%s" %
        (self.scenario.cluster.name(), exc))
    self.create_annotation("Finished cluster cleanup")
