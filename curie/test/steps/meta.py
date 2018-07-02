#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

from _base_step import BaseStep
from curie.exception import CurieTestException
from curie.test.steps.nodes import Shutdown, PowerOn
from curie.test.steps.vm_group import MigrateGroup, ResetGroupPlacement


class RollingUpgrade(BaseStep):
  """Perform a rolling upgrade sequence.

  A rolling upgrade is supposed to be an online, sequential upgrade of nodes in
  a cluster. Assume a four node cluster; during a rolling upgrade, VMs from
  the first node are migrated to the second, then the first node is rebooted.
  After the node comes back online, the VMs originally placed on it are
  migrated back and the sequence continues with the second, third, and fourth
  nodes in the cluster.

  The length of the sequence, or number of nodes to reboot, can be limited by
  the 'node_count' parameter. If set, the sequence will start at the first
  node until 'node_count' nodes have been upgraded (rebooted).

  Args:
    scenario (Scenario): Scenario this step belongs to.
    node_count (int): The number of nodes to simulate a rolling upgrade on.
    annotate (bool): If True (default), annotate key points in the step in the
      test's results. Only the shutdown and poweron steps are annotated. VM
      migrations are not annotated.
  """

  @classmethod
  def requirements(cls):
    """
    See 'BaseStep.requirements' for info.
    """
    return super(RollingUpgrade, cls).requirements().union(
      [BaseStep.OOB_CONFIG,])

  def __init__(self, scenario, node_count=None, annotate=True):
    super(RollingUpgrade, self).__init__(scenario, annotate=annotate)
    self.description = "Performing rolling upgrade"
    self.requested_node_count = node_count

  def node_count(self):
    """Get the node count.

    This should only be called after self.scenario.cluster is set.

    Returns:
      int: Node count.
    """
    if self.requested_node_count is None:
      return self.scenario.cluster.node_count()
    else:
      return self.requested_node_count

  def verify(self):
    if self.node_count() > self.scenario.cluster.node_count():
      raise CurieTestException(
        "There aren't enough nodes (%d) in the cluster to perform "
        "RollingUpgrade with a node_count of %d" % (
          self.scenario.cluster.node_count(), self.node_count()))
    if self.node_count() < 1:
      raise CurieTestException(
        "Cannot have a node_count less than 1 for RollingUpgrade.")

  def _run(self):
    """Performs a rolling upgrade sequence for the number of nodes specified.
    """
    for reboot_node_num in range(self.node_count()):
      dest_node_num = (reboot_node_num + 1) % self.scenario.cluster.node_count()
      for vm_group_name in self.scenario.vm_groups.iterkeys():
        MigrateGroup(self.scenario, vm_group_name, reboot_node_num, dest_node_num)()
      Shutdown(self.scenario, reboot_node_num, annotate=self._annotate)()
      PowerOn(self.scenario, reboot_node_num, annotate=self._annotate)()
      for vm_group_name in self.scenario.vm_groups.iterkeys():
        ResetGroupPlacement(self.scenario, vm_group_name)()
