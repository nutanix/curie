#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
from curie.steps.nodes import Shutdown, PowerOn
from curie.steps.vm_group import MigrateGroup, ResetGroupPlacement


class MetaStep(object):
  """
  Group a sequence of steps together, which are expanded at run time.

  MetaSteps may yield instances of BaseStep, or instances of other
  MetaSteps.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True (default), annotate key points in the step
      in the test's results.
  """
  def __init__(self, scenario, annotate=False):
    self.scenario = scenario
    self.name = self.__class__.__name__
    self.description = None
    self._annotate = annotate

  def __call__(self, *args, **kwargs):
    for step in self.itersteps():
      step()

  def itersteps(self):
    """
    Iterate over each of the substeps.

    This is the standard, public interface which adds some hierarchical
    information to each substep's `name` and `description` fields.
    Subclasses should not need to override this method, and instead
    should implement _itersteps().

    Yields:
      BaseStep: Each substep, in execution order.
    """
    for step in self._itersteps():
      # If the step is a MetaStep, iterate over it (and so on).
      if isinstance(step, MetaStep):
        for nested_step in step.itersteps():
          yield nested_step
      else:
        # Give the step a dotted name to make debugging easier, e.g.
        # ClusterCleanup.PowerOffCurieVMs.
        if step.name is not None and self.name is not None:
          step.name = "%s.%s" % (self.name, step.name)
        if step.description is not None and self.description is not None:
          step.description = "%s - %s" % (self.description, step.description)
        yield step

  def _itersteps(self):
    """
    Yield instances of BaseStep or MetaStep in execution order.
    """
    raise NotImplementedError()


class RollingUpgrade(MetaStep):
  """
  Perform a rolling upgrade sequence.

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
  def __init__(self, scenario, node_count=None, annotate=True):
    super(RollingUpgrade, self).__init__(scenario, annotate=annotate)
    self.description = "Rolling upgrade"
    self.requested_node_count = node_count

  def _itersteps(self):
    """
    Yields steps to perform a simulated rolling upgrade sequence.
    """
    for reboot_node_num in range(self._node_count()):
      dest_node_num = (reboot_node_num + 1) % self.scenario.cluster.node_count()
      for vm_group_name in self.scenario.vm_groups.iterkeys():
        yield MigrateGroup(
          self.scenario, vm_group_name, reboot_node_num, dest_node_num)
      yield Shutdown(self.scenario, reboot_node_num, annotate=self._annotate)
      yield PowerOn(self.scenario, reboot_node_num, annotate=self._annotate)
      for vm_group_name in self.scenario.vm_groups.iterkeys():
        yield ResetGroupPlacement(self.scenario, vm_group_name)

  def _node_count(self):
    """
    Get the node count.

    This should only be called after self.scenario.cluster is set.

    Returns:
      int: Node count.
    """
    if self.requested_node_count is None:
      return self.scenario.cluster.node_count()
    else:
      return self.requested_node_count
