#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
from curie.name_util import NameUtil
from curie.steps._base_step import GenericStep, BaseStep
from curie.steps.meta import MetaStep
from curie.steps.nodes import PowerOn, WaitForPowerOn


class CleanUp(MetaStep):
  """
  Power on nodes, power off and delete Curie VMs, remove templates.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, annotate=False):
    super(CleanUp, self).__init__(scenario, annotate=annotate)
    self.description = "Cluster cleanup"

  def _itersteps(self):
    """
    Yields steps to clean up the cluster.
    """
    nodes = self.scenario.cluster.nodes()
    for index, node in enumerate(nodes):
      if node.power_management_is_configured():
        yield PowerOn(self.scenario, nodes=index, wait_secs=0)
    for index, node in enumerate(nodes):
      if node.power_management_is_configured():
        yield WaitForPowerOn(self.scenario, nodes=index)
    yield PowerOffCurieVMs(self.scenario)
    yield DeleteCurieVMs(self.scenario)
    yield GenericStep(self.scenario, "Deleting templates").set_callable(
      self.scenario.cluster.cleanup)


class PowerOffCurieVMs(BaseStep):
  """
  Power off all Curie VMs on the cluster.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, annotate=False):
    super(PowerOffCurieVMs, self).__init__(scenario, annotate=annotate)
    self.description = "Powering off all Curie VMs"

  def _run(self):
    curie_vms = _get_curie_vms(self.scenario)
    self.create_annotation("Powering off all Curie VMs")
    self.scenario.cluster.power_off_vms(curie_vms)
    self.create_annotation("Finished powering off all Curie VMs")


class DeleteCurieVMs(BaseStep):
  """
  Delete all Curie VMs on the cluster.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """
  def __init__(self, scenario, annotate=False):
    super(DeleteCurieVMs, self).__init__(scenario, annotate=annotate)
    self.description = "Deleting all Curie VMs"

  def _run(self):
    curie_vms = _get_curie_vms(self.scenario)
    self.create_annotation("Deleting all Curie VMs")
    self.scenario.cluster.delete_vms(curie_vms)
    self.create_annotation("Finished deleting all Curie VMs")


def _get_curie_vms(scenario):
  vms = scenario.cluster.vms()
  curie_vm_names, _ = NameUtil.filter_test_vm_names(
    [vm.vm_name() for vm in vms], [])
  return [vm for vm in vms if vm.vm_name() in curie_vm_names]
