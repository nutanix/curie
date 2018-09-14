#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import math
import threading
import time

from curie.exception import CurieTestException
from curie.exception import NoVMGroupDefinedError, NoVMsFoundError
from curie.name_util import NameUtil
from curie.steps._base_step import BaseStep
from curie.steps.test import Wait

log = logging.getLogger(__name__)


class CloneFromTemplate(BaseStep):
  """Deploy a number of VMs based on a template.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group associated with the workload.
    power_on (bool): If True, power on the VM group after clone.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, vm_group_name, power_on=False, annotate=False,
               linked_clone=False):
    """
    Raises:
      CurieTestException:
        If no VM group named vm_group_name exists.
    """
    super(CloneFromTemplate, self).__init__(scenario, annotate=annotate)
    self.power_on = power_on
    self.linked_clone = linked_clone
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Cloning from template" % self.vm_group.name()

  def verify(self):
    if not self.scenario.goldimages_directory:
      raise CurieTestException(
        cause=
        "scenario.goldimages_directory must be set before running %s (value: "
        "%r)" % (self.name, self.scenario.goldimages_directory),
        impact=
        "The scenario '%s' can not be started successfully until Curie is "
        "configured correctly. Other scenarios that deploy goldimages also "
        "can not start." % self.scenario.display_name,
        corrective_action=
        "Please ensure that the 'goldimages_directory' parameter is set "
        "correctly in the Scenario object.")

  def _run(self):
    """Deploy a number of VMs based on a template.

    Returns:
      List of CurieVMs: VMs created.

    Raises:
      CurieException:
        If no image named goldimage_name exists.
      CurieTestException:
        Propagated from __deploy_from_template on failure to deploy base vm.
    """
    clone_placement_map = self.vm_group.get_clone_placement_map()
    base_vm = self.__deploy_from_template()
    vm_names = clone_placement_map.keys()
    node_ids = [node.node_id() for node in clone_placement_map.values()]
    self.create_annotation("%s: Cloning VMs" % self.vm_group.name())
    self.scenario.cluster.clone_vms(vm=base_vm,
                                    vm_names=vm_names,
                                    node_ids=node_ids,
                                    linked_clone=self.linked_clone)
    vms = self.vm_group.lookup_vms_by_name(vm_names)
    self.scenario.cluster.disable_ha_vms(vms)
    self.scenario.cluster.disable_drs_vms(vms)
    self.create_annotation("%s: Finished cloning VMs" %
                           self.vm_group.name())
    if self.power_on:
      PowerOn(self.scenario, self.vm_group.name(), annotate=self._annotate)()
    return vms

  def __deploy_from_template(self):
    """Deploy a VM template.

    If the template has already been deployed on the cluster, this function is
    a no-op.

    Returns:
      CurieVM: VM of the deployed template.

    Raises:
      CurieException:
        If no image named goldimage_name exists.
      CurieTestException:
        If the VM fails to be created properly.
    """
    image_name = "%s_%s" % (self.vm_group.template_name(),
                            NameUtil.sanitize_filename(self.vm_group.name()))
    vm_name = NameUtil.goldimage_vm_name(self.scenario, image_name)
    vm = self.scenario.cluster.find_vm(vm_name)
    if vm is not None:
      return vm
    node_id = self.scenario.cluster.nodes()[0].node_id()
    if self.vm_group.template_type() == "OVF":
      return self.scenario.cluster.import_vm(
        self.scenario.goldimages_directory,
        self.vm_group.template_name(),
        vm_name,
        node_id=node_id)
    elif self.vm_group.template_type() == "DISK":
      return self.scenario.cluster.create_vm(
        self.scenario.goldimages_directory,
        self.vm_group.template_name(),
        vm_name,
        node_id=node_id,
        vcpus=self.vm_group.vcpus(),
        ram_mb=self.vm_group.ram_mb(),
        data_disks=self.vm_group.data_disks())


class CreatePeriodicSnapshots(BaseStep):
  """Create a number of snapshots periodically on a VM group.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group associated with the snapshots.
    num_snapshots (int): Number of snapshots to take.
    interval_minutes (int): Amount of time to wait between snapshots.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
    async (bool): If False, blocks until all snapshots are created.
      If True, step returns immediately, and a daemon thread is run to create
      all snapshots in the background.
  """

  def __init__(self, scenario, vm_group_name, num_snapshots,
               interval_minutes, annotate=True, async=False):
    """
    Raises:
      CurieTestException:
        If no VM group named vm_group_name exists.
    """
    super(CreatePeriodicSnapshots, self).__init__(scenario, annotate=annotate)
    self.num_snapshots = num_snapshots
    self.num_intervals = num_snapshots - 1
    self.interval_minutes = interval_minutes
    self.nutanix_snapshot_id_set = set()
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Creating periodic snapshots" % self.vm_group.name()
    self.async = async
    self.snapshot_num = 0

  def _run(self):
    if self.async:
      log.info("Creating snapshots asynchronously in the background thread")
      thread = threading.Thread(target=self._periodic_snapshots_func)
      thread.daemon = True
      thread.start()
    else:
      log.info("Creating snapshots synchronously")
      self._periodic_snapshots_func()

  def _periodic_snapshots_func(self):
    """Create a number of snapshots periodically on a VM group.

    This function blocks for up to
    interval_minutes * (num_snapshots - 1) minutes. It may block for less
    time if the test is marked to stop.

    Raises:
      CurieTestException:
        If the test's cluster's hypervisor is unsupported.
        Propagated from __create_nutanix_snapshots.
    """
    log.info("Creating %d periodic snapshots (period = %d minutes(s))",
             self.num_snapshots, self.interval_minutes)
    sanitized_name = NameUtil.sanitize_filename(self.vm_group.name())
    snapshot_tag = NameUtil.entity_name(
      self.scenario, "%s_%d" % (sanitized_name, self.num_snapshots))
    for snapshot_idx in xrange(self.num_snapshots):
      if self.scenario.should_stop():
        log.warning("Test stopped (created %d snapshots of %d)",
                    snapshot_idx, self.num_snapshots)
        break
      else:
        self.snapshot_num = snapshot_idx + 1
        self.scenario.cluster.snapshot_vms(self.vm_group.get_vms(),
                                           tag=snapshot_tag)
        self.create_annotation("%s: Snapshot %d" % (self.vm_group.name(),
                                                    snapshot_idx + 1))
        if snapshot_idx < self.num_intervals:
          Wait(self.scenario, self.interval_minutes * 60)()


class PowerOff(BaseStep):
  """Power off a group of VMs, and wait for the power state to be off.

  This is analogous to shutting off the power to the VMs; This does not
  perform a clean shutdown.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group to power off.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, vm_group_name, annotate=False):
    """
    Raises:
      CurieTestException:
        If no VM group named vm_group_name exists.
    """
    super(PowerOff, self).__init__(scenario, annotate=annotate)
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Powering off VMs" % self.vm_group.name()

  def _run(self):
    """Power off a group of VMs, and wait for the power state to be off.

    Returns:
      List of CurieVMs: VMs created.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
    """
    vms = self.vm_group.get_vms()
    if not vms:
      raise NoVMsFoundError("Failed to power off VM Group '%s'" %
                            self.vm_group.name(), self)
    self.create_annotation("%s: Powering off VMs" % self.vm_group.name())
    self.scenario.cluster.power_off_vms(vms)
    return self.vm_group.get_vms()


class PowerOn(BaseStep):
  """Power on a group of VMs, and wait for IP addresses to be assigned.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group to power off.
    max_concurrent (int): Maximum number of VMs to power on at the same time.
      Used to limit the severity of a "boot storm".
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, vm_group_name, max_concurrent=100,
               annotate=False):
    """
    Raises:
      CurieTestException:
        If no VM group named vm_group_name exists.
    """
    super(PowerOn, self).__init__(scenario, annotate=annotate)
    self.max_concurrent = max_concurrent
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Powering on VMs" % self.vm_group.name()

  def _run(self):
    """Power on a group of VMs, and wait for the VMs to become accessible.

    Returns:
      List of CurieVMs: VMs powered on.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If IP addresses are not assigned to the VMs within the timeout.
    """
    vms = self.vm_group.get_vms()
    if not vms:
      raise NoVMsFoundError("Failed to power on VM Group '%s'" %
                            self.vm_group.name(), self)
    self.create_annotation("%s: Powering on VMs" % self.vm_group.name())
    try:
      self.scenario.cluster.power_on_vms(
        vms, max_parallel_tasks=self.max_concurrent)
    except Exception:
      raise CurieTestException(
        cause=
        "An error occurred while powering on VM(s) in VM Group '%s'." %
        self.vm_group.name(),
        impact=
        "One or more VMs either did not transition to a powered on state, or "
        "did not receive an IP address.",
        corrective_action=
        "Please verify that the cluster has enough available resources to "
        "start all of the VMs. Please also check the cluster management "
        "software for any failed VM power on tasks. For information about "
        "which VMs failed to power on correctly, please check "
        "curie.debug.log.")
    # TODO (jklein): See about cleaning up timeout once Prism cleanup is
    # integrated with the task changes. Currently the primary source of lag
    # is discrete Prism queries rather than some form of batching.
    timeout_secs = 60 * int(1 + math.ceil(len(vms) / 20.0))

    def refresh_and_check_accessible():
      vms = self.vm_group.get_vms()
      for vm in vms:
        if not vm.is_accessible():
          log.debug("VM '%s' is not accessible", vm)
          return False
      return True

    try:
      ret = self.scenario.wait_for(
        refresh_and_check_accessible,
        "all VMs in '%s' to become accessible" % self.vm_group.name(),
        timeout_secs=timeout_secs)
    except RuntimeError:
      # Change the error message to be something that's a little more
      # understandable than the generic one returned by wait_for_all.
      raise CurieTestException(
        cause=
        "A timeout occurred waiting for VM(s) in VM Group '%s' to become "
        "responsive within %d seconds." % (self.vm_group.name(), timeout_secs),
        impact=
        "The VM(s) are not responding due to a network connectivity issue, or "
        "because of an unsuccessful startup (boot).",
        corrective_action=
        "Please check the network connectivity to the VMs on the cluster, and "
        "that the VMs received IP addresses in the expected IP subnet. For "
        "information about which VMs were unresponsive and for details about "
        "the assigned IP addresses, please check curie.debug.log.")
    if ret is None:
      log.warning("Test stopped before all VMs were powered on")
    else:
      self.create_annotation(
        "%s: Finished powering on VMs" % self.vm_group.name())
    return vms


class Grow(BaseStep):
  """Grow a VM group by adding new VMs.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group associated with the workload.
    count_per_node (int): Number of VMs in the VM group that should be on each
      node after the resize operation. Can not be used with count_per_cluster.
    count_per_cluster (int): Number of VMs in the VM group that should be on
      the cluster after the resize operation. Can not be used with
      count_per_node.
    annotate (bool): If True, annotate key points in the step in the test's
      results.

  Raises:
    CurieTestException:
      If the VM group does not exist.
      If neither count_per_node nor count_per_cluster are not None.
      If both count_per_node and count_per_cluster are not None.
      If count_per_node is not None, but is None in the VM Group.
      If count_per_cluster is not None, but is None in the VM Group.
      If the value of count_per_node or count_per_cluster matches the existing
        value.
  """

  def __init__(self, scenario, vm_group_name, count_per_node=None,
               count_per_cluster=None, annotate=False, linked_clone=False):
    super(Grow, self).__init__(scenario, annotate=annotate)
    self.count_per_node = count_per_node
    self.count_per_cluster = count_per_cluster
    self.linked_clone = linked_clone
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    if self.count_per_node is None and self.count_per_cluster is None:
      raise CurieTestException(
        cause=
        "Must specify either 'count_per_node' or 'count_per_cluster' in %s." %
        self.name,
        impact=
        "The scenario '%s' is not valid, and can not be started." %
        self.scenario.display_name,
        corrective_action=
        "If you are the author of the scenario, please check the syntax of "
        "the %s step; Ensure that either 'count_per_node' or "
        "'count_per_cluster' is given." % self.name)
    if self.count_per_node is not None and self.count_per_cluster is not None:
      raise CurieTestException(
        cause=
        "Must specify either 'count_per_node' or 'count_per_cluster' in %s, "
        "but not both." % self.name,
        impact=
        "The scenario '%s' is not valid, and can not be started." %
        self.scenario.display_name,
        corrective_action=
        "If you are the author of the scenario, please check the syntax of "
        "the %s step; Ensure that either 'count_per_node' or "
        "'count_per_cluster' is given, but not both." % self.name)
    self.description = "%s: Cloning new VMs" % self.vm_group.name()

  def verify(self):
    if self.count_per_node is not None:
      new_count = self.count_per_node * self.scenario.cluster.node_count()
    else:
      new_count = self.count_per_cluster
    if new_count <= self.vm_group.total_count():
      raise CurieTestException(
        cause=
        "Invalid parameter to %s." % self.name,
        impact=
        "The given 'count_per_node' or 'count_per_cluster' value would "
        "decrease the total VM count, which is not supported by %s. The "
        "scenario '%s' is not valid for this cluster, and can not be "
        "started." % (self.scenario.display_name, self.name),
        corrective_action=
        "If you are the author of the scenario, please check the syntax of "
        "the %s step; Ensure that the new VM count, given by either the "
        "'count_per_node' or 'count_per_cluster' parameters, when used with "
        "a cluster with %d nodes, increases the total number of VMs." %
        (self.name, self.scenario.cluster.node_count()))

  def _run(self):
    """Grow a VM group by adding new VMs.

    Returns:
      List of CurieVM: New VMs added during the resize.

    Raises:
      CurieTestException:
        If no VMs exist in the VM group being resized.
        If the VMs being added fail to clone.
    """
    existing_vms = self.vm_group.get_vms()
    if not existing_vms:
      raise NoVMsFoundError("Failed to resize VM Group '%s'" %
                            self.vm_group.name(), self)
    self.vm_group._count_per_node = self.count_per_node
    self.vm_group._count_per_cluster = self.count_per_cluster
    ResetGroupPlacement(self.scenario, self.vm_group.name(),
                        annotate=False)()
    clone_placement_map = self.vm_group.get_clone_placement_map()
    diff_placement_map = dict()
    existing_vm_names = [vm.vm_name() for vm in existing_vms]
    for vm_name, node in clone_placement_map.iteritems():
      if vm_name not in existing_vm_names:
        diff_placement_map[vm_name] = node.node_id()
    self.create_annotation("%s: Growing VM count from %d to %d" % (
      self.vm_group.name(), len(existing_vms), len(clone_placement_map)))
    self.scenario.cluster.clone_vms(vm=existing_vms[0],
                                    vm_names=diff_placement_map.keys(),
                                    node_ids=diff_placement_map.values(),
                                    linked_clone=self.linked_clone)
    vms = self.vm_group.lookup_vms_by_name(diff_placement_map.keys())
    self.create_annotation("%s: Finished growing VM group" %
                           self.vm_group.name())
    if existing_vms[0].is_powered_on():
      self.scenario.cluster.power_on_vms(vms)
      vms = self.vm_group.lookup_vms_by_name(diff_placement_map.keys())
    return vms


class RunCommand(BaseStep):
  """Run a command on every VM in a VM group.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    vm_group_name (str): Name of VM group to run the command.
    command (str): Command to execute in the shell on the VMs.
    fail_on_error (bool): If True, fail the test if the command returns a
      non-zero exit status.
    user (str): User used to execute the command.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
    timeout_secs (int): Time, in seconds, that 'command' is allowed to run
      before it times out.
  """

  def __init__(self, scenario, vm_group_name, command, fail_on_error=True,
               user="nutanix", annotate=False, timeout_secs=120):
    """
    Raises:
      CurieTestException:
        If no VM group named vm_group_name exists.
    """
    super(RunCommand, self).__init__(scenario, annotate=annotate)
    self.command = command
    self.timeout_secs = timeout_secs
    self.fail_on_error = fail_on_error
    self.user = user
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Executing '%s'" % (self.vm_group.name(),
                                               self.command)

  def _run(self):
    """Run a command on every VM in a VM group.

    Raises:
      CurieTestException:
        If vm_group_name does not match any existing VMs.
        If fail_on_error is True and the command fails on any VM.
    """
    vms = self.vm_group.get_vms()
    if not vms:
      raise NoVMsFoundError("Failed to execute '%s' on VM Group '%s'" %
                            (self.command, self.vm_group.name()), self)
    self.create_annotation("%s: Executing '%s'" % (self.vm_group.name(), self.command))
    for vm in vms:
      cmd_id = "%s_%s_%d" % (vm.vm_name(),
                             NameUtil.sanitize_filename(self.command),
                             time.time() * 1e6)
      try:
        exit_status, _, _ = vm.execute_sync(cmd_id, self.command, self.timeout_secs,
                                            user=self.user)
      except Exception:
        if self.fail_on_error:
          log.exception("An unhandled exception occurred in execute_sync")
          raise
        else:
          log.warning("An unhandled exception occurred in execute_sync",
                      exc_info=True)
      else:
        if exit_status != 0:
          msg = ("VM '%s' command '%s' returned non-zero status" %
                 (vm.vm_name(), self.command))
          if self.fail_on_error:
            raise CurieTestException(
              cause=msg,
              impact="The command did not complete successfully.",
              corrective_action=
              "If you are the author of the scenario, please check the syntax "
              "of the command requested in the %s step." % self.name)
          else:
            log.warning(msg)


class RelocateGroupDatastore(BaseStep):
  def __init__(self, scenario, vm_group_name, dest_datastore_name,
               annotate=False):
    """Relocates an entire VMGroup to a new datastore/container.
    (Storage VMotion).

    Args:
      scenario (Scenario): Scenario this step belongs to.
      vm_group_name (str): Name of VM group to run the command.
      dest_datastore_name (str): The name of the destination datastore.
      annotate (bool): Whether or not to annotate events for this step.
    """
    super(RelocateGroupDatastore, self).__init__(scenario, annotate=annotate)
    self.dest_datastore_name = dest_datastore_name
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = ("%s: Relocating VMs to datastore '%s'" %
                        (self.vm_group.name(), self.dest_datastore_name))

  def _run(self):
    vms = self.vm_group.get_vms()
    datastore_names = [self.dest_datastore_name] * len(vms)
    self.create_annotation("%s: Relocating VMs to datastore '%s'" %
                           (self.vm_group.name(), self.dest_datastore_name))
    self.scenario.cluster.relocate_vms_datastore(vms, datastore_names)
    self.create_annotation("%s: Finished relocating VMs to datastore '%s'" %
                           (self.vm_group.name(), self.dest_datastore_name))


class MigrateGroup(BaseStep):
  def __init__(self, scenario, vm_group_name, from_node, to_node,
               annotate=False):
    """Migrates a VMGroup with VMs on from_node to to_node.

    Args:
      scenario (Scenario): Scenario this step belongs to.
      vm_group_name (str): Name of VM group to run the command.
      from_node (int): The index of a node to migrate VMs from.
      to_node (int): The index of a node to migrate VMs to.
      annotate (bool): Whether or not to annotate events for this step.
    """
    super(MigrateGroup, self).__init__(scenario, annotate=annotate)
    self.from_node_index = int(from_node)
    self.to_node_index = int(to_node)
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = ("%s: Migrating VMs from node '%s' to node '%s'" %
                        (self.vm_group.name(), from_node, to_node))

  def verify(self):
    node_metadata = self.scenario.cluster.metadata().cluster_nodes
    try:
      node_metadata[self.from_node_index].id
    except IndexError:
      raise CurieTestException(
        cause=
        "Invalid 'from_node' index '%s'. Cluster contains %s nodes (max index "
        "is %s)." %
        (self.from_node_index, len(node_metadata), len(node_metadata) - 1),
        impact=
        "The scenario '%s' can not be used with cluster '%s' because the %s "
        "step's 'from_node' parameter is either larger or smaller than the "
        "number of nodes in the cluster (%d)." %
        (self.scenario.display_name, self.scenario.cluster.name(), self.name,
         len(node_metadata)),
        corrective_action=
        "Please retry the scenario using a cluster with a compatible number "
        "of nodes. If you are the author of the scenario, please check the "
        "syntax of the %s step to make it compatible with clusters of this "
        "size." % self.name)
    try:
      node_metadata[self.to_node_index].id
    except IndexError:
      raise CurieTestException(
        cause=
        "Invalid 'to_node' index '%s'. Cluster contains %s nodes (max index "
        "is %s)." %
        (self.to_node_index, len(node_metadata), len(node_metadata) - 1),
        impact=
        "The scenario '%s' can not be used with cluster '%s' because the %s "
        "step's 'to_node' parameter is either larger or smaller than the "
        "number of nodes in the cluster (%d)." %
        (self.scenario.display_name, self.scenario.cluster.name(), self.name,
         len(node_metadata)),
        corrective_action=
        "Please retry the scenario using a cluster with a compatible number "
        "of nodes. If you are the author of the scenario, please check the "
        "syntax of the %s step to make it compatible with clusters of this "
        "size." % self.name)


  def _run(self):
    nodes = self.scenario.cluster.nodes()
    to_node = nodes[self.to_node_index]
    from_node = nodes[self.from_node_index]

    vms = self.vm_group.get_vms()
    vms_to_move = []
    for vm in vms:
      if vm.node_id() == from_node.node_id():
        vms_to_move.append(vm)
    to_nodes = [to_node] * len(vms_to_move)
    self.create_annotation("%s: Migrating VMs from node '%s' to node '%s'" %
                           (self.vm_group.name(), from_node.node_id(),
                   to_node.node_id()))
    self.scenario.cluster.migrate_vms(vms_to_move, to_nodes)
    self.create_annotation("%s: Finished migrating VMs from node '%s' to node "
                           "'%s'" % (self.vm_group.name(), from_node.node_id(),
                   to_node.node_id()))


class ResetGroupPlacement(BaseStep):
  """VMotions VMs in a VMGroup back to their original nodes.

  Args:
    scenario (Scenario): Scenario this step belongs to.
      vm_group_name (str): Name of VM group to run the command.
      annotate (bool): Whether or not to annotate events for this step.
  """

  def __init__(self, scenario, vm_group_name, annotate=False):
    super(ResetGroupPlacement, self).__init__(scenario, annotate=annotate)
    self.vm_group = scenario.vm_groups.get(vm_group_name)
    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = ("%s: Migrating VMs to original placement" %
                        self.vm_group.name())

  def _run(self):
    vms_to_move = []
    dest_nodes = []
    placement_map = self.vm_group.get_clone_placement_map()
    vms = self.vm_group.get_vms()
    for vm in vms:
      dest_node = placement_map[vm.vm_name()]
      if dest_node.node_id() != vm.node_id():
        vms_to_move.append(vm)
        dest_nodes.append(dest_node)
    self.create_annotation("%s: Migrating %d VMs to original placement" % (
      self.vm_group.name(), len(vms_to_move)))
    self.scenario.cluster.migrate_vms(vms_to_move, dest_nodes)
    self.create_annotation("%s: Finished migrating %d VMs to original placement" % (
      self.vm_group.name(), len(vms_to_move)))


class EnableHA(BaseStep):
  def __init__(self, scenario, vm_group_name, annotate=False):
    """
    Enables HA on a VMGroup's VMs.

    Args:
      scenario (Scenario): Scenario this step belongs to.
      vm_group_name (str): Name of VM group to enable HA.
      annotate (bool): Whether or not to annotate events for this step.
    """
    super(EnableHA, self).__init__(scenario, annotate=annotate)

    self.vm_group = scenario.vm_groups.get(vm_group_name)

    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Enabling HA on VMs" % self.vm_group.name()

  def _run(self):
    test_vms, _ = NameUtil.filter_test_vms(self.scenario.cluster.vms(),
                                           [self.scenario.id])
    self.scenario.cluster.enable_ha_vms(test_vms)
    self.create_annotation("%s: Enabled HA on VMs" %self.vm_group.name())


class DisableHA(BaseStep):
  def __init__(self, scenario, vm_group_name, annotate=False):
    """
    Disables HA on a VMGroup's VMs.

    Args:
      scenario (Scenario): Scenario this step belongs to.
      vm_group_name (str): Name of VM group to disable HA.
      annotate (bool): Whether or not to annotate events for this step.
    """
    super(DisableHA, self).__init__(scenario, annotate=annotate)

    self.vm_group = scenario.vm_groups.get(vm_group_name)

    if self.vm_group is None:
      raise NoVMGroupDefinedError(vm_group_name=vm_group_name, step=self)
    self.description = "%s: Disabling HA on VMs" % self.vm_group.name()

  def _run(self):
    test_vms, _ = NameUtil.filter_test_vms(self.scenario.cluster.vms(),
                                           [self.scenario.id])
    self.scenario.cluster.disable_ha_vms(test_vms)
    self.create_annotation("%s: Disabled HA on VMs" % self.vm_group.name())
