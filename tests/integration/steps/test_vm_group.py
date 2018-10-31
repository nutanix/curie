#
#  Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import json
import logging
import unittest
import uuid

import gflags
import mock

from curie import steps
from curie.acropolis_cluster import AcropolisCluster
from curie.exception import CurieTestException, CurieException
from curie.hyperv_cluster import HyperVCluster
from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.testing import environment, util
from curie.vm_group import VMGroup

log = logging.getLogger(__name__)


class TestIntegrationStepsVm(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.uuid = str(uuid.uuid4())
    log.debug("Setting up test %s - tagged with UUID %s", self._testMethodName,
              self.uuid)
    self.group_name = self.uuid[0:VMGroup.MAX_NAME_LENGTH]

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_create_periodic_snapshots_take_snapshot_then_stop_test(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1,
                               data_disks=[1])}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    step = steps.vm_group.CreatePeriodicSnapshots(self.scenario,
                                                  self.group_name,
                                                  num_snapshots=2,
                                                  interval_minutes=60)
    # Set should_stop to return False for 120 seconds, and True after that.
    self.scenario.should_stop = util.return_until(False, True, 120)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(steps.vm_group.log,
                             "warning",
                             wraps=steps.vm_group.log.warning) as mocked_warning:
        step()
        # Use create_annotation as a platform-agnostic proxy for taking a snapshot.
        self.assertEqual(mock_annotate.call_count, 1)
        self.assertEqual(mocked_warning.call_count, 1)

  def test_create_periodic_snapshots_multiple_then_stop_test(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=2,
                               data_disks=[1])}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    step = steps.vm_group.CreatePeriodicSnapshots(self.scenario,
                                                  self.group_name,
                                                  num_snapshots=3,
                                                  interval_minutes=1)
    # Set should_stop to return False for 120 seconds, and True after that.
    self.scenario.should_stop = util.return_until(False, True, 120)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(steps.vm_group.log,
                             "warning",
                             wraps=steps.vm_group.log.warning) as mocked_warning:
        step()
        # Use create_annotation as a platform-agnostic proxy for taking a snapshot.
        self.assertEqual(mock_annotate.call_count, 2)
        self.assertEqual(mocked_warning.call_count, 1)

  def test_template_clone_default(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1,
                               data_disks=[1])}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    expected = ["__curie_test_%d_%s_0000" %
                (self.scenario.id,
                 NameUtil.sanitize_filename(self.group_name))]
    self.assertEqual(set([vm.vm_name() for vm in vms]), set(expected))

  def test_template_clone_linked(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1,
                               data_disks=[1])}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name,
                                           linked_clone=True)()
    expected = ["__curie_test_%d_%s_0000" %
                (self.scenario.id,
                 NameUtil.sanitize_filename(self.group_name))]
    self.assertEqual(set([vm.vm_name() for vm in vms]), set(expected))

  def test_template_clone_linked_count(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count,
                               data_disks=[1])}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name,
                                           linked_clone = True)()
    expected = ["__curie_test_%d_%s_%04d" %
                (self.scenario.id,
                 NameUtil.sanitize_filename(self.group_name),
                 index)
                for index in xrange(count)]
    self.assertEqual(set([vm.vm_name() for vm in vms]), set(expected))

  def test_template_clone_count(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count,
                               data_disks=[1])}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    expected = ["__curie_test_%d_%s_%04d" %
                (self.scenario.id,
                 NameUtil.sanitize_filename(self.group_name),
                 index)
                for index in xrange(count)]
    self.assertEqual(set([vm.vm_name() for vm in vms]), set(expected))

  def test_template_clone_missing(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="bad_template",
                               template_type="DISK",
                               count_per_cluster=1,
                               data_disks=[1])}
    step = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)
    with self.assertRaises(Exception) as ar:
      step()
    if isinstance(ar.exception, CurieException):
      self.assertIn("Goldimage bad_template-x86_64.vmdk does not exist.",
                    str(ar.exception))
    elif isinstance(ar.exception, CurieTestException):
      self.assertIn("Error copying disk image", str(ar.exception))
    else:
      self.fail("Unexpected exception type %r: %s" %
                (type(ar.exception), ar.exception))

  def test_template_clone_duplicate_vm_names(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1,
                               data_disks=[1])}
    step = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)
    step()
    vm_group = self.scenario.vm_groups[self.group_name]
    self.assertEqual(len(set(vm_group.get_vms())), 1)
    if (isinstance(self.scenario.cluster, AcropolisCluster) or isinstance(self.scenario.cluster, HyperVCluster)):
      # AHV and Hyper-V can create duplicate-named VMs. We should probably prevent this.
      pass
    else:
      with self.assertRaises(CurieTestException):
        step()
      self.assertEqual(len(set(vm_group.get_vms())), 1)

  def test_power_off(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.vm_group.PowerOff(self.scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertFalse(vm.is_powered_on())
      self.assertFalse(vm.is_accessible())

  def test_power_off_already_off(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOff(self.scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertFalse(vm.is_powered_on())
      self.assertFalse(vm.is_accessible())

  def test_power_off_missing(self):
    self.scenario.vm_groups = {}
    with self.assertRaises(CurieTestException):
      steps.vm_group.PowerOff(self.scenario, "not-a-group")

  def test_power_on(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_powered_on())
      self.assertTrue(vm.is_accessible())

  def test_power_on_already_on(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_powered_on())
      self.assertTrue(vm.is_accessible())

  def test_power_on_missing(self):
    self.scenario.vm_groups = {}
    with self.assertRaises(CurieTestException):
      steps.vm_group.PowerOn(self.scenario, "not-a-group")

  def test_power_on_with_cluster_using_only_a_subset_of_nodes(self):
    with open(gflags.FLAGS.cluster_config_path) as f:
      config = json.load(f)
    config["nodes"] = config["nodes"][:1]  # Keep only the first node.
    cluster = util.cluster_from_metis_config(config)
    cluster.update_metadata(False)
    scenario = Scenario(
      cluster=cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

    scenario.vm_groups = {
      self.group_name: VMGroup(scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1)}
    vms = steps.vm_group.CloneFromTemplate(scenario, self.group_name)()
    steps.vm_group.PowerOn(scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_powered_on())
      self.assertTrue(vm.is_accessible())

  def test_Grow_count_per_cluster(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_cluster=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    new_vms = steps.vm_group.Grow(self.scenario, self.group_name,
                                  count_per_cluster=2)()
    self.assertEqual(len(new_vms), 1)
    self.assertEqual(len(vm_group.get_vms()), 2)

  def test_Grow_count_per_node(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_node=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    new_vms = steps.vm_group.Grow(self.scenario, self.group_name,
                                  count_per_node=2)()
    self.assertEqual(len(new_vms), len(self.cluster.nodes()))
    self.assertEqual(len(vm_group.get_vms()),
                     len(self.cluster.nodes()) * 2)

  def test_Grow_mixed_starting_with_count_per_cluster(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_cluster=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    new_vms = steps.vm_group.Grow(self.scenario, self.group_name,
                                  count_per_node=1)()
    self.assertEqual(len(new_vms), len(self.scenario.cluster.nodes()) - 1)
    self.assertEqual(len(vm_group.get_vms()),
                     len(self.scenario.cluster.nodes()))

  def test_Grow_mixed_starting_with_count_per_node(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_node=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    new_vms = steps.vm_group.Grow(self.scenario, self.group_name,
                                  count_per_cluster=8)()
    self.assertEqual(len(new_vms), 8 - len(self.cluster.nodes()))
    self.assertEqual(len(vm_group.get_vms()), 8)

  def test_Grow_verify_mixed_count_per_cluster_less_than_existing(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_cluster=len(self.cluster.nodes()) + 1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    step = steps.vm_group.Grow(self.scenario, self.group_name,
                               count_per_node=1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_verify_mixed_count_per_node_less_than_existing(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_node=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    step = steps.vm_group.Grow(self.scenario, self.group_name,
                               count_per_cluster=len(self.cluster.nodes()) - 1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_power_on(self):
    vm_group = VMGroup(self.scenario, self.group_name,
                       template="ubuntu1604",
                       template_type="DISK",
                       count_per_cluster=1)
    self.scenario.vm_groups = {self.group_name: vm_group}
    steps.vm_group.CloneFromTemplate(self.scenario,
                                     self.group_name,
                                     power_on=True)()
    new_vms = steps.vm_group.Grow(self.scenario, self.group_name,
                                  count_per_cluster=2)()
    self.assertEqual(len(new_vms), 1)
    self.assertEqual(len(vm_group.get_vms()), 2)
    self.assertTrue(new_vms[0].is_powered_on())

  def test_run_command(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.vm_group.RunCommand(self.scenario, self.group_name,
                              "ls /home/nutanix/ > "
                              "/home/nutanix/test_run_command.txt")()
    steps.vm_group.RunCommand(self.scenario, self.group_name,
                              "cat /home/nutanix/test_run_command.txt")()

  def test_run_command_invalid(self):
    count = 2
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=count)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    with self.assertRaises(CurieException):
      steps.vm_group.RunCommand(self.scenario, self.group_name,
                                "not_a_valid_command")()

  def test_run_command_vms_not_exist(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1)}
    with self.assertRaises(CurieTestException):
      steps.vm_group.RunCommand(self.scenario, self.group_name,
                                "ls /home/nutanix/ > "
                                "/home/nutanix/test_run_command.txt")()

  def test_relocate_vms_datastore_not_exist(self):
    if isinstance(self.scenario.cluster, AcropolisCluster) or isinstance(self.scenario.cluster, HyperVCluster):
      log.info("Skipping datastore-related test on AHV or Hyper-V cluster")
      return

    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=2)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    step = steps.vm_group.RelocateGroupDatastore(self.scenario,
                                                 self.group_name,
                                                 "BAD_DATASTORE")
    with self.assertRaises(CurieTestException):
      step()

  def test_relocate_vms_datastore_same_datastore(self):
    if isinstance(self.scenario.cluster, AcropolisCluster) or isinstance(self.scenario.cluster, HyperVCluster):
      log.info("Skipping datastore-related test on AHV or Hyper-V cluster")
      return
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=2)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    datastore_name = self.scenario.cluster._vcenter_info.vcenter_datastore_name
    step = steps.vm_group.RelocateGroupDatastore(self.scenario,
                                                 self.group_name,
                                                 datastore_name)
    step()

  def test_relocate_vms_datastore_bad_vmgroup(self):
    if isinstance(self.scenario.cluster, AcropolisCluster) or isinstance(self.scenario.cluster, HyperVCluster):
      log.info("Skipping datastore-related test on AHV or Hyper-V cluster")
      return
    self.scenario.vm_groups = {}
    with self.assertRaises(CurieTestException):
      steps.vm_group.RelocateGroupDatastore(self.scenario,
                                            "not_a_valid_vmgroup",
                                            "IRRELEVANT_DATASTORE")

  def test_migrate_vms(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=2)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.vm_group.MigrateGroup(self.scenario, self.group_name, 0, 1)()
    first_node_id = steps._util.get_node(self.scenario, 0).node_id()
    second_node_id = steps._util.get_node(self.scenario, 1).node_id()
    node_vm_map = self.__get_node_vm_map()
    # Ensure there are no VMs from the group on the first node.
    self.assertEqual(node_vm_map.get(first_node_id, 0), 0)
    # Ensure there are 2 VMs from the group on the second node.
    self.assertEqual(node_vm_map.get(second_node_id, 0), 2)

  def test_migrate_vms_same_host(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_node=1)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.vm_group.MigrateGroup(self.scenario, self.group_name, 1, 1)()
    node_vm_map = self.__get_node_vm_map()
    for node in self.scenario.cluster.nodes():
      # Ensure there is 1 VM from the group on each node.
      self.assertEqual(node_vm_map.get(node.node_id()), 1)

  def test_migrate_vms_bad_host(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=1)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    with self.assertRaises(CurieTestException):
      steps.vm_group.MigrateGroup(self.scenario, self.group_name, 1, 1000)()

  def test_reset_group_placement(self):
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_cluster=4)}
    vm_group = self.scenario.vm_groups[self.group_name]
    orig_placement = vm_group.get_clone_placement_map()
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    # Verify the original placement is true.
    for vm in vm_group.get_vms():
      self.assertEqual(vm.node_id(), orig_placement[vm.vm_name()].node_id(),
                       "VM %s not on expected node %s (on %s)" %
                       (vm.vm_name(), orig_placement[vm.vm_name()].node_id(),
                        vm.node_id()))
    # Migrate some VMs around and verify placement again.
    steps.vm_group.MigrateGroup(self.scenario, self.group_name, 0, 1)
    steps.vm_group.MigrateGroup(self.scenario, self.group_name, 1, 2)
    for vm in vm_group.get_vms():
      if orig_placement[vm.vm_name()].node_id() == 0:
        target_node_id = steps._util.get_node(1).node_id()
      elif orig_placement[vm.vm_name()].node_id() == 1:
        target_node_id = steps._util.get_node(2).node_id()
      else:
        target_node_id = orig_placement[vm.vm_name()].node_id()
      self.assertEqual(vm.node_id(), target_node_id,
                       "VM %s not on expected node %s (on %s)" %
                       (vm.vm_name(), target_node_id, vm.node_id()))
    # Reset the placement and verify the original placement holds true.
    steps.vm_group.ResetGroupPlacement(self.scenario, self.group_name)
    for vm in vm_group.get_vms():
      self.assertEqual(vm.node_id(), orig_placement[vm.vm_name()].node_id(),
                       "VM %s not on expected node %s (on %s)" %
                       (vm.vm_name(), orig_placement[vm.vm_name()].node_id(),
                        vm.node_id()))

  def __get_node_vm_map(self):
    node_map = {}
    for vm in self.scenario.vm_groups[self.group_name].get_vms():
      node_id = vm.node_id()
      node_map[node_id] = node_map.setdefault(node_id, 0) + 1
    return node_map
