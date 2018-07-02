#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import time
import unittest

import mock

from curie.curie_error_pb2 import CurieError
from curie.curie_server_state_pb2 import CurieSettings
from curie.cluster import Cluster
from curie.exception import CurieException, CurieTestException
from curie.node import Node
from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.vm import Vm
from curie.testing import environment


class TestStepsVmGeneric(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.cluster.find_vms_regex.return_value = self.vms
    _nodes = [mock.Mock(spec=Node) for _ in xrange(4)]

    self.cluster_metadata = CurieSettings.Cluster()
    self.cluster_metadata.cluster_name = "mock_cluster"
    self.cluster_metadata.cluster_hypervisor_info.CopyFrom(
      CurieSettings.Cluster.ClusterHypervisorInfo())
    self.cluster_metadata.cluster_management_server_info.CopyFrom(
      CurieSettings.Cluster.ClusterManagementServerInfo())
    self.cluster_metadata.cluster_software_info.CopyFrom(
      CurieSettings.Cluster.ClusterSoftwareInfo())
    for id, node in enumerate(_nodes):
      node.node_id.return_value = id
      curr_node = self.cluster_metadata.cluster_nodes.add()
      curr_node.id = str(id)
    self.cluster.metadata.return_value = self.cluster_metadata

    self.cluster.nodes.return_value = _nodes
    self.cluster.node_count.return_value = len(_nodes)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory="/fake/goldimages/directory/")
    self.vm_group = VMGroup(self.scenario, "group_0.*[(-!&")
    for mock_vm, vm_name in zip(self.vms, self.vm_group.get_vms_names()):
      mock_vm.vm_name.return_value = vm_name
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}

  def test_CloneFromTemplate_valid(self):
    self.vm_group._template = "valid-template"
    template_vm = mock.Mock(spec=Vm)
    self.cluster.find_vm.side_effect = [None]
    self.cluster.import_vm.side_effect = [template_vm]
    step = steps.vm_group.CloneFromTemplate(self.scenario,
                                            self.vm_group.name())
    step()
    self.cluster.create_vm.assert_called_once_with(
      "/fake/goldimages/directory/", "valid-template",
      "__curie_goldimage_%d_valid-template_group_0._-_" % self.scenario.id,
      data_disks=(), node_id=0, ram_mb=2048, vcpus=2)
    self.assertEqual(self.cluster.clone_vms.call_count, 1)
    self.assertEqual(self.cluster.power_on_vms.call_count, 0)

  def test_CloneFromTemplate_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.CloneFromTemplate(self.scenario,
                                            self.vm_group.name())
    self.assertIsInstance(step, steps.vm_group.CloneFromTemplate)

  def test_CloneFromTemplate_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.CloneFromTemplate(self.scenario, "not_a_vm_group")

  def test_CloneFromTemplate_missing_goldimages_directory(self):
    self.scenario.goldimages_directory = None
    step = steps.vm_group.CloneFromTemplate(self.scenario, self.vm_group.name())
    with self.assertRaises(CurieTestException) as ar:
      step()
    self.assertEqual(str(ar.exception),
                     "scenario.goldimages_directory must be set before "
                     "running CloneFromTemplate (value: None)")

  def test_CloneFromTemplate_power_on(self):
    self.vm_group._template = "valid-template"
    template_vm = mock.Mock(spec=Vm)
    self.cluster.find_vm.side_effect = [None]
    self.cluster.import_vm.side_effect = [template_vm]
    step = steps.vm_group.CloneFromTemplate(self.scenario,
                                            self.vm_group.name(),
                                            power_on=True,
                                            annotate=True)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(self.cluster.create_vm.call_count, 1)
      self.assertEqual(self.cluster.clone_vms.call_count, 1)
      self.assertEqual(self.cluster.power_on_vms.call_count, 1)
      self.assertEqual(mock_annotate.call_count, 2)

  def test_CloneFromTemplate_already_exist(self):
    self.vm_group._template = "valid-template"
    template_vm = mock.Mock(spec=Vm)
    self.cluster.find_vm.side_effect = [template_vm, template_vm]
    step = steps.vm_group.CloneFromTemplate(self.scenario,
                                            self.vm_group.name())
    step()
    self.assertEqual(self.cluster.import_vm.call_count, 0)

  def test_CloneFromTemplate_failed_clone(self):
    self.vm_group._template = "valid-template"
    template_vm = mock.Mock(spec=Vm)
    self.cluster.find_vm.side_effect = [None]
    self.cluster.import_vm.side_effect = [template_vm]
    self.cluster.find_vms_regex.return_value = self.vms[:-1]
    step = steps.vm_group.CloneFromTemplate(self.scenario,
                                            self.vm_group.name())
    with self.assertRaises(CurieTestException):
      step()

  def test_RelocateGroupDatastore(self):
    datastore_name = "random_datastore"
    step = steps.vm_group.RelocateGroupDatastore(
      self.scenario, self.vm_group.name(), datastore_name, annotate=True)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 2)
      self.cluster.relocate_vms_datastore.called_once_with(
        self.vms, [datastore_name] * len(self.vms))

  def test_RelocateGroupDatastore_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.RelocateGroupDatastore(
      self.scenario, self.vm_group.name(), "random_datastore", annotate=True)
    self.assertIsInstance(step, steps.vm_group.RelocateGroupDatastore)

  def test_RelocateGroupDatastore_bad_vm_group(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.RelocateGroupDatastore(self.scenario, "NOT_A_VALID_GROUP",
                                            "NOT_A_VALID_DATASTORE")

  def test_RelocateGroupDatastore_bad_datastore(self):
    self.cluster.relocate_vms_datastore.side_effect = CurieTestException
    step = steps.vm_group.RelocateGroupDatastore(self.scenario,
                                                 self.vm_group.name(),
                                                 "NOT_A_VALID_DATASTORE")
    with self.assertRaises(CurieTestException):
      step()

  def test_MigrateGroup(self):
    # Node IDs are just integers. Place one VM per node.
    for id, vm in enumerate(self.vms):
      vm.node_id.return_value = id
    step = steps.vm_group.MigrateGroup(self.scenario, self.vm_group.name(), 0,
                                       1,
                                       annotate=True)
    self.assertEqual(
      "%s: Migrating VMs from node '0' to node '1'" % self.vm_group.name(),
      step.description)
    self.cluster.migrate_vms.return_value = [True]
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 2)
      self.cluster.migrate_vms.called_once_with(self.vms[0], [1])

  def test_MigrateGroup_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.MigrateGroup(
      self.scenario, self.vm_group.name(), 0, 1, annotate=True)
    self.assertIsInstance(step, steps.vm_group.MigrateGroup)

  def test_MigrateGroup_verify_to_node_out_of_bounds(self):
    step = steps.vm_group.MigrateGroup(
      self.scenario, self.vm_group.name(), 0, 4, annotate=True)
    with self.assertRaises(CurieTestException) as ar:
      step.verify()
    self.assertEqual("Invalid to_node index '4'. Cluster contains 4 nodes "
                     "(max index is 3).",
                     str(ar.exception))

  def test_MigrateGroup_verify_from_node_out_of_bounds(self):
    step = steps.vm_group.MigrateGroup(
      self.scenario, self.vm_group.name(), 4, 0, annotate=True)
    with self.assertRaises(CurieTestException) as ar:
      step.verify()
    self.assertEqual("Invalid from_node index '4'. Cluster contains 4 nodes "
                     "(max index is 3).",
                     str(ar.exception))

  def test_MigrateGroup_bad_vmgroup(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.MigrateGroup(self.scenario, "not_a_vm_group", 0, 1)

  def test_MigrateGroup_same_node(self):
    # Node IDs are just integers. Place one VM per node.
    for id, vm in enumerate(self.vms):
      vm.node_id.return_value = id
    step = steps.vm_group.MigrateGroup(self.scenario, self.vm_group.name(), 0,
                                       0)
    self.cluster.migrate_vms.return_value = [True]
    step()
    self.cluster.migrate_vms.called_once_with(self.vms[0], [0])

  def test_CreatePeriodicSnapshots_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.CreatePeriodicSnapshots(
      self.scenario, self.vm_group.name(),
      num_snapshots=12, interval_minutes=60)
    self.assertIsInstance(step, steps.vm_group.CreatePeriodicSnapshots)

  def test_create_periodic_snapshots_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.CreatePeriodicSnapshots(
        self.scenario, "not_a_vm_group", num_snapshots=12, interval_minutes=60)

  @mock.patch.object(steps.test.Wait, "_run")
  def test_create_periodic_snapshots(self, Wait_mock):
    self.cluster.snapshot_vms.return_value = None
    step = steps.vm_group.CreatePeriodicSnapshots(self.scenario,
                                                  self.vm_group.name(),
                                                  num_snapshots=12,
                                                  interval_minutes=60)
    Wait_mock.return_value = 3600
    with mock.patch.object(steps.vm_group.log,
                           "warning",
                           wraps=steps.vm_group.log.warning) as mock_warning:
      with mock.patch.object(step,
                             "create_annotation") as mock_annotate:
        step()
        self.assertEqual(self.cluster.snapshot_vms.call_count, 12)
        self.assertEqual(Wait_mock.call_count, 11)
        self.assertEqual(mock_annotate.call_count, 12)
        self.assertEqual(mock_warning.call_count, 0)

  def test_create_periodic_snapshots_async(self):
    start_time_secs = time.time()
    max_delay_secs = 60
    should_return = False
    clean_exit = []

    def function_that_must_be_called_asynchronously():
      while time.time() - start_time_secs < max_delay_secs:
        if should_return:
          clean_exit.append(True)
          break
        else:
          time.sleep(0.1)  # 100 ms
      else:
        raise RuntimeError("Test function ran past its maximum delay; Was it "
                           "called asynchronously?")

    step = steps.vm_group.CreatePeriodicSnapshots(self.scenario,
                                                  self.vm_group.name(),
                                                  num_snapshots=12,
                                                  interval_minutes=60,
                                                  async=True)
    with mock.patch.object(step, "_periodic_snapshots_func") as psf:
      psf.side_effect = function_that_must_be_called_asynchronously
      step()  # If called incorrectly, blocks until a RuntimeError is raised.
      should_return = True  # Request that the function exit cleanly.
      while time.time() - start_time_secs < max_delay_secs:
        if clean_exit:
          break
      else:
        self.fail("Test function failed to exit cleanly within the timeout")
      psf.assert_called_once_with()

  @mock.patch.object(steps.test.Wait, "_run")
  def test_create_periodic_snapshots_should_stop(self, Wait_mock):
    self.cluster.snapshot_vms.return_value = None
    step = steps.vm_group.CreatePeriodicSnapshots(self.scenario,
                                                  self.vm_group.name(),
                                                  num_snapshots=12,
                                                  interval_minutes=60)
    Wait_mock.return_value = 3600
    with mock.patch.object(self.scenario,
                           "should_stop") as mock_should_stop:
      # First two calls to should_stop return False; Third returns True.
      mock_should_stop.side_effect = [False, False, True]
      with mock.patch.object(steps.vm_group.log,
                             "warning",
                             wraps=steps.vm_group.log.warning) as mock_warning:
        with mock.patch.object(step, "create_annotation") as mock_annotate:
          step()
          self.assertEqual(self.cluster.snapshot_vms.call_count, 2)
          self.assertEqual(Wait_mock.call_count, 2)
          self.assertEqual(mock_annotate.call_count, 2)
          self.assertEqual(mock_warning.call_count, 1)

  def test_PowerOff_valid(self):
    step = steps.vm_group.PowerOff(self.scenario, self.vm_group.name())
    vms = step()
    self.assertEqual(vms, self.vm_group.get_vms())
    self.cluster.power_off_vms.assert_called_once_with(vms)

  def test_PowerOff_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.PowerOff(self.scenario, self.vm_group.name())
    self.assertIsInstance(step, steps.vm_group.PowerOff)

  def test_PowerOff_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.PowerOff(self.scenario, "not_a_vm_group")

  def test_PowerOff_missing_vms(self):
    step = steps.vm_group.PowerOff(self.scenario, self.vm_group.name())
    self.cluster.find_vms_regex.return_value = []
    with self.assertRaises(CurieTestException):
      step()

  def test_PowerOn_valid(self):
    step = steps.vm_group.PowerOn(self.scenario, self.vm_group.name())
    vms = step()
    self.assertEqual(vms, self.vm_group.get_vms())
    self.cluster.power_on_vms.assert_called_once_with(vms,
                                                      max_parallel_tasks=100)

  def test_PowerOn_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.PowerOn(self.scenario, self.vm_group.name())
    self.assertIsInstance(step, steps.vm_group.PowerOn)

  def test_PowerOn_max_parallel(self):
    step = steps.vm_group.PowerOn(self.scenario, self.vm_group.name(),
                                  max_concurrent=8)
    vms = step()
    self.assertEqual(vms, self.vm_group.get_vms())
    self.cluster.power_on_vms.assert_called_once_with(vms,
                                                      max_parallel_tasks=8)

  def test_PowerOn_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.PowerOn(self.scenario, "not_a_vm_group")

  def test_PowerOn_missing_vms(self):
    step = steps.vm_group.PowerOn(self.scenario, self.vm_group.name())
    self.cluster.find_vms_regex.return_value = []
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.util.time")
  def test_PowerOn_inaccessible(self, mock_time):
    now = time.time()
    mock_time.time.side_effect = lambda: mock_time.time.call_count + now
    mock_time.sleep.return_value = 0
    for mock_vm in self.vms:
      mock_vm.is_powered_on.return_value = True
      mock_vm.vm_ip.return_value = "169.254.0.1"
      mock_vm.is_accessible.return_value = False
    step = steps.vm_group.PowerOn(self.scenario, self.vm_group.name())
    with mock.patch.object(self.vm_group, "get_vms",
                           wraps=self.vm_group.get_vms) as mock_get_vms:
      with self.assertRaises(CurieTestException) as ar:
        step()
      self.assertEqual(122, mock_get_vms.call_count)
      self.assertIn("Not all VMs in VM Group %s accessible within 120 "
                    "seconds." % self.vm_group.name(),
                    str(ar.exception))

  def test_Grow_count_per_node(self):
    for mock_vm in self.vms:
      mock_vm.is_powered_on.return_value = False
    self.vm_group._count_per_node = 1
    new_vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.vm_group.lookup_vms_by_name = mock.Mock(return_value=new_vms)
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_node=2, annotate=True)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 2)
      mock_annotate.assert_has_calls([
        mock.call("%s: Growing VM count from %d to %d" % (
          self.vm_group.name(),
          1 * self.cluster.node_count(),
          2 * self.cluster.node_count())),
        mock.call("%s: Finished growing VM group" % self.vm_group.name())
      ])
      self.assertEqual(self.cluster.clone_vms.call_count, 1)
      self.assertEqual(self.cluster.power_on_vms.call_count, 0)

  def test_Grow_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_node=2, annotate=True)
    self.assertIsInstance(step, steps.vm_group.Grow)

  def test_Grow_count_per_cluster(self):
    for mock_vm in self.vms:
      mock_vm.is_powered_on.return_value = False
    self.vm_group._count_per_cluster = 4
    new_vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.vm_group.lookup_vms_by_name = mock.Mock(return_value=new_vms)
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=8)
    step()
    self.assertEqual(self.cluster.clone_vms.call_count, 1)
    self.assertEqual(self.cluster.power_on_vms.call_count, 0)

  def test_Grow_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.Grow(self.scenario, "not_a_vm_group", count_per_node=1)

  def test_Grow_count_per_node_and_count_per_cluster_None(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.Grow(self.scenario, self.vm_group.name())

  def test_Grow_count_per_node_and_count_per_cluster_not_None(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                          count_per_node=1, count_per_cluster=4)

  def test_Grow_verify_count_per_node_same_as_existing(self):
    self.vm_group._count_per_node = 1
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_node=1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_verify_count_per_cluster_same_as_existing(self):
    self.vm_group._count_per_cluster = 4
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=4)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_verify_count_per_node_less_than_existing(self):
    self.vm_group._count_per_node = 2
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_node=1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_verify_count_per_cluster_less_than_existing(self):
    self.vm_group._count_per_cluster = 5
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=4)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_count_mixed_starting_with_count_per_cluster(self):
    self.vm_group._count_per_cluster = 1
    with mock.patch("curie.test.steps."
                    "vm_group.ResetGroupPlacement") as mock_rgp:
      new_vms = [mock.Mock(spec=Vm) for _ in xrange(3)]
      self.vm_group.lookup_vms_by_name = mock.Mock(return_value=new_vms)
      steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                          count_per_node=1)()
      self.assertEqual(mock_rgp.call_count, 1)

  def test_Grow_count_mixed_starting_with_count_per_node(self):
    self.vm_group._count_per_node = 1
    with mock.patch("curie.test.steps."
                    "vm_group.ResetGroupPlacement") as mock_rgp:
      new_vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
      self.vm_group.lookup_vms_by_name = mock.Mock(return_value=new_vms)
      steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                          count_per_cluster=6)()
      self.assertEqual(mock_rgp.call_count, 1)

  def test_Grow_verify_count_mixed_count_per_node_less_than_existing(self):
    self.vm_group._count_per_node = 2
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=4)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_verify_count_mixed_count_per_cluster_less_than_existing(self):
    self.vm_group._count_per_cluster = 8
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_node=1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_Grow_power_on(self):
    for mock_vm in self.vms:
      mock_vm.is_powered_on.return_value = True
    self.vm_group._count_per_cluster = 4
    new_vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.vm_group.lookup_vms_by_name = mock.Mock(return_value=new_vms)
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=8)
    step()
    self.assertEqual(self.cluster.power_on_vms.call_count, 1)

  def test_Grow_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    self.vm_group._count_per_cluster = 4
    step = steps.vm_group.Grow(self.scenario, self.vm_group.name(),
                               count_per_cluster=8)
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.vm_group.time")
  def test_RunCommand_valid(self, time_mock):
    fake_time = 1467215634.0
    time_mock.time.return_value = fake_time
    for vm in self.vms:
      vm.execute_sync.return_value = (0, "stdout", "stderr")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps")
    with mock.patch.object(steps.vm_group.log,
                           "warning",
                           wraps=steps.vm_group.log.warning) as mock_warning:
      step()
      self.assertEqual(mock_warning.call_count, 0)
    for vm in self.vms:
      cmd_id = "%s_ps_%d" % (vm.vm_name(), fake_time * 1e6)
      vm.execute_sync.assert_called_once_with(cmd_id, "ps", 120,
                                              user="nutanix")

  def test_RunCommand_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps")
    self.assertIsInstance(step, steps.vm_group.RunCommand)

  def test_RunCommand_fail_on_error(self):
    for vm in self.vms:
      vm.execute_sync.return_value = (1, "stdout", "stderr")
      vm.execute_sync.side_effect = CurieException(CurieError.kInternalError,
                                                    "Message")
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group._name, "ps")
    with self.assertRaises(CurieException):
      step()

  def test_RunCommand_invalid_vm_group_name(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.RunCommand(self.scenario, "ps", "not_a_vm_group")

  @mock.patch("curie.test.steps.vm_group.time")
  def test_RunCommand_fail_on_error_false(self, time_mock):
    fake_time = 1467215634.0
    time_mock.time.return_value = fake_time
    for vm in self.vms:
      vm.execute_sync.return_value = (1, "stdout", "stderr")
      vm.execute_sync.side_effect = CurieException(CurieError.kInternalError,
                                                    "Message")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps",
                                     fail_on_error=False)
    with mock.patch.object(steps.vm_group.log,
                           "warning",
                           wraps=steps.vm_group.log.warning) as mock_warning:
      step()
      self.assertEqual(mock_warning.call_count, len(self.vms))
    for vm in self.vms:
      cmd_id = "%s_ps_%d" % (vm.vm_name(), fake_time * 1e6)
      vm.execute_sync.assert_called_once_with(cmd_id, "ps", 120,
                                              user="nutanix")

  def test_RunCommand_fail_on_error_status_code(self):
    for vm in self.vms:
      vm.execute_sync.return_value = (1, "stdout", "stderr")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps")
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.vm_group.time")
  def test_RunCommand_fail_on_error_false_status_code(self, time_mock):
    fake_time = 1467215634.0
    time_mock.time.return_value = fake_time
    for vm in self.vms:
      vm.execute_sync.return_value = (1, "stdout", "stderr")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps",
                                     fail_on_error=False)
    with mock.patch.object(steps.vm_group.log,
                           "warning",
                           wraps=steps.vm_group.log.warning) as mock_warning:
      step()
      self.assertEqual(mock_warning.call_count, len(self.vms))
    for vm in self.vms:
      cmd_id = "%s_ps_%d" % (vm.vm_name(), fake_time * 1e6)
      vm.execute_sync.assert_called_once_with(cmd_id, "ps", 120,
                                              user="nutanix")

  def test_RunCommand_missing_vms(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group._name, "ps")
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.vm_group.time")
  def test_RunCommand_user(self, time_mock):
    fake_time = 1467215634.0
    time_mock.time.return_value = fake_time
    for vm in self.vms:
      vm.execute_sync.return_value = (0, "stdout", "stderr")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps",
                                     user="root")
    step()
    for vm in self.vms:
      cmd_id = "%s_ps_%d" % (vm.vm_name(), fake_time * 1e6)
      vm.execute_sync.assert_called_once_with(cmd_id, "ps", 120, user="root")

  @mock.patch("curie.test.steps.vm_group.time")
  def test_RunCommand_timeout_secs(self, time_mock):
    fake_time = 1467215634.0
    time_mock.time.return_value = fake_time
    for vm in self.vms:
      vm.execute_sync.return_value = (0, "stdout", "stderr")
    self.cluster.find_vms_regex.return_value = self.vms
    step = steps.vm_group.RunCommand(self.scenario, self.vm_group.name(), "ps",
                                     user="nutanix", timeout_secs=300)
    step()
    for vm in self.vms:
      cmd_id = "%s_ps_%d" % (vm.vm_name(), fake_time * 1e6)
      vm.execute_sync.assert_called_once_with(cmd_id, "ps", 300, user="nutanix")

  def test_ResetGroupPlacement_default(self):
    # Node IDs are just integers. Place one VM per node, but increment the
    # placement id such that the VM originally supposed to be on node 0 is on
    #  1, and 1 on 2, etc. until the vm->node_id wraps.
    for id, vm in enumerate(self.vms):
      vm.node_id.return_value = (id + 1) % len(self.cluster.nodes())
    step = steps.vm_group.ResetGroupPlacement(self.scenario,
                                              self.vm_group.name())
    step()
    # We expect to see the original list of VMs (4), with one per node.
    self.cluster.migrate_vms.assert_called_once_with(
      self.vms, self.cluster.nodes())

  def test_ResetGroupPlacement_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.vm_group.ResetGroupPlacement(self.scenario,
                                              self.vm_group.name())
    self.assertIsInstance(step, steps.vm_group.ResetGroupPlacement)

  def test_ResetGroupPlacement_no_movement(self):
    # Node IDs are just integers. Place one VM per node.
    for id, vm in enumerate(self.vms):
      vm.node_id.return_value = id
    step = steps.vm_group.ResetGroupPlacement(self.scenario,
                                              self.vm_group.name())
    step()
    # No VMs should have to be moved, so lists will be empty.
    self.cluster.migrate_vms.assert_called_once_with([], [])

  def test_ResetGroupPlacement_bad_vm_group(self):
    with self.assertRaises(CurieTestException):
      steps.vm_group.ResetGroupPlacement(self.scenario, "NOT_A_VALID_GROUP")
