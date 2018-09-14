#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#

import unittest

import mock

from curie import steps
from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie.testing import environment
from curie.testing.util import mock_cluster
from curie.vm_group import VMGroup


class TestStepsMeta(unittest.TestCase):
  def setUp(self):
    self.cluster = mock_cluster()
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.vm_group = VMGroup(self.scenario, "group_0")
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}

  @mock.patch("curie.steps.meta.MigrateGroup")
  @mock.patch("curie.steps.meta.ResetGroupPlacement")
  @mock.patch("curie.steps.meta.Shutdown")
  @mock.patch("curie.steps.meta.PowerOn")
  def test_RollingUpgrade_default(self, mock_poweron, mock_shutdown,
                                  mock_resetgroupplacement, mock_migrategroup):
    poweron_instance = mock_poweron.return_value
    poweron_instance.return_value = None
    poweron_instance.name = "PowerOn"
    poweron_instance.description = "Powering on nodes"
    shutdown_instance = mock_shutdown.return_value
    shutdown_instance.return_value = None
    shutdown_instance.name = "Shutdown"
    shutdown_instance.description = "Shutting down nodes"
    resetgroupplacement_instance = mock_resetgroupplacement.return_value
    resetgroupplacement_instance.return_value = None
    resetgroupplacement_instance.name = "ResetGroupPlacement"
    resetgroupplacement_instance.description = "Resetting VM placement"
    migrategroup_instance = mock_migrategroup.return_value
    migrategroup_instance.return_value = None
    migrategroup_instance.name = "MigrateGroup"
    migrategroup_instance.description = "Migrating VMs"
    meta_step = steps.meta.RollingUpgrade(self.scenario)
    for step in meta_step.itersteps():
      step()

    mock_migrategroup.assert_has_calls([
      mock.call(self.scenario, self.vm_group._name, 0, 1),
      mock.call()(),  # The actual call to the mocked instance callable.
      mock.call(self.scenario, self.vm_group._name, 1, 2),
      mock.call()(),
      mock.call(self.scenario, self.vm_group._name, 2, 3),
      mock.call()(),
      mock.call(self.scenario, self.vm_group._name, 3, 0),
      mock.call()()
    ])

    mock_shutdown_calls = []
    for xx in range(len(self.cluster.nodes())):
      mock_shutdown_calls.append(mock.call(self.scenario, xx, annotate=True))
      mock_shutdown_calls.append(mock.call()())
    mock_shutdown.assert_has_calls(mock_shutdown_calls)

    mock_poweron_calls = []
    for xx in range(len(self.cluster.nodes())):
      mock_poweron_calls.append(mock.call(self.scenario, xx, annotate=True))
      mock_poweron_calls.append(mock.call()())
    mock_poweron.assert_has_calls(mock_poweron_calls)

    mock_resetgroupplacement_calls = []
    for xx in range(len(self.cluster.nodes())):
      mock_resetgroupplacement_calls.append(
        mock.call(self.scenario, self.vm_group._name))
      mock_resetgroupplacement_calls.append(mock.call()())
    mock_resetgroupplacement.assert_has_calls(mock_resetgroupplacement_calls)

  def test_RollingUpgrade_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.meta.RollingUpgrade(self.scenario)
    self.assertIsInstance(step, steps.meta.RollingUpgrade)

  @mock.patch("curie.steps.meta.MigrateGroup")
  @mock.patch("curie.steps.meta.ResetGroupPlacement")
  @mock.patch("curie.steps.meta.Shutdown")
  @mock.patch("curie.steps.meta.PowerOn")
  def test_RollingUpgrade_single_node_annotate(self,
                                               mock_poweron, mock_shutdown,
                                               mock_resetgroupplacement,
                                               mock_migrategroup):
    poweron_instance = mock_poweron.return_value
    poweron_instance._run.return_value = None
    poweron_instance.name = "PowerOn"
    poweron_instance.description = "Powering on nodes"
    shutdown_instance = mock_shutdown.return_value
    shutdown_instance._run.return_value = None
    shutdown_instance.name = "Shutdown"
    shutdown_instance.description = "Shutting down nodes"
    resetgroupplacement_instance = mock_resetgroupplacement.return_value
    resetgroupplacement_instance._run.return_value = None
    resetgroupplacement_instance.name = "ResetGroupPlacement"
    resetgroupplacement_instance.description = "Resetting VM placement"
    migrategroup_instance = mock_migrategroup.return_value
    migrategroup_instance._run.return_value = None
    migrategroup_instance.name = "MigrateGroup"
    migrategroup_instance.description = "Migrating VMs"
    meta_step = steps.meta.RollingUpgrade(self.scenario, node_count=1,
                                          annotate=False)
    for step in meta_step.itersteps():
      step()

    mock_migrategroup.assert_has_calls([
      mock.call(self.scenario, self.vm_group._name, 0, 1),
      mock.call()()])
    mock_shutdown.assert_has_calls([
      mock.call(self.scenario, 0, annotate=False),
      mock.call()()])
    mock_poweron.assert_has_calls([
      mock.call(self.scenario, 0, annotate=False),
      mock.call()()])
    mock_resetgroupplacement.assert_has_calls([
      mock.call(self.scenario, self.vm_group._name),
      mock.call()()
    ])

  def test_RollingUpgrade_too_many_nodes(self):
    meta_step = steps.meta.RollingUpgrade(
      self.scenario, node_count=len(self.cluster.nodes()) + 1)
    with self.assertRaises(CurieTestException):
      for step in meta_step.itersteps():
        step.verify()
