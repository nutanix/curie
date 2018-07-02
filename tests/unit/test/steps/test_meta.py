#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#

import unittest

import mock

from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.node import Node
from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.testing import environment


class TestStepsMeta(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.nodes = [mock.Mock(spec=Node) for _ in xrange(4)]
    for id, node in enumerate(self.nodes):
      node.node_id.return_value = id
    self.cluster.nodes.return_value = self.nodes
    self.cluster.node_count.return_value = len(self.nodes)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.vm_group = VMGroup(self.scenario, "group_0")
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}

  @mock.patch("curie.test.steps.meta.MigrateGroup")
  @mock.patch("curie.test.steps.meta.ResetGroupPlacement")
  @mock.patch("curie.test.steps.meta.Shutdown")
  @mock.patch("curie.test.steps.meta.PowerOn")
  def test_RollingUpgrade_default(self, mock_poweron, mock_shutdown,
                                  mock_resetgroupplacement, mock_migrategroup):
    poweron_instance = mock_poweron.return_value
    poweron_instance.return_value = None
    shutdown_instance = mock_shutdown.return_value
    shutdown_instance.return_value = None
    resetgroupplacement_instance = mock_resetgroupplacement.return_value
    resetgroupplacement_instance.return_value = None
    migrategroup_instance = mock_migrategroup.return_value
    migrategroup_instance.return_value = None
    node_count = len(self.nodes)
    step = steps.meta.RollingUpgrade(self.scenario)
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
    for xx in range(node_count):
      mock_shutdown_calls.append(mock.call(self.scenario, xx, annotate=True))
      mock_shutdown_calls.append(mock.call()())
    mock_shutdown.assert_has_calls(mock_shutdown_calls)

    mock_poweron_calls = []
    for xx in range(node_count):
      mock_poweron_calls.append(mock.call(self.scenario, xx, annotate=True))
      mock_poweron_calls.append(mock.call()())
    mock_poweron.assert_has_calls(mock_poweron_calls)

    mock_resetgroupplacement_calls = []
    for xx in range(node_count):
      mock_resetgroupplacement_calls.append(
        mock.call(self.scenario, self.vm_group._name))
      mock_resetgroupplacement_calls.append(mock.call()())
    mock_resetgroupplacement.assert_has_calls(mock_resetgroupplacement_calls)

  def test_RollingUpgrade_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.meta.RollingUpgrade(self.scenario)
    self.assertIsInstance(step, steps.meta.RollingUpgrade)

  @mock.patch("curie.test.steps.meta.MigrateGroup")
  @mock.patch("curie.test.steps.meta.ResetGroupPlacement")
  @mock.patch("curie.test.steps.meta.Shutdown")
  @mock.patch("curie.test.steps.meta.PowerOn")
  def test_RollingUpgrade_single_node_annotate(self,
                                               mock_poweron, mock_shutdown,
                                               mock_resetgroupplacement,
                                               mock_migrategroup):
    poweron_instance = mock_poweron.return_value
    poweron_instance._run.return_value = None
    shutdown_instance = mock_shutdown.return_value
    shutdown_instance._run.return_value = None
    resetgroupplacement_instance = mock_resetgroupplacement.return_value
    resetgroupplacement_instance._run.return_value = None
    migrategroup_instance = mock_migrategroup.return_value
    migrategroup_instance._run.return_value = None
    step = steps.meta.RollingUpgrade(self.scenario, node_count=1,
                                     annotate=False)
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
    step = steps.meta.RollingUpgrade(self.scenario,
                                     node_count=len(self.nodes) + 1)
    with self.assertRaises(CurieTestException):
      step.verify()

  def test_verify_too_few_nodes(self):
    step = steps.meta.RollingUpgrade(self.scenario, node_count=0)
    with self.assertRaises(CurieTestException):
      step.verify()
