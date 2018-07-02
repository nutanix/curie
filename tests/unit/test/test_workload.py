#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import os
import unittest

import mock

from curie.cluster import Cluster
from curie.iogen.fio_config import FioConfiguration
from curie.node import Node
from curie.scenario import Scenario
from curie.test.vm_group import VMGroup
from curie.test.workload import Workload
from curie.vm import Vm
from curie.testing import environment


class TestWorkload(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.nodes = []
    for index in range(4):
      node = mock.Mock(spec=Node)
      node.node_id.return_value = "node_%d" % index
      self.nodes.append(node)
    self.cluster.nodes.return_value = self.nodes
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.scenario = Scenario(
      cluster=self.cluster,
      source_directory=os.path.join(environment.resource_dir(), "fio"),
      output_directory=environment.test_output_dir(self))
    self.vm_group = VMGroup(self.scenario, "group_0")
    for mock_vm, vm_name in zip(self.vms, self.vm_group.get_vms_names()):
      mock_vm.vm_name.return_value = vm_name
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}

  def test_workload_prefill_default(self):
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen()
      self.assertEqual(mock_fio_config.load.call_count, 1)
      mock_config_instance.convert_prefill.assert_called_once_with()

  def test_workload_prefill_concurrent_4_node(self):
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=8)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # There are 4 nodes and 4 VMs, iodepth is 128.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=128)

  def test_workload_prefill_concurrent_4_node_limited_by_parameter(self):
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=2)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # Only 2 concurrent but each has own node, so iodepth is 128.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=128)

  def test_workload_prefill_concurrent_2_node_limited_by_3_vms(self):
    self.cluster.nodes.return_value = self.nodes[:2]
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.vm_group = VMGroup(self.scenario, "group_0", count_per_cluster=4)
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=8)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # There are 2 nodes and 3 VMs, iodepth is 64.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=64)

  def test_workload_prefill_concurrent_2_node_limited_by_4_vms(self):
    self.cluster.nodes.return_value = self.nodes[:2]
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.vm_group = VMGroup(self.scenario, "group_0", count_per_cluster=4)
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=8)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # There are 2 nodes and 4 VMs, iodepth is 64.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=64)

  def test_workload_prefill_concurrent_2_node_limited_by_3_concurrent(self):
    self.cluster.nodes.return_value = self.nodes[:2]
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.vm_group = VMGroup(self.scenario, "group_0", count_per_cluster=4)
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=3)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # 3 concurrent on 2 nodes, so iodepth is 64.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=64)

  def test_workload_prefill_concurrent_2_node_limited_by_4_concurrent(self):
    self.cluster.nodes.return_value = self.nodes[:2]
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.vm_group = VMGroup(self.scenario, "group_0", count_per_cluster=4)
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=4)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # 3 concurrent on 2 nodes, so iodepth is 64.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=64)

  def test_workload_prefill_concurrent_1_node_limited_by_3_concurrent(self):
    self.cluster.nodes.return_value = self.nodes[:1]
    self.cluster.node_count.return_value = len(self.cluster.nodes())
    self.vm_group = VMGroup(self.scenario, "group_0", count_per_cluster=4)
    self.scenario.vm_groups = {self.vm_group._name: self.vm_group}
    workload = Workload.parse(self.scenario, "test_workload1",
                              {"vm_group": "group_0",
                               "config_file": "oltp.fio"})
    with mock.patch.object(workload, "_config_class") as mock_fio_config:
      mock_config_instance = mock.Mock(spec=FioConfiguration)
      mock_fio_config.load.return_value = mock_config_instance
      workload.prefill_iogen(max_concurrent=3)
      self.assertEqual(mock_fio_config.load.call_count, 1)
      # 3 concurrent on 1 node, so iodepth is 128/3 or 42.
      mock_config_instance.convert_prefill.assert_called_once_with(iodepth=42)
