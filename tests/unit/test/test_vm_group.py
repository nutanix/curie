#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

import mock
import yaml

from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.node import Node
from curie.scenario import Scenario
from curie.test.vm_group import VMGroup
from curie.vm import Vm
from curie.testing import environment


class TestVMGroup(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    nodes = []
    for index in range(4):
      node = mock.Mock(spec=Node)
      node.node_id.return_value = "node_%d" % index
      nodes.append(node)
    self.cluster.nodes.return_value = nodes
    self.cluster.node_count.return_value = len(nodes)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.vm_count = 2
    self.valid_config = yaml.load(
      "template: unix-fio-db\n"
      "nodes: \":\"\n"
      "count_per_cluster: %d" % self.vm_count)

  def test_vmgroup_init_name_limit(self):
    VMGroup.parse(self.scenario, "*" * 18, self.valid_config)

  def test_vmgroup_init_name_exceeds_limit(self):
    with self.assertRaises(CurieTestException):
      VMGroup.parse(self.scenario, "*" * 19, self.valid_config)

  def test_parse_valid_vmgroup_config(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    self.assertEqual(vmgroup.template_name(), "unix-fio-db",
                     "Incorrect template name in vmgroup.")
    self.assertEqual(vmgroup.total_count(), self.vm_count,
                     "Incorrect total_count computed from valid definition.")

  def test_parse_extra_args_vmgroup_config(self):
    config = self.valid_config
    config["not_valid_arg"] = "not_valid_value"
    with self.assertRaises(CurieTestException):
      VMGroup.parse(self.scenario, "test_group1", config)

  def test_total_count_per_node(self):
    config = self.valid_config
    del config["count_per_cluster"]
    config["count_per_node"] = 1
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.nodes(), vmgroup.nodes(),
                     "Incorrect set of nodes chosen.")
    self.assertEqual(vmgroup.total_count(), len(vmgroup.nodes()),
                     "Incorrect total_count computed for per_node value.")

  def test_total_count_per_node_subset(self):
    config = self.valid_config
    del config["count_per_cluster"]
    config["count_per_node"] = 1
    config["nodes"] = "0:2"
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.nodes(), vmgroup.nodes()[0:2],
                     "Incorrect set of nodes chosen.")
    self.assertEqual(vmgroup.total_count(), len(vmgroup.nodes()[0:2]),
                     "Incorrect total_count computed for per_node value.")

  def test_total_count_per_cluster_subset(self):
    config = self.valid_config
    config["count_per_cluster"] = 8
    config["nodes"] = "0:2"
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.nodes(), vmgroup.nodes()[0:2],
                     "Incorrect set of nodes chosen.")
    self.assertEqual(vmgroup.total_count(), config["count_per_cluster"],
                     "Incorrect total_count computed for per_cluster value.")

  def test_both_count_values_specified(self):
    config = self.valid_config
    config["count_per_node"] = 2
    with self.assertRaises(CurieTestException):
      VMGroup.parse(self.scenario, "test_group1", config)
    with self.assertRaises(CurieTestException):
      VMGroup(self.scenario, "test_group1", count_per_cluster=2,
              count_per_node=2)

  def test_get_vms_names(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    names = vmgroup.get_vms_names()
    self.assertEqual(len(names), self.vm_count,
                     "Size of list of names created for vmgroup does not "
                     "match desired length.")
    self.assertEqual(
      names,
      ["__curie_test_%s_test_group1_0000" % self.scenario.id,
       "__curie_test_%s_test_group1_0001" % self.scenario.id])

  def test_get_placement_map(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    placement = vmgroup.get_clone_placement_map()
    nodes = self.scenario.cluster.nodes()
    self.assertEqual(len(placement), self.vm_count,
                     "Incorrect placement map length.")
    expected_vm_names = [
      "__curie_test_%s_test_group1_0000" % self.scenario.id,
      "__curie_test_%s_test_group1_0001" % self.scenario.id]
    self.assertEqual(sorted(placement.keys()), expected_vm_names)
    # VM Names are ordered in the list and we expect one vm per node.
    for xx, vm_name in enumerate(expected_vm_names):
      self.assertEqual(placement[vm_name], nodes[xx])

  def test_lookup_vms_by_name_default(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    vm_names = ["__curie_test_%s_test_group1_0000" % self.scenario.id,
                "__curie_test_%s_test_group1_0001" % self.scenario.id]
    mock_vms = [mock.Mock(spec=Vm) for _ in xrange(self.vm_count)]
    for mock_vm, vm_name in zip(mock_vms, vm_names):
      mock_vm.vm_name.return_value = vm_name
    self.cluster.find_vms_regex.return_value = mock_vms
    matching_vms = vmgroup.lookup_vms_by_name(vm_names)
    self.assertEqual(matching_vms, mock_vms)

  def test_lookup_vms_by_name_one_missing(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    vm_names = ["__curie_test_%s_test_group1_0000" % self.scenario.id,
                "__curie_test_%s_test_group1_0001" % self.scenario.id]
    mock_vms = [mock.Mock(spec=Vm) for _ in xrange(self.vm_count)]
    mock_vms[0].vm_name.return_value = vm_names[0]
    mock_vms[1].vm_name.return_value = "not_a_vm_name"
    self.cluster.find_vms_regex.return_value = mock_vms
    with self.assertRaises(CurieTestException):
      vmgroup.lookup_vms_by_name(vm_names)

  def test_lookup_vms_by_name_all_missing(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    vm_names = ["__curie_test_%s_test_group1_0000" % self.scenario.id,
                "__curie_test_%s_test_group1_0001" % self.scenario.id]
    mock_vms = [mock.Mock(spec=Vm) for _ in xrange(self.vm_count)]
    mock_vms[0].vm_name.return_value = "not_a_vm_name"
    mock_vms[1].vm_name.return_value = "also_not_a_vm_name"
    self.cluster.find_vms_regex.return_value = mock_vms
    with self.assertRaises(CurieTestException):
      vmgroup.lookup_vms_by_name(vm_names)

  def test_lookup_vms_by_name_subset(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    vm_names = ["__curie_test_%s_test_group1_0000" % self.scenario.id,
                "__curie_test_%s_test_group1_0001" % self.scenario.id]
    mock_vms = [mock.Mock(spec=Vm) for _ in xrange(self.vm_count)]
    for mock_vm, vm_name in zip(mock_vms, vm_names):
      mock_vm.vm_name.return_value = vm_name
    mock_vms.append(mock.Mock(spec=Vm))
    mock_vms[-1].vm_name.return_value = "the_last_fake_vm"
    self.cluster.find_vms_regex.return_value = mock_vms
    matching_vms = vmgroup.lookup_vms_by_name(vm_names)
    self.assertEqual(matching_vms, mock_vms[:-1])
    self.assertNotEqual(matching_vms, mock_vms)

  def test_get_nodes_for_placement_all(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    nodes = self.scenario.cluster.nodes()
    placement_nodes = vmgroup._get_nodes_from_string(":")
    self.assertEqual(placement_nodes, nodes[:])

  def test_get_nodes_for_placement_subset(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    nodes = self.scenario.cluster.nodes()
    placement_nodes = vmgroup._get_nodes_from_string("0")
    self.assertEqual(placement_nodes, [nodes[0]])
    placement_nodes = vmgroup._get_nodes_from_string("0:2")
    self.assertEqual(placement_nodes, nodes[0:2])
    placement_nodes = vmgroup._get_nodes_from_string("1:")
    self.assertEqual(placement_nodes, nodes[1:])

  def test_get_nodes_for_placement_mix(self):
    vmgroup = VMGroup.parse(self.scenario, "test_group1", self.valid_config)
    nodes = self.scenario.cluster.nodes()
    placement_nodes = vmgroup._get_nodes_from_string("0,1")
    self.assertEqual(placement_nodes, [nodes[0], nodes[1]])
    placement_nodes = vmgroup._get_nodes_from_string("0,1:2")
    self.assertEqual(placement_nodes, [nodes[0]] + nodes[1:2])
    placement_nodes = vmgroup._get_nodes_from_string("2:,0")
    self.assertEqual(placement_nodes, nodes[2:] + [nodes[0]])

  def test_get_nodes_for_placement_skip_one_node(self):
    config = self.valid_config.copy()
    config["nodes"] = "1:"
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    nodes = self.scenario.cluster.nodes()
    placement = vmgroup.get_clone_placement_map()
    self.assertEqual(len(placement), self.vm_count,
                     "Incorrect placement map length.")
    expected_vm_names = [
      "__curie_test_%s_test_group1_0000" % self.scenario.id,
      "__curie_test_%s_test_group1_0001" % self.scenario.id]
    self.assertEqual(sorted(placement.keys()), expected_vm_names)
    # VM Names are ordered in the list and we expect one vm per node, starting
    # with node 1.
    for xx, vm_name in enumerate(expected_vm_names):
      self.assertEqual(placement[vm_name], nodes[xx + 1])

  def test_data_disks_sequence(self):
    config = self.valid_config
    config["data_disks"] = [32, 32, 32, 32]
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.data_disks(), [32, 32, 32, 32])

  def test_data_disks_sequence_empty(self):
    config = self.valid_config
    config["data_disks"] = []
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.data_disks(), [])

  def test_data_disks_sequence_unequal_sizes(self):
    config = self.valid_config
    config["data_disks"] = [4, 4, 32, 32]
    with self.assertRaises(CurieTestException) as ar:
      VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(str(ar.exception),
                     "All data disks are required to be the same size "
                     "(given [4, 4, 32, 32])")

  def test_data_disks_dict(self):
    config = self.valid_config
    config["data_disks"] = {"size": 32, "count": 4}
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.data_disks(), [32, 32, 32, 32])

  def test_data_disks_dict_count_zero(self):
    config = self.valid_config
    config["data_disks"] = {"size": 32, "count": 0}
    vmgroup = VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(vmgroup.data_disks(), [])

  def test_data_disks_dict_keys_missing(self):
    config = self.valid_config
    config["data_disks"] = {"size": 32, "shape": "rectangle"}
    with self.assertRaises(CurieTestException) as ar:
      VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(str(ar.exception),
                     "data_disks is required to be either a list of disk "
                     "sizes or a dictionary with keys 'size' and 'count' "
                     "defined as integers (given {'shape': 'rectangle', "
                     "'size': 32})")

  def test_data_disks_not_an_integer(self):
    config = self.valid_config
    config["data_disks"] = [32, 32, 32, 32.5]
    with self.assertRaises(CurieTestException) as ar:
      VMGroup.parse(self.scenario, "test_group1", config)
    self.assertEqual(str(ar.exception),
                     "data_disks sizes must be defined as integers (given "
                     "32.5)")
