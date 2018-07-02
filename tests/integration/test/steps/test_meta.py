#
#  Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import random
import string
import unittest

import gflags

from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.testing import environment, util


class TestIntegrationStepsNode(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.group_name = "".join(
      [random.choice(string.printable)
       for _ in xrange(VMGroup.MAX_NAME_LENGTH)])
    self.scenario.vm_groups = {}

  def tearDown(self):
    # Do not remove VMs between tests, since these tests are written to run in
    # a specific order, preserving the state between each test.
    pass

  def test_monolithic(self):
    # Rolling upgrade one node
    # First make sure the cluster has all nodes powered on.
    for node in self.cluster.nodes():
      self.assertTrue(node.is_powered_on())
    # Deploy VMs, one per node.
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_node=1)}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    # Check that the VMs are placed appropriately
    placement_map = \
      self.scenario.vm_groups[self.group_name].get_clone_placement_map()
    for vm in self.scenario.vm_groups[self.group_name].get_vms():
      self.assertEqual(placement_map[vm.vm_name()].node_id(), vm.node_id(),
                       "VM %s not on expected node %s" %
                       (vm.vm_name(), vm.node_id()))
      self.assertTrue(vm.is_powered_on(),
                      "VM %s not powered on." % vm.vm_name())
      # TODO: Doesn't make sense to assert is_accessible yet. Haven't given
      # VMs a chance to become accessible.
      # self.assertTrue(vm.is_accessible(),
      #                 "VM %s not accessible." % vm.vm_name())

    steps.meta.RollingUpgrade(self.scenario, node_count=1, annotate=True)()
    # Check that the Node is back up
    for node in self.cluster.nodes():
      self.assertTrue(node.is_powered_on())
    # Check that the VMs are placed back where they were
    for vm in self.scenario.vm_groups[self.group_name].get_vms():
      self.assertEqual(placement_map[vm.vm_name()].node_id(), vm.node_id(),
                       "VM %s not on expected node %s" %
                       (vm.vm_name(), vm.node_id()))
      self.assertTrue(vm.is_powered_on(),
                      "VM %s not powered on." % vm.vm_name())
      # TODO: Doesn't make sense to assert is_accessible yet. Haven't given
      # VMs a chance to become accessible.
      # self.assertTrue(vm.is_accessible(),
      #                 "VM %s not accessible on." % vm.vm_name())
