#
#  Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

import gflags

from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie import steps
from curie.vm_group import VMGroup
from curie.testing import environment, util


class TestIntegrationStepsNodes(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.vm_group = VMGroup(self.scenario, "TestIntStepsNode",
                            template="ubuntu1604",
                            template_type="DISK",
                            count_per_node=1,
                            nodes=self.cluster.nodes()[0:2])
    self.scenario.vm_groups = {self.vm_group.name(): self.vm_group}

  def tearDown(self):
    # Do not remove VMs between tests, since these tests are written to run in
    # a specific order, preserving the state between each test.
    pass

  def test_monolithic(self):
    nodes = "0"
    # Node power off.
    # Create a VM group to be used in a later test.
    steps.vm_group.CloneFromTemplate(self.scenario, self.vm_group.name())()
    steps.vm_group.EnableHA(self.scenario, self.vm_group.name())()
    steps.nodes.PowerOff(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Node power off while node is already powered off.
    steps.nodes.PowerOff(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Powering off a non-existent node.
    with self.assertRaises(CurieTestException):
      steps.nodes.PowerOff(self.scenario, "99999")()

    # Node power on.
    steps.nodes.PowerOn(self.scenario, nodes)()
    # TODO (bostjan): ResetGroupPlacement also disables HA on migrated VMs on Hyper-V
    steps.vm_group.ResetGroupPlacement(self.scenario, self.vm_group.name())()
    steps.vm_group.DisableHA(self.scenario, self.vm_group.name())()
    # Perform VM group power on immediately after node power on.
    steps.vm_group.PowerOn(self.scenario, self.vm_group.name())()
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Node power on while node is already powered on.
    steps.nodes.PowerOn(self.scenario, nodes)()
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Powering on a non-existent node.
    with self.assertRaises(CurieTestException):
      steps.nodes.PowerOn(self.scenario, "99999")()

    # Waiting for power off.
    steps.vm_group.EnableHA(self.scenario, self.vm_group.name())()
    steps.nodes.PowerOff(self.scenario, nodes, wait_secs=0)()
    steps.nodes.WaitForPowerOff(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Waiting for power on.
    steps.nodes.PowerOn(self.scenario, nodes, wait_secs=0)()
    steps.nodes.WaitForPowerOn(self.scenario, nodes)()
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Power off timeout exceeded should throw exception.
    step = steps.nodes.PowerOff(self.scenario, nodes, wait_secs=1)
    with self.assertRaises(CurieTestException):
      step()
    # Wait for the node to get back to a known state before next test.
    steps.nodes.WaitForPowerOff(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Power on timeout exceeded should throw exception.
    step = steps.nodes.PowerOn(self.scenario, nodes, wait_secs=1)
    with self.assertRaises(CurieTestException):
      step()
    # Wait for the node to get back to a known state before next test.
    steps.nodes.WaitForPowerOn(self.scenario, nodes)()
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Shutdown node.
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())
    steps.nodes.Shutdown(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Shutdown an already off node.
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())
    steps.nodes.Shutdown(self.scenario, nodes)()
    self.assertFalse(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())
    # Prepare node for next test
    steps.nodes.PowerOn(self.scenario, nodes)()
    self.assertTrue(self.cluster.nodes()[0].is_powered_on())
    self.assertTrue(self.cluster.nodes()[1].is_powered_on())

    # Shutdown timeout exceeded should throw exception.
    step = steps.nodes.Shutdown(self.scenario, nodes, wait_secs=1)
    with self.assertRaises(CurieTestException):
      step()
    steps.nodes.WaitForPowerOff(self.scenario, nodes)()
    steps.nodes.PowerOn(self.scenario, nodes)()
