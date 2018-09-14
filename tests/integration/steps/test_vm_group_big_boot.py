#
#  Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
#
import random
import string
import unittest

import gflags

from curie.acropolis_cluster import AcropolisCluster
from curie.hyperv_cluster import HyperVCluster
from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie import steps
from curie.vm_group import VMGroup
from curie.testing import environment, util


class TestIntegrationStepsVmBigBoot(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.group_name = "".join(
      [random.choice(string.printable)
       for _ in xrange(VMGroup.MAX_NAME_LENGTH)])

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_big_boot_vdi_prism(self):
    if not isinstance(self.cluster, AcropolisCluster):
      raise unittest.SkipTest("Test requires a Prism cluster (found '%r')" %
                              type(self.cluster))
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_node=100)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()

    # As this is a large VM batch, it's inefficient to call 'is_powered_on'
    # on each VM individually, so generate a power state map with a bulk query.
    vm_id_power_state_map = self.scenario.cluster.get_power_state_for_vms(vms)
    power_states = set(map(lambda x: str(x).lower(),
                           vm_id_power_state_map.values()))
    self.assertTrue(power_states == set(["on"]),
                    msg="Not all VMs powered on. States: %s" % power_states)

    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_accessible())

  def test_big_boot_vdi_vmm(self):
    if not isinstance(self.cluster, HyperVCluster):
      raise unittest.SkipTest("Test requires a VMM cluster (found '%r')" %
                              type(self.cluster))
    self.scenario.vm_groups = {
      self.group_name: VMGroup(self.scenario, self.group_name,
                               template="ubuntu1604",
                               template_type="DISK",
                               count_per_node=100)}
    vms = steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()

    # As this is a large VM batch, it's inefficient to call 'is_powered_on'
    # on each VM individually, so generate a power state map with a bulk query.
    vm_id_power_state_map = self.scenario.cluster.get_power_state_for_vms(vms)
    power_states = set(map(lambda x: str(x).lower(),
                           vm_id_power_state_map.values()))
    self.assertTrue(power_states == set(["running"]),
                    msg="Not all VMs powered on. States: %s" % power_states)

    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_accessible())
