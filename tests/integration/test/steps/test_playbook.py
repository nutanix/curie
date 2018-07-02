#
#  Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import logging
import random
import string
import unittest
import uuid

import gflags
import mock

from curie.acropolis_cluster import AcropolisCluster
from curie.exception import CurieTestException, CurieException
from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.testing import environment, util

log = logging.getLogger(__name__)

class TestIntegrationStepsVm(unittest.TestCase):

  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.uuid = str(uuid.uuid4())
    log.debug("Setting up test %s - tagged with UUID %s", self._testMethodName,
              self.uuid)
    self.group_name = self.uuid[0:VMGroup.MAX_NAME_LENGTH]

  @unittest.skip("Implement this test.")
  def test_ping_cvms(self):
    pass

  @unittest.skip("Implement this test.")
  def test_ping_nodes(self):
    pass

  @unittest.skip("Implement this test.")
  def test_ping_uvmgroups(self):
    vm_groups = [uuid.uuid4()[:VMGroup.MAX_NAME_LENGTH],
                 uuid.uuid4()[:VMGroup.MAX_NAME_LENGTH]]
    for vm_group in vm_groups:
      self.scenario.vm_groups[vm_group] = VMGroup(self.scenario, vm_group,
                                                  template="ubuntu1604",
                                                  template_type="DISK",
                                                  count_per_cluster=2)

    vms = steps.vm_group.CloneFromTemplate(self.scenario, "alpha")()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    for vm in self.cluster.find_vms([vm.vm_name() for vm in vms]):
      self.assertTrue(vm.is_powered_on())
      self.assertTrue(vm.is_accessible())
