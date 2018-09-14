#
#  Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import logging
import os
import unittest
import uuid
from ipaddress import IPv4Address

import gflags
from mock import patch

from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.steps import playbook, vm_group
from curie.vm_group import VMGroup
from curie.testing import environment, util
from curie.hyperv_cluster import HyperVCluster

log = logging.getLogger(__name__)


class TestIntegrationStepsVm(unittest.TestCase):

  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    self.scenario = Scenario(
      cluster=self.cluster,
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir,
      output_directory=environment.test_output_dir(self),
      source_directory=os.path.join(environment.resource_dir(), "playbooks"))
    self.scenario.vm_groups = dict()
    self.uuid = str(uuid.uuid4())
    log.debug("Setting up test %s - tagged with UUID %s", self._testMethodName,
              self.uuid)
    self.vm_group_names = \
      [(self.uuid[0:(VMGroup.MAX_NAME_LENGTH - 2)] + "-" + str(i))
       for i in range(2)]

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_ping_cvms(self):
    playbook_name = "ping.yml"
    playbook_path = os.path.join(self.scenario.source_directory, playbook_name)
    step = playbook.Run(self.scenario, playbook_name, ["cvms"], "nutanix",
                        "nutanix/4u")
    cvm_ips = [str(IPv4Address(unicode(vm.vm_ip())))
               for vm in self.scenario.cluster.vms() if vm.is_cvm()]
    with patch.object(step, "create_annotation") as m_annotate:
      step()
    self.assertGreaterEqual(len(cvm_ips), 1)
    m_annotate.assert_called_once_with("Playbook %s changed hosts %s." %
                                       (playbook_path, sorted(cvm_ips)))

  def test_ping_nodes(self):
    if isinstance(self.scenario.cluster, HyperVCluster):
      remote_user = "Administrator"
      remote_pass = "nutanix/4u"
      variables = {"ansible_connection": "winrm",
                   "ansible_port": 5986,
                   "ansible_winrm_server_cert_validation": "ignore"}
    else:
      remote_user = "root"
      remote_pass = "nutanix/4u"
      variables = {}

    playbook_name = "ping.yml"
    playbook_path = os.path.join(self.scenario.source_directory, playbook_name)
    step = playbook.Run(self.scenario, playbook_name, ["nodes"], remote_user,
                        remote_pass, variables=variables)
    node_ips = [str(IPv4Address(unicode(node.node_ip())))
                for node in self.scenario.cluster.nodes()]
    with patch.object(step, "create_annotation") as m_annotate:
      step()
    self.assertGreaterEqual(len(node_ips), 1)
    m_annotate.assert_called_once_with("Playbook %s changed hosts %s." %
                                       (playbook_path, sorted(node_ips)))

  def test_ping_uvmgroups(self):
    playbook_name = "ping.yml"
    playbook_path = os.path.join(self.scenario.source_directory, playbook_name)
    for vm_group_name in self.vm_group_names:
      self.scenario.vm_groups[vm_group_name] = VMGroup(self.scenario,
                                                       vm_group_name,
                                                       template="ubuntu1604",
                                                       template_type="DISK",
                                                       count_per_cluster=2)
      vm_group.CloneFromTemplate(self.scenario, vm_group_name)()
      vm_group.PowerOn(self.scenario, vm_group_name)()
    for vm in self.cluster.find_vms([vm.vm_name()
                                     for name, group in self.scenario.vm_groups.iteritems()
                                     for vm in group.get_vms()]):
      self.assertTrue(vm.is_powered_on())
      self.assertTrue(vm.is_accessible())
    step = playbook.Run(self.scenario, playbook_name,
                        [name for name in self.scenario.vm_groups.keys()],
                        "nutanix", "nutanix/4u")
    uvm_ips = [str(IPv4Address(unicode(vm.vm_ip())))
               for name, group in self.scenario.vm_groups.iteritems()
               for vm in group.get_vms()]
    self.assertGreaterEqual(len(uvm_ips), 1)
    with patch.object(step, "create_annotation") as m_annotate:
      step()
    m_annotate.assert_called_once_with("Playbook %s changed hosts %s." %
                                       (playbook_path, sorted(uvm_ips)))
