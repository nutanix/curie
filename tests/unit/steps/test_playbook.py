#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import unittest
import uuid
from difflib import unified_diff

from mock import MagicMock, Mock, call, patch, sentinel as sen

from curie import anteater
from curie import steps
from curie.acropolis_cluster import AcropolisCluster
from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.node import Node
from curie.scenario import Scenario, Phase
from curie.testing import environment
from curie.vm import Vm
from curie.vm_group import VMGroup
from curie.vsphere_cluster import VsphereCluster


class TestStepsPlaybook(unittest.TestCase):

  def setUp(self):
    # Create a scenario
    self.scenario = Mock(spec=Scenario)
    self.scenario.display_name = "Mock Scenario"
    self.scenario.output_directory = environment.test_output_dir(self)
    self.scenario.vm_groups = {}
    self.scenario.phase = Phase(Phase.RUN)

    # Create a cluster
    self.cluster_size = 4
    self.scenario.cluster = Mock(spec=Cluster)
    self.scenario.cluster.nodes.return_value = []
    self.scenario.cluster.vms.return_value = []

    # Create nodes that get returned from APIs
    nodes = [Mock(spec=Node) for _ in xrange(self.cluster_size)]
    ipaddr = TestStepsPlaybook.__ipaddr(1)
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.node_ip.return_value = next(ipaddr)
    self.scenario.cluster.nodes.return_value = nodes

    # Create CVM VMs
    cvms = self.__create_mock_vms(self.cluster_size, is_cvm=True, ip_index=5)
    self.scenario.cluster.vms.return_value.extend(cvms)

    # Create nodes that are created from metadata/YAML
    cluster_nodes = [MagicMock(id=uuid.uuid4())
                     for _ in range(self.cluster_size)]
    ipaddr = TestStepsPlaybook.__ipaddr(self.cluster_size + 1)
    for cluster_node in cluster_nodes:
      cluster_node.svm_addr = next(ipaddr)
    self.scenario.cluster.metadata.return_value.cluster_nodes = cluster_nodes

  def __create_mock_uvmgroup(self, name, size=None, ip_index=9):
    if size is None:
      size = self.cluster_size
    uvms = self.__create_mock_vms(size, is_cvm=False, ip_index=ip_index)
    mock_vmgrp = Mock(spec=VMGroup)
    mock_vmgrp.name = name
    mock_vmgrp.scenario = self.scenario
    mock_vmgrp.get_vms.return_value = uvms
    self.scenario.vm_groups[name] = mock_vmgrp
    self.scenario.cluster.vms.return_value.extend(uvms)

  def __create_mock_vms(self, size=None, is_cvm=False, ip_index=0):
    if size is None:
      size = self.cluster_size
    mock_vms = []
    ipaddr = TestStepsPlaybook.__ipaddr(ip_index)
    for num in range(size):
      mock_vm = Mock(spec=Vm)
      mock_vm.vm_ip.return_value = next(ipaddr)
      mock_vm.is_cvm.return_value = is_cvm
      mock_vms.append(mock_vm)
    return mock_vms

  @staticmethod
  def __ipaddr(index=1):
    while True:
      yield "{i}.{i}.{i}.{i}".format(i=index)
      index += 1

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  def test_init_vars(self, _):
    self.scenario.resource_path.return_value = sen.path
    inventory = ["grp1", "grp2"]
    step = steps.playbook.Run(self.scenario, sen.filename, inventory,
                              sen.remote_user, sen.remote_pass, sen.variables)
    self.assertEqual(sen.path, step.filename)
    self.assertEqual(inventory, step.inventory)
    self.assertEqual(sen.remote_user, step.remote_user)
    self.assertEqual(sen.remote_pass, step.remote_pass)
    self.assertEqual(sen.variables, step.variables)
    self.assertEqual("running playbook sentinel.filename on %s with variables "
                     "sentinel.variables" % inventory, step.description)

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  def test_init_varsnone_defargs(self, _):
    self.scenario.resource_path.return_value = sen.path
    inventory = ["grp1", "grp2"]
    step = steps.playbook.Run(self.scenario, sen.filename, inventory,
                              sen.remote_user, sen.remote_pass)
    self.assertEqual(sen.path, step.filename)
    self.assertEqual(inventory, step.inventory)
    self.assertEqual({}, step.variables)
    self.assertEqual(sen.remote_user, step.remote_user)
    self.assertEqual(sen.remote_pass, step.remote_pass)
    self.assertEqual("running playbook sentinel.filename on %s" % inventory,
                     step.description)

  def test_groups_to_dict_invalid_uvmgroup(self):
    groups_in = ["badgroup"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run._groups_to_dict(groups_in, self.scenario)
    self.assertEqual("No VM Group named 'badgroup'.", err.exception.message)

  def test_groups_to_dict_empty_inventory(self):
    groups_in = ["group2", "group1"]
    self.__create_mock_uvmgroup("group1", size=0,
                                ip_index=(self.cluster_size * 2) + 1)
    self.__create_mock_uvmgroup("group2", size=0,
                                ip_index=(self.cluster_size * 2) + 1)
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run._groups_to_dict(groups_in, self.scenario)
    expected = "No hosts matching group 'group2' found in"
    actual = err.exception.message
    self.assertTrue(actual.startswith(expected),
                    "\n".join(unified_diff([expected], [actual])))

  def test_groups_to_dict_all(self):
    groups_in = ["cvms", "nodes", "group1", "group2"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    ipaddr = self.__ipaddr((self.cluster_size * 0) + 1)
    expected_groups = {"nodes": [next(ipaddr) for i in range(self.cluster_size)],
                       "cvms": [next(ipaddr) for i in range(self.cluster_size)],
                       "group1": [next(ipaddr) for i in range(3)],
                       "group2": [next(ipaddr) for i in range(5)]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_cvms(self):
    groups_in = ["cvms"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    ipaddr = self.__ipaddr((self.cluster_size * 1) + 1)
    expected_groups = {"cvms": [next(ipaddr) for i in range(self.cluster_size)]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_nodes(self):
    groups_in = ["nodes"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    ipaddr = self.__ipaddr((self.cluster_size * 0) + 1)
    expected_groups = {"nodes": [next(ipaddr) for i in range(self.cluster_size)]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_uvmgroup(self):
    groups_in = ["group1"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    ipaddr = self.__ipaddr((self.cluster_size * 2) + 1)
    expected_groups = {"group1": [next(ipaddr) for i in range(3)]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_uvmgroups(self):
    groups_in = ["group1", "group2"]
    self.__create_mock_uvmgroup("group1", size=3,
                                ip_index=((self.cluster_size * 2) + 1))
    self.__create_mock_uvmgroup("group2", size=5,
                                ip_index=((self.cluster_size * 2) + 1 + 3))
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    ipaddr = self.__ipaddr((self.cluster_size * 2) + 1)
    expected_groups = {"group1": [next(ipaddr) for i in range(3)],
                       "group2": [next(ipaddr) for i in range(5)]}
    self.assertEqual(expected_groups, actual_groups)

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.steps.playbook.Run._runtime_variables")
  @patch("curie.steps.playbook.Run._groups_to_dict")
  @patch("curie.steps.playbook.anteater")
  def test_run_normal(self, m_anteater, m_groups_to_dict,
                      m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.return_value = sen.changed_hosts
    step = steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                              sen.remote_user, sen.remote_pass, sen.variables)
    step()
    m_anteater.run.assert_has_calls([call(sen.path, sen.inventory,
                                          sen.runtime_variables,
                                          remote_user=sen.remote_user,
                                          remote_pass=sen.remote_pass)])

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.steps.playbook.Run._runtime_variables")
  @patch("curie.steps.playbook.Run._groups_to_dict")
  @patch("curie.steps.playbook.anteater")
  def test_run_varsnone_defargs(self, m_anteater, m_groups_to_dict,
                                m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.return_value = sen.changed_hosts
    step = steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                              sen.remote_user, sen.remote_pass)
    step()
    m_anteater.run.assert_has_calls([call(sen.path, sen.inventory,
                                          sen.runtime_variables,
                                          remote_user=sen.remote_user,
                                          remote_pass=sen.remote_pass)])

  @patch("curie.steps.playbook.os.path.isfile", return_value=False)
  @patch("curie.steps.playbook.Run._runtime_variables")
  @patch("curie.steps.playbook.Run._groups_to_dict")
  @patch("curie.steps.playbook.anteater")
  def test_run_playbook_notfile(self, m_anteater, m_groups_to_dict,
                                m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.return_value = sen.changed_hosts
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                         sen.remote_user, sen.remote_pass)
    self.assertIn(
      "Cause: Playbook 'sentinel.path' does not exist.\n\n"
      "Impact: The scenario 'Mock Scenario' is not valid, and can not be "
      "started.\n\n"
      "Corrective Action: If you are the author of the scenario, please check "
      "the syntax of the playbook.Run step; Ensure that the 'filename' "
      "parameter is set correctly, and that the playbook file has been "
      "included with the scenario.\n\n"
      "Traceback: None",
      str(err.exception))

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.steps.playbook.Run._runtime_variables")
  @patch("curie.steps.playbook.Run._groups_to_dict")
  @patch("curie.steps.playbook.anteater")
  def test_run_unreachable(self, m_anteater, m_groups_to_dict,
                           m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.side_effect = \
      anteater.UnreachableError("Hosts unreachable.")
    # give a real errors instead of a mock
    m_anteater.AnteaterError = anteater.AnteaterError
    step = steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                              sen.remote_user, sen.remote_pass)
    with self.assertRaises(CurieTestException) as err:
      step()
    self.assertEqual("Hosts unreachable.", err.exception.message)

  @patch("curie.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.steps.playbook.Run._runtime_variables")
  @patch("curie.steps.playbook.Run._groups_to_dict")
  @patch("curie.steps.playbook.anteater")
  def test_run_failure(self, m_anteater, m_groups_to_dict,
                       m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.side_effect = anteater.FailureError("Hosts failed.")
    # give a real error instead of a mock
    m_anteater.AnteaterError = anteater.AnteaterError
    step = steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                              sen.remote_user, sen.remote_pass)
    with self.assertRaises(CurieTestException) as err:
      step()
    self.assertEqual("Hosts failed.", err.exception.message)

  def test_runtime_variables_empty(self):
    ahv_cluster = Mock(spec=AcropolisCluster)
    self.scenario.cluster = ahv_cluster
    vars_in = {}
    vars_out_expected = {"hypervisor": "ahv",
                         "output_directory": environment.test_output_dir(self)}
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertDictEqual(vars_out_expected, vars_out_actual)

  def test_runtime_variables_hypervisor_ahv(self):
    ahv_cluster = Mock(spec=AcropolisCluster)
    self.scenario.cluster = ahv_cluster
    vars_in = {}
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertDictContainsSubset({"hypervisor": "ahv"}, vars_out_actual)

  def test_runtime_variables_hypervisor_esx(self):
    esx_cluster = Mock(spec=VsphereCluster)
    self.scenario.cluster = esx_cluster
    vars_in = {}
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertDictContainsSubset({"hypervisor": "esx"}, vars_out_actual)

  def test_runtime_variables_hypervisor_exists(self):
    ahv_cluster = Mock(spec=AcropolisCluster)
    self.scenario.cluster = ahv_cluster
    vars_in = {"hypervisor": "DEADBEEF"}
    vars_out_expected = vars_in
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertDictContainsSubset(vars_out_expected, vars_out_actual)

  def test_runtime_variables_hypervisor_unrecognized(self):
    vars_in = {}
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertIsNone(vars_out_actual.get("hypervisor"))

  def test_runtime_variables_outputdirectory_exists(self):
    vars_in = {"output_directory": "DEADBEEF"}
    vars_out_expected = vars_in
    vars_out_actual = steps.playbook.Run._runtime_variables(vars_in,
                                                            self.scenario)
    self.assertDictContainsSubset(vars_out_expected, vars_out_actual)
