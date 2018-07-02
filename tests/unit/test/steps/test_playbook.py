#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import unittest
from mock import Mock, call, patch, sentinel as sen

from curie.acropolis_cluster import AcropolisCluster
from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie import anteater
from curie.node import Node
from curie.scenario import Scenario, Phase
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.testing import environment
from curie.vsphere_cluster import VsphereCluster
from curie.vm import Vm


class TestStepsPlaybook(unittest.TestCase):

  def setUp(self):
    self.cluster_size = 4
    self.cluster = Mock(spec=Cluster)
    self.scenario = Mock(spec=Scenario)
    self.scenario.cluster = self.cluster
    self.scenario.phase = Phase(Phase.kRun)
    self.scenario.output_directory = environment.test_output_dir(self)
    self.scenario.vm_groups = {}
    self.scenario.cluster.vms.return_value = []
    self.scenario.cluster.nodes.return_value = []

  def __create_mock_nodes(self):
    nodes = [Mock(spec=Node) for _ in xrange(self.cluster_size)]
    ipaddr = TestStepsPlaybook.__ipaddr(1)
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.node_ip.return_value = next(ipaddr)
    self.scenario.cluster.nodes.return_value = nodes

  def __create_mock_cvms(self):
    cvms = self.__create_mock_vms(self.cluster_size, is_cvm=True, ip_index=5)
    self.scenario.cluster.vms.return_value.extend(cvms)

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

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
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
                     "sentinel.variables" % ", ".join(inventory),
                     step.description)

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
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
    self.assertEqual("running playbook sentinel.filename on %s" %
                     ", ".join(inventory), step.description)

  def test_groups_to_dict_invalid_uvmgroup(self):
    groups_in = ["badgroup"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run._groups_to_dict(groups_in, self.scenario)
    self.assertEqual("No VM Group named 'badgroup'.", err.exception.message)

  def test_groups_to_dict_empty_inventory(self):
    groups_in = ["cvms", "nodes"]
    #self.__create_mock_nodes()
    #self.__create_mock_cvms()  # do not add these hosts
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run._groups_to_dict(groups_in, self.scenario)
    expected_str = "No hosts matching group ['nodes', 'cvms'] found in"
    print(err.exception.message)
    self.assertTrue(err.exception.message.startswith(expected_str))

  def test_groups_to_dict_all(self):
    groups_in = ["cvms", "nodes", "group1", "group2"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    expected_groups = {"nodes": ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"],
                       "cvms": ["5.5.5.5", "6.6.6.6", "7.7.7.7", "8.8.8.8"],
                       "group1": ["9.9.9.9", "10.10.10.10", "11.11.11.11"],
                       "group2": ["12.12.12.12", "13.13.13.13", "14.14.14.14",
                                  "15.15.15.15", "16.16.16.16"]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_cvms(self):
    groups_in = ["cvms"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    expected_groups = {"cvms": ["5.5.5.5", "6.6.6.6", "7.7.7.7", "8.8.8.8"]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_nodes(self):
    groups_in = ["nodes"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    expected_groups = {"nodes": ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_uvmgroup(self):
    groups_in = ["group1"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    expected_groups = {"group1": ["9.9.9.9", "10.10.10.10", "11.11.11.11"]}
    self.assertEqual(expected_groups, actual_groups)

  def test_groups_to_dict_uvmgroups(self):
    groups_in = ["group1", "group2"]
    self.__create_mock_nodes()
    self.__create_mock_cvms()
    self.__create_mock_uvmgroup("group1", size=3, ip_index=9)
    self.__create_mock_uvmgroup("group2", size=5, ip_index=12)
    actual_groups = steps.playbook.Run._groups_to_dict(groups_in,
                                                       self.scenario)
    expected_groups = {"group1": ["9.9.9.9", "10.10.10.10", "11.11.11.11"],
                       "group2": ["12.12.12.12", "13.13.13.13", "14.14.14.14",
                                  "15.15.15.15", "16.16.16.16"]}
    self.assertEqual(expected_groups, actual_groups)

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.test.steps.playbook.Run._runtime_variables")
  @patch("curie.test.steps.playbook.Run._groups_to_dict")
  @patch("curie.test.steps.playbook.anteater")
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

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.test.steps.playbook.Run._runtime_variables")
  @patch("curie.test.steps.playbook.Run._groups_to_dict")
  @patch("curie.test.steps.playbook.anteater")
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

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=False)
  @patch("curie.test.steps.playbook.Run._runtime_variables")
  @patch("curie.test.steps.playbook.Run._groups_to_dict")
  @patch("curie.test.steps.playbook.anteater")
  def test_run_playbook_notfile(self, m_anteater, m_groups_to_dict,
                                m_add_runtime_variables, _):
    m_groups_to_dict.return_value = sen.inventory
    m_add_runtime_variables.return_value = sen.runtime_variables
    self.scenario.resource_path.return_value = sen.path
    m_anteater.run.return_value = sen.changed_hosts
    with self.assertRaises(CurieTestException) as err:
      steps.playbook.Run(self.scenario, sen.filename, ["grp1", "grp2"],
                         sen.remote_user, sen.remote_pass)
    self.assertEqual("Playbook sentinel.path does not exist.",
                     err.exception.message)

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.test.steps.playbook.Run._runtime_variables")
  @patch("curie.test.steps.playbook.Run._groups_to_dict")
  @patch("curie.test.steps.playbook.anteater")
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

  @patch("curie.test.steps.playbook.os.path.isfile", return_value=True)
  @patch("curie.test.steps.playbook.Run._runtime_variables")
  @patch("curie.test.steps.playbook.Run._groups_to_dict")
  @patch("curie.test.steps.playbook.anteater")
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
