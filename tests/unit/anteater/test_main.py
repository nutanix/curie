#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import unittest

from mock import MagicMock, call, patch, sentinel as sen
from ansible.errors import AnsibleError
from ansible.executor.stats import AggregateStats
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from curie import anteater


class TestAnteater(unittest.TestCase):

  def setUp(self):
    pass

  def test_check_playbook_result_ok(self):
    m_execution = MagicMock()
    stats = MagicMock(spec=AggregateStats)
    # sentinels don't sort unless converted to strings
    stats.processed = {str(sen.hosta): 4, str(sen.hostb): 4,
                       str(sen.hostc): 4, str(sen.hostd): 4}
    stats.summarize.side_effect = [
      {"ok": 5, "failures": 0, "unreachable": 0, "changed": 0, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
      {"ok": 5, "failures": 0, "unreachable": 0, "changed": 0, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
    ]
    m_execution._tqm._stats = stats
    retval = anteater.main._check_playbook_result(sen.playbook, m_execution)
    stats.summarize.has_calls([call(sen.hosta), call(sen.hostb),
                               call(sen.hostc)])
    self.assertEqual(len(stats.processed), stats.summarize.call_count)
    self.assertEqual([str(sen.hostb), str(sen.hostd)], retval)

  def test_check_playbook_result_failure(self):
    m_execution = MagicMock()
    stats = MagicMock(spec=AggregateStats)
    # sentinels don't sort unless converted to strings
    stats.processed = {str(sen.hosta): 4, str(sen.hostb): 4,
                       str(sen.hostc): 4, str(sen.hostd): 4}
    stats.summarize.side_effect = [
      {"ok": 4, "failures": 1, "unreachable": 0, "changed": 0, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
      {"ok": 4, "failures": 1, "unreachable": 0, "changed": 0, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
    ]
    m_execution._tqm._stats = stats
    with self.assertRaises(anteater.FailureError) as err:
      anteater.main._check_playbook_result(sen.playbook, m_execution)
    stats.summarize.has_calls([call(sen.hosta), call(sen.hostb),
                               call(sen.hostc)])
    self.assertEqual("Ansible playbook sentinel.playbook failed on "
                     "hosts: ['sentinel.hosta', 'sentinel.hostc']",
                     err.exception.message)

  def test_check_playbook_result_unreachable(self):
    m_execution = MagicMock()
    stats = MagicMock(spec=AggregateStats)
    # sentinels don't sort unless converted to strings
    stats.processed = {str(sen.hosta): 4, str(sen.hostb): 4,
                       str(sen.hostc): 4, str(sen.hostd): 4}
    stats.summarize.side_effect = [
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 1, "changed": 0, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 0, "changed": 1, "skipped": 1},
      {"ok": 4, "failures": 0, "unreachable": 1, "changed": 0, "skipped": 1},
    ]
    m_execution._tqm._stats = stats
    with self.assertRaises(anteater.UnreachableError) as err:
      anteater.main._check_playbook_result(sen.playbook, m_execution)
    stats.summarize.has_calls([call(sen.hosta), call(sen.hostb),
                               call(sen.hostc)])
    self.assertEqual("Ansible playbook sentinel.playbook unreachable on "
                     "hosts: ['sentinel.hostb', 'sentinel.hostd']",
                     err.exception.message)

  @patch("curie.anteater.main.InventoryManager")
  def test_create_inventory_groups(self, m_inventory_manager):
    groups = {"cvms":  ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"],
              "nodes": ["5.5.5.5", "6.6.6.6", "7.7.7.7", "8.8.8.8"]}

    m_inv_get_host = MagicMock()
    m_inventory_manager.return_value.attach_mock(m_inv_get_host, 'get_host')

    # `name` hack for breaking mock parent/child relationship
    m_host = MagicMock(name="not-a-child")
    m_inv_get_host.return_value = m_host

    inventory = anteater.main._create_inventory(sen.loader, groups=groups)

    # inventory = InventoryManager(loader=loader)
    m_inventory_manager.assert_called_once_with(loader=sen.loader)

    # inventory.add_group(group_name)
    expected_inv_add_group_calls = [call(key) for key in groups.keys()]
    m_inventory_manager.return_value.add_group.has_calls(expected_inv_add_group_calls)
    self.assertEqual(len(expected_inv_add_group_calls),
                     m_inventory_manager.return_value.add_group.call_count)

    # inventory.add_host(host_name, group=group_name)
    expected_inv_add_host_calls = [call(host, group=group_name)
                                   for group_name, host_list in groups.iteritems()
                                   for host in host_list]
    m_inventory_manager.return_value.add_host.assert_has_calls(
      expected_inv_add_host_calls)
    self.assertEqual(len(expected_inv_add_host_calls),
                     m_inventory_manager.return_value.add_host.call_count)

    # inventory.get_host(host_name)
    expected_inv_get_host_calls = [call(host)
                                   for group_name, host_list in groups.iteritems()
                                   for host in host_list]
    m_inv_get_host.assert_has_calls(expected_inv_get_host_calls)
    self.assertEqual(len(expected_inv_get_host_calls),
                     m_inv_get_host.call_count)

    # host.add_group(group)
    expected_host_add_group_calls = [call(group_name)
                                     for group_name, host_list in groups.iteritems()
                                     for host in host_list]
    self.assertEqual(len(expected_host_add_group_calls),
                     m_host.add_group.call_count)

    # group.add_host(host)
    expected_group_add_host_calls = [call(host)
                                     for group_name, host_list in groups.iteritems()
                                     for host in host_list]
    self.assertEqual(len(expected_group_add_host_calls),
                     m_inventory_manager.return_value.groups.__getitem__.return_value.add_host.call_count)

    # inventory.reconcile_inventory()
    m_inventory_manager.return_value.reconcile_inventory\
      .assert_called_once_with()

    # return inventory
    self.assertEqual(m_inventory_manager.return_value, inventory)

  @patch("curie.anteater.main.InventoryManager")
  def test_create_inventory_host_list(self, m_inventory_manager):
    host_list = ["1.1.1.1", "2.2.2.2", "3.3.3.3", "4.4.4.4"]

    inventory = anteater.main._create_inventory(sen.loader,
                                                host_list=host_list)

    # inventory = InventoryManager(loader=loader)
    m_inventory_manager.assert_called_once_with(loader=sen.loader)

    # inventory.add_host(host_name, group=group_name)
    expected_inv_add_host_calls = [call(host) for host in host_list]
    m_inventory_manager.return_value.add_host.assert_has_calls(
      expected_inv_add_host_calls)
    self.assertEqual(len(expected_inv_add_host_calls),
                     m_inventory_manager.return_value.add_host.call_count)

    # inventory.reconcile_inventory()
    m_inventory_manager.return_value.reconcile_inventory \
      .assert_called_once_with()

    # return inventory
    self.assertEqual(m_inventory_manager.return_value, inventory)

  @patch("curie.anteater.main.InventoryManager")
  def test_create_inventory_none(self, m_inventory_manager):
    inventory = anteater.main._create_inventory(sen.loader)

    # inventory = InventoryManager(loader=loader)
    m_inventory_manager.assert_called_once_with(loader=sen.loader)

    # inventory.add_group(group_name)
    m_inventory_manager.return_value.add_group.assert_not_called()

    # inventory.add_host(host_name, group=group_name)
    m_inventory_manager.return_value.add_host.assert_not_called()

    # inventory.reconcile_inventory()
    m_inventory_manager.return_value.reconcile_inventory\
      .assert_called_once_with()

    # return inventory
    self.assertEqual(m_inventory_manager.return_value, inventory)

  @patch("curie.anteater.main._check_playbook_result",
         return_value=sen.changed_hosts)
  @patch("curie.anteater.main.pbe")
  @patch("curie.anteater.main.ssh")
  @patch("curie.anteater.main.logger", return_value=sen.logger)
  @patch("curie.anteater.main.display")
  @patch("curie.anteater.main._create_inventory", return_value=sen.inventory)
  @patch("curie.anteater.main.VariableManager", spec=VariableManager)
  @patch("curie.anteater.main.DataLoader", spec=DataLoader,
         return_value=sen.dataloader)
  @patch("curie.anteater.main.Options", spec=anteater.Options,
         return_value=sen.options)
  def test_run_host_list(self, m_options, m_dataloader, m_variablemanager,
                      m_create_inventory, m_display, m_logger, m_ssh,
                      m_pbe_mod, m_check_playbook_result):
    m_pbe = MagicMock(spec=PlaybookExecutor)
    m_execution = MagicMock()
    m_pbe.return_value = m_execution
    m_pbe_mod.PlaybookExecutor = m_pbe
    inventory = [sen.hosta, sen.hostb]
    retval = anteater.main.run(sen.playbook, inventory, sen.vars,
                               sen.remote_user, sen.remote_pass,
                               sen.become_method, sen.become_user,
                               sen.verbosity)

    expected_options = {"become": False,
                        "become_user": sen.become_user,
                        "become_method": sen.become_method,
                        "diff": False,
                        "remote_user": sen.remote_user,
                        "verbosity": sen.verbosity}
    m_options.assert_called_once_with(expected_options)
    m_dataloader.assert_called_once()
    m_variablemanager.assert_called_once_with(loader=m_dataloader.return_value,
                                              inventory=sen.inventory)
    self.assertEqual(sen.vars, m_variablemanager.return_value.extra_vars)
    self.assertEqual(m_logger, m_display.logger)
    self.assertEqual(sen.verbosity, m_ssh.display.verbosity)
    self.assertTrue(m_pbe_mod.C.DEFAULT_SCP_IF_SSH)
    self.assertFalse(m_pbe_mod.C.HOST_KEY_CHECKING)
    m_pbe.assert_called_once_with([sen.playbook],
                                  inventory=m_create_inventory.return_value,
                                  variable_manager=m_variablemanager.return_value,
                                  loader=m_dataloader.return_value,
                                  options=m_options.return_value,
                                  passwords=dict(conn_pass=sen.remote_pass))
    m_execution.run.assert_called_once()
    m_check_playbook_result.assert_called_once_with(sen.playbook,
                                                    m_pbe.return_value)
    self.assertEqual(sen.changed_hosts, retval)

  @patch("curie.anteater.main._check_playbook_result",
         return_value=sen.changed_hosts)
  @patch("curie.anteater.main.pbe")
  @patch("curie.anteater.main.ssh")
  @patch("curie.anteater.main.logger", return_value=sen.logger)
  @patch("curie.anteater.main.display")
  @patch("curie.anteater.main._create_inventory", return_value=sen.inventory)
  @patch("curie.anteater.main.VariableManager", spec=VariableManager)
  @patch("curie.anteater.main.DataLoader", spec=DataLoader,
         return_value=sen.dataloader)
  @patch("curie.anteater.main.Options", spec=anteater.Options,
         return_value=sen.options)
  def test_run_host_list(self, m_options, m_dataloader, m_variablemanager,
                         m_create_inventory, m_display, m_logger, m_ssh,
                         m_pbe_mod, m_check_playbook_result):
    m_pbe = MagicMock(spec=PlaybookExecutor)
    m_execution = MagicMock()
    m_pbe.return_value = m_execution
    m_pbe_mod.PlaybookExecutor = m_pbe
    inventory = {sen.groupa: [sen.hosta, sen.hostb]}
    retval = anteater.main.run(sen.playbook, inventory, sen.vars,
                               sen.remote_user, sen.remote_pass,
                               sen.become_method, sen.become_user,
                               sen.verbosity)

    expected_options = {"become": False,
                        "become_user": sen.become_user,
                        "become_method": sen.become_method,
                        "diff": False,
                        "remote_user": sen.remote_user,
                        "verbosity": sen.verbosity}
    m_options.assert_called_once_with(expected_options)
    m_dataloader.assert_called_once()
    m_variablemanager.assert_called_once_with(loader=m_dataloader.return_value,
                                              inventory=sen.inventory)
    self.assertEqual(sen.vars, m_variablemanager.return_value.extra_vars)
    self.assertEqual(m_logger, m_display.logger)
    self.assertEqual(sen.verbosity, m_ssh.display.verbosity)
    self.assertTrue(m_pbe_mod.C.DEFAULT_SCP_IF_SSH)
    self.assertFalse(m_pbe_mod.C.HOST_KEY_CHECKING)
    m_pbe.assert_called_once_with([sen.playbook],
                                  inventory=m_create_inventory.return_value,
                                  variable_manager=m_variablemanager.return_value,
                                  loader=m_dataloader.return_value,
                                  options=m_options.return_value,
                                  passwords=dict(conn_pass=sen.remote_pass))
    m_execution.run.assert_called_once()
    m_check_playbook_result.assert_called_once_with(sen.playbook,
                                                    m_pbe.return_value)
    self.assertEqual(sen.changed_hosts, retval)

  @patch("curie.anteater.main._check_playbook_result",
         return_value=sen.changed_hosts)
  @patch("curie.anteater.main.pbe")
  @patch("curie.anteater.main.ssh")
  @patch("curie.anteater.main.logger", return_value=sen.logger)
  @patch("curie.anteater.main.display")
  @patch("curie.anteater.main._create_inventory", return_value=sen.inventory)
  @patch("curie.anteater.main.VariableManager", spec=VariableManager)
  @patch("curie.anteater.main.DataLoader", spec=DataLoader,
         return_value=sen.dataloader)
  @patch("curie.anteater.main.Options", spec=anteater.Options,
         return_value=sen.options)
  def test_run_host_list(self, m_options, m_dataloader, m_variablemanager,
                         m_create_inventory, m_display, m_logger, m_ssh,
                         m_pbe_mod, m_check_playbook_result):
    m_pbe = MagicMock(spec=PlaybookExecutor)
    m_execution = MagicMock()
    m_pbe.return_value = m_execution
    m_pbe_mod.PlaybookExecutor = m_pbe
    inventory = sen.badtype
    with self.assertRaises(anteater.AnteaterError) as err:
      anteater.main.run(sen.playbook, inventory, sen.vars, sen.remote_user,
                        sen.remote_pass, sen.become_method, sen.become_user,
                        sen.verbosity)
    self.assertEqual("Unable to parse inventory type "
                     "<class 'mock.mock._SentinelObject'>: sentinel.badtype",
                     err.exception.message)

  @patch("curie.anteater.main.DataLoader", spec=DataLoader,
         side_effect=AnsibleError(sen.message))
  def test_run_ansibleerror(self, _):
    with self.assertRaises(anteater.AnteaterError) as err:
      anteater.main.run(sen.playbook, sen.host_list, sen.vars, sen.remote_user,
                        sen.remote_pass, sen.become_method, sen.become_user,
                        sen.verbosity)
    # Ansible modifies exception.message instead passing things in/out cleanly
    self.assertEqual(str(sen.message), err.exception.message)
