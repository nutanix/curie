# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#

"""Module for executing actions on multiple hosts in parallel using Ansible.

See https://serversforhackers.com/running-ansible-2-programmatically for
inspiration.
"""

import logging

from ansible.errors import AnsibleError
from ansible.executor import playbook_executor as pbe
from ansible.inventory.group import Group
from ansible.inventory.host import Host
from ansible.inventory.manager import InventoryManager
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from ansible.plugins.connection import ssh
from ansible.utils import display

from .errors import AnteaterError
from .errors import FailureError
from .errors import UnreachableError
from .options import Options


logger = logging.getLogger(__name__)


def _check_playbook_result(playbook, execution):
  """Check the results of an Ansible playbook execution.

  Args:
    playbook (str): path to the playbook
    execution: object containing post-execution host status

  Returns:
    list of changed hosts

  Raises:
    curie.anteater.FailureError: if any hosts generate an error
    curie.anteater.UnreachableError: if any hosts are unreachable
  """
  stats = execution._tqm._stats  # pylint: disable=protected-access
  hosts = sorted(stats.processed.keys())

  host_status = {"unreachable": [], "failures": [], "changed": []}
  for host in hosts:
    stats_summary = stats.summarize(host)
    logger.debug("Host %s summary: %s", host, stats_summary)
    for status, host_list in host_status.items():
      if stats_summary[status]:
        host_list.append(host)

  if host_status["failures"]:
    raise FailureError("Ansible playbook %r failed on hosts: %s" % (playbook,
                       host_status["failures"]))
  if host_status["unreachable"]:
    raise UnreachableError("Ansible playbook %r unreachable on hosts: %s" %
                           (playbook, host_status["unreachable"]))
  return host_status["changed"]


def _create_inventory(loader, host_list=None, groups=None):
  """ Returns an inventory object for playbook execution.

  Args:
    loader (ansible.parsing.dataloader.DataLoader): Ansible data loader for
        PlaybookExecutor
    host_list (list): list of IP addresses or resolvable host names for hosts
        that do not belong to a group
    groups (dict): dictionary of group name keys whose values contain a list of
        IP addresses or resolvable host names belonging to that group

  Returns: ansible.inventory.manager.InventoryManager for PlaybookExecutor
  """
  if groups is None:
    groups = {}
  if host_list is None:
    host_list = []

  inventory = InventoryManager(loader=loader)

  for host in host_list:
    inventory.add_host(host)

  for group_name, host_names in groups.iteritems():
    inventory.add_group(group_name)
    # for some reason it's necessary to use Ansible's reference to this group,
    # not the reference returned from Group()
    group = inventory.groups[group_name]
    for host_name in host_names:
      inventory.add_host(host_name, group=group_name)
      host = inventory.get_host(host_name)
      host.add_group(group)
      group.add_host(host)

  inventory.reconcile_inventory()
  return inventory


def run(playbook, inventory, variables, remote_user, remote_pass=None,
        become_method="sudo", become_user="root", verbosity=0):
  """ Execute an Ansible playbook.

  Args:
    playbook (str): path to playbook to be executed
    inventory (list or dict): list of IP addresses or resolvable host names, OR
        dictionary of group name keys whose values contain a list of IP
        addresses or resolvable host names
    variables (dict): Ansible variables to use with the playbook
    remote_user (str): host_list credential username
    remote_pass (str): host_list credential password
    become_method (str): method used to become another user after logging in.
        See
        http://docs.ansible.com/ansible/become.html#new-command-line-options
        for more information
    become_user (str): name of user to become after logging in.
        See http://docs.ansible.com/ansible/become.html#new-command-line-options
        for more information.
    verbosity (int): how verbose to make stdout output, 5 is very verbose

  Returns:
    list of changed hosts

  Raises:
    curie.anteater.FailureError: if any hosts generate an error
    curie.anteater.UnreachableError: if any hosts are unreachable
  """
  logger.debug("Running playbook %r with inventory=%r, variables=%r, "
               "verbosity=%r.", playbook, inventory, variables, verbosity)

  opts = Options({"become": False,
                  "become_user": become_user,
                  "become_method": become_method,
                  "diff": False,
                  "remote_user": remote_user,
                  "verbosity": verbosity})

  passwords = dict(conn_pass=remote_pass)

  try:
    loader = DataLoader()

    if isinstance(inventory, dict):
      inventory_manager = _create_inventory(loader, groups=inventory)
    elif isinstance(inventory, list):
      inventory_manager = _create_inventory(loader, host_list=inventory)
    else:
      raise AnteaterError("Unable to parse inventory type %r: %r" %
                          (type(inventory), inventory))

    variable_manager = VariableManager(loader=loader,
                                       inventory=inventory_manager)
    variable_manager.extra_vars = variables

    # Set display.logger and verbosity on modules, have to do this per-module.
    display.logger = logger
    ssh.display.verbosity = verbosity

    pbe.C.DEFAULT_SCP_IF_SSH = True
    pbe.C.HOST_KEY_CHECKING = False
    execution = pbe.PlaybookExecutor([playbook],
                                     inventory=inventory_manager,
                                     variable_manager=variable_manager,
                                     loader=loader,
                                     options=opts,
                                     passwords=passwords)
    execution.run()
    changed_hosts = _check_playbook_result(playbook, execution)
    logger.debug("Ansible playbook %r changed hosts: %r", playbook,
                 changed_hosts)
    return changed_hosts
  except AnsibleError as err:
    raise AnteaterError(err.message)
