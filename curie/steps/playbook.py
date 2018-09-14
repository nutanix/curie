#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

"""Curie Step for leveraging Ansible to execute actions on multiple hosts
in parallel.

"""

import logging
import os

from ipaddress import IPv4Address

from curie import anteater
from curie.exception import CurieTestException
from curie.steps._base_step import BaseStep

log = logging.getLogger(__name__)


class Run(BaseStep):
  """Executes an Ansible playbook found in the scenario directory.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    filename (str): Path to the playbook to execute relative to the scenario
        directory.
    inventory (list): list of groups on which to execute the playbook, can be
        combination of ['cvms', 'hosts', VM group names]
    remote_user (str): username used to initiate session to inventory hosts
    remote_pass (str): password for `remote_user` account
    variables (dict): variables to pass into the playbook

  Raises:
    CurieTestException:
      If unable to successfully execute the playbook due to errors.
      If unable to successfully execute the playbook due to unreachability.
  """

  def __init__(self, scenario, filename, inventory, remote_user, remote_pass,
               variables=None, annotate=False):
    super(Run, self).__init__(scenario, annotate=annotate)
    if variables is None:
      self.variables = {}
    else:
      self.variables = variables

    self.filename = self.scenario.resource_path(filename)
    if not os.path.isfile(self.filename):
      raise CurieTestException(
        cause=
        "Playbook '%s' does not exist." % self.filename,
        impact=
        "The scenario '%s' is not valid, and can not be started." %
        self.scenario.display_name,
        corrective_action=
        "If you are the author of the scenario, please check the syntax of "
        "the %s step; Ensure that the 'filename' parameter is set correctly, "
        "and that the playbook file has been included with the scenario." %
        self.name)

    self.inventory = inventory
    self.remote_user = remote_user
    self.remote_pass = remote_pass
    self.description = "running playbook %s on %s" % (filename, self.inventory)
    self.description += " with variables %s" % self.variables \
                        if self.variables else ""

  @staticmethod
  def _groups_to_dict(groups, scenario):
    """ Generates dictionary of group name keys whose values contain a list
    of sorted IP addresses or resolvable host names from the list of
    `groups` provided.

    The `groups` parameter is a list of strings chosen from `cvms`, `nodes`,
    or the name of any UVM group specified in the test YAML.

    Args:
      groups (list): list of strings identifying host groups

    Returns:
      Dictionary of group name keys whose values contain a list of sorted IP
      addresses or resolvable host names
    """
    group_dict = {}
    for group in groups:  # type: str
      if group == "cvms":
        cvm_ips = []
        for node in scenario.cluster.metadata().cluster_nodes:
          if getattr(node, "svm_addr"):
            cvm_ips.append(IPv4Address(node.svm_addr))
        group_dict[group] = [str(cvm_ip) for cvm_ip in sorted(cvm_ips)]
      elif group == "nodes":
        node_ips = []
        for node in scenario.cluster.metadata().cluster_nodes:
          if getattr(node, "id"):
            node_ips.append(IPv4Address(node.id))
        group_dict[group] = [str(node_ip) for node_ip in sorted(node_ips)]
      else:
        vm_group = scenario.vm_groups.get(group)
        if vm_group is None:
          raise CurieTestException("No VM Group named %r." % group)
        uvm_ips = [IPv4Address(unicode(vm.vm_ip()))
                   for vm in vm_group.get_vms()]
        group_dict[group] = [str(uvm_ip) for uvm_ip in sorted(uvm_ips)]
      if not group_dict[group]:
        raise CurieTestException("No hosts matching group %r found in %r. "
                                 % (group, scenario))
      log.debug("Adding Ansible inventory group %r with hosts: %r", group,
                group_dict[group])
    return group_dict

  @staticmethod
  def _runtime_variables(variables, scenario):
    """Add some useful variables to playbooks to allow them to be smarter.

    For instance, tell the playbook what kind of hypervisor the cluster is
    running so that the playbook may optionally use this information in
    conditional tasks.

    If a key to be added already exists in the dictionary, a warning is
    given. Users may override the variables below with their own values.

    Returned variables:
      variables["hypervisor"]: set to "ahv" or "esx"
      variables["output_directory"]: contain absolute path where files can
          be placed to be exported with the test

    Args:
      variables (dict): variables provided by the user via an YAML step
          definition

    Returns: dictionary parameter with additional keys containing useful
        information about the test and cluster
    """
    # Local imports to avoid circular dependencies.
    from curie.acropolis_cluster import AcropolisCluster
    from curie.vsphere_cluster import VsphereCluster

    # Set `hypervisor`
    if "hypervisor" in variables:
      log.warning("User has overriden playbook 'hypervisor' variable with "
                  "value: %r", variables["hypervisor"])
    elif isinstance(scenario.cluster, AcropolisCluster):
      variables["hypervisor"] = "ahv"
      log.debug("AHV cluster detected, adding 'hypervisor'=%r to variables.",
                variables["hypervisor"])
    elif isinstance(scenario.cluster, VsphereCluster):
      variables["hypervisor"] = "esx"
      log.debug("ESX cluster detected, adding 'hypervisor'=%r to variables.",
                variables["hypervisor"])
    else:
      log.debug("No hypervisor detected.")

    # Set `output_dir`
    if "output_directory" in variables:
      log.warning("User has overriden playbook 'output_directory' variable "
                  "with value: %r", variables["output_directory"])
    else:
      variables["output_directory"] = scenario.output_directory
    return variables

  def _run(self):
    """Run the Ansible playbook against the selected groups using the provided
    variables.

    Raises:
      CurieTestException:
        If unable to successfully execute the playbook due to errors.
        If unable to successfully execute the playbook due to unreachability.
    """
    log.info("Running playbook %r on groups %r as user %r with variables: %r",
             self.filename, self.inventory, self.remote_user, self.variables)
    group_dict = self._groups_to_dict(self.inventory, self.scenario)
    log.debug("Ansible runtime inventory: %r", group_dict)
    self.variables = self._runtime_variables(self.variables, self.scenario)
    log.debug("Ansible runtime variables: %r", self.variables)
    try:
      changed_hosts = anteater.run(self.filename, group_dict, self.variables,
                                   remote_user=self.remote_user,
                                   remote_pass=self.remote_pass)
    except anteater.AnteaterError as err:
      # use exception chaining in Python3, as `from err`
      raise CurieTestException(err.message)
    self.create_annotation("Playbook %s changed hosts %s." %
                           (self.filename, changed_hosts))
