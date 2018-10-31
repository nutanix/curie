#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import inspect
import logging
from collections import OrderedDict

from curie.exception import CurieTestException
from curie.log import patch_trace
from curie.name_util import NameUtil
from curie.steps import _util

log = logging.getLogger(__name__)
patch_trace()


class VMGroup(object):
  """
  The VMGroup object serves as a grouping of virtual machine objects.
  It groups their initial characteristics, and serves as a parser. It serves
  as a convenience and abstraction layer for use by step functions as well as
  the interpreter for the "vms" secion of a YAML test definition.
  """
  MAX_NAME_LENGTH = 18
  VALID_TEMPLATE_TYPES = set(["OVF", "DISK"])
  DEFAULT_TEMPLATE_TYPE = "DISK"
  DEFAULT_VCPUS = 2
  DEFAULT_RAM_MB = 2048
  DEFAULT_EXPORTER_PORTS = [9100]

  def __init__(self, scenario, name, template=None,
               template_type=DEFAULT_TEMPLATE_TYPE, vcpus=DEFAULT_VCPUS,
               ram_mb=DEFAULT_RAM_MB, data_disks=(), nodes=None,
               count_per_cluster=None, count_per_node=None,
               exporter_ports=DEFAULT_EXPORTER_PORTS):
    self._scenario = scenario
    self._name = name
    if len(self._name) > VMGroup.MAX_NAME_LENGTH:
      raise CurieTestException("VM group name '%s' must be less than %d "
                                "characters long" % (
                                self._name, VMGroup.MAX_NAME_LENGTH))
    if count_per_cluster is not None and count_per_node is not None:
      raise CurieTestException("Cannot specify both count_per_cluster and "
                                "count_per_node in VM group specification.")
    self._template = template
    self._template_type = template_type
    self._vcpus = vcpus
    self._ram_mb = ram_mb
    self._data_disks = data_disks
    self._nodes = nodes
    self._node_slice_str = ":"
    self._count_per_cluster = count_per_cluster
    self._count_per_node = count_per_node
    self.exporter_ports = exporter_ports

  def __str__(self):
    return "%s %s" % (self.__class__.__name__, self._name)

  @staticmethod
  def parse(scenario, name, definition):
    """Creates a VMGroup object using a definition structure.

    The definition dictionary must contain:
      - template (required): a string identifying a base template.
      - nodes (optional, default=":" or "all"): A string identifying how
      nodes should be selected from a list. The string is used as a list
      selection string directly in a python list. Multiple selections can
      be made by separating by a ",". Valid strings include: ":" to select
      all nodes, "1:3" to select nodes 1 through 2, "1:" to select nodes 1 to n,
      "0" to select node 0. Additionally "0,1" selects nodes 0 and 1, "0,
      2:4" selects node 0 and 2 through 3 etc.
      - count_per_cluster: an integer defining the total number of VMs to
      spread across the cluster on the nodes defined by "nodes", this value is
      mutually exclusive to count_per_node.
      - count_per_node: an integer defining the number of VMs to place on
      each node selected by "nodes".
      - exporter_ports: a list of ports on which the workload VMs are expected
      to provide prometheus-scrapable data. Defaults to [9100], the normal port
      for a node-exporter.

    Args:
      scenario: The Scenario object.
      name: The name of the vm group.
      definition: A dictionary that contains the necessary information to
        create a VMGroup object and its protected members.

    Returns:
      vmgroup: a new VMGroup object with protected members filled out using
        definition.
    """
    log.trace("Initializing VM group via parse")
    vmgroup = VMGroup(scenario, name)
    log.trace("Created %s" % vmgroup)
    log.trace("Parsing definition for %s" % vmgroup)
    vmgroup.__parse_definition(definition)
    log.trace("Done parsing definition for %s" % vmgroup)
    return vmgroup

  def total_count(self):
    """The total number of VMs for the VMGroup. Defaults to 1 per node."""
    if self._count_per_cluster:
      return self._count_per_cluster
    elif self._count_per_node:
      return self._count_per_node * len(self.nodes())
    if not self._count_per_node and not self._count_per_cluster:
      return len(self.nodes())

  def template_name(self):
    """The name of the template or goldimage the VMGroup is based on."""
    return self._template

  def template_type(self):
    """The type of the template. Could be OVF or DISK. Default is OVF."""
    return self._template_type

  def vcpus(self):
    """The number of vCPUs to provision for a DISK template."""
    return self._vcpus

  def ram_mb(self):
    """The amount of RAM in MB to provision for a DISK template."""
    return self._ram_mb

  def data_disks(self):
    """ Additional disks to attach to the template VM of DISK type.

    This is a list of integers representing the size of each disk in GB.
    """
    return self._data_disks

  def name(self):
    return self._name

  def nodes(self):
    """The nodes the on which members of the vmgroup may be placed."""
    if not self._nodes:
      self._nodes = self._get_nodes_from_string(self._node_slice_str)
    return self._nodes

  def get_vms(self):
    """Find VMs that match a group name for a given test.

    Returns:
      List of CurieVMs: Matching VMs.
    """
    pattern = self.__test_vm_name(".*")
    return self._scenario.cluster.find_vms_regex(pattern)

  def get_vms_names(self):
    """Generate a list of VM names for a VM group.

    Returns:
      List of str: VM names.

    Raises:
      CurieTestException:  If count exceeds the maximum group size.
    """
    max_digits = 4
    max_vms = 10 ** max_digits
    if self.total_count() > max_vms:
      raise CurieTestException("Too many VMs in group %s (%d, max: %d)" %
                                (self._name, self.total_count(), max_vms))
    return [self.__test_vm_name("%0*d" % (max_digits, i))
            for i in xrange(self.total_count())]

  def get_clone_placement_map(self):
    """Provides a mapping of vm names to target nodes for cloning.

    Returns:
      Dictionary: where keys are the vm name and the value is the node.
    """
    placement_map = OrderedDict()
    node_count = len(self.nodes())
    for number, vm_name in enumerate(self.get_vms_names()):
      placement_node = self.nodes()[number % node_count]
      placement_map[vm_name] = placement_node
    return placement_map

  def lookup_vms_by_name(self, vm_names):
    """Check that VMs exist for a list of VM names.

    Args:
      vm_names (list of str): VM names to verify.

    Returns:
      List of CurieVMs: Matching VMs.

    Raises:
      CurieTestException:
        If any VM name in vm_names is not found.
    """
    vms = self.get_vms()
    matching_vms = [vm for vm in vms if vm.vm_name() in vm_names]
    missing_vm_names = sorted(set(vm_names) -
                              set([vm.vm_name() for vm in matching_vms]))
    if len(missing_vm_names) > 0:
      raise CurieTestException(
        "%d VM(s) not found [%s]" %
        (len(missing_vm_names),
         ", ".join([vm_name for vm_name in missing_vm_names])))
    return matching_vms

  def __parse_definition(self, definition):
    """Translates a definition dictionary to protected variables."""
    self._template = definition.get("template", None)
    self.exporter_ports = definition.get("exporter_ports",
                                         self.DEFAULT_EXPORTER_PORTS)
    if self._template is None:
      raise CurieTestException("Template must be defined for VMGroup %s"
                                % self.name())
    self._template_type = definition.get("template_type",
                                         self.DEFAULT_TEMPLATE_TYPE)
    if self._template_type not in self.VALID_TEMPLATE_TYPES:
      raise CurieTestException(
        "Template type %s is not a valid template type. Must be one of %s." % (
          self._template_type, str(self.VALID_TEMPLATE_TYPES)))
    self._vcpus = definition.get("vcpus", self.DEFAULT_VCPUS)
    self._ram_mb = definition.get("ram_mb", self.DEFAULT_RAM_MB)
    self._data_disks = definition.get("data_disks", [])
    # data_disks may be defined as a dict, so convert that to a list of sizes,
    # e.g. [32, 32, 32]
    try:
      self._data_disks = [self._data_disks["size"]
                          for _ in xrange(self._data_disks["count"])]
    except (KeyError, TypeError):
      if not isinstance(self._data_disks, list):
        raise CurieTestException("data_disks is required to be either a list "
                                  "of disk sizes or a dictionary with keys "
                                  "'size' and 'count' defined as integers "
                                  "(given %r)" % self._data_disks)
    for size in self._data_disks:
      if not isinstance(size, int):
        raise CurieTestException("data_disks sizes must be defined as "
                                  "integers (given %r)" % size)
    # TODO(ryan.hardin): Remove this check if unequal disk sizes is supported.
    if len(set(self._data_disks)) > 1:
      raise CurieTestException("All data disks are required to be the same "
                                "size (given %r)" % self._data_disks)
    self._node_slice_str = definition.get("nodes", ":")
    self._count_per_cluster = definition.get("count_per_cluster", None)
    self._count_per_node = definition.get("count_per_node", None)
    if self._count_per_cluster is not None and self._count_per_node is not None:
      raise CurieTestException("Cannot specify both count_per_cluster and "
                                "count_per_node in VM group specification.")
    argspec = inspect.getargspec(VMGroup.__init__)
    for kwarg in definition.keys():
      if kwarg not in argspec.args:
        raise CurieTestException("Unexpected keyword argument '%s'" % kwarg)

  def _get_nodes_from_string(self, slice_string):
    """Allow for the user to specify list selection information. e.g. 2: or 2
    """
    return _util.get_nodes(self._scenario, slice_string)

  def __test_vm_name(self, vm_id):
    """Construct a VM name.

    Args:
      vm_id (str): Unique identifier for this VM.

    Returns:
      str: VM name.
    """
    pattern = "%s_%s" % (NameUtil.sanitize_filename(self._name), vm_id)
    return NameUtil.test_vm_name(self._scenario, pattern)
