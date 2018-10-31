#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

import logging
import re
from abc import ABCMeta, abstractmethod
from functools import partial

from curie.curie_metrics_pb2 import CurieMetric
from curie.error import CurieError
from curie.exception import CurieException, CurieTestException
from curie.log import CHECK
from curie.metrics_util import MetricsUtil
from curie.node import NodePropertyNames
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class Cluster(object):
  __metaclass__ = ABCMeta
  __CURIE_METRIC_NAME_MAP__ = None

  def __init__(self, cluster_metadata):
    # Cluster name. This is a unique name in the curie server's settings.
    self._name = cluster_metadata.cluster_name

    # Cluster metadata (a CurieSettings.Cluster protobuf).
    self._metadata = cluster_metadata

    # Map of node IDs to their node data (CurieSettings.Cluster.Node)
    self._node_id_metadata_map = dict(
      [(node.id, node) for node in cluster_metadata.cluster_nodes])

    # Number of nodes in the cluster. (Stored to avoid the more expensive
    # lookup involved in something like len(self.nodes()).
    self._node_count = len(self._node_id_metadata_map.keys())

  def __eq__(self, other):
    return self.name() == other.name()

  def __hash__(self):
    return hash(self.name())

  def name(self):
    return self._name

  def metadata(self):
    return self._metadata

  @classmethod
  def metrics(cls):
    metrics = [
      CurieMetric(name=CurieMetric.kCpuUsage,
                   description="Average CPU usage for all cores.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kPercent,
                   experimental=True),
      # TODO(ryan.hardin): Use kHertz here instead of kMegahertz.
      CurieMetric(name=CurieMetric.kCpuUsage,
                   description="Average CPU usage for all cores.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kMegahertz,
                   experimental=True),
      CurieMetric(name=CurieMetric.kMemUsage,
                   description="Average memory usage.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kPercent,
                   experimental=True),
      CurieMetric(name=CurieMetric.kNetReceived,
                   description="Average network data received across all "
                               "interfaces.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kKilobytes,
                   rate=CurieMetric.kPerSecond),
      CurieMetric(name=CurieMetric.kNetTransmitted,
                   description="Average network data transmitted across all "
                               "interfaces.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kKilobytes,
                   rate=CurieMetric.kPerSecond),
    ]
    return metrics

  @abstractmethod
  def update_metadata(self, include_reporting_fields):
    """
    Update the cluster's metadata with additional information (if available)
    about the corresponding cluster. If 'include_reporting_fields' is True,
    also include additional information intended for reporting.

    Note: this should only be called before the test begins executing steps in
    the SETUP, RUN, and TEARDOWN phases.
    """

  @abstractmethod
  def nodes(self):
    "Returns a list of Node objects for the nodes in the cluster."

  def node_count(self):
    """
    Returns the number of nodes in the cluster.
    """
    return self._node_count

  @abstractmethod
  def power_off_nodes_soft(self, nodes, timeout_secs, async=False):
    """
    Power off 'nodes' via cluster management software.

    Args:
      'nodes' (list<? extends Node>): List of nodes to power off.
      'timeout_secs' (int|float): Timeout in seconds for all nodes to power
        off.  If 'async' is True this is inclusive of the time until power
        status is confirmed.
      'async' (bool): Optional. If False, block until all of 'nodes' are
        confirmed to be powered off, or until 'timeout_secs' elapses.
        (Default False)

    Raises:
      CurieException<kTimeout> on timeout.
    """

  @abstractmethod
  def vms(self):
    """Returns a list of VM objects for the VMs in the cluster."""

  @abstractmethod
  def power_on_vms(self, vms, max_parallel_tasks=None):
    """Powers on VMs and verifies that an IP address is assigned.

    Args:
      vms: List of VMs to power on.
      max_parallel_tasks (int): The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: VMs fail to acquire an IP address within the
      timeout.
    """

  @abstractmethod
  def power_off_vms(self, vms, max_parallel_tasks=None):
    """Powers off VMs.

    Args:
      vms: List of VMs to power off.
      max_parallel_tasks int: The number of VMs to power off in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: VMs fail to power off within the timeout.
    """

  @abstractmethod
  def get_power_state_for_vms(self, vms):
    """
    Returns a map of VM IDs for 'vms' to their respective power states.

    A VM's ID will map to None upon error querying its power state.

    NB: The representation of the VM's power state is currently dependent
    on the management software running on the cluster.
    """

  @abstractmethod
  def get_power_state_for_nodes(self, nodes):
    """
    Returns a map of Node IDs for 'nodes' to their respective power states.

    A Node's ID will map to None upon error querying its power state.

    Args:
      nodes (list<Node>): Nodes whose power state to check.
      sync_with_oob (bool): Optional. If True, confirm power state via OOB
        checks and refresh management software if necessary.

    NB: The representation of the Node's power state is currently dependent
    on the management software running on the cluster.
    """

  @abstractmethod
  def sync_power_state_for_nodes(self, nodes, timeout_secs=None):
    """
    Synchronize management software and OOB power state for 'nodes'.

    Confirm power state via OOB checks matches value reported by management
    software. If there is a mismatch, refresh management software, and verify
    that the state is synchronized.

    Args:
      nodes (list<Node>): Nodes whose power state to sync.
      timeout_secs (int): Maximum amount of time to try to sync, in seconds.

    Returns:
      dict: Map of Node IDs for 'nodes' to their respective power states.

    Raises:
      CurieTestException:
        If power states cannot be synchronized.

    NB: The representation of the Node's power state is currently dependent
    on the management software running on the cluster.
    """

  @abstractmethod
  def delete_vms(self, vms, ignore_errors=False, max_parallel_tasks=None):
    """Delete VMs.

    Acropolis DELETE requests for /vms/{vm_id} are async. This method collects
    all taskUuids and polls until completion.

    Args:
      vms (list<Vm>): List of VMs to delete.
      ignore_errors (bool): Optional. Whether to allow individual tasks to
        fail. Default False.
      max_parallel_tasks (int): Max number of requests to have in-flight at
        any given time. (Currently ignored)

    Raises:
      CurieTestException:
        - If any VM is not already powered off.
        - All VMs are not destroyed with in the timeout.
        - Destroy task fails and ignore_errors is False.
    """

  @abstractmethod
  def import_vm(self, goldimages_directory, goldimage_name, vm_name,
                node_id=None):
    """
    Creates a VM from the specified gold image. The gold image 'goldimage_name'
    must be a gold image that the curie server has access to. If 'vm_name' is
    specified and the underlying cluster supports assigning a name to a VM on
    an import, assign the imported VM this name. If the underlying cluster
    supports creating a VM and binding it to a specific node in the cluster,
    'node_id' can be specified for this purpose.
    """

  @abstractmethod
  def create_vm(self, goldimages_directory, goldimage_name, vm_name, vcpus=1,
                ram_mb=1024, node_id=None, datastore_name=None, data_disks=()):
    """
    Creates a VM with the specifications provided.

    The new VM will have an OS disk copied from the goldimage provided. It is
    placed on the requested node and datastore. Additional data disks are
    allocated if requested.

    Args:
      goldimage_name (str): Name of the goldimage.
      vm_name (str): Name of the VM
      vcpus (int): Number of vCPUs.
      ram_mb (int): RAM in MB
      node_id (str): Node identifier
      datastore_name (str): Name of the datastore to put VM on.
      data_disks (list): List of additional data disk sizes to attach to the
        VM in gigabytes. e.g. [10,10] will create 2 disks of 10 GB each.

    Returns:
      curie.vm: VM that was created.
    """

  @abstractmethod
  def clone_vms(self, vm, vm_names, node_ids=(), datastore_name=None,
                max_parallel_tasks=None, linked_clone=False):
    """
    Clones the VM 'vm' to create VMs with names 'vm_names'. If the underlying
    cluster supports cloning VMs and binding them to specific nodes in the
    cluster, 'node_ids' can be specified for this purpose.

    Note:
      Subclasses should provide a meaningful default value for
      'max_parallel_tasks' or None if it is not used by its implementation.
    """

  @abstractmethod
  def migrate_vms(self, vms, nodes, max_parallel_tasks=None):
    """Move 'vms' to 'nodes'.

    For each VM 'vms[xx]' move to the corresponding Node 'nodes[xx]'.

    Args:
      vms list of Vms: List of VMs to migrate.
      nodes  list of Nodes: Must be the same length as 'vms'. Each VM in 'vms'
        wll be moved to the corresponding node in 'nodes'.
      max_parallel_tasks int: The number of VMs to power on in parallel.

    Note:
      Subclasses should provide a meaningful default value for
      'max_parallel_tasks' or None if it is not used by its implementation.
    """

  @abstractmethod
  def snapshot_vms(self, vms, tag=None, max_parallel_tasks=None):
    """
    For each VM in vms on the cluster, creates a snapshot with snapshot name
    'snapshot_names[xx]' and optional description 'snapshot_descriptions[xx]'.

    Args
      vms (list<CurieVMs>): List of VMs to create snapshots for.
      snapshot_names list of strings: Names for snapshot which must be the same
        length as 'vms'.
      snapshot_descriptions (list<str>): List of escriptions for each
        snapshot corresponding to 'vms' and 'snapshot_names'. If provided it
        must be the same length as 'vms'.
      max_parallel_tasks (int|None): Optional. If provided, max number of
        management server tasks to perform in parellel. Default None.
    """

  @abstractmethod
  def relocate_vms_datastore(self, vms, datastore_names,
                             max_parallel_tasks=None):
    """Relocate 'vms' to a datastore/container with names 'datastore_names'.

    For each VM 'vms[xx]' move to the datastore_names[xx].

    Args:
      vms list of CurieVms: List of VMs to migrate.
      datastore_names : Must be the same length as 'vms'. Each
        VM in 'vms' wll be moved to the corresponding datastore in
        'datastore_names'.
      max_parallel_tasks (int): The number of VMs to power on in parallel.

    Note:
      Subclasses should provide a meaningful default value for
      'max_parallel_tasks' or None if it is not used by its implementation.
    """

  @abstractmethod
  def enable_ha_vms(self, vms):
    """
    Enable HA on VMs.

    Args:
      vms: list of CurieVms: List of VMs enable HA on.
    """

  @abstractmethod
  def disable_ha_vms(self, vms):
    """
    Disable HA on VMs.

    Args:
      vms: list of CurieVms: List of VMs disable HA on.

    """

  @abstractmethod
  def disable_drs_vms(self, vms):
    """
    Disable DRS on VMs.

    Args:
      vms: list of CurieVms: List of VMs disable DRS on.

    """

  @abstractmethod
  def cleanup(self, test_ids=()):
    """
    Clean up any state (e.g., lingering goldimages) on the cluster
    for specified tests. If no test IDs are specified (empty list), then state
    is cleaned up for all tests regardless of their state.
    """

  @abstractmethod
  def collect_performance_stats(self,
                                start_time_secs=None,
                                end_time_secs=None):
    """Collect performance statistics for all nodes in the cluster.

    Args:
      start_time_secs (int): The oldest sample to return, in seconds since
        epoch.
      end_time_secs (int): The newest sample to return, in seconds since epoch.

    Returns:
      (dict) Dict of list of curie_metrics_pb2.CurieMetric. Top level dict
        keyed by node ID. List contains one entry per metric.
    """

  @abstractmethod
  def is_ha_enabled(self):
    """
    Checks whether cluster has any high availbility service enabled.

    Returns:
      (bool) True if HA is enabled, else False.
    """

  @abstractmethod
  def is_drs_enabled(self):
    """
    Checks whether cluster has any dynamic resource scheduling service enabled.

    Returns:
      (bool) True if DRS is enabled, else False.
    """

  def get_unready_nodes(self, nodes=None, sync_with_oob=True):
    """
    Performs minimal checks to see if cluster nodes are in in a ready state.
    Specifically:
      -- All associated nodes report ready via their 'is_ready' method.

    Args:
      nodes (list<Node>): Optional. List of nodes to check.
        Defaults to all nodes in the cluster.
      sync_with_oob (bool): Optional. If True, sync management reported data
        with OOB data.

    Returns:
      (list<Node>) List (possibly empty) of Nodes
        which are not ready.
    """
    if nodes is None:
      nodes = self.nodes()
    powered_off_nodes = self.get_powered_off_nodes(
      nodes, sync_with_oob=sync_with_oob)

    unready_nodes = []
    unready_nodes.extend(powered_off_nodes)
    for node in set(nodes) - set(powered_off_nodes):
      if not node.is_ready(check_power_state=False):
        unready_nodes.append(node)

    if unready_nodes:
      log.info("Nodes %s are not ready",
               ", ".join(node.node_id() for node in unready_nodes))
    else:
      log.info("All nodes ready")
    return unready_nodes

  def check_cluster_ready(self, sync_with_oob=True):
    """
    Performs minimal checks to see if cluster is in a functioning state.
    Specifically:
      -- All associated nodes report ready via their 'is_ready' method.
      -- Subclasses may extend this method to provide additional cluster-level
      checks specific to a given cluster type.
      -- If applicable, will ensure that the host power states in management
        software are synced with those reported via OOB queries.

    Args:
      sync_with_oob (bool): Optional. If True, ensure host power states are
        consistent as reported by OOB and management software prior to
        performing subsequent checks.

    Returns:
      (bool) True if cluster is ready, else False.
    """
    return self.check_nodes_ready(self.nodes(), sync_with_oob=sync_with_oob)

  def check_nodes_ready(self, nodes, sync_with_oob=True):
    """
    Performs minimal checks to see if nodes are in a functioning state.
    Specifically:
      -- All nodes in 'nodes' report ready via their 'is_ready' method.
      -- Subclasses may extend this method to provide additional cluster-level
      checks specific to a given cluster type.

    Args:
      nodes (list<Node>): List of nodes to check.
      sync_with_oob (bool): Optional. If True, ensure host power states are
        consistent as reported by OOB and management software prior to
        performing subsequent checks.

    Returns:
      (bool) True if all nodes are ready, else False.
    """
    nodes_not_in_cluster = set(nodes).difference(self.nodes())
    CHECK(len(nodes_not_in_cluster) == 0, "%s are not members of %s" % (
      [node.node_id() for node in nodes_not_in_cluster], self._name))
    return len(self.get_unready_nodes(nodes=nodes,
                                      sync_with_oob=sync_with_oob)) == 0

  def get_powered_off_nodes(self, nodes=None, sync_with_oob=True):
    """
    Gets list of those 'nodes' which are not powered on.

    If 'nodes' is None, returns all nodes which are not powered on.

    Args:
      nodes (list<Node>|None): List of nodes to check.
      sync_with_oob (bool): Optional. If True, sync management reported data
        with OOB reported data.

    Returns:
      (list<Node>) List (possibly empty) of Nodes
      which are not powered on.
    """
    if nodes is None:
      nodes = self.nodes()
    powered_off_nodes = []
    if sync_with_oob:
      node_id_power_state_map = self.sync_power_state_for_nodes(nodes)
    else:
      node_id_power_state_map = self.get_power_state_for_nodes(nodes)
    for node in nodes:
      curr_power_state = node_id_power_state_map[node.node_id()]
      if curr_power_state != node.get_management_software_property_name_map()[
          NodePropertyNames.POWERED_ON]:
        log.debug("%s '%s' != %s", node.node_id(), curr_power_state,
                  node.get_management_software_property_name_map()[
                    NodePropertyNames.POWERED_ON])
        powered_off_nodes.append(node)

    if powered_off_nodes:
      log.debug("Nodes %s are not powered on",
                ", ".join(node.node_id() for node in powered_off_nodes))
    else:
      log.info("All nodes powered on")
    return powered_off_nodes

  def find_vms(self, vm_names, sort_key=None):
    """Return list of VMs whose names are equal to the list of VM names.

    The length of the returned list will be equal to the length of the input
    iterable. The ordering of the returned list will be the same as the input.
    If a VM is not found by a given name, None will be returned in its place.

    Args:
      vm_names: Iterable containing VM name strings.
      sort_key: key parameter passed to sorted(). If None, the ordering of the
        returned list is the same as the input iterable.

    Returns:
      List of VMs that match the provided list of vm_names.
    """
    vms_by_name = {}
    for vm in self.vms():
      vms_by_name[vm.vm_name()] = vm
    # Select and return the VMs.
    found_vms = []
    for vm_name in vm_names:
      found_vms.append(vms_by_name.get(vm_name, None))
    # Sort, if necessary.
    if sort_key is not None:
      found_vms = sorted(found_vms, key=sort_key)
    return found_vms

  def find_vms_regex(self, pattern):
    """Return list of VMs whose names match a regex pattern.

    The VMs in the returned list will be sorted alphabetically by name.

    Args:
      pattern (str): regex pattern.

    Returns:
      List of VMs whose names match the provided pattern.
    """
    found_vms = [vm for vm in self.vms() if re.match(pattern, vm.vm_name())]
    found_vms.sort(key=lambda vm: vm.vm_name())
    return found_vms

  def find_vm(self, vm_name):
    """Return a VM whose name is equal to vm_name.

    Args:
      vm_name: VM name string.

    Returns:
      VM whose name is equal to vm_name, or None if not found.
    """
    return self.find_vms([vm_name])[0]

  def power_on_nodes(self, nodes=None, async=False, timeout_mins=40):
    """Power on nodes. Optionally, wait for those nodes to become available.

    Args:
      nodes (list<Node>): List of nodes to power on.
      async (bool): If False, block until nodes are ready. Otherwise, return
        immediately.
      timeout_mins (int): If async is False, block for no more than
        timeout_mins minutes. If async is True, this value has no effect.

    Raises:
      CurieException if cluster is not ready after wait_timeout_mins minutes.
    """
    if nodes is None:
      nodes = self.nodes()
    nodes_being_powered_on = []
    for node in nodes:
      if not node.is_powered_on():
        log.info("Powering on node %s", node.node_id())
        node.power_on()
        nodes_being_powered_on.append(node)
    # Wait for the nodes to become ready.
    if nodes_being_powered_on and not async:
      timeout_secs = timeout_mins * 60
      func = partial(self.check_nodes_ready, nodes_being_powered_on)
      nodes_ready = CurieUtil.wait_for(
        func,
        "%s to be ready" % nodes_being_powered_on,
        timeout_secs,
        poll_secs=5)
      if not nodes_ready:
        raise CurieException(CurieError.kTimeout,
                              "Cluster %s not ready after %d minutes" %
                              (self._name, timeout_mins))

  def _curie_metric_to_metric_name(self, curie_metric):
    if self.__CURIE_METRIC_NAME_MAP__ is None:
      raise NotImplementedError("Subclasses must define "
                                "__CURIE_METRIC_NAME_MAP__")
    metric_name = self.__CURIE_METRIC_NAME_MAP__.get(
      MetricsUtil.metric_name(curie_metric))
    if metric_name is None:
      raise CurieTestException("Unsupported metric '%s'\n%s" %
                                (curie_metric.name, curie_metric))
    return metric_name

  def _get_vm_id_set(self, vms):
    return frozenset([vm.vm_id() for vm in vms])
