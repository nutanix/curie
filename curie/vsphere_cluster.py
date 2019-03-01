#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

import logging
import time
from datetime import datetime

import gflags
import pyVmomi
from dateutil import tz

from curie.cluster import Cluster
from curie.curie_error_pb2 import CurieError
from curie.curie_metrics_pb2 import CurieMetric
from curie.exception import CurieException, CurieTestException
from curie.goldimage_manager import GoldImageManager
from curie.log import CHECK, CHECK_EQ
from curie.node import NodePropertyNames
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.util import CurieUtil
from curie.vm import VmParams
from curie.vsphere_node import VsphereNode
from curie.vsphere_unix_vm import VsphereUnixVm
from curie.vsphere_vcenter import VsphereVcenter, get_optional_vim_attr
from curie.vsphere_vm import CURIE_GUEST_OS_TYPE_KEY, VsphereVm

log = logging.getLogger(__name__)

FLAGS = gflags.FLAGS


class VsphereCluster(Cluster):
  def __init__(self, cluster_metadata):
    super(VsphereCluster, self).__init__(cluster_metadata)
    CHECK(cluster_metadata.cluster_hypervisor_info.HasField(
      "esx_info"))
    CHECK(cluster_metadata.cluster_management_server_info.HasField(
      "vcenter_info"))
    # vCenter information for the vCenter server that manages this cluster.
    self._vcenter_info = \
      self._metadata.cluster_management_server_info.vcenter_info

    # Datacenter name of the datacenter where the cluster to run on lives.
    self.__datacenter_name = self._vcenter_info.vcenter_datacenter_name

    # Cluster name of the cluster to run on.
    self.__cluster_name = self._vcenter_info.vcenter_cluster_name

    # Datastore name of the datastore to deploy test VMs on the cluster.
    self.__datastore_name = self._vcenter_info.vcenter_datastore_name

  #----------------------------------------------------------------------------
  #
  # Public base Cluster methods.
  #
  #----------------------------------------------------------------------------

  @classmethod
  def metrics(cls):
    metrics = [
      CurieMetric(name=CurieMetric.kMemActive,
                   description="Average active memory.",
                   instance="Aggregated",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kKilobytes),
      CurieMetric(name=CurieMetric.kDatastoreRead,
                   description="Average number of read commands issued per "
                               "second to the datastore.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kOperations,
                   rate=CurieMetric.kPerSecond),
      CurieMetric(name=CurieMetric.kDatastoreWrite,
                   description="Average number of write commands issued per "
                               "second to the datastore.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kOperations,
                   rate=CurieMetric.kPerSecond),
      CurieMetric(name=CurieMetric.kDatastoreRead,
                   description="Average rate of reading from the datastore.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kKilobytes,
                   rate=CurieMetric.kPerSecond),
      CurieMetric(name=CurieMetric.kDatastoreWrite,
                   description="Average rate of writing to the datastore.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kKilobytes,
                   rate=CurieMetric.kPerSecond),
      CurieMetric(name=CurieMetric.kDatastoreReadLatency,
                   description="Average time a read from the datastore takes.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kMilliseconds),
      CurieMetric(name=CurieMetric.kDatastoreWriteLatency,
                   description="Average time a write to the datastore takes.",
                   instance="*",
                   type=CurieMetric.kGauge,
                   consolidation=CurieMetric.kAvg,
                   unit=CurieMetric.kMilliseconds),
    ]
    return super(VsphereCluster, cls).metrics() + metrics

  def update_metadata(self, include_reporting_fields):
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vcenter.fill_cluster_metadata(vim_cluster,
                                    self._metadata,
                                    include_reporting_fields)
      vcenter.match_node_metadata_to_vcenter(vim_cluster, self._metadata)

      if (self._metadata.cluster_software_info.HasField("vsan_info") or
          self._metadata.cluster_software_info.HasField("generic_info")):

        # TODO (jklein): Clean-up/properly abstract out these checks.
        mgmt_info = self._metadata.cluster_management_server_info
        if mgmt_info.HasField("vcenter_info"):
          ds_name = mgmt_info.vcenter_info.vcenter_datastore_name
        else:
          raise CurieTestException("Invalid metadata")
        vim_ds = vcenter.lookup_datastore(vim_cluster, ds_name)
        if vim_ds.summary.type == "vsan":
          if self._metadata.cluster_software_info.HasField("generic_info"):
            self._metadata.cluster_software_info.ClearField("generic_info")
            self._metadata.cluster_software_info.vsan_info.SetInParent()
        else:
          if self._metadata.cluster_software_info.HasField("vsan_info"):
            raise CurieTestException("Target is not configured for VSAN")

      if include_reporting_fields:
        version_pairs = VsphereVcenter.get_esx_versions(
          vim_cluster, self._metadata.cluster_nodes)

        esx_versions = [pair[0] for pair in version_pairs]
        esx_builds = [pair[1] for pair in version_pairs]
        self._metadata.cluster_hypervisor_info.esx_info.version.extend(
          esx_versions)
        self._metadata.cluster_hypervisor_info.esx_info.build.extend(
          esx_builds)
        cluster_software_info = self._metadata.cluster_software_info
        if cluster_software_info.HasField("nutanix_info"):
          client = NutanixRestApiClient.from_proto(
            cluster_software_info.nutanix_info)
          nutanix_metadata = client.get_nutanix_metadata()
          if nutanix_metadata.version is not None:
            cluster_software_info.nutanix_info.version = \
                nutanix_metadata.version
          if nutanix_metadata.cluster_uuid is not None:
            cluster_software_info.nutanix_info.cluster_uuid = \
                nutanix_metadata.cluster_uuid
          if nutanix_metadata.cluster_incarnation_id is not None:
            cluster_software_info.nutanix_info.cluster_incarnation_id = \
                nutanix_metadata.cluster_incarnation_id
        elif cluster_software_info.HasField("vsan_info"):
          pass
        elif cluster_software_info.HasField("generic_info"):
          pass
        else:
          raise CurieException(
            CurieError.kInternalError,
            "Unsupported software on vSphere cluster, %s" % self._metadata)
    self._node_id_metadata_map = dict(
      [(node.id, node) for node in self._metadata.cluster_nodes])

  def nodes(self):
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      nodes = []
      if any(map(self._metadata.cluster_software_info.HasField,
                 ["nutanix_info", "vsan_info", "generic_info"])):
        vim_host_names = [vim_host.name for vim_host in vim_cluster.host]
        for index, cluster_node in enumerate(self._metadata.cluster_nodes):
          if cluster_node.id not in vim_host_names:
            raise CurieTestException(
              cause=
              "Node with ID '%s' is in the Curie cluster metadata, but not "
              "found in vSphere cluster '%s'." %
              (cluster_node.id, vim_cluster.name),
              impact=
              "The cluster configuration is invalid.",
              corrective_action=
              "Please check that all of the nodes in the Curie cluster "
              "metadata are part of the vSphere cluster. For example, if the "
              "cluster configuration has four nodes, please check that all "
              "four nodes are present in the vSphere cluster. If the nodes "
              "are managed in vSphere by FQDN, please check that the nodes "
              "were also added by their FQDN to the Curie cluster metadata."
            )
          else:
            nodes.append(VsphereNode(self, cluster_node.id, index))
        return nodes
      else:
        raise CurieTestException(
          "Unsupported software on vSphere cluster, %s" % self._metadata)
    return nodes

  def power_off_nodes_soft(self, nodes, timeout_secs=None, async=False):
    """See 'Cluster.power_off_nodes_soft' for definition."""
    # Set a rational default if timeout_secs is not established elsewhere up
    # the stack. Timeout based on node poweroff and poweron functions.
    if timeout_secs is None:
      timeout_secs = 40 * 60
    t0 = time.time()
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      node_ids_to_power_off = []
      for node in nodes:
        if node.is_powered_on():
          node_ids_to_power_off.append(node.node_id())
      vim_hosts = vcenter.lookup_hosts(vim_cluster, node_ids_to_power_off)
      # NB: This invokes the power off tasks synchronously, but the tasks
      # themselves are essentially async. This block only until the requests
      # are acknowledged by vCenter.
      vcenter.power_off_hosts(vim_cluster, vim_hosts, timeout_secs)

    if async:
      return

    to_power_off_nodes = set(nodes)
    while True:
      to_power_off_nodes -= set(node for node in to_power_off_nodes
                                if not node.is_powered_on())
      if not to_power_off_nodes:
        log.info("All nodes are powered off")
        return
      log.info("Waiting for nodes '%s' to power off",
               ", ".join([node.node_id() for node in to_power_off_nodes]))
      elapsed_secs = time.time() - t0
      if elapsed_secs > timeout_secs:
        raise CurieTestException("Timed out waiting to power off nodes.")
      time.sleep(5)

  def vms(self):
    with self._open_vcenter_connection() as vcenter:
      return self.__vms_internal(vcenter)

  def get_power_state_for_vms(self, vms):
    """See 'Cluster.get_power_state_for_vms' for documentation."""
    vm_ids = set([vm.vm_id() for vm in vms])
    vm_id_power_state_map = {}
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vim_vms = []
      map(vim_vms.extend, [vim_host.vm for vim_host in vim_cluster.host])
      for vim_vm in vim_vms:
        try:
          if vim_vm.name in vm_ids:
            vm_ids.remove(vim_vm.name)
            vm_id_power_state_map[vim_vm.name] = vim_vm.runtime.powerState
        except pyVmomi.vmodl.fault.ManagedObjectNotFound:
          continue
    if vm_ids:
      raise CurieTestException("Invalid VM ID(s) '%s'" % ", ".join(vm_ids))
    return vm_id_power_state_map

  def get_power_state_for_nodes(self, nodes):
    """See 'Cluster.get_power_state_for_nodes' for documentation."""
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      node_ids = set([node.node_id() for node in nodes])
      node_id_soft_power_state_map = {}
      for vim_host in vim_cluster.host:
        if vim_host.name in node_ids:
          node_ids.remove(vim_host.name)
          log.debug("vCenter reports host %s in power state: '%s'",
                    vim_host.name, vim_host.runtime.powerState)
          node_id_soft_power_state_map[
            vim_host.name] = vim_host.runtime.powerState
      if node_ids:
        raise CurieTestException("Invalid Node ID(s) '%s'"
                                  % ", ".join(node_ids))
    return node_id_soft_power_state_map

  def sync_power_state_for_nodes(self, nodes, timeout_secs=(40 * 60)):
    """See 'Cluster.sync_power_state_for_nodes' for documentation."""
    log.info("Synchronizing power state for nodes '%s'",
             ", ".join([node.node_id() for node in nodes]))
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      node_power_state_map = self.get_power_state_for_nodes(nodes)
      oob_powered_on, oob_powered_off = \
          self.__verify_mgmt_oob_power_states_match(nodes,
                                                    node_power_state_map)

      def _done_syncing_nodes():
        node_power_state_map = self.get_power_state_for_nodes(nodes)
        oob_powered_on, oob_powered_off = \
          self.__verify_mgmt_oob_power_states_match(nodes,
                                                    node_power_state_map)
        to_sync_curie_nodes = oob_powered_on + oob_powered_off
        if not to_sync_curie_nodes:
          return True
        # Reconnect might fail, as hosts may be powered off, but this may
        # force vCenter to refresh their power states.
        to_sync_vim_hosts = vcenter.lookup_hosts(vim_cluster, [
          node.node_id() for node in to_sync_curie_nodes])
        vcenter.reconnect_hosts(to_sync_vim_hosts)
        return False

      # Ignore the boolean returned by 'wait_for'. Whether or not this
      # succeeds, we want to refresh the values for 'oob_powered_on',
      # 'oob_powered_off' to provide more detailed logging.
      CurieUtil.wait_for(_done_syncing_nodes,
                          "vCenter to sync state for mismatched hosts",
                          timeout_secs=timeout_secs,
                          poll_secs=5)

      node_power_state_map = self.get_power_state_for_nodes(nodes)
      oob_powered_on, oob_powered_off = \
          self.__verify_mgmt_oob_power_states_match(nodes,
                                                    node_power_state_map)
      unsynced_curie_nodes = oob_powered_on + oob_powered_off

      if unsynced_curie_nodes:
        raise CurieTestException(
          "Unable to sync vCenter power states for %s" %
          ", ".join([node.node_id() for node in unsynced_curie_nodes]))

      return node_power_state_map

  def power_on_vms(self, vms, max_parallel_tasks=None):
    """
    See 'Cluster.power_on_vms' for documentation.
    """
    if not max_parallel_tasks:
      max_parallel_tasks = FLAGS.vsphere_vcenter_max_parallel_tasks
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vm_names = [vm.vm_name() for vm in vms]
      vcenter.power_on_vms(
        vim_cluster, vm_names, max_parallel_tasks=max_parallel_tasks)

  def power_off_vms(self, vms, max_parallel_tasks=None):
    """Powers off VMs.

    Args:
      vms: List of VMs to power on.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: VMs fail to power off within the timeout.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vm_names = [vm.vm_name() for vm in vms]
      vcenter.power_off_vms(vim_cluster, vm_names,
                            max_parallel_tasks=max_parallel_tasks)

  def delete_vms(self, vms, ignore_errors=False, max_parallel_tasks=None):
    """Delete VMs.

    Args:
      vm_names: List of VM names to power on.
      ignore_errors bool: The default value is False which requires all
        destroy tasks to complete with "success". Set to True to disable this
        check.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException:
        - If any VM is not already powered off.
        - All VMs are not destroyed with in the timeout.
        - Destroy task fails and ignore_errors is False.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vm_names = [vm.vm_name() for vm in vms]
      vcenter.delete_vms(vim_cluster, vm_names, ignore_errors=ignore_errors,
                         max_parallel_tasks=max_parallel_tasks)

  def import_vm(self, goldimages_directory, goldimage_name, vm_name, node_id=None):
    """
    Creates a VM from the specified gold image. If 'node_id' is specified, the
    VM is created on that node, else a random node is selected. The VM will be
    created on the datastore associated with the curie server's settings for
    this cluster.
    """
    with self._open_vcenter_connection() as vcenter:
      vim_datacenter, vim_cluster, vim_datastore = \
        self._lookup_vim_objects(vcenter)
      vim_network = None
      if self._vcenter_info.HasField("vcenter_network_name"):
        for vim_datacenter_network in vim_datacenter.network:
          if (vim_datacenter_network.name ==
              self._vcenter_info.vcenter_network_name):
            vim_network = vim_datacenter_network
            break
        CHECK(vim_network is not None,
              msg=self._vcenter_info.vcenter_network_name)
      # We use the HostSystem (node) name as a node ID for vSphere clusters.
      host_name = node_id
      vim_vm = vcenter.import_vm(goldimages_directory,
                                 goldimage_name,
                                 vim_datacenter,
                                 vim_cluster,
                                 vim_datastore,
                                 vim_network,
                                 vm_name,
                                 host_name=host_name)
      return self.__vim_vm_to_curie_vm(vim_vm)

  def create_vm(self, goldimages_directory, goldimage_name, vm_name, vcpus=1,
                ram_mb=1024, node_id=None, datastore_name=None, data_disks=()):
    """
    See 'Cluster.create_vm' for documentation.
    """
    log.info("Creating VM %s based on %s with %d vCPUs, %d MB RAM and %s "
             "disks on node %s in datastore %s ",
             vm_name, goldimage_name, vcpus, ram_mb, str(data_disks),
             str(node_id), datastore_name)
    image_manager = GoldImageManager(goldimages_directory)
    # vSphere does not support anything but x86-64 platforms.
    goldimage_path = image_manager.get_goldimage_path(
      goldimage_name,
      arch=GoldImageManager.ARCH_X86_64,
      format_str=GoldImageManager.FORMAT_VMDK)
    with self._open_vcenter_connection() as vcenter:
      vim_datacenter, vim_cluster, vim_datastore = \
        self._lookup_vim_objects(vcenter)
      vim_host = vcenter.lookup_hosts(vim_cluster, [node_id])[0]
      vim_network = vcenter.lookup_network(
        vim_datacenter, self._vcenter_info.vcenter_network_name)
      vim_vm = vcenter.create_vm(vm_name, vim_datacenter, vim_cluster,
                                 vim_datastore, vim_network, vim_host,
                                 goldimage_path, ram_mb, vcpus, data_disks)
      vm = self.__vim_vm_to_curie_vm(vim_vm)
      log.info("Created VM %s based on %s", vm.vm_name(), goldimage_name)
      return vm

  def clone_vms(self, vm, vm_names, node_ids=(), datastore_name=None,
                max_parallel_tasks=None, linked_clone=False):
    """
    Clones 'vm' and creates the VMs with names 'vm_names'.

    Args:
      vm  CurieVM: Base VM that clones will be created from.
      vm_names list of strings: One clone will be created for each name in
        list.
      node_ids list of node ids: If provided, must be the same length
        as 'vm_names', then 'vm_names[xx]' will be cloned to 'node_ids[xx]'.
        Otherwise VMs will be cloned to random nodes on cluster.
      datastore_name: If provided, name of datastore VMs will be cloned to.
        Otherwise the VMs will be created on the datastore associated with the
        curie server's settings for this cluster.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
      linked_clone (bool): Whether or not the clones should be "normal" full
        clones or linked clones.

    Returns:
      List of cloned VMs.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    if node_ids:
      CHECK_EQ(len(vm_names), len(node_ids))
    with self._open_vcenter_connection() as vcenter:
      vim_datacenter, vim_cluster, vim_datastore = \
        self._lookup_vim_objects(vcenter)
      vim_vm = vcenter.lookup_vm(vim_cluster, vm.vm_name())
      if datastore_name is not None:
        vim_datastore = vcenter.lookup_datastore(vim_cluster, datastore_name)
        if vim_datastore is None:
          raise CurieTestException("Datastore %s not found" % datastore_name)
      # We use the HostSystem (node) name as a node ID for vSphere clusters.
      host_names = node_ids
      vim_clone_vms = vcenter.clone_vms(
        vim_vm, vim_datacenter, vim_cluster, vim_datastore, vm_names,
        host_names=host_names,
        max_parallel_tasks=max_parallel_tasks,
        linked_clone=linked_clone)
      return [self.__vim_vm_to_curie_vm(vim_clone_vm)
              for vim_clone_vm in vim_clone_vms]

  def migrate_vms(self, vms, nodes, max_parallel_tasks=None):
    """Move 'vms' to 'nodes'.

    For each VM 'vms[xx]' move to the corresponding Node 'nodes[xx]'.

    Args:
      vms list of CurieVms: List of VMs to migrate.
      nodes  list of CurieClusterNodes: Must be the same length as 'vms'. Each
        VM in 'vms' wll be moved to the corresponding node in 'nodes'.
      max_parallel_tasks int: The number of VMs to power on in parallel.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK_EQ(len(vms), len(nodes))
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vim_vms = vcenter.lookup_vms(vim_cluster,
                                   vm_names=[vm.vm_name() for vm in vms])
      # Generate list of vim hosts from CurieClusterNodes.
      vim_hosts = []
      vim_host_map = dict([(vim_host.name, vim_host)
                           for vim_host in vim_cluster.host])
      for node in nodes:
        if node.node_id() in vim_host_map:
          vim_hosts.append(vim_host_map[node.node_id()])
        else:
          raise CurieTestException("Unknown node %s" % node.node_id())
      vcenter.migrate_vms(vim_vms, vim_hosts,
                          max_parallel_tasks=max_parallel_tasks)

  def relocate_vms_datastore(self, vms, datastore_names,
                             max_parallel_tasks=None):
    """Relocate 'vms' to 'datastore_names'."""
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    if len(vms) != len(datastore_names):
      raise CurieTestException("Unequal list length of vms and destination "
                                "datastores for relocation.")
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vim_vms = vcenter.lookup_vms(vim_cluster,
                                   vm_names=[vm.vm_name() for vm in vms])
      vim_datastores = []
      for datastore_name in datastore_names:
        vim_datastore = vcenter.lookup_datastore(vim_cluster, datastore_name)
        if vim_datastore is not None:
          vim_datastores.append(vim_datastore)
        else:
          raise CurieTestException(
            "Unable to find datastore: %s in cluster %s" % (
              datastore_name, vim_cluster.name))
      vcenter.relocate_vms_datastore(vim_vms, vim_datastores,
                                     max_parallel_tasks=max_parallel_tasks)

  def enable_ha_vms(self, vms):
    # TODO(ryan.hardin): Add this.
    log.debug("Enabling HA for VMs on vSphere not supported yet, and will be "
              "skipped")

  def disable_ha_vms(self, vms):
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      if not vcenter.is_ha_enabled(vim_cluster):
        log.info("HA not enabled on cluster %s, no need to disable per VM.",
                 vim_cluster.name)
        return
      # Generate list of vim VMs from CurieVms.
      vim_vms = vcenter.lookup_vms(vim_cluster,
                                   vm_names=[vm.vm_name() for vm in vms])
      log.info("Disabling HA for %d VMs on %s", len(vim_vms), vim_cluster.name)
      vcenter.disable_ha_vms(vim_cluster, vim_vms)

  def disable_drs_vms(self, vms):
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      if not vcenter.is_drs_enabled(vim_cluster):
        log.info("DRS not enabled on cluster %s, no need to disable per VM.",
                 vim_cluster.name)
        return
        # Generate list of vim VMs from CurieVms.
      vim_vms = vcenter.lookup_vms(vim_cluster,
                                   vm_names=[vm.vm_name() for vm in vms])
      log.info("Disabling DRS for %d VMs on %s",
               len(vim_vms), vim_cluster.name)
      vcenter.disable_drs_vms(vim_cluster, vim_vms)

  def cleanup(self, test_ids=()):
    """Remove all Curie templates and state from this cluster.
    """
    log.info("Cleaning up state on cluster %s", self.metadata().cluster_name)

    with self._open_vcenter_connection() as vcenter:
      self._cleanup_datastore(vcenter)
      cluster_software_info = self._metadata.cluster_software_info
      if cluster_software_info.HasField("nutanix_info"):
        client = NutanixRestApiClient.from_proto(
          cluster_software_info.nutanix_info)
        client.cleanup_nutanix_state(test_ids)

  @CurieUtil.log_duration
  def collect_performance_stats(self,
                                start_time_secs=None,
                                end_time_secs=None,
                                max_samples=15):
    """Collect performance statistics for all nodes in the cluster.

    Optional arguments 'start_time_secs' and 'end_time_secs' can be used to
    limit the results to a specific time range. Note that these times are
    relative to the vSphere clock. If the clock of the Curie server is not
    synchronized, the samples returned might appear to be outside of the given
    time boundaries.

    Args:
      start_time_secs (int): Optionally specify the oldest sample to return, in
        seconds since epoch.
      end_time_secs (int): Optionally specify the newest sample to return, in
        seconds since epoch.
      max_samples (int): Optionally specify the maximum number of samples to
        get per counter. Defaults to the latest fifteen samples, unless a time
        range is specified.

    Returns:
      (dict) Dict of list of curie_metrics_pb2.CurieMetric. Top level dict
        keyed by node ID. List contains one entry per metric.
    """
    start_dt = None
    if start_time_secs is not None:
      start_dt = datetime.fromtimestamp(float(start_time_secs), tz=tz.tzutc())
    end_dt = None
    if end_time_secs is not None:
      end_dt = datetime.fromtimestamp(float(end_time_secs), tz=tz.tzutc())
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      results_map = vcenter.collect_cluster_performance_stats(
        vim_cluster,
        self.metrics(),
        vim_perf_query_spec_start_time=start_dt,
        vim_perf_query_spec_end_time=end_dt,
        vim_perf_query_spec_max_sample=max_samples)
    return results_map

  def is_ha_enabled(self):
    """See 'Cluster.is_ha_enabled' for documentation."""
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      return vcenter.is_ha_enabled(vim_cluster)

  def is_drs_enabled(self):
    """See 'Cluster.is_drs_enabled' for documentation."""
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      return vcenter.is_drs_enabled(vim_cluster)

  #----------------------------------------------------------------------------
  #
  # Public VsphereCluster-specific methods.
  #
  #----------------------------------------------------------------------------

  def datastore_visible(self, datastore_name, host_name=None):
    """
    Returns True if the datastore with name 'datastore_name' is visible on all
    nodes of the cluster.
    """
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vim_datastore = vcenter.lookup_datastore(vim_cluster, datastore_name,
                                               host_name=host_name)
      return vim_datastore is not None

  def refresh_datastores(self):
    """
    Refreshes datastores state for all nodes in vCenter. This is necessary in
    case changes have been made directly on ESX nodes and the ESX nodes fail to
    synchronize this state with vCenter.
    """
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vcenter.refresh_datastores(vim_cluster)

  def snapshot_vms(self, vms, snapshot_names, snapshot_descriptions=(),
                   max_parallel_tasks=None):
    """
    For each VM with name 'vm_names[xx]' on the cluster 'vim_cluster', creates
    a snapshot with snapshot name 'snapshot_names[xx]' and optional description
    'snapshot_descriptions[xx]'.

    Args
      vms list of CurieVMs: List of VMs to create snapshots for.
      snapshot_names list of strings: Names for snapshot which must be the same
        length as 'vms'.
      snapshot_descriptions List of strings: List of descriptions for each
        snapshot corresponding to 'vms' and 'snapshot_names'. If provided it
        must be the same length as 'vms'.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK_EQ(len(vms), len(snapshot_names))
    CHECK(len(snapshot_descriptions) == 0 or
          len(snapshot_descriptions) == len(snapshot_names))
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vcenter.snapshot_vms(vim_cluster,
                           [vm.vm_name() for vm in vms],
                           snapshot_names,
                           snapshot_descriptions=snapshot_descriptions,
                           max_parallel_tasks=max_parallel_tasks)

  #----------------------------------------------------------------------------
  #
  # Protected VsphereCluster-specific methods.
  #
  #----------------------------------------------------------------------------

  def _get_max_parallel_tasks(self, arg):
    if arg is None:
      return FLAGS.vsphere_vcenter_max_parallel_tasks
    return arg

  #----------------------------------------------------------------------------
  #
  # Private VsphereCluster-specific methods.
  #
  #----------------------------------------------------------------------------

  def _open_vcenter_connection(self):
    return VsphereVcenter(
      self._vcenter_info.vcenter_host,
      self._vcenter_info.decrypt_field("vcenter_user"),
      self._vcenter_info.decrypt_field("vcenter_password"))

  def _lookup_vim_objects(self, vcenter):
    """
    Look up and return the datacenter, cluster, and datastore objects
    corresponding to this cluster's curie server settings.
    """
    vim_datacenter = vcenter.lookup_datacenter(self.__datacenter_name)
    if vim_datacenter is None:
      raise CurieTestException("Datacenter %s not found" %
                                self.__datacenter_name)
    vim_cluster = vcenter.lookup_cluster(vim_datacenter, self.__cluster_name)
    if vim_cluster is None:
      raise CurieTestException("Cluster %s not found" %
                                self.__cluster_name)
    vim_datastore = vcenter.lookup_datastore(
      vim_cluster, self.__datastore_name)
    if vim_datastore is None:
      raise CurieTestException("Datastore %s not found" %
                                self.__datastore_name)
    return (vim_datacenter, vim_cluster, vim_datastore)

  def _lookup_vim_cluster(self, vcenter):
    """
    Look up and return the VIM cluster object.

    This method should be used where the datastore is not needed especially in
    cases where the underlying storage system may not be fully functional.

    Raises:
      CurieTestEexception if the datacenter or cluster can not be found.
    """
    vim_datacenter = vcenter.lookup_datacenter(self.__datacenter_name)
    if vim_datacenter is None:
      raise CurieTestException("Datacenter %s not found" %
                                self.__datacenter_name)
    vim_cluster = vcenter.lookup_cluster(vim_datacenter, self.__cluster_name)
    if vim_cluster is None:
      raise CurieTestException("Cluster %s not found" %
                                self.__cluster_name)
    return vim_cluster

  def _cleanup_datastore(self, vcenter):
    vim_datacenter, vim_cluster, vim_datastore = \
      self._lookup_vim_objects(vcenter)
    paths = vcenter.find_datastore_paths("__curie_goldimage*", vim_datastore)
    for path in paths:
      vcenter.delete_datastore_folder_path(path, vim_datacenter)

  def __vms_internal(self, vcenter, vm_names=None):
    "Returns all VMs on the cluster."
    vim_cluster = self._lookup_vim_cluster(vcenter)
    if (self._metadata.cluster_software_info.HasField("nutanix_info") or
        self._metadata.cluster_software_info.HasField("vsan_info") or
        self._metadata.cluster_software_info.HasField("generic_info")):
      vim_vms = vcenter.lookup_vms(vim_cluster, vm_names=vm_names)
      vms = []
      for vim_vm in vim_vms:
        try:
          curie_vm = self.__vim_vm_to_curie_vm(vim_vm)
        except pyVmomi.vmodl.fault.ManagedObjectNotFound as err:
          log.debug("Skipping missing VM: %s (%s)", err.msg, err.obj)
        else:
          vms.append(curie_vm)
      return vms
    else:
      raise CurieTestException("Unsupported software on vSphere cluster, %s" %
                                self._metadata)

  def __vim_vm_to_curie_vm(self, vim_vm):
    "Returns an object of the appropriate subclass of Vm for 'vim_vm'."
    # On a vSphere cluster, the VM name should be unique on the cluster where
    # the VM resides, so we can just use the VM name as the VM ID.
    vm_id = vim_vm.name
    curie_guest_os_type_value = None
    vim_vm_config = get_optional_vim_attr(vim_vm, "config")
    if vim_vm_config is not None:
      for config_option in vim_vm_config.extraConfig:
        if config_option.key == CURIE_GUEST_OS_TYPE_KEY:
          curie_guest_os_type_value = config_option.value
          break
    vm_ip = VsphereVcenter.get_vim_vm_ip_address(vim_vm)
    vim_host = get_optional_vim_attr(vim_vm.runtime, "host")
    if vim_host is not None:
      node_id = vim_host.name
    else:
      node_id = None
    vm_params = VmParams(self, vm_id)
    vm_params.vm_name = vim_vm.name
    vm_params.vm_ip = vm_ip
    vm_params.node_id = node_id
    if curie_guest_os_type_value is None:
      # We use the generic VsphereVm class for all VMs that don't have the
      # curie guest OS type option since non-curie VMs won't have the proper
      # configuration to enable, for example, remote file transfers to/from the
      # VM and remote execution of commands in the VM.
      vm_params.is_cvm = self.__vim_vm_is_nutanix_cvm(vim_vm)
      return VsphereVm(vm_params)
    else:
      CHECK_EQ(curie_guest_os_type_value, "unix")
      return VsphereUnixVm(vm_params)

  def __vim_vm_is_nutanix_cvm(self, vim_vm):
    "Returns True if the VM 'vim_vm' corresponds to a Nutanix CVM."
    if not self._metadata.cluster_software_info.HasField("nutanix_info"):
      return False
    return VsphereVcenter.vim_vm_is_nutanix_cvm(vim_vm)

  def __verify_mgmt_oob_power_states_match(self, nodes, node_power_state_map):
    """
    Verifies vCenter and OOB power states match for 'nodes'.

    Args:
      nodes (list<Node>) Nodes whose power states to verify.
      node_power_state_map (dict): Map of node IDs to vCenter power states.

    Returns:
      (list<Node>, list<Node>) A pair,
        (oob_powered_on, oob_powered_off). The former a list of nodes which are
        incorrectly reported by vCenter as powered off, the latter those which
        are incorrectly reported as powered on.
    """
    _vim_host_powered_on = VsphereNode._NODE_PROPERTY_NAMES[
      NodePropertyNames.POWERED_ON]
    _vim_host_powered_off = VsphereNode._NODE_PROPERTY_NAMES[
      NodePropertyNames.POWERED_OFF]
    _vim_host_unknown = VsphereNode._NODE_PROPERTY_NAMES[
      NodePropertyNames.UNKNOWN]
    oob_powered_on = []
    oob_powered_off = []
    for node in nodes:
      curr_vim_host_power_state = node_power_state_map[node.node_id()]
      if node.is_powered_on():
        if curr_vim_host_power_state != _vim_host_powered_on:
          oob_powered_on.append(node)
          log.warning("Mismatch in power states for node '%s'. OOB reports "
                      "node is powered on, while vCenter reports state '%s'",
                      node.node_id(), curr_vim_host_power_state)
      # In the case that host is powered off out-of-band, vCenter will report
      # only 'unknown' for the power state.
      elif curr_vim_host_power_state not in [_vim_host_powered_off,
                                             _vim_host_unknown]:
        oob_powered_off.append(node)
        log.warning("Mismatch in power states for node '%s'. OOB reports node "
                    "is powered off, while vCenter reports state '%s'",
                    node.node_id(), curr_vim_host_power_state)

    return oob_powered_on, oob_powered_off
