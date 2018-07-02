#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

import glob
import logging
import os
import random
import time
from collections import namedtuple

import gflags

from curie.acropolis_node import AcropolisNode
from curie.acropolis_types import AcropolisTaskInfo
from curie.acropolis_unix_vm import AcropolisUnixVm
from curie.cluster import Cluster
from curie.curie_error_pb2 import CurieError
from curie.curie_metrics_pb2 import CurieMetric
from curie.exception import CurieException, CurieTestException
from curie.goldimage_manager import GoldImageManager
from curie.log import CHECK, patch_trace
from curie.metrics_util import MetricsUtil
from curie.name_util import CURIE_GOLDIMAGE_VM_DISK_PREFIX, NameUtil
from curie.node import NodePropertyNames
from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.task import PrismTask, PrismTaskPoller, TaskPoller, TaskStatus
from curie.test.scenario_util import ScenarioUtil
from curie.util import CurieUtil
from curie.vm import VmDescriptor, VmParams

log = logging.getLogger(__name__)
patch_trace()
FLAGS = gflags.FLAGS


class AcropolisCluster(NutanixClusterDPMixin, Cluster):
  __CURIE_METRIC_NAME_MAP__ = {
    "CpuUsage.Avg.Percent": "hypervisor_cpu_usage_ppm",
    "CpuUsage.Avg.Megahertz": "hypervisor_cpu_usage_ppm",
    "MemUsage.Avg.Percent": "hypervisor_memory_usage_ppm",
    "NetReceived.Avg.KilobytesPerSecond": "hypervisor_num_received_bytes",
    "NetTransmitted.Avg.KilobytesPerSecond":
      "hypervisor_num_transmitted_bytes",
  }

  @classmethod
  def identifier_to_cluster_uuid(cls, rest_client, cluster_id_or_name):
    try:
      return rest_client.clusters_get(
        cluster_name=cluster_id_or_name)["clusterUuid"]
    except Exception:
      log.warning("Failed to lookup cluster by name, assuming '%s' is a UUID.",
                  cluster_id_or_name, exc_info=True)
      # This will raise an appropriate exception on failure.
      return rest_client.clusters_get(
        cluster_id=cluster_id_or_name)["clusterUuid"]

  @classmethod
  def identifier_to_node_uuid(cls, rest_client, node_id_name_or_ip):
    # These will raise appropriate exceptions on failure, so it's safe to
    # assume that otherwise accessing the 'uuid' key is safe.
    if CurieUtil.is_ipv4_address(node_id_name_or_ip):
      return rest_client.hosts_get(host_ip=node_id_name_or_ip)["uuid"]
    elif CurieUtil.is_uuid(node_id_name_or_ip):
      try:
        return rest_client.hosts_get(host_id=node_id_name_or_ip)["uuid"]
      except Exception:
        log.debug("Failed to lookup node via UUID '%s'", node_id_name_or_ip)

    # The provided node identifier is not an IPv4 address or a UUID. It may
    # be either an unresolved hostname or a Prism name. Try Prism name first
    # to avoid potential overhead in name resolution.
    try:
      return rest_client.hosts_get(host_name=node_id_name_or_ip)["uuid"]
    except Exception:
      log.debug("Failed to lookup node via Prism name '%s'",
                node_id_name_or_ip)

    try:
      ip = CurieUtil.resolve_hostname(node_id_name_or_ip)
    except Exception:
      raise CurieException(
        CurieError.kInvalidParameter, "Unable to resolve IP address for '%s'"
        % node_id_name_or_ip)

    # Allow this to raise it's own exception on failure, as there are no
    # further methods to which we can fall back.
    return rest_client.hosts_get(host_ip=ip)["uuid"]

  def __init__(self, cluster_metadata):
    # TODO (jklein): Would be nice to standardize this in a cleaner way.
    CHECK(cluster_metadata.cluster_hypervisor_info.HasField("ahv_info"))
    CHECK(cluster_metadata.cluster_software_info.HasField("nutanix_info"))
    CHECK(
      cluster_metadata.cluster_management_server_info.HasField("prism_info"))

    # Prism information for the PE/PC server that manages this cluster.
    self._mgmt_server_info = \
        cluster_metadata.cluster_management_server_info.prism_info

    cluster_metadata.cluster_software_info.nutanix_info.prism_host = \
        self._mgmt_server_info.prism_host

    # Map of VM UUIDs to host UUIDs on which they should be scheduled.
    self.__vm_uuid_host_uuid_map = {}

    # ID Caches
    self.__cluster_id = None
    self.__container_id = None
    self.__network_id = None
    self.__host_ip_cvm_ip_map = None

    super(AcropolisCluster, self).__init__(cluster_metadata)

  # Allow clusters to be specified by name or UUID.
  @property
  def _cluster_id(self):
    if self.__cluster_id is None:
      self.__cluster_id = self.identifier_to_cluster_uuid(
        self._prism_client, self._mgmt_server_info.prism_cluster_id)
    return self.__cluster_id

  # Allow containers to be specified by name or UUID.
  @property
  def _container_id(self):
    if self.__container_id is None:
      self.__container_id = self.__identifier_to_container_uuid(
        self._mgmt_server_info.prism_container_id)
    return self.__container_id

  @property
  def _network_id(self):
    if self.__network_id is None:
      mgmt_info = self._metadata.cluster_management_server_info
      network_name = mgmt_info.prism_info.prism_network_id
      for network_json in self._prism_client.networks_get()["entities"]:
        if network_json["name"] == network_name:
          self.__network_id = network_json["uuid"]
          break
      else:
        raise CurieException(CurieError.kInvalidParameter,
                             "Unknown network '%s'" % network_name)
    return self.__network_id

  @property
  def _host_ip_cvm_ip_map(self):
    if self.__host_ip_cvm_ip_map is None:
      self.__host_ip_cvm_ip_map = {}
      entities = self._prism_client.hosts_get().get("entities")
      for entity in entities:
        log.debug("Adding CVM for host %s: %s",
                  entity["hypervisorAddress"], entity["serviceVMExternalIP"])
        self.__host_ip_cvm_ip_map[entity["hypervisorAddress"]] = \
          entity["serviceVMExternalIP"]
    return self.__host_ip_cvm_ip_map

  #----------------------------------------------------------------------------
  #
  # Public base Cluster methods.
  #
  #----------------------------------------------------------------------------

  def node_metadata(self, node_id):
    node_uuid_metadata_id_map = self.get_node_uuid_metadata_id_map()
    if node_id in node_uuid_metadata_id_map:
      node_id = node_uuid_metadata_id_map[node_id]
    CHECK(node_id in self._node_id_metadata_map,
          "Invalid node_id '%s'" % node_id)
    return self._node_id_metadata_map.get(node_id)

  def update_metadata(self, include_reporting_fields):
    cluster_json = self.__lookup_cluster_json()

    self._node_id_metadata_map = dict(
      [(node.id, node) for node in self._metadata.cluster_nodes])

    found_node_id_set = set()
    for node_json in self._prism_client.hosts_get().get("entities", []):
      if node_json["clusterUuid"] != cluster_json["clusterUuid"]:
        continue
      curr_node_identifier = self.get_node_uuid_metadata_id_map()[
        node_json["uuid"]]
      node_proto = self._node_id_metadata_map.get(curr_node_identifier)
      CHECK(node_proto)
      found_node_id_set.add(curr_node_identifier)
      # TODO (jklein): Expand proto definition to allow for display name,
      # UUID, and IP address as separate entries.
      node_proto.id = curr_node_identifier

      if include_reporting_fields:
        node_hw = node_proto.node_hardware
        node_hw.num_cpu_packages = node_json["numCpuSockets"]
        node_hw.num_cpu_cores = node_json["numCpuCores"]
        node_hw.num_cpu_threads = node_json["numCpuThreads"]
        node_hw.cpu_hz = node_json["cpuFrequencyInHz"]
        node_hw.memory_size = node_json["memoryCapacityInBytes"]

    CHECK(found_node_id_set == set(self._node_id_metadata_map.keys()))

    if include_reporting_fields:
      # TODO (jklein): AHV info per-node.
      cluster_software_info = self._metadata.cluster_software_info
      nutanix_version = self._prism_client.get_nutanix_metadata().version
      if nutanix_version is not None:
        cluster_software_info.nutanix_info.version = nutanix_version

  def get_node_uuid_metadata_id_map(self):
    node_uuid_metadata_id_map = {}
    for node_id in [n.id for n in self._metadata.cluster_nodes]:
      curr_node_uuid = self.identifier_to_node_uuid(self._prism_client,
                                                    node_id)
      node_uuid_metadata_id_map[curr_node_uuid] = node_id
    return node_uuid_metadata_id_map

  def nodes(self):
    node_id_set = set(self._node_id_metadata_map.keys())
    id_index_map = dict((n.id, ii)
                        for ii, n in enumerate(self._metadata.cluster_nodes))

    def lookup_node_index(node_json):
      for key in ["uuid", "name", "hypervisorAddress"]:
        if node_json.get(key) in id_index_map:
          return id_index_map[node_json[key]]

      raise CurieException(CurieError.kInvalidParameter,
                            "Unknown node '%s'" % (node_json["uuid"]))
    host_json_list = filter(
      lambda host_json: self.get_node_uuid_metadata_id_map()[
        host_json["uuid"]] in node_id_set,
      self._prism_client.hosts_get().get("entities", []))

    return [
      AcropolisNode(self, host["uuid"], lookup_node_index(host))
      for host in host_json_list]

  def power_off_nodes_soft(self, nodes, timeout_secs=None, async=False):
    """See 'Cluster.power_off_nodes_soft' for definition."""
    success = True
    node_power_state_map = self.get_power_state_for_nodes(nodes)
    powered_off = \
      AcropolisNode.get_management_software_value_for_attribute(
        NodePropertyNames.POWERED_OFF)
    try:
      for node in nodes:
        if node_power_state_map[node.node_id()] == powered_off:
          log.info("Skipping power-off request for node '%s' which is already "
                   "powered off", node.node_id())
          continue
        log.info("Requesting genesis shutdown for node '%s'", node.node_id())
        curr_success = self._prism_client.genesis_prepare_node_for_shutdown(
          node.node_id())
        if curr_success:
          success = self._prism_client.genesis_shutdown_hypervisor(
            node.node_id())
        else:
          success = False
          log.error("Failed to perform soft power off on node %s",
                    node.node_id())
          break

      if not success:
        raise CurieTestException("Failed to power off nodes")
    except BaseException as exc:
      # Capture stacktrace here in case an exception is raised clearing the
      # genesis shutdown token.
      log.exception(str(exc))
      raise
    else:
      log.info("Successfully powered off nodes %s",
               ", ".join([n.node_id() for n in nodes]))
    finally:
      log.info("Clearing genesis shutdown token")
      if not self._prism_client.genesis_clear_shutdown_token():
        raise CurieTestException("Failed to clear genesis shutdown token")

  def vms(self):
    return map(self.__vm_json_to_curie_vm,
               self._prism_client.vms_get().get("entities", []))

  def get_power_state_for_vms(self, vms):
    """See 'Cluster.get_power_state_for_vms' for documentation."""
    vm_ids = set([vm.vm_id() for vm in vms])
    vm_id_power_state_map = {}
    for vm_json in self._prism_client.vms_get().get("entities", []):
      if vm_json["uuid"] not in vm_ids:
        continue
      vm_ids.discard(vm_json["uuid"])
      vm_id_power_state_map[vm_json["uuid"]] = vm_json["powerState"]

    if vm_ids:
      raise CurieTestException("Invalid VM ID(s) '%s'" % ", ".join(vm_ids))
    return vm_id_power_state_map

  def get_power_state_for_nodes(self, nodes):
    """See 'Cluster.get_power_state_for_nodes' for documentation."""
    ret = {}
    ip_status_map = self._prism_client.genesis_cluster_status().get("svms", {})
    for node in nodes:
      status_map = ip_status_map.get(self._host_ip_cvm_ip_map[node.node_ip()])
      if status_map and status_map["state"].strip().lower() == "down":
        log.debug("Translating 'state' == 'down' to 'kNormalDisconnected'")
        curr_status = "kNormalDisconnected"
      else:
        log.debug("Translating 'state' == '%s' to 'kNormalConnected'",
                  status_map.get("state"))
        curr_status = "kNormalConnected"

      host_json = self._prism_client.hosts_get_by_id(node.node_id())
      for key in ["hypervisorState", "state"]:
        log.debug("Via REST API, AHV reports '%s' == '%s'",
                  key, host_json.get(key))

      ret[node.node_id()] = curr_status
    return ret

  def sync_power_state_for_nodes(self, nodes, timeout_secs=None):
    """See 'Cluster.sync_power_state_for_nodes' for documentation."""
    # No-op: It is not known that syncing is required on AHV.
    return self.get_power_state_for_nodes(nodes)

  def power_on_vms(self, vms, max_parallel_tasks=None):
    """
    See 'Cluster.power_on_vms' for documentation.
    """
    self.__set_power_state_for_vms(
      vms, "on", wait_for_ip=True,
      max_parallel_tasks=self._get_max_parallel_tasks(max_parallel_tasks))

  def power_off_vms(self, vms, max_parallel_tasks=None):
    """
    See 'Cluster.power_off_vms' for documentation.
    """
    self.__set_power_state_for_vms(
      vms, "off", max_parallel_tasks=max_parallel_tasks)

  def delete_vms(self, vms, ignore_errors=False, max_parallel_tasks=None,
                 timeout_secs=None):
    """Delete VMs.

    Acropolis DELETE requests for /vms/{vm_id} are async. This method collects
    all taskUuids and polls until completion.

    Args:
      vms (list<CurieVM>): List of VMs to delete.
      ignore_errors (bool): Optional. Whether to allow individual tasks to
        fail. Default False.
      max_parallel_tasks (int): Max number of requests to have in-flight at
        any given time. (Currently ignored)
      timeout_secs (int): If provided, overall timeout for VM deletion tasks.

    Raises:
      CurieTestException:
        - If any VM is not already powered off.
        - All VMs are not destroyed with in the timeout.
        - Destroy task fails and ignore_errors is False.
    """
    # TODO (jklein): max_parallel_tasks won't work unless this is changed to
    # use task descriptors.
    if timeout_secs is None:
      timeout_secs = len(vms) * 60

    task_t0 = self._prism_client.get_cluster_timestamp_usecs()

    vm_id_task_map = {}
    for vm_id, tid in self._prism_client.vms_delete(
      [vm.vm_id() for vm in vms]).iteritems():
      if tid is None:
        raise CurieTestException("Failed to delete VM %s" % vm_id)
      vm_id_task_map[vm_id] = PrismTask.from_task_id(self._prism_client, tid)

    try:
      PrismTaskPoller.execute_parallel_tasks(
        tasks=vm_id_task_map.values(),
        max_parallel=self._get_max_parallel_tasks(max_parallel_tasks),
        timeout_secs=timeout_secs,
        prism_client=self._prism_client,
        cutoff_usecs=task_t0)
    except CurieTestException:
      if not ignore_errors:
        raise
      log.debug("Ignoring exception in delete_vms", exc_info=True)

    failed_to_delete_vm_ids = []
    for vm_id, task in vm_id_task_map.iteritems():
      if task.get_status() != TaskStatus.kSucceeded:
        failed_to_delete_vm_ids.append(vm_id)

    if failed_to_delete_vm_ids:
      msg = "Failed to delete vms: %s" % ", ".join(failed_to_delete_vm_ids)
      if ignore_errors:
        log.error(msg)
      else:
        raise CurieTestException(msg)

  def import_vm(self, goldimages_directory, goldimage_name, vm_name, node_id=None):
    """
    Creates a VM from the specified gold image. If 'node_id' is specified, the
    VM is created on that node, else a random node is selected. The VM will be
    created on the datastore associated with the curie server's settings for
    this cluster.
    """
    if node_id is None:
      node_id = random.choice(self.nodes()).node_id()

    ovfs = glob.glob(os.path.join(goldimages_directory,
                                  goldimage_name, "*.ovf"))
    if len(ovfs) == 0:
      raise CurieException(CurieError.kInternalError,
                            "Unable to locate .ovf file in '%s'" %
                            os.path.join(goldimages_directory,
                                         goldimage_name))
    elif len(ovfs) > 1:
      raise CurieException(CurieError.kInternalError,
                            "Unique .ovf file expected. Found: '%s'" % ovfs)

    vm = self.__vm_json_to_curie_vm(
      self._prism_client.deploy_ovf(
        vm_name, node_id, self._container_id, ovf_abs_path=ovfs[0],
        network_uuids=[self._network_id]))
    self.__vm_uuid_host_uuid_map[vm.vm_id()] = node_id
    vm._node_id = node_id
    return vm

  def create_vm(self, goldimages_directory, goldimage_name, vm_name, vcpus=1,
                ram_mb=1024, node_id=None, datastore_name=None, data_disks=()):
    """
    See 'Cluster.create_vm' for documentation.
    """
    log.info("Creating VM %s based on %s with %d vCPUs, %d MB RAM and %s "
             "disks on node %s in datastore %s ",
             vm_name, goldimage_name, vcpus, ram_mb, str(data_disks),
             str(node_id), datastore_name)
    image_uuid = self.deploy_goldimage_image_service(goldimages_directory,
                                                     goldimage_name)

    # This namedtuple hackery is to handle the expectations in vm.py which
    # expects information directly parsed from an OVF file.
    Units = namedtuple("Units", ["multiplier"])
    Disk = namedtuple("Disk", ["capacity", "units"])
    attach_disks = [Disk(gb, Units(1024*1024*1024)) for gb in data_disks]

    vm_desc = VmDescriptor(name=vm_name,
                           memory_mb=ram_mb,
                           num_vcpus=vcpus,
                           vmdisk_uuid_list=[image_uuid],
                           attached_disks=attach_disks,
                           container_uuid=self._container_id
                           )
    # Create the VM
    log.info("Creating VM '%s' with %s MB RAM and %s vCPUs",
             vm_desc.name, vm_desc.memory_mb, vm_desc.num_vcpus)
    nic_specs = \
      [vm_desc.to_ahv_vm_nic_create_spec(self._network_id)["specList"][0]]
    resp = self._prism_client.vms_create(vm_desc, nic_specs)
    tid = resp.get("taskUuid")
    if not tid:
      raise CurieException(CurieError.kManagementServerApiError,
                            "Failed to deploy VM: %s" % resp)

    TaskPoller.execute_parallel_tasks(
      tasks=PrismTask.from_task_id(self._prism_client, tid), timeout_secs=60)

    task_json = self._prism_client.tasks_get_by_id(tid)
    vm_uuid = task_json["entityList"][0]["uuid"]

    # Make a Curie VM descriptor and assign it to the requested node
    vm = self.__vm_json_to_curie_vm(self._prism_client.vms_get_by_id(vm_uuid))
    vm._node_id = node_id
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
        default value is FLAGS.prism_max_parallel_tasks.
      linked_clone (bool): Whether or not the clones should be "normal" full
        clones or linked clones.

    Returns:
      List of cloned VMs.
    """
    # TODO (jklein): Max parallel tasks
    if not node_ids:
      nodes = self.nodes()
      node_ids = []
      for _ in range(len(vm_names)):
        node_ids.append(random.choice(nodes).node_id())

    vm_desc = VmDescriptor.from_prism_entity_json(
      self._prism_client.vms_get_by_id(vm.vm_id()))

    if datastore_name is None:
      target_ctr_uuid = self._container_id
    else:
      target_ctr_uuid = self.__identifier_to_container_uuid(datastore_name)

    clone_spec = vm_desc.to_ahv_vm_clone_spec(vm_names,
                                              ctr_uuid=target_ctr_uuid)
    cutoff = self._prism_client.get_cluster_timestamp_usecs()
    task = PrismTask.from_task_id(
      self._prism_client,
      self._prism_client.vms_clone(vm.vm_id(), clone_spec))
    PrismTaskPoller.execute_parallel_tasks(tasks=[task,],
                                           timeout_secs=len(vm_names) * 900,
                                           prism_client=self._prism_client,
                                           cutoff_usecs=cutoff)
    log.info("Clone task complete")
    task_json = AcropolisTaskInfo(
      **self._prism_client.tasks_get_by_id(task.id(), True))
    created_uuids = set(e.uuid
                        for e in task_json.entity_list
                        if e.entity_type.strip().upper() == "VM")
    # Block until all VMs are found via /vms API.
    vms = self.__wait_for_vms(created_uuids)

    vm_name_map = {vm["vmName"]: vm for vm in vms}
    sorted_vms = [vm_name_map[vm_name] for vm_name in vm_names]
    # Create placement map which controls where VMs are placed when powered on.
    for node_id, vm in zip(node_ids, sorted_vms):
      self.__vm_uuid_host_uuid_map[vm["uuid"]] = node_id
    return sorted_vms

  def migrate_vms(self, vms, nodes, max_parallel_tasks=None):
    """Move 'vms' to 'nodes'.

    For each VM 'vms[xx]' move to the corresponding Node 'nodes[xx]'.

    Args:
      vms (list<Vm>): List of VMs to migrate.
      nodes (list<Node>):  List of nodes to which 'vms' should be
        migrated. Must be the same length as 'vms'.

        Each VM in 'vms' wll be moved to the corresponding node in 'nodes'.
      max_parallel_tasks (int): The number of VMs to migrate in parallel.
    """
    cutoff = self._prism_client.get_cluster_timestamp_usecs()
    # TODO (jklein): Max parallel tasks won't work unless this is converted
    # to a descriptor.
    log.info("Migrating VMS")
    if len(vms) != len(nodes):
      raise CurieException(CurieError.kInvalidParameter,
                            "Must provide a destination node for each VM")

    ret = {}
    for ii, vm in enumerate(vms):
      ret[vm.vm_id()] = self._prism_client.vms_migrate(vm.vm_id(),
                                                       nodes[ii].node_id())

    return PrismTaskPoller.execute_parallel_tasks(
      tasks=[PrismTask.from_task_id(self._prism_client, tid)
             for tid in ret.values()],
      max_parallel=self._get_max_parallel_tasks(max_parallel_tasks),
      timeout_secs=len(vms) * 1200, prism_client=self._prism_client,
      cutoff_usecs=cutoff)

  def relocate_vms_datastore(
    self, vms, datastore_names, max_parallel_tasks=None):
    """Relocate 'vms' to 'datastore_names'."""
    if len(vms) != len(datastore_names):
      raise CurieTestException("Unequal list length of vms and destination "
                                "datastores for relocation.")
    raise NotImplementedError("Not currently supported for AHV")

  def cleanup(self, test_ids=()):
    """Shutdown and remove all curie VMs from this cluster.

    Raises:
      CurieException if cluster is not ready for cleanup after 40 minutes.
    """
    log.info("Cleaning up state on cluster %s", self.metadata().cluster_name)

    if not ScenarioUtil.prereq_runtime_cluster_is_ready(self):
      ScenarioUtil.prereq_runtime_cluster_is_ready_fix(self)

    cluster_json = self.__lookup_cluster_json()
    log.info("Cleaning up state on cluster %s", cluster_json["name"])

    vms = self.vms()
    test_vm_names, _ = NameUtil.filter_test_vm_names(
      [vm.vm_name() for vm in vms], test_ids)

    test_vms = [vm for vm in vms if vm.vm_name() in test_vm_names]

    # Allow exceptions from power off or delete to propagate upwards.
    self.__set_power_state_for_vms(test_vms, "off")
    self.delete_vms(test_vms)

    self._prism_client.cleanup_nutanix_state(test_ids)
    self.cleanup_images()

  @CurieUtil.log_duration
  def collect_performance_stats(self,
                                start_time_secs=None,
                                end_time_secs=None):
    """Collect performance statistics for all nodes in the cluster.

    Optional arguments 'start_time_secs' and 'end_time_secs' can be used to
    limit the results to a specific time range. Note that these times are
    relative to the vSphere clock. If the clock of the Curie server is not
    synchronized, the samples returned might appear to be outside of the given
    time boundaries.

    Args:
      start_time_secs (int): Optionally specify the oldest sample to return, in
        seconds since epoch. Defaults to five minutes in the past.
      end_time_secs (int): Optionally specify the newest sample to return, in
        seconds since epoch.

    Returns:
      (dict) Dict of list of curie_metrics_pb2.CurieMetric. Top level dict
        keyed by node ID. List contains one entry per metric.
    """
    if start_time_secs is None:
      start_time_secs = time.time() - 300
    start_time_usecs = int(start_time_secs * 1e6)
    end_time_usecs = int(end_time_secs * 1e6) if end_time_secs else None
    results_map = {}
    metric_names = [self._curie_metric_to_metric_name(metric)
                    for metric in self.metrics()]
    for node in self.nodes():
      result = self._prism_client.hosts_stats_get_by_id(
        host_id=node.node_id(),
        metric_names=set(metric_names),
        startTimeInUsecs=start_time_usecs,
        endTimeInUsecs=end_time_usecs)
      results_map[node.node_id()] = self.__produce_curie_metrics(
        result["statsSpecificResponses"], node)
    return MetricsUtil.sorted_results_map(results_map)

  def is_ha_enabled(self):
    """See 'Cluster.is_ha_enabled' for documentation."""
    return self._prism_client.ha_get().get("failoverEnabled", True)

  def is_drs_enabled(self):
    """See 'Cluster.is_drs_enabled' for documentation."""
    # TODO
    return False

  def disable_drs_vms(self, *args, **kwargs):
    # TODO
    pass

  def enable_ha_vms(self, vms):
    # TODO(ryan.hardin): Add this.
    log.debug("Enabling HA for VMs on AHV not supported yet, and will be "
              "skipped")

  def disable_ha_vms(self, *args, **kwargs):
    # TODO
    pass

  #----------------------------------------------------------------------------
  #
  # Public AcropolisCluster-specific methods.
  #
  #----------------------------------------------------------------------------
  def cleanup_images(self):
    """
    Cleans up image service, removing any images associated with curie.
    """
    images = self._prism_client.images_get().get("entities", {})
    to_delete_image_uuids = []
    for image in images:
      if image["name"].startswith(CURIE_GOLDIMAGE_VM_DISK_PREFIX):
        to_delete_image_uuids.append(image["uuid"])
    log.info("Deleting images %s",
             ", ".join([i for i in to_delete_image_uuids]))
    task_map = self._prism_client.images_delete(to_delete_image_uuids)
    image_id_tid_map = {}
    for image_id, tid in task_map.iteritems():
      image_id_tid_map[image_id] = PrismTask.from_task_id(self._prism_client,
                                                          tid)
    TaskPoller.execute_parallel_tasks(
      tasks=image_id_tid_map.values(),
      timeout_secs=300)

  def get_cluster_architecture(self):
    """
    Get the processor architecture used by the cluster.

    Returns:
      str: Cluster processor architecture.
    """
    cluster_json = self.__lookup_cluster_json()
    try:
      arch = cluster_json["clusterArch"].lower()
    except KeyError:
      log.info("clusterArch not in cluster json, defaulting to %s",
               GoldImageManager.ARCH_X86_64)
      return GoldImageManager.ARCH_X86_64
    if arch == GoldImageManager.ARCH_X86_64:
      return GoldImageManager.ARCH_X86_64
    elif arch == GoldImageManager.ARCH_PPC64LE:
      return GoldImageManager.ARCH_PPC64LE
    else:
      raise CurieTestException(
        "Invalid cluster architecture detected: %s" % arch)

  def deploy_goldimage_image_service(self, goldimages_directory, goldimage_name):
    """
    Deploy a gold image to the image service.

    Args:
      goldimage_name (str): Name of the gold image to deploy.

    Returns:
      str: ID of the created disk image.
    """
    arch = self.get_cluster_architecture()
    # Select a vdisk format to use. Currently PPC64LE goldimages are only built
    # using qcow2 format and the x86_64 in vmdk. We could have the manager
    # perform a conversion, but acropolis can already do the image conversion
    # for us.
    if arch == GoldImageManager.ARCH_PPC64LE:
      disk_format = GoldImageManager.FORMAT_QCOW2
    else:
      disk_format = GoldImageManager.FORMAT_VMDK

    # Use the GoldImage manager to get a path to our appropriate goldimage
    goldimage_manager = GoldImageManager(goldimages_directory)
    goldimage_path = goldimage_manager.get_goldimage_path(
      goldimage_name, format_str=disk_format, arch=arch)
    log.debug("Deploying %s to cluster", goldimage_path)

    # Deploy the image to service
    disk_name = os.path.splitext(os.path.basename(goldimage_path))[0]
    img_uuid, tid, _ = self._prism_client.images_create(
      NameUtil.goldimage_vmdisk_name(disk_name, "os"),
      goldimage_path, self._container_id)
    TaskPoller.execute_parallel_tasks(
      tasks=PrismTask.from_task_id(self._prism_client, tid), timeout_secs=3600)

    # NB: Required due to possible AHV bug. See XRAY-225.
    num_images_get_retries = 5
    for attempt_num in xrange(num_images_get_retries):
      images_get_data = self._prism_client.images_get(image_id=img_uuid)
      image_state = images_get_data["image_state"]
      if image_state.lower() == "active":
        # Return the disk image
        return images_get_data["vm_disk_id"]
      else:
        log.info("Waiting for created image to become active "
                 "(imageState: %s, retry %d of %d)",
                 image_state, attempt_num + 1, num_images_get_retries)
        log.debug(images_get_data)
        time.sleep(1)
    else:
      raise CurieException(CurieError.kInternalError,
                            "Created image failed to become active within "
                            "%d attempts" % num_images_get_retries)

  #----------------------------------------------------------------------------
  #
  # Protected AcropolisCluster-specific methods.
  #
  #----------------------------------------------------------------------------

  def _get_max_parallel_tasks(self, arg):
    return FLAGS.prism_max_parallel_tasks if arg is None else arg

  #----------------------------------------------------------------------------
  #
  # Private AcropolisCluster-specific methods.
  #
  #----------------------------------------------------------------------------

  # TODO (jklein): Fix this entire function.

  def __set_power_state_for_vms(
      self, vms, state, wait_for_ip=False,
      max_parallel_tasks=None,
      power_on_retries=10,
      timeout_secs=900):

    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    t0 = time.time()
    cutoff = self._prism_client.get_cluster_timestamp_usecs()

    vm_host_map = dict((vm.vm_id(), vm.node_id()) for vm in vms)
    power_op_vm_ids = vm_host_map.keys()
    # TODO (jklein): Why are these APIs broken :(
    for ii in xrange(power_on_retries):
      vm_id_task_id_map = self._prism_client.vms_set_power_state_for_vms(
        power_op_vm_ids, state,
        host_ids=[vm_host_map[vm] for vm in power_op_vm_ids])
      task_id_vm_id_map = dict((v, k)
                               for k, v in vm_id_task_id_map.iteritems())

      tasks = []
      failed_for_vm_ids = []
      # Filter out tasks which immediately failed.
      for vm_id, tid in vm_id_task_id_map.iteritems():
        # TODO (jklein): Don't use literal True to indicate a state.
        if tid is True:
          continue
        if tid is None:
          failed_for_vm_ids.append(vm_id)
          continue
        tasks.append(PrismTask.from_task_id(self._prism_client, tid))

      PrismTaskPoller.execute_parallel_tasks(tasks=tasks,
                                             max_parallel=max_parallel_tasks,
                                             timeout_secs=timeout_secs,
                                             prism_client=self._prism_client,
                                             cutoff_usecs=cutoff,
                                             raise_on_failure=False)

      failed_for_vm_ids.extend(task_id_vm_id_map[t.id()] for t in tasks if
                               TaskStatus.cannot_succeed(t._state.status))
      if not failed_for_vm_ids:
        break

      power_op_vm_ids = failed_for_vm_ids

      log.warning("Failed to perform power op %s on %d VMs (attempt %d of %d)",
                  state, len(power_op_vm_ids), ii + 1, power_on_retries)

    else:
      raise CurieTestException("Failed to power %s VMs %s" %
                                (state, ", ".join(failed_for_vm_ids)))

    # TODO (jklein): Fix the terrible handling of time here when brain is more
    # functional.
    timeout_secs -= time.time() - t0
    t0 = time.time()
    while timeout_secs > 0:
      vm_id_status_map = dict(
        (vm["uuid"], vm) for vm in self._prism_client.vms_get()["entities"])
      failed_for_vm_ids = []
      for vm_id in vm_host_map.iterkeys():
        status = vm_id_status_map.get(vm_id)
        if not status or status.get("powerState") != state:
          failed_for_vm_ids.append(vm_id)

      if failed_for_vm_ids:
        log.info("Waiting for %d of %d VMs to transition to state %s",
                 len(failed_for_vm_ids), len(power_op_vm_ids), state)
        timeout_secs -= time.time() - t0
        t0 = time.time()
        time.sleep(1)
      else:
        break

    if failed_for_vm_ids:
      raise CurieTestException("Failed to power %s VMs %s" %
                                (state, ", ".join(failed_for_vm_ids)))

    if not wait_for_ip:
      return

    if state != "on":
      raise CurieTestException(
        "Cannot wait for IPs to be assigned to powered off VMs")

    timeout_secs -= time.time() - t0
    t0 = time.time()
    needs_ip_vm_ids = set(vm.vm_id() for vm in vms)
    has_ip_vm_ids = set()
    while timeout_secs > 0:
      vm_id_status_map = dict(
        (vm["uuid"], vm) for vm in self._prism_client.vms_get()["entities"])
      for vm_id in (needs_ip_vm_ids - has_ip_vm_ids):
        if vm_id not in vm_id_status_map:
          # NB: Prism API may temporarily return an incomplete list of VMs.
          continue
        ip_addr = vm_id_status_map[vm_id].get("ipAddresses", [])
        if ip_addr:
          log.debug("VM %r has IP addresses: %r", vm_id, ip_addr)
          has_ip_vm_ids.add(vm_id)
        else:
          log.debug("VM %r IP addresses are %r, retrying", vm_id, ip_addr)

      timeout_secs -= time.time() - t0
      t0 = time.time()
      if needs_ip_vm_ids - has_ip_vm_ids:
        log.info(
          "Waiting for %d of %d VMs to acquire IPs (%d seconds remaining)",
          len(needs_ip_vm_ids - has_ip_vm_ids), len(needs_ip_vm_ids),
          timeout_secs)
        time.sleep(1)
      else:
        return

    raise CurieTestException(
      "Timed out waiting for %d of %d VMs to acquire IPs" %
      (len(needs_ip_vm_ids), len(power_op_vm_ids)))

  def __identifier_to_container_uuid(self, ctr_id_or_name):
    try:
      return self._prism_client.containers_get(
        container_name=ctr_id_or_name)["containerUuid"]
    except Exception as exc:
      log.warning(
        "Failed to lookup container by name, assuming '%s' is a UUID. Err: %s",
        ctr_id_or_name, exc)
      # This will raise an appropriate exception on failure.
      return self._prism_client.containers_get(
        container_id=ctr_id_or_name)["containerUuid"]

  def __vm_json_to_curie_vm(self, vm_json):
    "Returns an object of the appropriate subclass of Vm for 'vim_vm'."
    vm_params = VmParams(self, vm_json["uuid"])
    # TODO (jklein): Need to better standardize on how we treat UnixVms beyond
    # just adding mixin.
    # curie_guest_os_type_value = vm_json["guestOperatingSystem"]
    vm_params.is_cvm = vm_json["controllerVm"]
    vm_params.node_id = vm_json["hostUuid"]
    # NB: If the VM is not powered on, hostUuid is None.
    if vm_params.node_id is None:
      vm_params.node_id = self.__vm_uuid_host_uuid_map.get(vm_json["uuid"])
    ips = vm_json.get("ipAddresses", [])
    if ips:
      vm_params.vm_ip = ips[0]
    vm_params.vm_name = vm_json["vmName"]

    return AcropolisUnixVm(vm_params)

  def __lookup_cluster_json(self):
    for cluster_json in self._prism_client.clusters_get().get("entities", []):
      if cluster_json["clusterUuid"] == self._cluster_id:
        return cluster_json
    raise CurieTestException("Unable to locate cluster '%s' in Prism" %
                              self._cluster_id)

  def __produce_curie_metrics(self, stats_specific_responses, node):
    responses_by_counter_name = {}
    for metric in stats_specific_responses:
      responses_by_counter_name[metric["metric"]] = metric
    results = []
    for curie_metric in self.metrics():
      ahv_counter_name = self._curie_metric_to_metric_name(curie_metric)
      metric = responses_by_counter_name[ahv_counter_name]
      start_time_secs = int(metric["startTimeInUsecs"] / 1e6)
      interval_secs = int(metric["intervalInSecs"])
      values = metric["values"]
      offsets = [index * interval_secs for index in range(len(values))]
      timestamps = [start_time_secs + offset for offset in offsets]
      # If any values are None, remove it and its corresponding timestamp.
      timestamp_value_tuples = [tup for tup in zip(timestamps, values)
                                if tup[1] is not None]
      if timestamp_value_tuples:
        timestamps, values = zip(*timestamp_value_tuples)
      else:
        timestamps, values = [], []
      result = CurieMetric()
      result.CopyFrom(curie_metric)
      # TODO(ryan.hardin): Generalize unit conversion, move to utility module.
      if result.rate == CurieMetric.kPerSecond:
        # Convert units per interval into units per second.
        values = [(value / float(interval_secs)) for value in values]
      if result.unit == CurieMetric.kPercent:
        # Assume metric in ppm (parts per million) - convert to percentage.
        values = [(value / 1e4) for value in values]
      elif result.unit == CurieMetric.kKilobytes:
        # Assume metric in bytes - convert to kilobytes.
        values = [(value / float(2**10)) for value in values]
      elif (result.unit == CurieMetric.kMegahertz and
            result.name == CurieMetric.kCpuUsage):
        # Assume metric in ppm (parts per million) - convert total megahertz.
        # TODO(ryan.hardin): Should node.cpu_capacity_in_hz ever return None?
        if node.cpu_capacity_in_hz is None:
          log.debug("node.cpu_capacity_in_hz returned None")
          timestamps, values = [], []
        else:
          values = [(cpu_ppm * node.cpu_capacity_in_hz / 1e12)
                    for cpu_ppm in values]
      CHECK(len(result.timestamps) == 0)
      result.timestamps.extend(timestamps)
      CHECK(len(result.values) == 0)
      result.values.extend([int(value) for value in values])
      results.append(result)
    return results

  def __wait_for_vms(self, vm_uuids, poll_interval_secs=2, timeout_secs=300):
    """
    Wait for VM UUIDs to appear in GET /vms response.

    Args:
      vm_uuids (iterable): UUIDs to search for.
      poll_interval_secs (int): Polling interval in seconds.
      timeout_secs (int): Maximum number of seconds to wait.

    Returns:
      List of CurieVM: Found VMs

    Raises:
      CurieTestException:
        If timeout is reached.
    """
    start_time_secs = time.time()
    deadline_secs = start_time_secs + timeout_secs
    while time.time() < deadline_secs:
      vms = self._prism_client.vms_get()["entities"]
      uuid_vm_map = {vm["uuid"]: vm for vm in vms}
      missing_uuids = vm_uuids - set(uuid_vm_map.keys())
      if missing_uuids:
        log.debug("%d VMs not yet found in vms_get response (%ds remaining)",
                  len(missing_uuids), deadline_secs - time.time())
        time.sleep(poll_interval_secs)
      else:
        log.debug("All VMs found in vms_get response")
        return [uuid_vm_map[uuid] for uuid in vm_uuids]  # Maintain order.
    else:
      raise CurieTestException("Timed out waiting for VMs to be visible via "
                                "the /vms API (%ds elapsed)" %
                                deadline_secs - start_time_secs)
