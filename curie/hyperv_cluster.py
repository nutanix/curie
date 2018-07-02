#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import random
import time
from functools import partial
import socket
import os

from curie.cluster import Cluster
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException, CurieTestException
from curie.goldimage_manager import GoldImageManager
from curie.hyperv_node import HyperVNode
from curie.hyperv_unix_vm import HyperVUnixVM
from curie.log import CHECK
from curie.name_util import CURIE_GOLDIMAGE_VM_NAME_PREFIX, NameUtil
from curie.task import HypervTaskDescriptor, HypervTaskPoller
from curie.test.scenario_util import ScenarioUtil
from curie.vm import VmParams
from curie.vmm_client import VmmClient

log = logging.getLogger(__name__)


class HyperVCluster(Cluster):

  TIMEOUT = 900

  def __init__(self, cluster_metadata):
    super(HyperVCluster, self).__init__(cluster_metadata)
    CHECK(cluster_metadata.cluster_management_server_info.HasField(
      "vmm_info"))

    vmm_info = self._metadata.cluster_management_server_info.vmm_info

    # # Datacenter name of the datacenter where the cluster to run on lives.
    # self.__datacenter_name = vmm_info.vmm_datacenter_name

    # Cluster name of the cluster to run on.
    self.__cluster_name = vmm_info.vmm_cluster_name

    # Datastore name of the datastore to deploy test VMs on the cluster.
    self.__share_name = vmm_info.vmm_share_name

    # Datastore path of the datastore to deploy test VMs on the cluster.
    self.__share_path = vmm_info.vmm_share_path

    # VMM Library Share path
    self.__vmm_library_server_share_path = vmm_info.vmm_library_server_share_path

    # VM Network Name
    self.__vmm_network_name = vmm_info.vmm_network_name

    # Map of VM UUIDs to host UUIDs on which they should be scheduled.
    self.__vm_uuid_host_uuid_map = {}

    # VMM Client
    self.__vmm_client = None

  def get_power_state_for_vms(self, vms):
    """See 'Cluster.get_power_state_for_vms' for documentation."""
    vm_ids = set([vm.vm_id() for vm in vms])
    with self.__get_vmm_client() as vmm_client:
      vm_id_power_state_map = {vm_json["id"]: vm_json["status"] for vm_json in
                               vmm_client.get_vms(self.__cluster_name)
                               if vm_json["id"] in vm_ids}
    missing_ids = vm_ids - set(vm_id_power_state_map)
    if missing_ids:
      raise CurieTestException(
        "Expected VM ID(s) missing from 'get_vms' response: %s" %
        ",".join(sorted([str(vm_id) for vm_id in missing_ids])))
    else:
      return vm_id_power_state_map

  def import_vm(self, goldimages_directory, goldimage_name, vm_name,
                node_id=None):
    # If node is not specified, select random node.
    if node_id is None:
      node_id = random.choice(self.nodes()).node_id()

    with self.__get_vmm_client() as vmm_client:
      # Use the GoldImage manager to get a path to our appropriate goldimage
      goldimage_zip_name = GoldImageManager.get_goldimage_filename(
        goldimage_name, GoldImageManager.FORMAT_VHDX_ZIP,
        GoldImageManager.ARCH_X86_64)

      goldimage_name = GoldImageManager.get_goldimage_filename(
        goldimage_name, GoldImageManager.FORMAT_VHDX,
        GoldImageManager.ARCH_X86_64)

      goldimage_url = self.__get_http_goldimages_address(goldimage_zip_name)

      target_dir = self.__cluster_name + "\\" + vm_name
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Uploading image '%s'",
        post_task_msg="Uploaded image '%s'",
        create_task_func=partial(
          vmm_client.upload_image, [goldimage_url],
          goldimage_target_dir=target_dir,
          transfer_type="bits"))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Create GoldImage VM Template.
      goldimage_disk_path = "\\".join([self.__vmm_library_server_share_path,
                                       goldimage_name.split(".")[0],
                                       goldimage_name])
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Updating SCVMM library path '%s'" % goldimage_disk_path,
        post_task_msg="Updated SCVMM library path '%s'" % goldimage_disk_path,
        create_task_func=partial(
          vmm_client.update_library, goldimage_disk_path))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      vmm_client.create_vm_template(
        self.__cluster_name, vm_name, node_id, goldimage_disk_path,
        self.__share_path, self.__vmm_network_name)

      # Create GoldImage VM with default disks and hardware configuration.
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Creating VM '%s' on node '%s'" % (vm_name, node_id),
        post_task_msg="Created VM '%s'" % vm_name,
        create_task_func=partial(
          vmm_client.create_vm, self.__cluster_name, None,
          [{"vm_name": vm_name, "node_id": node_id}], self.__share_path,
          [16, 16, 16, 16, 16, 16]))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Search for new VM and return as result.
      for vm in self.vms():
        if vm.vm_name() == vm_name:
          self.__vm_uuid_host_uuid_map[vm.vm_id()] = node_id
          vm._node_id = node_id
          return vm
      else:
        raise CurieTestException("Imported VM '%s' not found in self.vms()" %
                                  vm_name)

  def create_vm(self, goldimages_directory, goldimage_name, vm_name, vcpus=1,
                ram_mb=1024, node_id=None, datastore_name=None, data_disks=()):
    goldimage_zip_name = GoldImageManager.get_goldimage_filename(
      goldimage_name, GoldImageManager.FORMAT_VHDX_ZIP,
      GoldImageManager.ARCH_X86_64)

    goldimage_name = GoldImageManager.get_goldimage_filename(
      goldimage_name, GoldImageManager.FORMAT_VHDX,
      GoldImageManager.ARCH_X86_64)

    goldimage_url = self.__get_http_goldimages_address(goldimage_zip_name)

    with self.__get_vmm_client() as vmm_client:
      target_dir = self.__cluster_name + "\\" + vm_name
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Uploading image '%s'" % goldimage_url,
        post_task_msg="Uploaded image '%s'" % goldimage_url,
        create_task_func=partial(
          vmm_client.upload_image, [goldimage_url],
          goldimage_target_dir=target_dir,
          transfer_type="bits"))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      goldimage_disk_path = "\\".join([self.__vmm_library_server_share_path,
                                       target_dir, goldimage_name])

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Updating SCVMM library path '%s'" % goldimage_disk_path,
        post_task_msg="Updated SCVMM library path '%s'" % goldimage_disk_path,
        create_task_func=partial(
          vmm_client.update_library, goldimage_disk_path))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      vmm_client.create_vm_template(
        self.__cluster_name, vm_name, node_id, goldimage_disk_path,
        self.__share_path, self.__vmm_network_name, vcpus, ram_mb)
      # TODO (iztokp): Currently create vm template does not start a task. But
      # it could in the future...
      # self.__wait_for_tasks(vmm_client, response)

      # Create GoldImage virtual machine
      if node_id is None:
        node_id = random.choice(self.nodes()).node_id()

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Creating VM '%s' on node '%s'" % (vm_name, node_id),
        post_task_msg="Created VM '%s'" % vm_name,
        create_task_func=partial(
          vmm_client.create_vm, self.__cluster_name, vm_name,
          [{"vm_name": vm_name, "node_id": node_id}], self.__share_path,
          data_disks))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Search for new VM and return as result.
      new_vm = None
      for vm in self.vms():
        if vm.vm_name() == vm_name:
          if new_vm is None:
            self.__vm_uuid_host_uuid_map[vm.vm_id()] = node_id
            vm._node_id = node_id
            new_vm = vm
          else:   # Handle ScVMM bug with duplicate VM objects for one Hyper-V virtual machine
            raise CurieTestException("Duplicate VM '%s' found in self.vms()" % vm_name)

      if new_vm:
          return new_vm
      else:
        raise CurieTestException("Created VM '%s' not found in self.vms()" %
                                  vm_name)

  def delete_vms(self, vms, ignore_errors=False, max_parallel_tasks=100):
    if vms:
      with self.__get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, vm in enumerate(vms):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Deleting VM '%s' (%d/%d)" %
                         (vm.vm_name(), index + 1, len(vms)),
            post_task_msg="Deleted VM '%s'" % vm.vm_name(),
            create_task_func=partial(
              vmm_client.vms_delete, self.__cluster_name, [vm.vm_id()]))
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=max_parallel_tasks, poll_secs=10,
          timeout_secs=len(vms) * 900)

  def migrate_vms(self, vms, nodes, max_parallel_tasks=8):
    if len(vms) != len(nodes):
      raise CurieException(CurieError.kInvalidParameter,
                            "Must provide a destination node for each VM")
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    vm_node_map = []
    task_list_both = []
    task_list_new = []

    for ii, vm in enumerate(vms):
      vm_node_map.append(
        {"vm_id": vm.vm_id(),
         "node_id": nodes[ii].node_id(),
         })
      task_list_both.append(
        {"id": vm.vm_id(),
         "possible_owners": [vm.node_id(), nodes[ii].node_id()],
         })
      task_list_new.append(
        {"id": vm.vm_id(),
         "possible_owners": [nodes[ii].node_id()],
         })

    if not vms:
      # Return if there is nothing to do
      return

    with self.__get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, task_list_both_chunk in enumerate(task_list_both):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Setting possible owners of VM '%s' to nodes '%s' and "
                       "'%s' (%d/%d)" %
                       (vm_names_by_id[task_list_both_chunk["id"]],
                        task_list_both_chunk["possible_owners"][0],
                        task_list_both_chunk["possible_owners"][1],
                        index + 1, len(vms)),
          post_task_msg="Set possible owners of VM '%s'" %
                        vm_names_by_id[task_list_both_chunk["id"]],
          create_task_func=partial(
            vmm_client.vms_set_possible_owners_for_vms, self.__cluster_name,
            [task_list_both_chunk]))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, timeout_secs=len(vms) * 60)

      task_desc_list = []
      for index, vm_node_map_chunk in enumerate(vm_node_map):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Migrating VM '%s' to node '%s' (%d/%d)" %
                       (vm_names_by_id[vm_node_map_chunk["vm_id"]],
                        vm_node_map_chunk["node_id"],
                        index + 1, len(vms)),
          post_task_msg="Migrated VM '%s'" %
                        vm_names_by_id[vm_node_map_chunk["vm_id"]],
          create_task_func=partial(
            vmm_client.migrate_vm, self.__cluster_name, [vm_node_map_chunk],
            self.__share_path))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=10,
        timeout_secs=len(vms) * 900)

      task_desc_list = []
      for index, task_list_new_chunk in enumerate(task_list_new):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Setting possible owners of VM '%s' to node '%s' "
                       "(%d/%d)" %
                       (vm_names_by_id[task_list_new_chunk["id"]],
                        task_list_new_chunk["possible_owners"][0],
                        index + 1, len(vms)),
          post_task_msg="Set possible owners of VM '%s'" %
                        vm_names_by_id[task_list_new_chunk["id"]],
          create_task_func=partial(
            vmm_client.vms_set_possible_owners_for_vms, self.__cluster_name,
            [task_list_new_chunk]))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, timeout_secs=len(vms) * 60)

  def collect_performance_stats(self, start_time_secs=None,
                                end_time_secs=None):
    # TODO(ryan.hardin): Implement this.
    raise NotImplementedError("No cluster stats on Hyper-V")

  def is_ha_enabled(self):
    """See 'Cluster.is_ha_enabled' for documentation."""

    # HA is not defined on the cluster level. HA is a setting that can be
    # defined for each VM, depending on the storage location of the VM.
    # If a VM is placed on a shared storage or cluster volume, VM can be HA or
    # not. If a VM is placed on a cluster shared volume VM must be HA otherwise
    # it cannot be created. Currently we create all curie VMs with HA enabled.

    # If at least one VM in the cluster is HA we return 'true'.
    with self.__get_vmm_client() as vmm_client:
      vms_json = vmm_client.get_vms(self.__cluster_name)
      for vm in vms_json:
        if vm["is_dynamic_optimization_available"]:
          return True

      return False

  def is_drs_enabled(self):
    """See 'Cluster.is_drs_enabled' for documentation."""

    # DRS is called does dynamic optimization in VMM. We are going to return
    # false since we would duplicate the same code here as in is_ha_enabled().

    # TODO(ryan.hardin): Should this return self.is_ha_enabled()?
    return False

  def enable_ha_vms(self, vms):
    # Possible owners will be cleared on all machines
    node_list = [node.node_id() for node in self.nodes()]
    task_req_list = [{"id": vm.vm_id(), "possible_owners": node_list}
                     for vm in vms]
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    if task_req_list:
      with self.__get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Enabling HA for VM '%s' by setting all nodes as "
                         "possible owners (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["id"]],
                          index + 1, len(vms)),
            post_task_msg="Enabled HA for VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["id"]],
            create_task_func=partial(
              vmm_client.vms_set_possible_owners_for_vms, self.__cluster_name,
              [task_req_list_chunk]))
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60)

  def disable_ha_vms(self, vms):
    # (iztokp): All VMs will stay HA, but we will change the possible owners to
    # only one node so VMs will not be able to migrate to other nodes.
    task_req_list = [{"id": vm.vm_id(), "possible_owners": [vm.node_id()]}
                     for vm in vms]
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    if task_req_list:
      with self.__get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Disabling HA for VM '%s' by setting only possible "
                         "owner to node '%s' (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["id"]],
                          task_req_list_chunk["possible_owners"][0],
                          index + 1, len(vms)),
            post_task_msg="Disabled HA for VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["id"]],
            create_task_func=partial(
              vmm_client.vms_set_possible_owners_for_vms, self.__cluster_name,
              [task_req_list_chunk]))
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60)

  def disable_drs_vms(self, vms):
    # DRS is called Dynamic Optimization in VMM. Disabling Dynamic Optimization
    # requires the same approach taken as in disable_ha_vms() so will skip
    # duplicating the code here.
    # TODO(ryan.hardin): Should this return self.disable_ha_vms()?
    pass

  def snapshot_vms(self, vms, tag, snapshot_description=None):
    snapshot_req_list = []
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    for vm in vms:
      snapshot_hash = {"vm_id": vm.vm_id(), "name": tag}
      if snapshot_description:
        snapshot_hash["description"] = snapshot_description
      snapshot_req_list.append(snapshot_hash)

    if snapshot_req_list:
      with self.__get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, snapshot_req_list_chunk in enumerate(snapshot_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Creating snapshot for VM '%s' named '%s' (%d/%d)" %
                         (vm_names_by_id[snapshot_req_list_chunk["vm_id"]],
                          snapshot_req_list_chunk["name"],
                          index + 1, len(vms)),
            post_task_msg="Created snapshot for VM '%s'" %
                          vm_names_by_id[snapshot_req_list_chunk["vm_id"]],
            create_task_func=partial(
              vmm_client.vms_set_snapshot, self.__cluster_name,
              [snapshot_req_list_chunk]))
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60)

  def power_on_vms(self, vms, max_parallel_tasks=100):
    timeout_secs = self.TIMEOUT
    t0 = time.time()

    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    task_req_list = []
    for vm in vms:
      if not vm.is_powered_on():
        task_req_list.append({"vm_id": vm.vm_id(), "power_state": "on"})

    if not task_req_list:
      # Return if there is nothing to do
      return

    with self.__get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, task_req_list_chunk in enumerate(task_req_list):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Powering on VM '%s' (%d/%d)" %
                       (vm_names_by_id[task_req_list_chunk["vm_id"]],
                        index + 1, len(vms)),
          post_task_msg="Powered on VM '%s'" %
                        vm_names_by_id[task_req_list_chunk["vm_id"]],
          create_task_func=partial(
            vmm_client.vms_set_power_state_for_vms, self.__cluster_name,
            [task_req_list_chunk]))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=5,
        timeout_secs=len(vms) * 900)

      # Wait for IPs
      timeout_secs -= time.time() - t0
      t0 = time.time()
      while timeout_secs > 0:
        all_available = True
        task_req_list = []
        task_desc_list = []
        vms_refresh_list = []
        vms_list = map(self.__json_to_curie_vm,
            vmm_client.get_vms(self.__cluster_name))
        for vm in vms_list:
          if vm in vms:
            if not vm.vm_ip():
              all_available = False
              task_req_list.append({"vm_id": vm.vm_id()})
              vms_refresh_list.append(vm)
        if all_available:
          break
        else:
          # Start VM refresh
          for index, task_req_list_chunk in enumerate(task_req_list):
            task_desc = HypervTaskDescriptor(
              pre_task_msg="Refreshing on VM '%s' (%d/%d)" %
                           (vm_names_by_id[task_req_list_chunk["vm_id"]],
                            index + 1, len(vms_refresh_list)),
              post_task_msg="Refreshed on VM '%s'" %
                            vm_names_by_id[task_req_list_chunk["vm_id"]],
              create_task_func=partial(
                vmm_client.refresh_vms, self.__cluster_name,
                [task_req_list_chunk]))
            task_desc_list.append(task_desc)
          HypervTaskPoller.execute_parallel_tasks(
            vmm=vmm_client, task_descriptors=task_desc_list,
            max_parallel=max_parallel_tasks, poll_secs=5,
            timeout_secs=len(vms_refresh_list) * 900,
            raise_on_failure=False)
        timeout_secs -= time.time() - t0
        t0 = time.time()
        time.sleep(10)
      else:
        raise CurieTestException("Timed out waiting for all VMs to become "
                                  "accessible within %d seconds" % self.TIMEOUT)

  def power_off_vms(self, vms, max_parallel_tasks=100):
    task_req_list = []
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    for vm in vms:
      if vm.is_powered_on():
        task_req_list.append({"vm_id": vm.vm_id(), "power_state": "off"})

    if task_req_list:
      with self.__get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Powering off VM '%s' (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["vm_id"]],
                          index + 1, len(vms)),
            post_task_msg="Powered off VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["vm_id"]],
            create_task_func=partial(
              vmm_client.vms_set_power_state_for_vms, self.__cluster_name,
              [task_req_list_chunk]))
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=max_parallel_tasks, poll_secs=5,
          timeout_secs=len(vms) * 900)

  def nodes(self, node_ids=None):
    with self.__get_vmm_client() as vmm_client:
      response = vmm_client.get_nodes(
        self.__cluster_name, node_ids)
      nodes = [self.__json_to_curie_node(node, index)
               for index, node in enumerate(response)]
      return nodes

  def power_off_nodes_soft(self, nodes, timeout_secs=None, async=False):
    """See 'Cluster.power_off_nodes_soft' for definition."""
    # TODO(ryan.hardin): Async.
    if timeout_secs is None:
      timeout_secs = self.TIMEOUT

    t0 = time.time()
    nodes_req_list = []
    for node in nodes:
      if node.is_powered_on_soft():
        nodes_req_list.append({"id": node.node_id(),
                               "fqdn": node.get_fqdn(),
                               })

    with self.__get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, nodes_req_list_chunk in enumerate(nodes_req_list):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Powering off node '%s' (%d/%d)" %
                       (nodes_req_list_chunk["id"],
                        index + 1, len(nodes)),
          post_task_msg="Powered off node '%s'" % nodes_req_list_chunk["id"],
          create_task_func=partial(
            vmm_client.nodes_power_state, self.__cluster_name,
            [nodes_req_list_chunk]))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=8,
        poll_secs=10, timeout_secs=len(nodes) * 900)

    timeout_secs -= time.time() - t0
    t0 = time.time()
    while timeout_secs > 0:
      all_shutdown = True
      nodes = self.nodes()
      for node in nodes:
        for node_req in nodes_req_list:
          if node.node_id() == node_req["id"]:
            if node.is_powered_on_soft():
              all_shutdown = False
      if all_shutdown:
        break
      timeout_secs -= time.time() - t0
      t0 = time.time()
      time.sleep(10)
    else:
      raise CurieTestException("Nodes could not be shutdown. Timeout "
                                "occurred while waiting for nodes to "
                                "shutdown.")

  def relocate_vms_datastore(self, vms, datastore_names,
                             max_parallel_tasks=16):
    """Relocate 'vms' to 'datastore_names'."""
    if len(vms) != len(datastore_names):
      raise CurieTestException("Unequal list length of vms and destination "
                                "datastores for relocation.")
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    vm_datastore_map = []

    for datastore_name, vm in zip(datastore_names, vms):
      vm_datastore_map.append({"vm_id": vm.vm_id(),
                               "datastore_name": datastore_name,
                               })

    with self.__get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, vm_datastore_map_chunk in enumerate(vm_datastore_map):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Relocating VM '%s' to datastore '%s' (%d/%d)" %
                       (vm_names_by_id[vm_datastore_map_chunk["vm_id"]],
                        vm_datastore_map_chunk["datastore_name"],
                        index + 1, len(vms)),
          post_task_msg="Relocated VM '%s' to datastore '%s'" %
                        (vm_names_by_id[vm_datastore_map_chunk["vm_id"]],
                         vm_datastore_map_chunk["datastore_name"]),
          create_task_func=partial(
            vmm_client.migrate_vm_datastore, self.__cluster_name,
            [vm_datastore_map_chunk]))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=10,
        timeout_secs=len(vms) * 900)

  def clone_vms(self, vm, vm_names, node_ids=(), datastore_name=None,
                max_parallel_tasks=16, linked_clone=False):
    # TODO(ryan.hardin): Linked clone.
    if not node_ids:
      nodes = self.nodes()
      node_ids = []
      for _ in range(len(vm_names)):
        node_ids.append(random.choice(nodes).node_id())
    if len(vm_names) != len(node_ids):
      raise ValueError("Length of vm_names must be equal to length of "
                       "node_ids (got vm_names=%r, node_ids=%r)" %
                       (vm_names, node_ids))

    vm_node_map = [{"vm_name": vm_name, "node_id": node_id}
                   for vm_name, node_id in zip(vm_names, node_ids)]

    power_on_vms = vm.is_powered_on()
    with self.__get_vmm_client() as vmm_client:
      template_name = vm.vm_name()
      # If source VM is not a goldimage, clone it to template so that VMM can
      # clone in parallel.
      if not vm.vm_name().startswith(CURIE_GOLDIMAGE_VM_NAME_PREFIX):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Creating VM '%s' to be converted to template" %
                       template_name,
          post_task_msg="Created VM '%s'" % template_name,
          create_task_func=partial(
            vmm_client.clone_vm, self.__cluster_name, vm.vm_id(),
            template_name, self.__share_path))
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
          timeout_secs=900)

        task_desc = HypervTaskDescriptor(
          pre_task_msg="Converting VM '%s' to a template" % template_name,
          post_task_msg="Converted VM '%s' to a template" % template_name,
          create_task_func=partial(
            vmm_client.convert_to_template, self.__cluster_name,
            template_name))
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
          timeout_secs=900)

      task_desc_list = []
      for index, vm_node_map_chunk in enumerate(vm_node_map):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Creating VM '%s' on node '%s' (%d/%d)" %
                       (vm_node_map_chunk["vm_name"],
                        vm_node_map_chunk["node_id"],
                        index + 1, len(vm_names)),
          post_task_msg="Created VM '%s'" % vm_node_map_chunk["vm_name"],
          create_task_func=partial(
            vmm_client.create_vm, self.__cluster_name, template_name,
            [vm_node_map_chunk], self.__share_path, None))
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=5,
        timeout_secs=len(vm_names) * 900)

    new_vms = [vm for vm in self.vms() if vm.vm_name() in vm_names]

    if power_on_vms:
      self.power_on_vms(new_vms)

    return new_vms

  def sync_power_state_for_nodes(self, nodes, timeout_secs=None):
    # No-op: It is not known that syncing is required on Hyper-V.
    return self.get_power_state_for_nodes(nodes)

  def update_metadata(self, include_reporting_fields):
    pass

  def cleanup(self, test_ids=()):
    """Shutdown and remove all curie VMs from this cluster.

     Raises:
       CurieException if cluster is not ready for cleanup after 40 minutes.
    """
    log.info("Cleaning up state on cluster %s", self.__cluster_name)

    if not ScenarioUtil.prereq_runtime_cluster_is_ready(self):
      ScenarioUtil.prereq_runtime_cluster_is_ready_fix(self)

    vms = self.vms()
    test_vm_names, _ = NameUtil.filter_test_vm_names(
      [vm.vm_name() for vm in vms], test_ids)
    test_vms = [vm for vm in vms if vm.vm_name() in test_vm_names]

    if test_vms:
      pass
      # Shutdown the VMs
      self.power_off_vms(test_vms)
      # Delete the VMs
      self.delete_vms(test_vms)

    with self.__get_vmm_client() as vmm_client:
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Cleaning VMM server",
        post_task_msg="Cleaned VMM server",
        create_task_func=partial(
          vmm_client.clean_vmm, self.__cluster_name, self.__cluster_name,
          NameUtil.get_vm_name_prefix()))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Cleaning library server share",
        post_task_msg="Cleaned library server share",
        create_task_func=partial(
          vmm_client.clean_library_server, self.__cluster_name,
          NameUtil.get_vm_name_prefix()))
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

  def get_power_state_for_nodes(self, nodes):
    node_ids = set([node.node_id() for node in nodes])
    with self.__get_vmm_client() as vmm_client:
      response = vmm_client.get_nodes(self.__cluster_name)

    node_id_soft_power_state_map = {}

    for node in response:
      if node["id"] in node_ids:
        node_ids.remove(node["id"])
        log.debug("SCVMM reports host %s in power state: '%s'",
                  node["name"], node["state"])
        node_id_soft_power_state_map[node["id"]] = node["state"]

    if node_ids:
      raise CurieTestException("Invalid Node ID(s) '%s'"
                                  % ", ".join(node_ids))

    return node_id_soft_power_state_map

  def vms(self):
    with self.__get_vmm_client() as vmm_client:
      return map(self.__json_to_curie_vm,
                 vmm_client.get_vms(self.__cluster_name))

  def __json_to_curie_vm(self, json_vm):
    "Returns an object of the appropriate subclass of Vm for 'vim_vm'."
    # On a vSphere cluster, the VM name should be unique on the cluster where
    # the VM resides, so we can just use the VM name as the VM ID.
    vm_id = json_vm["id"]
    curie_guest_os_type_value = None

    vm_params = VmParams(self, vm_id)
    vm_params.vm_name = json_vm["name"]
    vm_params.node_id = json_vm["node_id"]
    # Set first IP from list
    if json_vm["ips"].__len__() != 0:
      vm_params.vm_ip = json_vm["ips"][0]

    # if curie_guest_os_type_value is None:
    #   return HyperVVm(vm_params,json_vm)
    # else:
    #  CHECK_EQ(curie_guest_os_type_value, "unix")
    return HyperVUnixVM(vm_params, json_vm)

  def __json_to_curie_node(self, json_node, ii):
    "Returns an object of the appropriate subclass of Vm for 'vim_vm'."
    # On a vSphere cluster, the VM name should be unique on the cluster where
    # the VM resides, so we can just use the VM name as the VM ID.
    node_properties = {
      "power_state": json_node["state"],
      "fqdn": json_node["fqdn"],
      "ips": json_node["ips"],
      "name": json_node["name"],
      "version": json_node["version"],
      "overall_state": json_node["overall_state"],
    }

    node = HyperVNode(self, json_node["id"], ii, node_properties)
    return node

  def __get_vmm_client(self):
    vmm_info = self._metadata.cluster_management_server_info.vmm_info
    return VmmClient(
      address=vmm_info.vmm_server,
      username=vmm_info.vmm_user,
      password=vmm_info.vmm_password,
      host_address=vmm_info.vmm_host,
      host_username=vmm_info.vmm_host_user,
      host_password=vmm_info.vmm_host_password,
      library_server_address=vmm_info.vmm_library_server,
      library_server_username=vmm_info.vmm_library_server_user,
      library_server_password=vmm_info.vmm_library_server_password,
      library_server_share_path=vmm_info.vmm_library_server_share_path)

  def __get_http_goldimages_address(self, image_name):
    http_server = os.environ.get("CURIE_GOLDIMAGE_HTTP_SERVER", None)
    if http_server is None:
      vmm_info = self._metadata.cluster_management_server_info.vmm_info
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      sock.connect((vmm_info.vmm_server, 5985))  # Bogus port, we don't care
      ip_addr = sock.getsockname()[0]
      http_server = "http://%s/goldimages" % ip_addr
    return http_server.strip("/") + "/" + image_name
