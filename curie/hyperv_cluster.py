#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import os
import random
import socket
import time
from collections import Counter

from curie.cluster import Cluster
from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException, CurieTestException
from curie.goldimage_manager import GoldImageManager
from curie.hyperv_node import HyperVNode
from curie.hyperv_unix_vm import HyperVUnixVM
from curie.log import CHECK
from curie.name_util import NameUtil
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.task import HypervTaskDescriptor, HypervTaskPoller
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
    self.cluster_name = vmm_info.vmm_cluster_name

    # Datastore name of the datastore to deploy test VMs on the cluster.
    self.share_name = vmm_info.vmm_share_name

    # Datastore path of the datastore to deploy test VMs on the cluster.
    self.share_path = vmm_info.vmm_share_path

    # VMM Library Share path
    self.vmm_library_server_share_path = vmm_info.vmm_library_server_share_path

    # VM Network Name
    self.vmm_network_name = vmm_info.vmm_network_name

    # Map of VM UUIDs to host UUIDs on which they should be scheduled.
    self.vm_uuid_host_uuid_map = {}

    # VMM Client
    self.vmm_client = None

    # List of VMs used to store self.vms() result to detect if a VM was moved
    # to some other node.
    self.old_vms_list = []

    # A list of VMs that need to be refreshed in self.vms()
    self.vms_refresh_list = []

  def get_power_state_for_vms(self, vms):
    """See 'Cluster.get_power_state_for_vms' for documentation."""
    vm_ids = set([vm.vm_id() for vm in vms])
    with self._get_vmm_client() as vmm_client:
      vm_id_power_state_map = {vm_json["id"]: vm_json["status"] for vm_json in
                               vmm_client.get_vms(self.cluster_name)
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

    with self._get_vmm_client() as vmm_client:
      # Use the GoldImage manager to get a path to our appropriate goldimage
      goldimage_zip_name = GoldImageManager.get_goldimage_filename(
        goldimage_name, GoldImageManager.FORMAT_VHDX_ZIP,
        GoldImageManager.ARCH_X86_64)

      goldimage_disk_name = GoldImageManager.get_goldimage_filename(
        goldimage_name, GoldImageManager.FORMAT_VHDX,
        GoldImageManager.ARCH_X86_64)

      goldimage_url = self.__get_http_goldimages_address(goldimage_zip_name)

      target_dir = NameUtil.library_server_goldimage_path(self.cluster_name,
                                                          goldimage_name)
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Uploading image '%s'",
        post_task_msg="Uploaded image '%s'",
        create_task_func=vmm_client.upload_image,
        task_func_kwargs={"goldimage_disk_list": [goldimage_url],
                          "goldimage_target_dir": target_dir,
                          "transfer_type": "bits"})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Create GoldImage VM Template.
      goldimage_disk_path = "\\".join([self.vmm_library_server_share_path,
                                       target_dir, goldimage_disk_name])

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Updating SCVMM library path '%s'" % goldimage_disk_path,
        post_task_msg="Updated SCVMM library path '%s'" % goldimage_disk_path,
        create_task_func=vmm_client.update_library,
        task_func_kwargs={"goldimage_disk_path": goldimage_disk_path})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      vmm_client.create_vm_template(
        self.cluster_name, vm_name, node_id, goldimage_disk_path,
        self.share_path, self.vmm_network_name)

      # Create GoldImage VM with default disks and hardware configuration.
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Creating VM '%s' on node '%s'" % (vm_name, node_id),
        post_task_msg="Created VM '%s'" % vm_name,
        create_task_func=vmm_client.create_vm,
        task_func_kwargs={"cluster_name": self.cluster_name,
                          "vm_template_name": None,
                          "vm_host_map": [{"vm_name": vm_name, "node_id": node_id}],
                          "vm_datastore_path": self.share_path,
                          "data_disks": [16, 16, 16, 16, 16, 16],
                          "differencing_disks_path": None})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Search for new VM and return as result.
      for vm in self.vms():
        if vm.vm_name() == vm_name:
          self.vm_uuid_host_uuid_map[vm.vm_id()] = node_id
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

    goldimage_url = self.__get_http_goldimages_address(goldimage_zip_name)

    with self._get_vmm_client() as vmm_client:
      target_dir = NameUtil.library_server_goldimage_path(self.cluster_name,
                                                          vm_name)
      disk_name = vm_name + ".vhdx"
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Uploading image '%s'" % goldimage_url,
        post_task_msg="Uploaded image '%s'" % goldimage_url,
        create_task_func=vmm_client.upload_image,
        task_func_kwargs={"goldimage_disk_list": [goldimage_url],
                          "goldimage_target_dir": target_dir,
                          "disk_name" : disk_name,
                          "transfer_type": "bits"})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      goldimage_disk_path = "\\".join([self.vmm_library_server_share_path,
                                       target_dir, disk_name])

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Updating SCVMM library path '%s'" % goldimage_disk_path,
        post_task_msg="Updated SCVMM library path '%s'" % goldimage_disk_path,
        create_task_func=vmm_client.update_library,
        task_func_kwargs={"goldimage_disk_path": goldimage_disk_path})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      vmm_client.create_vm_template(
        self.cluster_name, vm_name, node_id, goldimage_disk_path,
        self.share_path, self.vmm_network_name, vcpus, ram_mb)
      # TODO (iztokp): Currently create vm template does not start a task. But
      # it could in the future...
      # self.__wait_for_tasks(vmm_client, response)

      # Create GoldImage virtual machine
      if node_id is None:
        node_id = random.choice(self.nodes()).node_id()

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Creating VM '%s' on node '%s'" % (vm_name, node_id),
        post_task_msg="Created VM '%s'" % vm_name,
        create_task_func=vmm_client.create_vm,
        task_func_kwargs={"cluster_name": self.cluster_name,
                          "vm_template_name": vm_name,
                          "vm_host_map": [{"vm_name": vm_name, "node_id": node_id}],
                          "vm_datastore_path": self.share_path,
                          "data_disks": data_disks,
                          "differencing_disks_path": None},
        vmm_restart_count=3)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        poll_secs=10, timeout_secs=900)

      # Search for new VM and return as result.
      new_vm = None
      for vm in self.vms():
        if vm.vm_name() == vm_name:
          if new_vm is None:
            self.vm_uuid_host_uuid_map[vm.vm_id()] = node_id
            vm._node_id = node_id
            new_vm = vm
          else:   # Handle ScVMM bug with duplicate VM objects for one Hyper-V virtual machine
            log.debug("Duplicate VM '%s' found in self.vms()", vm_name)
            # raise CurieTestException("Duplicate VM '%s' found in self.vms()" % vm_name)


      if new_vm:
          return new_vm
      else:
        raise CurieTestException("Created VM '%s' not found in self.vms()" %
                                  vm_name)

  def delete_vms(self, vms, ignore_errors=False, max_parallel_tasks=100):
    if vms:
      with self._get_vmm_client() as vmm_client:
        # VMM sometimes creates duplicate objects for the same VM. We need to
        # list again all the VMs including duplicates so that we delete all VM objects.
        vm_map = []
        for ii, vm in enumerate(vms):
          vm_map.append({"vm_name": vm.vm_name()})
        vm_list_ret = vmm_client.get_vms(self.cluster_name, vm_input_list=vm_map)
        vm_list = map(self.__json_to_curie_vm, vm_list_ret)

        # Track down any duplicate VMs and refresh the state of all duplicate VM objects
        unique_vm_names = []
        duplicate_vm_name_list = []
        for vm in vm_list_ret:
          if vm["name"] in unique_vm_names:
            if vm["name"] not in duplicate_vm_name_list:
              duplicate_vm_name_list.append(vm["name"])
          else:
            unique_vm_names.append(vm["name"])

        duplicate_vms_by_id = []
        duplicate_vm_names_by_id = {}
        for vm in vm_list:
          if vm.vm_name() in duplicate_vm_name_list:
            duplicate_vms_by_id.append({"vm_id": vm.vm_id()})
            duplicate_vm_names_by_id[vm.vm_id()] = vm.vm_name()

        # Refresh VMs that are
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(duplicate_vms_by_id):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Refreshing on VM '%s' (%d/%d)" %
                         (duplicate_vm_names_by_id[task_req_list_chunk["vm_id"]],
                          index + 1, len(duplicate_vm_names_by_id)),
            post_task_msg="Refreshed on VM '%s'" %
                          duplicate_vm_names_by_id[task_req_list_chunk["vm_id"]],
            create_task_func=vmm_client.refresh_vms,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "vm_input_list": [task_req_list_chunk]})
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=max_parallel_tasks, poll_secs=5,
          timeout_secs=len(duplicate_vm_names_by_id) * 900,
          raise_on_failure=False, batch_var="vm_input_list")

        # Delete all VMs (including duplicates)
        task_desc_list = []
        for index, vm in enumerate(vm_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Deleting VM '%s' (%d/%d)" %
                         (vm.vm_name(), index + 1, len(vm_list)),
            post_task_msg="Deleted VM '%s'" % vm.vm_name(),
            create_task_func=vmm_client.vms_delete,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "vm_ids": [vm.vm_id()]},
            vmm_restart_count=1)
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=max_parallel_tasks, poll_secs=10,
          timeout_secs=len(vms) * 900, batch_var="vm_ids", cancel_on_failure=False)

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

    with self._get_vmm_client() as vmm_client:
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
          create_task_func=vmm_client.vms_set_possible_owners_for_vms,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "task_req_list": [task_list_both_chunk]},
          vmm_restart_count=2)
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, timeout_secs=len(vms) * 60,
        batch_var="task_req_list")

      task_desc_list = []
      for index, vm_node_map_chunk in enumerate(vm_node_map):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Migrating VM '%s' to node '%s' (%d/%d)" %
                       (vm_names_by_id[vm_node_map_chunk["vm_id"]],
                        vm_node_map_chunk["node_id"],
                        index + 1, len(vms)),
          post_task_msg="Migrated VM '%s'" %
                        vm_names_by_id[vm_node_map_chunk["vm_id"]],
          create_task_func=vmm_client.migrate_vm,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "vm_host_map": [vm_node_map_chunk],
                            "vm_datastore_path": self.share_path})
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=10,
        timeout_secs=len(vms) * 900, batch_var="vm_host_map")

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
          create_task_func=vmm_client.vms_set_possible_owners_for_vms,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "task_req_list": [task_list_new_chunk]},
          vmm_restart_count=2)
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, timeout_secs=len(vms) * 60,
        batch_var="task_req_list")

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
    with self._get_vmm_client() as vmm_client:
      vms_json = vmm_client.get_vms(self.cluster_name)
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
      with self._get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Enabling HA for VM '%s' by setting all nodes as "
                         "possible owners (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["id"]],
                          index + 1, len(vms)),
            post_task_msg="Enabled HA for VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["id"]],
            create_task_func=vmm_client.vms_set_possible_owners_for_vms,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "task_req_list": [task_req_list_chunk]},
            vmm_restart_count=2)
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60, batch_var="task_req_list")

  def disable_ha_vms(self, vms):
    # (iztokp): All VMs will stay HA, but we will change the possible owners to
    # only one node so VMs will not be able to migrate to other nodes.
    task_req_list = [{"id": vm.vm_id(), "possible_owners": [vm.node_id()]}
                     for vm in vms]
    vm_names_by_id = {vm.vm_id(): vm.vm_name() for vm in vms}
    if task_req_list:
      with self._get_vmm_client() as vmm_client:
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
            create_task_func=vmm_client.vms_set_possible_owners_for_vms,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "task_req_list": [task_req_list_chunk]},
            vmm_restart_count=2)
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60, batch_var="task_req_list")

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
      with self._get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, snapshot_req_list_chunk in enumerate(snapshot_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Creating snapshot for VM '%s' named '%s' (%d/%d)" %
                         (vm_names_by_id[snapshot_req_list_chunk["vm_id"]],
                          snapshot_req_list_chunk["name"],
                          index + 1, len(vms)),
            post_task_msg="Created snapshot for VM '%s'" %
                          vm_names_by_id[snapshot_req_list_chunk["vm_id"]],
            create_task_func=vmm_client.vms_set_snapshot,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "task_req_list": [snapshot_req_list_chunk]})
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=100,
          poll_secs=5, timeout_secs=len(vms) * 60, batch_var="task_req_list")

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

    with self._get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, task_req_list_chunk in enumerate(task_req_list):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Powering on VM '%s' (%d/%d)" %
                       (vm_names_by_id[task_req_list_chunk["vm_id"]],
                        index + 1, len(vms)),
          post_task_msg="Powered on VM '%s'" %
                        vm_names_by_id[task_req_list_chunk["vm_id"]],
          create_task_func=vmm_client.vms_set_power_state_for_vms,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "task_req_list": [task_req_list_chunk]})
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=5,
        timeout_secs=len(vms) * 900, batch_var="task_req_list")

      # Wait for IPs
      timeout_secs -= time.time() - t0
      t0 = time.time()
      while timeout_secs > 0:
        all_available = True
        task_req_list = []
        task_desc_list = []
        vms_refresh_list = []
        vms_list = map(self.__json_to_curie_vm,
                       vmm_client.get_vms(self.cluster_name))
        for vm in vms_list:
          if vm in vms:
            if not vm.vm_ip() or not vm.is_accessible():
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
              create_task_func=vmm_client.refresh_vms,
              task_func_kwargs={"cluster_name": self.cluster_name,
                                "vm_input_list": [task_req_list_chunk]})
            task_desc_list.append(task_desc)
          HypervTaskPoller.execute_parallel_tasks(
            vmm=vmm_client, task_descriptors=task_desc_list,
            max_parallel=max_parallel_tasks, poll_secs=5,
            timeout_secs=len(vms_refresh_list) * 900,
            raise_on_failure=False, batch_var="vm_input_list")
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
      with self._get_vmm_client() as vmm_client:
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Powering off VM '%s' (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["vm_id"]],
                          index + 1, len(vms)),
            post_task_msg="Powered off VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["vm_id"]],
            create_task_func=vmm_client.vms_set_power_state_for_vms,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "task_req_list": [task_req_list_chunk]})
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=max_parallel_tasks, poll_secs=5,
          timeout_secs=len(vms) * 900, batch_var="task_req_list")

  def nodes(self, node_ids=None):
    node_fqdn_map = {}
    with self._get_vmm_client() as vmm_client:
      response = vmm_client.get_nodes(self.cluster_name, node_ids)
      for index, node_data in enumerate(response):
        node_fqdn_map[node_data["fqdn"]] = node_data
    nodes = []
    for index, cluster_node in enumerate(self._metadata.cluster_nodes):
      if cluster_node.id not in node_fqdn_map:
        raise CurieTestException(
          cause=
          "Node with ID '%s' is in the Curie cluster metadata, but not "
          "found in Hyper-V cluster '%s'." %
          (cluster_node.id, self.cluster_name),
          impact=
          "The cluster configuration is invalid.",
          corrective_action=
          "Please check that all of the nodes in the Curie cluster metadata "
          "are part of the Hyper-V cluster. For example, if the cluster "
          "configuration has four nodes, please check that all four nodes are "
          "present in the Hyper-V cluster. If the nodes are managed in "
          "Hyper-V by FQDN, please check that the nodes were also added by "
          "their FQDN to the Curie cluster metadata."
        )
      else:
        nodes.append(self.__json_to_curie_node(node_fqdn_map[cluster_node.id],
                     index))
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

    with self._get_vmm_client() as vmm_client:
      task_desc_list = []
      for index, nodes_req_list_chunk in enumerate(nodes_req_list):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Powering off node '%s' (%d/%d)" %
                       (nodes_req_list_chunk["id"],
                        index + 1, len(nodes)),
          post_task_msg="Powered off node '%s'" % nodes_req_list_chunk["id"],
          create_task_func=vmm_client.nodes_power_state,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "nodes": [nodes_req_list_chunk]})
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list, max_parallel=8,
        poll_secs=10, timeout_secs=len(nodes) * 900, batch_var="nodes")

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

    with self._get_vmm_client() as vmm_client:
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
          create_task_func=vmm_client.migrate_vm_datastore,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "vm_datastore_map": [vm_datastore_map_chunk]})
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=10,
        timeout_secs=len(vms) * 900, batch_var="vm_datastore_map")

  def clone_vms(self, vm, vm_names, node_ids=(), datastore_name=None,
                max_parallel_tasks=None, linked_clone=False):
    if max_parallel_tasks is None:
      max_parallel_tasks = 16
    if not node_ids:
      nodes = self.nodes()
      node_ids = []
      for _ in range(len(vm_names)):
        node_ids.append(random.choice(nodes).node_id())
    if len(vm_names) != len(node_ids):
      raise ValueError("Length of vm_names must be equal to length of "
                       "node_ids (got vm_names=%r, node_ids=%r)" %
                       (vm_names, node_ids))
    # Minimize parallelization due to issue XRAY-1500
    if not linked_clone:
      # Override the max # of parallel tasks to # of cluster nodes * 2
      if max_parallel_tasks > self.node_count():
        max_parallel_tasks = self.node_count() * 2

    vm_node_map = [{"vm_name": vm_name, "node_id": node_id}
                   for vm_name, node_id in zip(vm_names, node_ids)]

    power_on_vms = vm.is_powered_on()
    differencing_disks_path = None
    with self._get_vmm_client() as vmm_client:
      template_name = vm.vm_name()
      # If source VM is not a goldimage, clone it to template so that VMM can
      # clone in parallel.
      if not NameUtil.is_goldimage_vm(vm.vm_name()):
        template_name = NameUtil.template_name_from_vm_name(template_name)
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Creating VM '%s' to be converted to template" %
                       template_name,
          post_task_msg="Created VM '%s'" % template_name,
          create_task_func=vmm_client.clone_vm,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "base_vm_id": vm.vm_id(),
                            "vm_name": template_name,
                            "vm_datastore_path": self.share_path})
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
          timeout_secs=900)

        task_desc = HypervTaskDescriptor(
          pre_task_msg="Converting VM '%s' to a template" % template_name,
          post_task_msg="Converted VM '%s' to a template" % template_name,
          create_task_func=vmm_client.convert_to_template,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "template_name": template_name})
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
          timeout_secs=900)

      # If linked clones are used we must set the path to where differencing
      # disks can be found.
      if (linked_clone):
        differencing_disks_path = self.share_path # put it on self.__share_path
      task_desc_list = []
      for index, vm_node_map_chunk in enumerate(vm_node_map):
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Creating VM '%s' on node '%s' (%d/%d)" %
                       (vm_node_map_chunk["vm_name"],
                        vm_node_map_chunk["node_id"],
                        index + 1, len(vm_names)),
          post_task_msg="Created VM '%s'" % vm_node_map_chunk["vm_name"],
          create_task_func=vmm_client.create_vm,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "vm_template_name": template_name,
                            "vm_host_map": [vm_node_map_chunk],
                            "vm_datastore_path": self.share_path,
                            "data_disks": None,
                            "differencing_disks_path": differencing_disks_path},
          vmm_restart_count=7)
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=5,
        timeout_secs=len(vm_names) * 900, batch_var="vm_host_map")

    new_vms = [vm for vm in self.vms() if vm.vm_name() in vm_names]
    for vm_name, count in Counter([vm.vm_name() for vm in new_vms]).items():
      if count > 1:
        log.debug("Duplicate VM '%s' found.", vm_name)
        #raise CurieTestException("Duplicate VM '%s' found in self.vms()" %
        #                         vm_name)
    #if len(new_vms) != len(vm_names):
    #  raise CurieTestException("Wrong number of cloned virtual machines found "
    #                           "(found %d, expected %d)" %
    #                           (len(new_vms), len(vm_names)))

    if power_on_vms:
      self.power_on_vms(new_vms)

    return new_vms

  def sync_power_state_for_nodes(self, nodes, timeout_secs=None):
    # No-op: It is not known that syncing is required on Hyper-V.
    return self.get_power_state_for_nodes(nodes)

  def update_metadata(self, include_reporting_fields):
    # There's nothing to update for now.
    pass

  def cleanup(self, test_ids=()):
    """Remove all Curie templates and state from this cluster.
    """
    log.info("Cleaning up state on cluster %s", self.cluster_name)
    with self._get_vmm_client() as vmm_client:
      target_dir = NameUtil.library_server_target_path(self.cluster_name)
      task_desc = HypervTaskDescriptor(
        pre_task_msg="Cleaning VMM server",
        post_task_msg="Cleaned VMM server",
        create_task_func=vmm_client.clean_vmm,

        task_func_kwargs={"cluster_name": self.cluster_name,
                          "target_dir": target_dir,
                          "vm_datastore_path": self.share_path,
                          "vm_name_prefix": NameUtil.get_vm_name_prefix()})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)

      task_desc = HypervTaskDescriptor(
        pre_task_msg="Cleaning library server share",
        post_task_msg="Cleaned library server share",
        create_task_func=vmm_client.clean_library_server,
        task_func_kwargs={"target_dir": target_dir,
                          "vm_name_prefix": NameUtil.get_vm_name_prefix()})
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=[task_desc], max_parallel=1,
        timeout_secs=900)
    cluster_software_info = self._metadata.cluster_software_info
    if cluster_software_info.HasField("nutanix_info"):
      client = NutanixRestApiClient.from_proto(
        cluster_software_info.nutanix_info)
      client.cleanup_nutanix_state(test_ids)

  def get_power_state_for_nodes(self, nodes):
    node_ids = set([node.node_id() for node in nodes])
    with self._get_vmm_client() as vmm_client:
      response = vmm_client.get_nodes(self.cluster_name)

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
    with self._get_vmm_client() as vmm_client:
      vm_list_ret = vmm_client.get_vms(self.cluster_name)
      vm_list = self.__filter_duplicate_vms(vm_list_ret)

      # Compare old vs new. Mark VMs that were moved from node to \
      # node and were running to be refreshed since SCVMM does not \
      # update the VM IP automatically.
      if self.old_vms_list:
        for vm in vm_list:
          old_vms_tmp = [old_vm for old_vm in self.old_vms_list \
              if vm.vm_id() == old_vm.vm_id()]
          if len(old_vms_tmp) == 1:
            old_vm = old_vms_tmp[0] # Should be only one VM
            # Check if VM was moved from node to node.
            if vm.node_id() != old_vm.node_id() and \
              vm.is_powered_on() and old_vm.is_powered_on():
              # Check if the same VM is already in the list and update
              # the list
              vms_refresh_list_remove = [vm_refresh for vm_refresh in
                self.vms_refresh_list if vm.vm_id() == vm_refresh.vm_id()]
              for vm_refresh in vms_refresh_list_remove:
                self.vms_refresh_list.remove(vm_refresh)
              self.vms_refresh_list.append(vm)

      # Check if there are any VMs that need to be refreshed.
      if len(self.vms_refresh_list) > 0:
        vm_names_by_id = {}
        task_req_list = []
        for vm in self.vms_refresh_list:
          task_req_list.append({"vm_id": vm.vm_id()})
          vm_names_by_id[vm.vm_id()] = vm.vm_name()

        # Refresh VMs to check for new IP
        task_desc_list = []
        for index, task_req_list_chunk in enumerate(task_req_list):
          task_desc = HypervTaskDescriptor(
            pre_task_msg="Refreshing on VM '%s' (%d/%d)" %
                         (vm_names_by_id[task_req_list_chunk["vm_id"]],
                          index + 1, len(self.vms_refresh_list)),
            post_task_msg="Refreshed on VM '%s'" %
                          vm_names_by_id[task_req_list_chunk["vm_id"]],
            create_task_func=vmm_client.refresh_vms,
            task_func_kwargs={"cluster_name": self.cluster_name,
                              "vm_input_list": [task_req_list_chunk]})
          task_desc_list.append(task_desc)
        HypervTaskPoller.execute_parallel_tasks(
          vmm=vmm_client, task_descriptors=task_desc_list,
          max_parallel=100, poll_secs=5,
          timeout_secs=len(self.vms_refresh_list) * 900,
          raise_on_failure=False, batch_var="vm_input_list")
        vm_list_ret = vmm_client.get_vms(self.cluster_name)
        vm_list = self.__filter_duplicate_vms(vm_list_ret)

        # Remove VMs from 'self.vms_refresh_list' that do not need to be \
        # refreshed any more.
        for vm in vm_list:
          refresh_vms = [refresh_vm for refresh_vm in self.vms_refresh_list
                                   if vm.vm_id() == refresh_vm.vm_id()]
          if len(refresh_vms) > 0:
            # If VM is accessible after it was moved we do not need
            # to refresh it any more
            if vm.vm_ip() and vm.is_accessible():
              self.vms_refresh_list.remove(refresh_vms[0])

      self.old_vms_list = vm_list
      return vm_list

  def __filter_duplicate_vms(self, json_vms):
    "Returns a list of 'HyperVUnixVM' objects."
    # The 'json_vms' input parameter can contain duplicate VM objects due to
    # SCVMM internal issues. These duplicates are filtered out in order to
    # have a single VM object for each VM.
    unique_vm_names = []
    filtered_vm_list_ret = []
    for vm in json_vms:
      if vm["name"] not in unique_vm_names:
        filtered_vm_list_ret.append(vm)
        unique_vm_names.append(vm["name"])
      else:
        log.debug("Duplicate VM '%s' found.", vm)
    return map(self.__json_to_curie_vm, filtered_vm_list_ret)

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

  def _get_vmm_client(self):
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
