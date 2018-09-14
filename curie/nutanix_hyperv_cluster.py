#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import random

from curie.exception import CurieTestException
from curie.hyperv_cluster import HyperVCluster
from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.task import HypervTaskPoller, HypervTaskDescriptor
from curie.util import chunk_iter

log = logging.getLogger(__name__)


class NutanixHypervCluster(NutanixClusterDPMixin, HyperVCluster):
  def __init__(self, cluster_metadata):
    super(NutanixHypervCluster, self).__init__(cluster_metadata)

  def clone_vms(self, vm, vm_names, node_ids=(), datastore_name=None,
                max_parallel_tasks=None, linked_clone=False):
    if linked_clone:
      return super(NutanixHypervCluster, self).clone_vms(
        vm, vm_names, node_ids, datastore_name, max_parallel_tasks,
        linked_clone)

    if max_parallel_tasks is None:
      max_parallel_tasks = 100

    if not node_ids:
      nodes = self.nodes()
      node_ids = []
      for _ in range(len(vm_names)):
        node_ids.append(random.choice(nodes).node_id())
    if len(vm_names) != len(node_ids):
      raise ValueError("Length of vm_names must be equal to length of "
                       "node_ids (got vm_names=%r, node_ids=%r)" %
                       (vm_names, node_ids))

    # Cmdlet for fastclone: https://opengrok.eng.nutanix.com/source/xref/main/hyperint/powershell/NutanixHyperintInterface.psm1#1948

    # This function is based on the cmdlet provided by Nutanix for fastcloning
    # on Hyper-V described here:
    # https://portal.nutanix.com/#/page/docs/details?targetId=HyperV-Admin-AOS-v58:hyp-hyperv-cmdlet-new-vmclone-t.html
    # Sequence:
    #   1. From source VM, gather the disk information(UNC paths)
    #   2. For each desired clone, create a new VM with no disks attached
    #   3. Clone each datadisk from the source VM
    #   4. After cloning disks, attach the disks to a newly created VM with
    #      matching specifications to the source.

    source_vm_disk_paths = []

    with self._get_vmm_client() as vmm_client:
      log.debug("Finding VM '%s'", vm)
      try:
        source_vm = vmm_client.get_vms(
          self.cluster_name, vm_input_list=[{"vm_name": vm.vm_name()}])[0]
        assert source_vm["name"] == vm.vm_name()
      except Exception:
        raise CurieTestException("Source VM named '%s' not found." %
                                 vm.vm_name())
      log.debug("Found clone source VM with configuration:\n"
                "%s", str(source_vm))
      source_cpu_count = source_vm["cpucount"]
      source_memory = source_vm["memory"]
      source_vm_disk_paths = [
        disk_path for disk_path in source_vm["virtualharddiskpaths"]]

      task_desc_list = []
      vm_info_maps = [{"vm_name": vm_name,
                       "node_id": node_id,
                       "vm_datastore_path": self.share_path,
                       "vmm_network_name": self.vmm_network_name,
                       "vcpus": source_cpu_count,
                       "ram_mb": source_memory}
                      for vm_name, node_id in zip(vm_names, node_ids)]
      for index, vm_info_map in enumerate(vm_info_maps):
        # Create VM.
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Creating VM '%s' on node '%s' (%d/%d)" %
                       (vm_info_map["vm_name"], vm_info_map["node_id"],
                        index + 1, len(vm_names)),
          post_task_msg="Created VM '%s'" % vm_info_map["vm_name"],
          create_task_func=vmm_client.create_vm_simple,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "vm_info_maps": [vm_info_map]},
          vmm_restart_count=3)
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=1,
        timeout_secs=len(vm_names) * 900, batch_var="vm_info_maps")

      source_dest_disk_paths = []
      vm_disk_maps = []
      for vm_name in vm_names:
        new_vm_disk_paths = []
        for source_vm_disk_path in source_vm_disk_paths:
          split_src_path = source_vm_disk_path.split("\\")
          src_container = split_src_path[3]
          src_filename = split_src_path[5]
          # Build the source and destination paths for the Nutanix API call.
          source_disk_path = "/" + "/".join(split_src_path[3:])
          dest_disk_path = "/" + "/".join(
            [src_container, vm_name, src_filename])
          source_dest_disk_paths.append(
            (source_disk_path, dest_disk_path))
          # Build the new Hyper-V disk path.
          new_vm_disk_paths.append(
            "\\".join([self.share_path, vm_name, src_filename])
          )
        vm_disk_maps.append({"vm_name": vm_name,
                             "disk_paths": new_vm_disk_paths})

      prism_client = NutanixRestApiClient.from_proto(
        self.metadata().cluster_software_info.nutanix_info)
      for chunk in chunk_iter(source_dest_disk_paths, max_parallel_tasks):
        source_chunk, destination_chunk = zip(*chunk)
        prism_client.vdisk_clone(source_chunk, destination_chunk)
        log.debug("Successfully cloned disks using Nutanix fast clone.\n"
                  "Sources: %r\n"
                  "Destinations: %r" % (source_chunk, destination_chunk))

      task_desc_list = []
      for index, vm_disk_map in enumerate(vm_disk_maps):
        # Attach disks.
        task_desc = HypervTaskDescriptor(
          pre_task_msg="Attaching disks to VM '%s' (%d/%d)" %
                       (vm_disk_map["vm_name"], index + 1, len(vm_disk_maps)),
          post_task_msg="Attached disks to VM '%s'" % vm_disk_map["vm_name"],
          create_task_func=vmm_client.attach_disks,
          task_func_kwargs={"cluster_name": self.cluster_name,
                            "vm_disk_maps": [vm_disk_map]},
          vmm_restart_count=3)
        task_desc_list.append(task_desc)
      HypervTaskPoller.execute_parallel_tasks(
        vmm=vmm_client, task_descriptors=task_desc_list,
        max_parallel=max_parallel_tasks, poll_secs=1,
        timeout_secs=len(vm_names) * 900, batch_var="vm_disk_maps")
