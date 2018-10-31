#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import json
import logging

import gflags

from curie import powershell
from curie.exception import CurieTestException
from curie.log import patch_trace

log = logging.getLogger(__name__)
patch_trace()

FLAGS = gflags.FLAGS


class VmmClientException(CurieTestException):
  # TODO(ryan.hardin): Should this inherit from a different exception type?
  pass


class VmmClient(object):
  def __init__(self, address, username, password,
               host_address=None, host_username=None, host_password=None,
               library_server_address=None, library_server_username=None,
               library_server_password=None, library_server_share_path=None):
    self.address = address
    self.username = username
    self.password = password
    self.host_address = host_address if host_address else address
    self.host_username = host_username if host_username else username
    self.host_password = host_password if host_password else password
    self.library_server_address = library_server_address
    self.library_server_username = library_server_username \
      if library_server_username else username
    self.library_server_password = library_server_password \
      if library_server_password else password
    self.library_server_share_path = library_server_share_path

    self.ps_client = powershell.PsClient(
      address=self.address, username=self.username, password=self.password)

  def __enter__(self):
    # TODO(ryan.hardin): This VmmClient does not require login in a context
    # manager, so __enter__ and __exit__ should be removed.
    return self

  def __exit__(self, a, b, c):
    pass

  @staticmethod
  def is_nutanix_cvm(vm):
    vm_name = vm.get("name", "")
    return vm_name.startswith("NTNX-") and vm_name.endswith("-CVM")

  def get_clusters(self, cluster_name=None, cluster_type=None):
    """
    Return clusters.

    Args:
      cluster_name (str): Cluster name filter (RegEx).
      cluster_type (str): Cluster type can be one of: Unknown, VirtualServer,
        HyperV, VMWareVC, VMWareESX, XENServer.
    """
    params = {}
    if cluster_name:
      params["name"] = cluster_name
    if cluster_type:
      params["type"] = cluster_type
    return self.ps_client.execute(
      "Get-VmmHypervCluster", json_params=json.dumps(params, sort_keys=True))

  def get_vmm_version(self):
    """
    Return ScVMM version.
    """
    return self.ps_client.execute(
      "Get-VmmVersion", json_params=json.dumps(None, sort_keys=True))

  def get_library_shares(self):
    """Returns all library shares on VMM server. """
    return self.ps_client.execute("Get-VmmLibraryShares")

  def get_vms(self, cluster_name, vm_input_list=None):
    if vm_input_list is None:
      vm_input_list = []
    return self.ps_client.execute(
      "Get-VmmVM", num_retries=10, cluster_name=cluster_name,
      json_params=json.dumps(vm_input_list, sort_keys=True))

  def refresh_vms(self, cluster_name, vm_input_list=None):
    if vm_input_list is None:
      vm_input_list = []
    return self.ps_client.execute(
      "Read-VmmVM", cluster_name=cluster_name,
      json_params=json.dumps(vm_input_list, sort_keys=True))

  def get_nodes(self, cluster_name, nodes=None):
    if nodes is None:
      nodes = []
    return self.ps_client.execute(
      "Get-VmmHypervClusterNode", num_retries=10, cluster_name=cluster_name,
      json_params=json.dumps(nodes, sort_keys=True))

  def nodes_power_state(self, cluster_name, nodes=None):
    if nodes is None:
      nodes = []
    for node in nodes:
      node["username"] = self.host_username
      node["password"] = self.host_password
    params = nodes
    return [self.ps_client.execute_async(
      "Set-VmmHypervClusterNodeShutdown", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True)).as_ps_task()]

  def vms_set_power_state_for_vms(self, cluster_name, task_req_list):
    return self.ps_client.execute(
      "Set-VmmVMPowerState", cluster_name=cluster_name,
      json_params=json.dumps(task_req_list, sort_keys=True))

  def vms_set_possible_owners_for_vms(self, cluster_name, task_req_list):
    return self.ps_client.execute(
      "Set-VmmVMPossibleOwners", cluster_name=cluster_name,
      json_params=json.dumps(task_req_list, sort_keys=True))

  def vms_set_snapshot(self, cluster_name, task_req_list):
    return self.ps_client.execute(
      "Set-VmmVMSnapshot", cluster_name=cluster_name,
      json_params=json.dumps(task_req_list, sort_keys=True))

  def vm_get_job_status(self, task_id_list):
    vmm_tasks = []
    ps_tasks = []
    for task in task_id_list:
      if task["task_type"].lower() == "vmm":
        vmm_tasks.append(task)
      elif task["task_type"].lower() == "ps":
        ps_tasks.append(task)
      else:
        raise ValueError("Unknown task type '%s'" % task["task_type"])
    vmm_task_response = []
    if vmm_tasks:
      vmm_task_response = self.ps_client.execute(
        "Get-Task", num_retries=10, json_params=json.dumps(vmm_tasks, sort_keys=True))
    ps_task_response = []
    for ps_task in ps_tasks:
      cmd = self.ps_client.poll(ps_task["task_id"])
      ps_task_response.append(cmd.as_ps_task())
    return vmm_task_response + ps_task_response

  def vm_restart_vmm_job(self, task_id_list):
    vmm_tasks = []

    for task in task_id_list:
      vmm_tasks.append(task)

    vmm_task_response = []
    if vmm_tasks:
      vmm_task_response = self.ps_client.execute(
        "Restart-Task", json_params=json.dumps(vmm_tasks, sort_keys=True))
    return vmm_task_response

  def vm_stop_job(self, task_id_list):
    vmm_tasks = []
    ps_tasks = []
    for task in task_id_list:
      if task["task_type"].lower() == "vmm":
        vmm_tasks.append(task)
      elif task["task_type"].lower() == "ps":
        ps_tasks.append(task)
      else:
        raise ValueError("Unknown task type '%s'" % task["task_type"])
    vmm_task_response = []
    if vmm_tasks:
      vmm_task_response = self.ps_client.execute(
        "Stop-Task", json_params=json.dumps(vmm_tasks, sort_keys=True))
    ps_task_response = []
    for ps_task in ps_tasks:
      cmd = self.ps_client.poll(ps_task["task_id"])
      cmd.terminate()
      ps_task_response.append(cmd.as_ps_task())
    return vmm_task_response + ps_task_response

  def vms_delete(self, cluster_name, vm_ids, force_delete=False):
    params = {"vm_ids": vm_ids, "force_delete": force_delete}
    return self.ps_client.execute(
      "Remove-VmmVM", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def create_vm_template(self, cluster_name, vm_name, vm_host_id,
                         goldimage_disk_path, vm_datastore_path,
                         vmm_network_name, vcpus=1, ram_mb=1024):
    params = {"vm_name": vm_name,
              "vm_host_id": vm_host_id,
              "goldimage_disk_path": goldimage_disk_path,
              "vm_datastore_path": vm_datastore_path,
              "vmm_network_name": vmm_network_name,
              "vcpus": vcpus,
              "ram_mb": ram_mb}
    return self.ps_client.execute(
      "Install-VmmVMTemplate", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def create_vm(self, cluster_name, vm_template_name, vm_host_map,
                vm_datastore_path, data_disks, differencing_disks_path):
    params = {"vm_template_name": vm_template_name,
              "vm_host_map": vm_host_map,
              "vm_datastore_path": vm_datastore_path,
              "data_disks": data_disks,
              "differencing_disks_path": differencing_disks_path}
    return self.ps_client.execute(
      "New-VmmVM", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def create_vm_simple(self, cluster_name, vm_info_maps):
    params = {"vm_info_maps": vm_info_maps}
    return self.ps_client.execute(
      "New-SimpleVmmVM", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def attach_disks(self, cluster_name, vm_disk_maps):
    params = {"vm_disk_maps": vm_disk_maps}
    return self.ps_client.execute(
      "Attach-DisksVM", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def clone_vm(self, cluster_name, base_vm_id, vm_name, vm_datastore_path):
    params = {"base_vm_id": base_vm_id,
              "vm_name": vm_name,
              "vm_datastore_path": vm_datastore_path}
    return self.ps_client.execute(
      "New-VmmVMClone", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def upload_image(self, goldimage_disk_list, goldimage_target_dir, disk_name,
                   transfer_type=None):
    assert self.library_server_share_path, "Library server share not set"
    assert self.library_server_address, "Library server address not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "vmm_library_server_user": self.library_server_username,
              "vmm_library_server_password": self.library_server_password,
              "vmm_library_server": self.library_server_address,
              "goldimage_disk_list": goldimage_disk_list,
              "goldimage_target_dir": goldimage_target_dir,
              "disk_name" : disk_name,
              "transfer_type": transfer_type}

    return [self.ps_client.execute_async(
      "Install-VmmDiskImage",
      json_params=json.dumps(params, sort_keys=True),
      overwriteFiles=False).as_ps_task()]

  def convert_to_template(self, cluster_name, template_name):
    params = {"vmm_library_server_share": self.library_server_share_path,
              "template_name": template_name}
    return self.ps_client.execute(
      "ConvertTo-Template", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def migrate_vm(self, cluster_name, vm_host_map, vm_datastore_path):
    params = {"vm_host_map": vm_host_map,
              "vm_datastore_path": vm_datastore_path}
    return self.ps_client.execute(
      "Move-VmmVM", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def migrate_vm_datastore(self, cluster_name, vm_datastore_map):
    params = {"vm_datastore_map": vm_datastore_map}
    return self.ps_client.execute(
      "Move-VmmVMDatastore", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True))

  def clean_vmm(self, cluster_name, target_dir, vm_datastore_path, vm_name_prefix):
    assert self.library_server_share_path, "Library server share not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "target_dir": target_dir,
              "vm_datastore_path" : vm_datastore_path,
              "vm_name_prefix": vm_name_prefix}

    return [self.ps_client.execute_async(
      "Remove-ClusterVmmObjects", cluster_name=cluster_name,
      json_params=json.dumps(params, sort_keys=True)).as_ps_task()]

  def clean_library_server(self, target_dir, vm_name_prefix):
    assert self.library_server_share_path, "Library server share not set"
    assert self.library_server_address, "Library server address not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "vmm_library_server_user": self.library_server_username,
              "vmm_library_server_password": self.library_server_password,
              "vmm_library_server": self.library_server_address,
              "target_dir": target_dir,
              "vm_name_prefix": vm_name_prefix}

    return [self.ps_client.execute_async(
      "Remove-VmmDiskImages", json_params=json.dumps(params, sort_keys=True)).as_ps_task()]

  def update_library(self, goldimage_disk_path):
    params = {"goldimage_disk_path": goldimage_disk_path}
    return [self.ps_client.execute_async(
      "Update-Library", num_retries=3, json_params=json.dumps(params, sort_keys=True)).as_ps_task()]
