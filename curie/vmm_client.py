#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import copy
import logging
import os
import threading
import time

import gflags
import requests

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

    # ServiceInstance object for interacting with VMM
    self._session_id = None

    # Powershell web server address
    self.__webserver_host = os.environ.get("PS_SERVER_ADDRESS",
                                           "hyperv-ps-server")
    self.__webserver_port = os.environ.get("PS_SERVER_PORT", 8888)
    self.__webserver = "http://%s:%s/v1" % (self.__webserver_host,
                                            str(self.__webserver_port))

    # Lock protecting the below fields.
    self._rlock = threading.RLock()

    # Map from Vsphere counter names to vim.PerformanceManager.CounterInfo.
    # self.__vim_perf_counter_by_name_map = None

  def __enter__(self):
    self.login()
    return self

  def __exit__(self, a, b, c):
    self.logout()

  @staticmethod
  def is_nutanix_cvm(vm):
    vm_name = vm.get("name", "")
    return vm_name.startswith("NTNX-") and vm_name.endswith("-CVM")

  def url(self, path):
    """
    Construct the correct URL.

    Args:
      path (str): URL path.

    Returns:
      URL
    """
    if self.is_logged_in:
      return "%s/vmm/%s/%s" % (self.__webserver, self._session_id, path)
    else:
      return "%s/vmm/%s" % (self.__webserver, path)

  def delete(self, path, **kwargs):
    return self.__request(requests.delete, path, **kwargs)

  def get(self, path, **kwargs):
    return self.__request(requests.get, path, **kwargs)

  def head(self, path, **kwargs):
    return self.__request(requests.head, path, **kwargs)

  def options(self, path, **kwargs):
    return self.__request(requests.options, path, **kwargs)

  def patch(self, path, **kwargs):
    return self.__request(requests.patch, path, **kwargs)

  def post(self, path, **kwargs):
    return self.__request(requests.post, path, **kwargs)

  def put(self, path, **kwargs):
    return self.__request(requests.put, path, **kwargs)

  def __request(self, func, path, raise_for_status=False, reconnect_retries=1,
                **kwargs):
    """
    Wrapper to execute HTTP requests.

    Args:
      func (callable): Request method.
      path (str): URL path.
      raise_for_status (bool): If True, raise an exception for non-2XX codes.

    Returns:
      requests.Response

    Raises:
      VmmClientException: If a non-2XX code is received and raise_for_status
        is True.
    """
    def __do_request():
      if path != "task":
        # Avoid printing passwords in the debug log.
        redacted_kwargs = kwargs
        if kwargs and kwargs.get("json") and isinstance(kwargs["json"], dict):
          redacted_kwargs = copy.deepcopy(kwargs)
          for key in redacted_kwargs["json"]:
            if "password" in key.lower():
              redacted_kwargs["json"][key] = r"<REDACTED>"
        log.debug("%s %s %r", func.func_name.upper(), path, redacted_kwargs)

      response = func(self.url(path), **kwargs)

      if path != "task":
        log.debug("RESPONSE '%s' (%d): %r", path, response.status_code,
                  response.content)
      return response

    response = __do_request()
    for attempt_number in xrange(reconnect_retries):
      # 410 code is returned if used session_id session is not available any
      # more. Try to create a new session and rerun the command.
      if response.status_code == 410:
        log.debug("Reconnecting since the session is not available any more "
                  "(%d of %d)", attempt_number + 1, reconnect_retries)
        # Do logout to force new session to be created.
        self.logout()
        self.login()
        response = __do_request()
      else:
        break
    else:
      log.warning("Exceeded maximum reconnection attempts (%d) in "
                  "vmm_client.__request", reconnect_retries)
    if raise_for_status:
      try:
        response.raise_for_status()
      except Exception as exc:
        logging.exception("Error requesting '%s' (%d): %s", path,
                          response.status_code, exc)
        try:
          data = response.json()
          resp_message = data[0]["message"]
          resp_details = data[0]["details"]
          logging.error("Response message: %s", resp_message)
          logging.debug("Response details:\n%s", resp_details)
        except Exception:
          resp_message = None
          logging.error("Failed to decode response: %s", response,
                        exc_info=True)
        msg = str(exc) if not resp_message else "%s: %s" % (exc, resp_message)
        raise VmmClientException(msg)
    return response

  def login(self, max_retries=5, timeout=90.0):
    with self._rlock:
      if self.is_logged_in:
        log.debug("Already logged in (session id: '%s')", self._session_id)
        return
      log.debug("Connecting to host '%s' and VMM server '%s'",
                self.host_address, self.address)
      logon_info = {"host_address": self.host_address,
                    "host_user": self.host_username,
                    "host_password": self.host_password,
                    "vmm_server_address": self.address,
                    "vmm_username": self.username,
                    "vmm_password": self.password}
      for attempt_number in xrange(max_retries):
        response = self.post("logon", json=logon_info, timeout=timeout)
        data = response.json()
        if response.status_code == 200:
          self._session_id = str(data["session_id"])
          log.debug("Successfully logged in. Session ID: '%s'",
                    self._session_id)
          assert self.is_logged_in
          return
        elif response.status_code == 401:
          msg = data[0]
          message = ("Error while logging in to host '%s' and VMM server '%s' "
                     "(%s): %s" % (self.host_address, self.address,
                                   msg["reason"], msg["message"]))
          log.exception(message)
          # Retries won't help a 401 error.
          raise VmmClientException(message)
        else:
          msg = data[0]
          log.exception("Unexpected error while logging in to host '%s' and "
                        "VMM server '%s' (attempt %d/%d, %s): %s",
                        self.host_address, self.address, attempt_number + 1,
                        max_retries, msg["reason"], msg["message"])
          time.sleep(1)
      else:
        raise VmmClientException("Could not log in to host '%s' and VMM "
                                 "server '%s'" % (self.host_address,
                                                  self.address))

  def logout(self):
    with self._rlock:
      if self.is_logged_in:
        log.debug("Disconnecting from VMM host %s", self.host_address)
        try:
          path = "logout"
          self.post(path, timeout=90.0)
        finally:
          self._session_id = None
        log.debug("Disconnected from VMM host %s", self.host_address)
      else:
        log.debug("VMM client already logged out")

  @property
  def is_logged_in(self):
    return self._session_id is not None

  def get_clusters(self, cluster_name=None, cluster_type=None):
    """
    Return clusters.

    Args:
      cluster_name (str): Cluster name filter (RegEx).
      cluster_type (str): Cluster type can be one of: Unknown, VirtualServer,
        HyperV, VMWareVC, VMWareESX, XENServer.
    """
    assert self.is_logged_in, "Must be logged in"
    params = {}
    if cluster_name:
      params["name"] = cluster_name
    if cluster_type:
      params["type"] = cluster_type
    response = self.post("clusters/list",
                         json=params, timeout=90.0, raise_for_status=True)
    return response.json()

  def get_library_shares(self):
    """Returns all library shares on VMM server. """
    assert self.is_logged_in, "Must be logged in"
    params = {}
    response = self.post("library_shares/list",
                         json=params, timeout=90.0, raise_for_status=True)
    return response.json()

  def get_vms(self, cluster_name, vm_input_list=None):
    assert self.is_logged_in, "Must be logged in"
    params = vm_input_list
    path = "cluster/%s/vms/list?refresh=false" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def refresh_vms(self, cluster_name, vm_input_list=None):
    assert self.is_logged_in, "Must be logged in"
    params = vm_input_list
    path = "cluster/%s/vms/refresh" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def get_nodes(self, cluster_name, nodes=None):
    assert self.is_logged_in, "Must be logged in"
    path = "cluster/%s/nodes/list" % cluster_name
    response = self.post(path, json=nodes, timeout=90.0, raise_for_status=True)
    return response.json()

  def nodes_power_state(self, cluster_name, nodes):
    assert self.is_logged_in, "Must be logged in"
    for node in nodes:
      node["username"] = self.host_username
      node["password"] = self.host_password
    params = nodes
    path = "cluster/%s/nodes/shutdown" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def vms_set_power_state_for_vms(self, cluster_name, task_req_list):
    assert self.is_logged_in, "Must be logged in"
    params = task_req_list
    path = "cluster/%s/vms/power_state" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def vms_set_possible_owners_for_vms(self, cluster_name, task_req_list):
    assert self.is_logged_in, "Must be logged in"
    params = task_req_list
    path = "cluster/%s/vms/placement" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def vms_set_snapshot(self, cluster_name, task_req_list):
    assert self.is_logged_in, "Must be logged in"
    params = task_req_list
    path = "cluster/%s/vms/snapshot" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def vm_get_job_status(self, task_id_list):
    assert self.is_logged_in, "Must be logged in"
    params = task_id_list
    response = self.post("task",
                         json=params, timeout=90.0, raise_for_status=True)
    return response.json()

  def vm_stop_job(self, task_id_list):
    assert self.is_logged_in, "Must be logged in"
    params = task_id_list
    response = self.put("task",
                        json=params, timeout=90.0, raise_for_status=True)
    return response.json()

  def vms_delete(self, cluster_name, vm_ids, force_delete=False):
    assert self.is_logged_in, "Must be logged in"
    path = "cluster/%s/vms/delete" % cluster_name
    params = {"vm_ids": vm_ids,
              "force_delete": force_delete}
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def create_vm_template(self, cluster_name, vm_name, vm_host_id,
                         goldimage_disk_path, vm_datastore_path,
                         vmm_network_name, vcpus=1, ram_mb=1024):
    assert self.is_logged_in, "Must be logged in"
    params = {"vm_name": vm_name,
              "vm_host_id": vm_host_id,
              "goldimage_disk_path": goldimage_disk_path,
              "vm_datastore_path": vm_datastore_path,
              "vmm_network_name": vmm_network_name,
              "vcpus": vcpus,
              "ram_mb": ram_mb
              }

    path = "cluster/%s/create_vm_template" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response

  def create_vm(self, cluster_name, vm_template_name, vm_host_map,
                vm_datastore_path, data_disks):
    assert self.is_logged_in, "Must be logged in"
    params = {"vm_template_name": vm_template_name,
              "vm_host_map": vm_host_map,
              "vm_datastore_path": vm_datastore_path,
              "data_disks": data_disks}

    path = "cluster/%s/vms/create" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def clone_vm(self, cluster_name, base_vm_id, vm_name, vm_datastore_path):
    assert self.is_logged_in, "Must be logged in"
    params = {"base_vm_id": base_vm_id,
              "vm_name": vm_name,
              "vm_datastore_path": vm_datastore_path}

    path = "cluster/%s/vms/clone" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def upload_image(self, goldimage_disk_list, goldimage_target_dir,
                   transfer_type=None):
    assert self.is_logged_in, "Must be logged in"
    assert self.library_server_share_path, "Library server share not set"
    assert self.library_server_address, "Library server address not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "vmm_library_server_user": self.library_server_username,
              "vmm_library_server_password": self.library_server_password,
              "vmm_library_server": self.library_server_address,
              "goldimage_disk_list": goldimage_disk_list,
              "goldimage_target_dir": goldimage_target_dir,
              "transfer_type": transfer_type}

    response = self.post("image_upload", json=params,
                         timeout=90.0, raise_for_status=True)
    return response.json()

  def convert_to_template(self, cluster_name, template_name):
    assert self.is_logged_in, "Must be logged in"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "template_name": template_name}

    path = "cluster/%s/vms/convert_to_template" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def migrate_vm(self, cluster_name, vm_host_map, vm_datastore_path):
    assert self.is_logged_in, "Must be logged in"
    params = {"vm_host_map": vm_host_map,
              "vm_datastore_path": vm_datastore_path}

    path = "cluster/%s/vms/migrate" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def migrate_vm_datastore(self, cluster_name, vm_datastore_map):
    assert self.is_logged_in, "Must be logged in"
    params = {"vm_datastore_map": vm_datastore_map}

    path = "cluster/%s/vms/migrate_datastore" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def clean_vmm(self, cluster_name, target_dir, vm_name_prefix):
    assert self.is_logged_in, "Must be logged in"
    assert self.library_server_share_path, "Library server share not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "target_dir": target_dir,
              "vm_name_prefix": vm_name_prefix}

    path = "cluster/%s/cleanup" % cluster_name
    response = self.post(path, json=params, timeout=90.0,
                         raise_for_status=True)
    return response.json()

  def clean_library_server(self, target_dir, vm_name_prefix):
    assert self.is_logged_in, "Must be logged in"
    assert self.library_server_share_path, "Library server share not set"
    assert self.library_server_address, "Library server address not set"
    params = {"vmm_library_server_share": self.library_server_share_path,
              "vmm_library_server_user": self.library_server_username,
              "vmm_library_server_password": self.library_server_password,
              "vmm_library_server": self.library_server_address,
              "target_dir": target_dir,
              "vm_name_prefix": vm_name_prefix}

    response = self.post("library_server_cleanup",
                         json=params, timeout=90.0, raise_for_status=True)
    return response.json()

  def update_library(self, goldimage_disk_path):
    assert self.is_logged_in, "Must be logged in"
    params = {"goldimage_disk_path": goldimage_disk_path}

    response = self.post("update_library", json=params,
                         timeout=90.0, raise_for_status=True)
    return response.json()
