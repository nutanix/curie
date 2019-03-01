#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#
# Example:
#
#   with VsphereVcenter(...) as vcenter:
#     datacenter = vcenter.lookup_datacenter("Foo-DC")
#     cluster = vcenter.lookup_cluster(datacenter, "Foo-cluster")
#     < do something with 'cluster' >
#
# NOTE: clients should not hold on to references to vim objects for too long
# since the underlying vCenter connection can break (e.g., due to connection
# timeouts, etc.). Instead, clients should connect, < perform work >, then
# disconnect. If subsequent interaction with vCenter is needed later, a new
# VsphereVcenter object should be created at that point in time.
#

import calendar
import glob
import logging
import os
import random
import requests
import subprocess
import threading
import time
import traceback
import urllib
from uuid import uuid4
from collections import OrderedDict
from functools import partial

import gflags
from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVim.task import WaitForTask
from pyVmomi import vim, vmodl
from pywbem import WBEMConnection

from curie.curie_error_pb2 import CurieError
from curie.curie_metrics_pb2 import CurieMetric
from curie.exception import CurieException, CurieTestException
from curie.log import patch_trace, CHECK, CHECK_EQ, CHECK_GT
from curie.metrics_util import MetricsUtil
from curie.task import TaskStatus, TaskPoller
from curie.task import VsphereTaskDescriptor, VsphereTask
from curie.util import CurieUtil, get_optional_vim_attr
from curie.vsphere_vm import CURIE_GUEST_OS_TYPE_KEY

log = logging.getLogger(__name__)
patch_trace()

FLAGS = gflags.FLAGS

# Note: this is set to 8 by default to deal with a known limit in vCenter
# when creating clones.
gflags.DEFINE_integer("vsphere_vcenter_max_parallel_tasks",
                      8,
                      "Maximum number of parallel tasks for calls to "
                      "clone_vms, power_on_vms, power_off_vms, delete_vms, "
                      "snapshot_vms, and migrate_vms. Also used by"
                      "power_off_hosts.")


class VsphereVcenter(object):
  "Class for interacting with vCenter for a specific cluster."

  @classmethod
  def from_proto(cls, proto):
    """
    Returns VsphereVcenter instance constructed with arguments from 'proto'.

    Args:
      proto (VcenterInfo|VcenterConnectionParams|ConnectionParams): Proto
        containing connection params for vCenter.

    Raises:
      CurieException<kInvalidParameter> if the proto does not contain the
        required fields.
    """
    fields_list = [
      ("address", "username", "password"),
      ("vcenter_host", "vcenter_user", "vcenter_password")]

    for field_triple in fields_list:
      if all(f in proto.DESCRIPTOR.fields_by_name.keys()
             for f in field_triple):
        break
    else:
      raise CurieException(CurieError.kInvalidParameter,
                            "Missing expected proto fields")

    # pylint doesn't appear to understand for/else.
    # pylint: disable=undefined-loop-variable
    args = [getattr(proto, field_triple[0])]
    args.extend(proto.decrypt_field(f) for f in field_triple[1:])

    return cls(*args)

  @classmethod
  def get_vim_vm_ip_address(cls, vim_vm):
    """
    Attempts to lookup VM IP from 'vim_vm' guest information.

    Returns:
      (str|None) IPv4 address for 'vim_vm' if VM has a primary IP and it is an
        IPv4, else None.
    """
    vim_guest = get_optional_vim_attr(vim_vm.summary, "guest")
    if not vim_guest:
      log.warning("Cannot determine VM IP address, unable to query VM guest "
                  "info for %s", vim_vm.name)
      return None

    ip_address = get_optional_vim_attr(vim_guest, "ipAddress")
    if not ip_address:
      # NB: Keeping this particular log at TRACE as it's common during VM boot
      # and logging is already handled by the VM boot/check ready code.
      log.trace("VM %s currently lacks a primary IP address", vim_vm.name)
      return None

    if not CurieUtil.is_ipv4_address(ip_address):
      log.warning("VM %s currently has a non-IPv4 primary IP %s",
                  vim_vm.name, ip_address)
      return None

    return ip_address

  @classmethod
  def vim_vm_is_nutanix_cvm(cls, vim_vm):
    vim_config = get_optional_vim_attr(vim_vm, "config")
    if vim_config is None:
      return False
    # If vim_config exists, the 'files' attribute is guaranteed to exist.
    vmx_path = get_optional_vim_attr(vim_config.files, "vmPathName")
    return "servicevm" in str(vmx_path).lower()

  def __init__(self, vcenter_host, vcenter_user, vcenter_password):
    # vCenter DNS name or IP address.
    self.__host = vcenter_host

    # vCenter user name.
    self.__user = vcenter_user

    # vCenter password.
    self.__password = vcenter_password

    # ServiceInstance object for interacting with vCenter.
    self.__si = None

    self.__vim_perf_counter_by_name_map = None

  def __enter__(self):
    CHECK(self.__si is None)
    log.trace("Connecting to vCenter %s", self.__host)
    max_retries = 5
    for attempt_num in range(1, max_retries + 1):
      try:
        self.__si = SmartConnectNoSSL(host=self.__host,
                                      user=self.__user,
                                      pwd=self.__password)
        break
      except vim.fault.InvalidLogin as ex:
        # Note: evidently vCenter seems to have a bug where it can transiently
        # fail with InvalidLogin faults even when presented with valid
        # credentials. Retry in the presence of such faults to guard against
        # this.
        if attempt_num == max_retries:
          raise
        else:
          log.exception("Authentication error on vCenter %s (attempt %d/%d)",
                        self.__host, attempt_num, max_retries)
      except Exception:
        if attempt_num == max_retries:
          raise
        else:
          log.exception("Error connecting to vCenter %s (attempt %d/%d)",
                        self.__host, attempt_num, max_retries)
          time.sleep(1)
    assert self.__si
    log.trace("Connected to vCenter %s", self.__host)
    return self

  def __exit__(self, type, value, tb):
    CHECK(self.__si is not None)
    log.trace("Disconnecting from vCenter %s", self.__host)
    Disconnect(self.__si)
    self.__si = None
    log.trace("Disconnected from vCenter %s", self.__host)

  def lookup_datacenter(self, datacenter_name):
    CHECK(self.__si is not None)
    for vim_object in self.walk():
      if (isinstance(vim_object, vim.Datacenter) and
          vim_object.name == datacenter_name):
        log.trace("Found datacenter %s in vCenter %s",
                  datacenter_name, self.__host)
        return vim_object
    log.warning("Datacenter %s not found in vCenter %s",
                datacenter_name, self.__host)
    return None

  def lookup_cluster(self, vim_datacenter, cluster_name):
    CHECK(self.__si is not None)
    for vim_object in self.walk(vim_datacenter):
      if (isinstance(vim_object, vim.ComputeResource) and
          vim_object.name == cluster_name):
        log.trace("Found cluster %s in vCenter %s", cluster_name, self.__host)
        return vim_object
    log.warning("Cluster %s not found in datacenter %s in vCenter %s",
                cluster_name, vim_datacenter.name, self.__host)
    return None

  def lookup_hosts(self, vim_cluster, host_names):
    """
    Look up all the nodes with names 'host_names' on cluster 'vim_cluster'.

    Returns:
      (list<vim.HostSystem>) vim objects for hosts corresponding to
        'host_names'.

    Raises:
      CurieTestException if any node is not found.
    """
    vim_hosts_map = dict((vim_host.name, vim_host) for vim_host
                         in vim_cluster.host)
    vim_hosts = []
    for host_name in host_names:
      if host_name not in vim_hosts_map:
        raise CurieTestException("No host with name %s in cluster %s" %
                                  (host_name, vim_cluster.name))
      vim_hosts.append(vim_hosts_map[host_name])
    return vim_hosts

  def lookup_network(self, vim_datacenter, network_name):
    for vim_datacenter_network in vim_datacenter.network:
      if vim_datacenter_network.name == network_name:
        return vim_datacenter_network
    raise CurieTestException(
      cause="No network with name '%s' found in vSphere datacenter '%s'." %
            (network_name, vim_datacenter.name),
      impact="VMs can not be mapped to the chosen network.",
      corrective_action=
      "Please check that a network with name '%s' is configured for the "
      "datacenter '%s', and is available to each of the ESXi hosts." %
      (network_name, vim_datacenter.name)
    )

  def lookup_datastore(self, vim_cluster, datastore_name, host_name=None):
    CHECK(self.__si is not None)
    vim_datastore_candidate = None
    for vim_datastore in vim_cluster.datastore:
      if vim_datastore.name == datastore_name:
        vim_datastore_candidate = vim_datastore
        log.trace("Found datastore %s in vCenter %s",
                  datastore_name, self.__host)
        break
    if vim_datastore_candidate is None:
      log.warning("Datastore %s not found on cluster %s in vCenter %s",
                  datastore_name, vim_cluster.name, self.__host)
      return None
    if host_name is None:
      vim_hosts = vim_cluster.host
    else:
      for vim_host in vim_cluster.host:
        if vim_host.name == host_name:
          vim_hosts = [vim_host]
          break
      else:
        raise CurieTestException("Host '%s' not found" % host_name)
    # Check that datastore is mounted on all nodes in the cluster.
    for vim_host in vim_hosts:
      datastore_mounted = False
      for vim_datastore in vim_host.datastore:
        if vim_datastore.name == datastore_name:
          datastore_mounted = True
          break
      if not datastore_mounted:
        log.warning("Datastore %s not mounted on host %s",
                    datastore_name, vim_host.name)
        return None
    log.trace("Verified datastore %s mounted on all nodes of cluster %s",
              datastore_name, vim_cluster.name)
    return vim_datastore_candidate

  def lookup_vm(self, vim_cluster, vm_name):
    return self.lookup_vms(vim_cluster, vm_names=[vm_name])[0]

  def lookup_vms(self, vim_cluster, vm_names=None):
    """
    Look up all the VMs with names 'vm_names' on cluster 'vim_cluster'. Returns
    a list of VirtualMachine objects or raises a CurieTestException if any VM
    is not found.
    """
    CHECK(self.__si is not None)
    vim_vm_map = OrderedDict()
    for vim_host in vim_cluster.host:
      for vim_vm in vim_host.vm:
        try:
          if (vim_vm.runtime.connectionState ==
              vim.VirtualMachine.ConnectionState.orphaned):
            log.warning("Skipping orphaned VM '%s: %s'",
                        vim_vm._moId, vim_vm.name)
            continue
          vm_name = vim_vm.name
        except vmodl.fault.ManagedObjectNotFound as err:
          log.debug("Skipping missing VM: %s (%s)", err.msg, err.obj)
        else:
          vim_vm_map[vm_name] = vim_vm
    if vm_names is None:
      return vim_vm_map.values()
    else:
      vim_vms = []
      for vm_name in vm_names:
        if vm_name not in vim_vm_map:
          raise CurieTestException("VM %s does not exist" % vm_name)
        vim_vms.append(vim_vm_map[vm_name])
      return vim_vms

  def walk(self, vim_object=None):
    """
    Perform a depth-first walk of 'vim_object'.

    Note that only vim.Folders and vim.hostFolders will be enumerated, meaning
    that datastores, networks, and VMs will not be yielded. These objects
    exist within a datacenter's datastoreFolder, networkFolder, and vmFolder
    properties.

    Args:
      vim_object: Object in which to walk. Defaults to the root folder.

    Yields:
      object: Each vim object.
    """
    # Default to the root.
    if vim_object is None:
      CHECK(self.__si is not None)
      vim_object = self.__si.content.rootFolder

    yield vim_object

    # Folders and Datacenters have children; Iterate for those types.
    if isinstance(vim_object, vim.Folder):
      for vim_child in vim_object.childEntity:
        for yielded_vim_object in self.walk(vim_child):
          yield yielded_vim_object
    elif isinstance(vim_object, vim.Datacenter):
      for yielded_vim_object in self.walk(vim_object.hostFolder):
        yield yielded_vim_object

  @CurieUtil.log_duration
  def get_vim_perf_counter_by_metric(self, metric):
    """Return a CounterInfo object corresponding to a CurieMetric.

    If no mapping exists for the given metric, None is returned.

    Args:
      metric (curie_metrics_pb2.CurieMetric): CurieMetric being matched.

    Returns:
      (vim.PerformanceManager.CounterInfo)

    """
    # Building the map is expensive, so it's cached.
    if self.__vim_perf_counter_by_name_map is None:
      self.__vim_perf_counter_by_name_map = dict()
      vim_name_to_curie_name = {
        "cpu.usage.percent.average": "CpuUsage.Avg.Percent",
        "cpu.usagemhz.megaHertz.average": "CpuUsage.Avg.Megahertz",
        "mem.usage.percent.average": "MemUsage.Avg.Percent",
        "mem.active.kiloBytes.average": "MemActive.Avg.Kilobytes",
        "net.received.kiloBytesPerSecond.average":
          "NetReceived.Avg.KilobytesPerSecond",
        "net.transmitted.kiloBytesPerSecond.average":
          "NetTransmitted.Avg.KilobytesPerSecond",
        "datastore.numberReadAveraged.number.average":
          "DatastoreRead.Avg.OperationsPerSecond",
        "datastore.numberWriteAveraged.number.average":
          "DatastoreWrite.Avg.OperationsPerSecond",
        "datastore.read.kiloBytesPerSecond.average":
          "DatastoreRead.Avg.KilobytesPerSecond",
        "datastore.write.kiloBytesPerSecond.average":
          "DatastoreWrite.Avg.KilobytesPerSecond",
        "datastore.totalReadLatency.millisecond.average":
          "DatastoreReadLatency.Avg.Milliseconds",
        "datastore.totalWriteLatency.millisecond.average":
          "DatastoreWriteLatency.Avg.Milliseconds",
      }
      vim_perf_manager = self.__si.content.perfManager
      for vim_perf_counter in vim_perf_manager.perfCounter:
        vim_counter_name = ".".join([vim_perf_counter.groupInfo.key,
                                     vim_perf_counter.nameInfo.key,
                                     vim_perf_counter.unitInfo.key,
                                     vim_perf_counter.rollupType])
        # Convert the Vsphere name to a curie name. If the mapping exists,
        # add the PerfCounter to the class-level map.
        curie_name = vim_name_to_curie_name.get(vim_counter_name)
        if curie_name is not None:
          self.__vim_perf_counter_by_name_map[curie_name] = vim_perf_counter
    return self.__vim_perf_counter_by_name_map.get(
      MetricsUtil.metric_name(metric))

  @CurieUtil.log_duration
  def collect_cluster_performance_stats(self,
                                        vim_cluster,
                                        metrics,
                                        vim_perf_query_spec_start_time=None,
                                        vim_perf_query_spec_end_time=None,
                                        vim_perf_query_spec_max_sample=None):
    """Simple interface to collect relevant vSphere cluster statistics.

    The arguments to this method are directly passed through to the
    vim.PerfQuerySpec argument of the same name.

    For more information about VMware PerfQuerySpec, which serves as the basis
    for this interface, visit the VMware documentation here:
    https://www.vmware.com/support/developer/vc-sdk/visdk41pubs/ApiReference/vim.PerformanceManager.QuerySpec.html

    Args:
      vim_cluster (vim.ClusterComputeResource): Cluster from which performance
        statistics will be gathered.
      metrics (list of curie_metrics_pb2.CurieMetric): Metrics to collect
        from the cluster.
      vim_perf_query_spec_start_time (datetime): Optionally specify the oldest
        (exclusive) sample to return. This defaults to the vSphere default
        start time, which is one hour ago.
      vim_perf_query_spec_end_time (datetime): Optionally specify the newest
        (inclusive) sample to return. This defaults to the vSphere default end
        time, which is now.
      vim_perf_query_spec_max_sample (int): Optionally specify the maximum
        number of samples to return. This defaults to the vSphere default,
        which is that "the most recent sample (or samples), unless a time range
        is specified."

    Returns:
      (dict) Dict of list of curie_metrics_pb2.CurieMetric. Top level dict
        keyed by node ID. List contains one entry per metric. If an error
        occurs during collection on a node, None will be inserted instead of a
        list.
    """
    # Use the map to construct a list of perf metric ids to sample.
    counter_id_to_metric = {}
    perf_metric_ids = []
    for metric in metrics:
      # Vsphere uses an empty string to represent the "aggregated" instance.
      if metric.instance == "Aggregated":
        metric.instance = ""
      vim_perf_counter = self.get_vim_perf_counter_by_metric(metric)
      if vim_perf_counter is None:
        log.error("No counter mapping exists for %s",
                  MetricsUtil.metric_name(metric))
      else:
        counter_id_to_metric[vim_perf_counter.key] = metric
        perf_metric_ids.append(vim.PerfMetricId(counterId=vim_perf_counter.key,
                                                instance=metric.instance))
    results_map = {}
    vim_perf_manager = self.__si.content.perfManager
    for vim_host in vim_cluster.host:
      results_map[vim_host.name] = None
      vim_perf_provider_summary = \
        vim_perf_manager.QueryPerfProviderSummary(vim_host)
      vim_perf_query_spec = vim.PerfQuerySpec()
      vim_perf_query_spec.entity = vim_host
      vim_perf_query_spec.intervalId = vim_perf_provider_summary.refreshRate
      vim_perf_query_spec.startTime = vim_perf_query_spec_start_time
      vim_perf_query_spec.endTime = vim_perf_query_spec_end_time
      vim_perf_query_spec.maxSample = vim_perf_query_spec_max_sample
      vim_perf_query_spec.metricId = perf_metric_ids
      try:
        query_perf_start_secs = time.time()
        vim_entity_metrics = vim_perf_manager.QueryPerf([vim_perf_query_spec])
        log.debug("QueryPerf %s finished in %d ms",
                  vim_host.name, (time.time() - query_perf_start_secs) * 1000)
      except vmodl.fault.SecurityError as e:
        log.warning("Authentication error during performance statistics "
                    "collection for %s; it will be retried. %s",
                    vim_host.name, e.msg)
        continue
      except vmodl.fault.SystemError as e:
        # If the node has failed, and an existing connection is severed, this
        # node can be skipped.
        log.warning("Performance statistics collection failed for %s, and it "
                    "will be skipped. %s", vim_host.name, e.msg)
        continue
      except vmodl.fault.InvalidArgument as e:
        log.error("Invalid argument to QueryPerf ([%s]): %s",
                  vim_perf_query_spec, e.msg)
        continue
      else:
        results_map[vim_host.name] = []
        for vim_entity_metric in vim_entity_metrics:
          timestamps = [calendar.timegm(sample_info.timestamp.utctimetuple())
                        for sample_info in vim_entity_metric.sampleInfo]
          for perf_entity_metric_value in vim_entity_metric.value:
            result = CurieMetric()
            result.CopyFrom(
              counter_id_to_metric[perf_entity_metric_value.id.counterId])
            result.instance = perf_entity_metric_value.id.instance
            # Vsphere uses an empty string to represent the "aggregated"
            # instance. Rename it to be more explicit.
            if result.instance == "":
              result.instance = "Aggregated"
            del result.timestamps[:]
            result.timestamps.extend(timestamps)
            del result.values[:]
            result.values.extend(perf_entity_metric_value.value)
            results_map[vim_host.name].append(result)
    return MetricsUtil.sorted_results_map(results_map)

  def fill_cluster_metadata(self,
                            vim_cluster,
                            metadata,
                            include_reporting_fields):
    """
    Fills in the cluster metadata 'metadata' corresponding to the cluster
    'vim_cluster' with additional information (if available).

    Args:
      vim_cluster (vim.ClusterComputeResource): vSphere cluster.
      metadata (CurieSettings.Cluster): cluster metadata.
      include_reporting_fields (bool): fill in fields intended for reporting.

    Raises:
      CurieException: if any of the nodes already specified in 'metadata'
      aren't found in the cluster.
    """
    node_id_metadata_map = {}
    for cluster_node in metadata.cluster_nodes:
      node_id_metadata_map[cluster_node.id] = cluster_node
    # Update metadata for all nodes in the metadata with additional information
    # (if available).
    if include_reporting_fields:
      for vim_host in vim_cluster.host:
        node_id = vim_host.name
        # Skip nodes not in metadata.
        if node_id not in node_id_metadata_map:
          continue
        vim_host_hw_info = get_optional_vim_attr(vim_host, "hardware")
        if vim_host_hw_info is None:
          log.warning("No hardware information for node %s on %s",
                      node_id, vim_cluster.name)
          continue
        node_hw = node_id_metadata_map[node_id].node_hardware
        node_hw.num_cpu_packages = vim_host_hw_info.cpuInfo.numCpuPackages
        node_hw.num_cpu_cores = vim_host_hw_info.cpuInfo.numCpuCores
        node_hw.num_cpu_threads = vim_host_hw_info.cpuInfo.numCpuThreads
        node_hw.cpu_hz = vim_host_hw_info.cpuInfo.hz
        node_hw.memory_size = vim_host_hw_info.memorySize

  def match_node_metadata_to_vcenter(self, vim_cluster, metadata):
    """
    Edits the user metadata to match the vCenter inventory node ID.

    Args:
      vim_cluster (vim.ClusterComputeResource): vSphere cluster.
      metadata (CurieSettings.Cluster): Cluster metadata.

    Raises:
      CurieTestException: If any of the nodes already specified in 'metadata'
        aren't found in the cluster or match is ambiguous.
    """
    vcenter_node_ip_map = {}
    for vim_host in vim_cluster.host:
      vim_host_vnics = vim_host.config.network.vnic
      vcenter_node_ip_map[vim_host.name] = [vnic.spec.ip.ipAddress
                                            for vnic in vim_host_vnics]

    for cluster_node in metadata.cluster_nodes:
      if cluster_node.id in vcenter_node_ip_map:
        continue
      else:
        matching_vim_host_names = [
          vim_host_name
          for vim_host_name, ips in vcenter_node_ip_map.iteritems()
          if cluster_node.id in ips]
        if not matching_vim_host_names:
          raise CurieTestException(
            cause=
            "Node with ID '%s' is in the Curie cluster metadata, but not "
            "found in vSphere cluster '%s'." %
            (cluster_node.id, vim_cluster.name),
            impact=
            "The cluster configuration is invalid.",
            corrective_action=
            "Please check that Curie node with ID '%s' is part of the vSphere "
            "cluster." % cluster_node.id
          )
        elif len(matching_vim_host_names) > 1:
          raise CurieTestException(
            cause=
            "More than one node in the vSphere cluster '%s' matches node ID "
            "'%s'. The matching nodes are: %s." %
            (vim_cluster.name, cluster_node.id,
             ", ".join(matching_vim_host_names)),
            impact=
            "The cluster configuration is invalid.",
            corrective_action=
            "Please check that all node IDs and management IP addresses for "
            "nodes in the Curie cluster metadata are unique for each node in "
            "the vSphere cluster, and that hostnames for these nodes are not "
            "ambiguous."
          )
        else:
          cluster_node.id = matching_vim_host_names[0]


  def upload_vmdk(self, local_path, remote_path, vim_host, vim_datastore):
    """
    Uploads a VMDK to a target datastore via a specific host.

    Args:
      local_path (str): Full path to the VMDK in the local filesystem.
      remote_path (str): Target location within a remote datastore.
      vim_host (vim.Host): The host to use for uploading.
      vim_datastore (vim.Datastore): The datastore to upload the VMDK to and
        where the remote_path is assumed.

    Returns:
      None
    """
    log.info("Uploading VMDK %s to %s in %s",
             local_path, remote_path, vim_datastore.name)
    host = vim_host.name
    params = {
      "dcPath": "ha-datacenter",
      "dsName": vim_datastore.name
    }
    upload_url = "https://%s/folder/%s?%s" % (
      host, remote_path, urllib.urlencode(params))
    sm = self.__si.content.sessionManager
    ticket = sm.AcquireGenericServiceTicket(
      spec=vim.SessionManagerHttpServiceRequestSpec(
        method="PUT", url=upload_url))
    log.debug("Acquired service ticket: %s", ticket)
    headers = {
      "Cookie": "vmware_cgi_ticket=%s" % ticket.id,
      "Content-Type": "application/octet-stream"
    }
    with open(local_path, "rb") as local_vmdk_data:
      log.debug("Uploading %s to %s", local_path, upload_url)
      response = requests.put(upload_url, data=local_vmdk_data,
                              headers=headers, verify=False)
      log.debug("Response: %s", response)
      if response.ok:
        log.info("Completed upload of VMDK %s", local_path)
      else:
        raise CurieException(
          CurieError.kInternalError,
          "Couldn't upload VMDK from %s to %s in datastore %s. Upload "
          "response: %s" %
          (local_path, remote_path, vim_datastore.name, response.content))

  def create_vm(self,
                vm_name,
                vim_datacenter,
                vim_cluster,
                vim_datastore,
                vim_network,
                vim_host,
                os_disk_path,
                ram_mb=1024,
                vcpus=1,
                data_disks=(),
                num_scsi_controllers=4):
    """
    Create a new VM on the cluster with the provided attributes.

    VM's created by this method automatically use VMXNet3 for network
    connectivity and scsi paravirtual controllers for storage. The number
    of controllers is automatically set to 4 and data disks are distributed
    across the controllers evenly in a sequential fashion.

    Args:
      vm_name (str): Name of the VM
      vim_datacenter (vim.datacenter): Data center to place the VM
      vim_cluster (vim.cluster): Cluster to place the VM
      vim_datastore (vim.datastore): Datastore to use for storage
      vim_network (vim.network): Network to connect the VM to
      vim_host (vim.host): Host to place the VM on.
      os_disk_path (str): Full local path to the vmdk to use as the OS disk.
      ram_mb (int): RAM to be allocated to the VM. Defaults to 1GB.
      vcpus (int): Number of vCPUs to allocate to the VM. Defaults to 1.
      data_disks (list): List of additional disk sizes to allocate to the VM in
        gigabytes. e.g. [1,1] will create 2 disks of 1 GB each.
      num_scsi_controllers (int): Number of SCSI controllers.

    Returns:
      vim.vm
    """
    if num_scsi_controllers <= 0:
      raise ValueError("num_scsi_controllers must be at least 1")

    log.debug("Creating a new VM: %s", vm_name)
    os_disk_dir, os_disk_name = os.path.split(os_disk_path)

    def get_additional_scsi_controllers(count, starting_instance_id):
      scsi_controller_strings = []
      for index in xrange(count):
        scsi_controller_string = """
          <Item>
            <rasd:Address>{address}</rasd:Address>
            <rasd:Description>SCSI Controller</rasd:Description>
            <rasd:ElementName>SCSI Controller {address}</rasd:ElementName>
            <rasd:InstanceID>{instance_id}</rasd:InstanceID>
            <rasd:ResourceSubType>{type}</rasd:ResourceSubType>
            <rasd:ResourceType>6</rasd:ResourceType>
          </Item>""".format(address=index + 1,
                            instance_id=starting_instance_id + index,
                            type="VirtualSCSI")
        scsi_controller_strings.append(scsi_controller_string)
      return "".join(scsi_controller_strings)

    ovf_str = """<?xml version="1.0" encoding="UTF-8"?>
    <!--Generated by Nutanix X-Ray-->
    <Envelope xmlns="http://schemas.dmtf.org/ovf/envelope/1"
              xmlns:cim="http://schemas.dmtf.org/wbem/wscim/1/common"
              xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1"
              xmlns:rasd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData"
              xmlns:vmw="http://www.vmware.com/schema/ovf"
              xmlns:vssd="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_VirtualSystemSettingData"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
      <References>
        <File ovf:href="{vmdk_name}" ovf:id="os_vmdk" ovf:size="{vmdk_size}"/>
      </References>
      <DiskSection>
        <Info>Virtual disk information</Info>
        <Disk ovf:capacity="{os_disk_capacity}"
              ovf:capacityAllocationUnits="byte * 2^30"
              ovf:diskId="os_disk"
              ovf:fileRef="os_vmdk"
              ovf:format="http://www.vmware.com/interfaces/specifications/vmdk.html#streamOptimized"/>
      </DiskSection>
      <NetworkSection>
        <Info>The list of logical networks</Info>
        <Network ovf:name="OVF_Network">
          <Description>OVF Network</Description>
        </Network>
      </NetworkSection>
      <VirtualSystem ovf:id="xray-base">
        <Info>A virtual machine</Info>
        <Name>xray-base</Name>
        <OperatingSystemSection ovf:id="123" vmw:osType="{os_type}">
          <Info>The kind of installed guest operating system</Info>
        </OperatingSystemSection>
        <VirtualHardwareSection>
          <Info>Virtual hardware requirements</Info>
          <System>
            <vssd:ElementName>Virtual Hardware Family</vssd:ElementName>
            <vssd:InstanceID>0</vssd:InstanceID>
            <vssd:VirtualSystemIdentifier>xray-base</vssd:VirtualSystemIdentifier>
            <vssd:VirtualSystemType>{version}</vssd:VirtualSystemType>
          </System>
          <Item>
            <rasd:AllocationUnits>hertz * 10^6</rasd:AllocationUnits>
            <rasd:Description>Number of Virtual CPUs</rasd:Description>
            <rasd:ElementName>{vcpus} virtual CPU(s)</rasd:ElementName>
            <rasd:InstanceID>1</rasd:InstanceID>
            <rasd:ResourceType>3</rasd:ResourceType>
            <rasd:VirtualQuantity>{vcpus}</rasd:VirtualQuantity>
          </Item>
          <Item>
            <rasd:AllocationUnits>byte * 2^20</rasd:AllocationUnits>
            <rasd:Description>Memory Size</rasd:Description>
            <rasd:ElementName>{ram_mb}MB of memory</rasd:ElementName>
            <rasd:InstanceID>2</rasd:InstanceID>
            <rasd:ResourceType>4</rasd:ResourceType>
            <rasd:VirtualQuantity>{ram_mb}</rasd:VirtualQuantity>
          </Item>
          <Item ovf:required="false">
            <rasd:AutomaticAllocation>false</rasd:AutomaticAllocation>
            <rasd:ElementName>VirtualVideoCard</rasd:ElementName>
            <rasd:InstanceID>3</rasd:InstanceID>
            <rasd:ResourceType>24</rasd:ResourceType>
          </Item>
          <Item ovf:required="false">
            <rasd:AutomaticAllocation>false</rasd:AutomaticAllocation>
            <rasd:ElementName>VirtualVMCIDevice</rasd:ElementName>
            <rasd:InstanceID>4</rasd:InstanceID>
            <rasd:ResourceSubType>vmware.vmci</rasd:ResourceSubType>
            <rasd:ResourceType>1</rasd:ResourceType>
          </Item>
          <Item>
            <rasd:AddressOnParent>7</rasd:AddressOnParent>
            <rasd:AutomaticAllocation>true</rasd:AutomaticAllocation>
            <rasd:Connection>OVF_Network</rasd:Connection>
            <rasd:Description>VmxNet3 ethernet adapter on &quot;OVF_Network&quot;</rasd:Description>
            <rasd:ElementName>Ethernet 1</rasd:ElementName>
            <rasd:InstanceID>5</rasd:InstanceID>
            <rasd:ResourceSubType>VmxNet3</rasd:ResourceSubType>
            <rasd:ResourceType>10</rasd:ResourceType>
          </Item>
          <Item>
            <rasd:Address>0</rasd:Address>
            <rasd:Description>IDE Controller</rasd:Description>
            <rasd:ElementName>VirtualIDEController 0</rasd:ElementName>
            <rasd:InstanceID>6</rasd:InstanceID>
            <rasd:ResourceType>5</rasd:ResourceType>
          </Item>
          <Item ovf:required="false">
            <rasd:AddressOnParent>0</rasd:AddressOnParent>
            <rasd:AutomaticAllocation>false</rasd:AutomaticAllocation>
            <rasd:ElementName>CD-ROM 1</rasd:ElementName>
            <rasd:InstanceID>7</rasd:InstanceID>
            <rasd:Parent>6</rasd:Parent>
            <rasd:ResourceSubType>vmware.cdrom.remotepassthrough</rasd:ResourceSubType>
            <rasd:ResourceType>15</rasd:ResourceType>
          </Item>
          <Item>
            <rasd:Address>0</rasd:Address>
            <rasd:Description>SCSI Controller</rasd:Description>
            <rasd:ElementName>SCSI Controller 0</rasd:ElementName>
            <rasd:InstanceID>8</rasd:InstanceID>
            <rasd:ResourceSubType>VirtualSCSI</rasd:ResourceSubType>
            <rasd:ResourceType>6</rasd:ResourceType>
          </Item>
          <Item>
            <rasd:AddressOnParent>0</rasd:AddressOnParent>
            <rasd:ElementName>Hard Disk 1</rasd:ElementName>
            <rasd:HostResource>ovf:/disk/os_disk</rasd:HostResource>
            <rasd:InstanceID>9</rasd:InstanceID>
            <rasd:Parent>8</rasd:Parent>
            <rasd:ResourceType>17</rasd:ResourceType>
          </Item>{additional_scsi_controllers}
          <vmw:Config ovf:required="false" vmw:key="cpuHotAddEnabled" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="cpuHotRemoveEnabled" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="firmware" vmw:value="bios"/>
          <vmw:Config ovf:required="false" vmw:key="virtualICH7MPresent" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="virtualSMCPresent" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="memoryHotAddEnabled" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="nestedHVEnabled" vmw:value="false"/>
          <vmw:Config ovf:required="false" vmw:key="powerOpInfo.powerOffType" vmw:value="soft"/>
          <vmw:Config ovf:required="false" vmw:key="powerOpInfo.resetType" vmw:value="soft"/>
          <vmw:Config ovf:required="false" vmw:key="powerOpInfo.standbyAction" vmw:value="checkpoint"/>
          <vmw:Config ovf:required="false" vmw:key="powerOpInfo.suspendType" vmw:value="soft"/>
          <vmw:Config ovf:required="false" vmw:key="tools.afterPowerOn" vmw:value="true"/>
          <vmw:Config ovf:required="false" vmw:key="tools.afterResume" vmw:value="true"/>
          <vmw:Config ovf:required="false" vmw:key="tools.beforeGuestShutdown" vmw:value="true"/>
          <vmw:Config ovf:required="false" vmw:key="tools.beforeGuestStandby" vmw:value="true"/>
          <vmw:Config ovf:required="false" vmw:key="tools.toolsUpgradePolicy" vmw:value="upgradeAtPowerCycle"/>
        </VirtualHardwareSection>
        <vmw:BootOrderSection vmw:type="cdrom">
          <Info>Virtual hardware device boot order</Info>
        </vmw:BootOrderSection>
      </VirtualSystem>
    </Envelope>""".format(
      os_disk_capacity=32,
      vmdk_name=os_disk_name,
      vmdk_size=os.path.getsize(os_disk_path),
      os_type="ubuntu64Guest",
      version="vmx-09",
      ram_mb=ram_mb,
      vcpus=vcpus,
      additional_scsi_controllers=get_additional_scsi_controllers(
        count=num_scsi_controllers - 1,
        starting_instance_id=10)
    )
    # TODO (ryan.hardin): ^ Guest ID might need to be different here.

    # Disable time synchronization of the VM with the host via VMware tools
    # https://pubs.vmware.com/vsphere-50/index.jsp?topic=%2Fcom.vmware.vmtools.install.doc%2FGUID-678DF43E-5B20-41A6-B252-F2E13D1C1C49.html
    extra_config = {"tools.syncTime": "FALSE",
                    "time.synchronize.continue": "FALSE",
                    "time.synchronize.resume.disk": "FALSE",
                    "time.synchronize.shrink": "FALSE",
                    "time.synchronize.tools.startup": "FALSE",
                    }

    # Using import_ovf here due to XRAY-1056; VMs created manually by attaching
    # the OS disk did not boot on VSAN.
    vm = self.import_ovf(ovf_str=ovf_str,
                         vmdk_dir=os_disk_dir,
                         vim_datacenter=vim_datacenter,
                         vim_cluster=vim_cluster,
                         vim_datastore=vim_datastore,
                         vim_network=vim_network,
                         vm_name=vm_name,
                         host_name=vim_host.name,
                         guest_os_type="unix",
                         extra_config=extra_config)
    # TODO (ryan.hardin): ^ Guest OS type might need to be different here.

    # Create a dictionary to keep track of how many disks have been added.
    controllers = []
    for device in vm.config.hardware.device:
      if isinstance(device, vim.vm.device.VirtualSCSIController):
        controllers.append({
          "controller": device,
          "attached_disks": 0
        })

    # Account for the OS disk.
    controllers[0]["attached_disks"] = 1

    # Create and attach the additional data disks
    disk_changes = []
    for num, size_gb in enumerate(data_disks):
      # Add one to put first data disk on different controller than OS disk.
      controller_num = (1 + num) % num_scsi_controllers

      unitNumber = controllers[controller_num]["attached_disks"]
      # Position 7 is reserved
      if unitNumber >= 7:
        unitNumber += 1
      if unitNumber >= 16:
        raise CurieException(CurieError.kInternalError,
                              "Attempting to attach too many disks.")

      disk_spec = vim.vm.device.VirtualDiskSpec(
        fileOperation="create",
        operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
        device=vim.vm.device.VirtualDisk(
          backing=vim.vm.device.VirtualDisk.FlatVer2BackingInfo(
            diskMode="persistent",
            thinProvisioned=True
          ),
          unitNumber=unitNumber,
          capacityInKB=(size_gb * 1024 * 1024),
          controllerKey=controllers[controller_num]["controller"].key
        )
      )
      controllers[controller_num]["attached_disks"] += 1
      disk_changes.append(disk_spec)

    # Modify the VM
    spec = vim.vm.ConfigSpec()
    spec.deviceChange = disk_changes
    modify_task_func = partial(vm.ReconfigVM_Task, spec=spec)
    pre_task_msg = "Adding data disks to VM '%s'" % vm_name
    post_task_msg = "Finished adding data disks to VM '%s'" % vm_name
    task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                      post_task_msg=post_task_msg,
                                      create_task_func=modify_task_func)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc,
                                      max_parallel=1, timeout_secs=600)
    return vm

  def import_vm(self,
                goldimages_directory,
                goldimage_name,
                vim_datacenter,
                vim_cluster,
                vim_datastore,
                vim_network,
                vm_name,
                host_name=None):
    """
    Creates a VM with name 'vm_name' from the specified gold image. If
    'host_name' is specified, the VM is created on that node, else a random
    node is selected. The VM will be created on the datastore 'vim_datastore'
    on the cluster 'vim_cluster' in the VM folder for 'vim_datastore'. If
    'vim_network' is specified, use that network for all network interfaces for
    the VM.
    """
    CHECK(self.__si is not None)
    # Read the goldimage OVF descriptor and compute goldimage vmdk pathnames.
    goldimage_dir = "%s/%s" % (goldimages_directory, goldimage_name)
    if not os.path.exists(goldimage_dir):
      raise CurieException(CurieError.kInternalError,
                            "Goldimage is missing: %s" % goldimage_dir)
    paths = glob.glob("%s/*.ovf" % goldimage_dir)
    CHECK_EQ(len(paths), 1, msg=paths)
    ovf_descriptor = open(paths[0]).read()
    # Determine the guest OS type. The guest OS type is encoded in the first
    # component of the goldimage name (<guest_os_type>-<...>).
    guest_os_type = goldimage_name.split("-")[0]
    CHECK(guest_os_type in ["unix"], msg=guest_os_type)

    return self.import_ovf(ovf_str=ovf_descriptor,
                           vmdk_dir=goldimage_dir,
                           vim_datacenter=vim_datacenter,
                           vim_cluster=vim_cluster,
                           vim_datastore=vim_datastore,
                           vim_network=vim_network,
                           vm_name=vm_name,
                           host_name=host_name,
                           guest_os_type=guest_os_type)

  def import_ovf(self,
                 ovf_str,
                 vmdk_dir,
                 vim_datacenter,
                 vim_cluster,
                 vim_datastore,
                 vim_network,
                 vm_name,
                 host_name=None,
                 guest_os_type="unix",
                 extra_config=None):
    """
    Creates a VM with name 'vm_name' from the given OVF string. If
    'host_name' is specified, the VM is created on that node, else a random
    node is selected. The VM will be created on the datastore 'vim_datastore'
    on the cluster 'vim_cluster' in the VM folder for 'vim_datastore'. If
    'vim_network' is specified, use that network for all network interfaces for
    the VM.
    """
    if extra_config is None:
      extra_config = {}
    CHECK(self.__si is not None)
    # Create the VM and upload the vmdk files for its virtual disks.
    vim_import_spec_params = vim.OvfManager.CreateImportSpecParams(
      diskProvisioning="thin", entityName=vm_name)
    if vim_network is not None:
      # We know what network we want to map to, so parse the OVF file and
      # acquire the network information for mapping.
      descriptor_result = self.__si.content.ovfManager.ParseDescriptor(
        ovf_str, pdp=vim.OvfManager.ParseDescriptorParams())
      if len(set(n.name for n in descriptor_result.network)) > 1:
        raise CurieTestException("More than one network included in OVF for "
                                  "%s." % vm_name)
      network_name = descriptor_result.network[0].name
      log.info("Mapping OVF network %s to cluster network %s",
               network_name, vim_network.name)
      net_mapping = vim.OvfManager.NetworkMapping(name=network_name,
                                                  network=vim_network)
      vim_import_spec_params.networkMapping = [net_mapping]
    else:
      log.info("No network specified in import request. Using default import "
               "behavior for network mapping.")
    vim_resource_pool = vim_cluster.resourcePool

    vim_import_spec_result = self.__si.content.ovfManager.CreateImportSpec(
      ovf_str, vim_resource_pool, vim_datastore,
      vim_import_spec_params)
    vim_import_spec = vim_import_spec_result.importSpec
    import_spec_warnings = get_optional_vim_attr(vim_import_spec_result,
                                                 "warning")
    import_spec_errors = get_optional_vim_attr(vim_import_spec_result,
                                               "error")
    if not vim_import_spec or import_spec_errors:
      raise CurieTestException(
        "Unable to create import spec for goldimage OVF: %s" %
        import_spec_errors)
    if not get_optional_vim_attr(vim_import_spec_result, "fileItem"):
      raise CurieTestException(
        "Malformed result for CreateImportSpec, missing expected attribute "
        "'fileItem'. Error: %s" % get_optional_vim_attr(
          vim_import_spec_result, "error"))
    if import_spec_warnings:
      log.warning("CreateImportSpec reports warnings: %s",
                  import_spec_warnings)

    if host_name is not None:
      vim_hosts = [vim_host for vim_host in vim_cluster.host
                   if vim_host.name == host_name]
      if len(vim_hosts) == 0:
        raise CurieTestException("No host with name %s in cluster %s" %
                                  (host_name, vim_cluster.name))
      CHECK_EQ(len(vim_hosts), 1, msg=host_name)
      vim_host = vim_hosts[0]
    else:
      host_idx = random.randint(0, len(vim_cluster.host) - 1)
      vim_host = vim_cluster.host[host_idx]
    vim_lease = vim_resource_pool.ImportVApp(vim_import_spec,
                                             folder=vim_datacenter.vmFolder,
                                             host=vim_host)
    vmdk_paths = ["%s/%s" % (vmdk_dir, file_item.path)
                  for file_item in vim_import_spec_result.fileItem]
    self.__upload_vmdks(vim_lease, vmdk_paths)
    vim_vm = self.lookup_vm(vim_cluster, vm_name)

    # Set the guest OS type extraConfig key/val pair.
    extra_config[CURIE_GUEST_OS_TYPE_KEY] = guest_os_type

    vim_config_spec = vim.vm.ConfigSpec()
    for key, value in extra_config.iteritems():
      option_value = vim.option.OptionValue(key=key, value=value)
      vim_config_spec.extraConfig.append(option_value)
    create_task_func = partial(vim_vm.ReconfigVM_Task, vim_config_spec)
    pre_task_msg = "Creating VM %s" % vm_name
    post_task_msg = "Created VM %s" % vm_name
    task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                      post_task_msg=post_task_msg,
                                      create_task_func=create_task_func)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc,
                                      max_parallel=1, timeout_secs=600)
    return vim_vm

  def clone_vms(
      self,
      vim_vm,
      vim_datacenter,
      vim_cluster,
      vim_datastore,
      vm_names,
      host_names=(),
      max_parallel_tasks=None,
      linked_clone=False):
    """
    Clones 'vm' and creates the VMs with names 'vm_names'.

    Args:
      vim_vm (vim.VirtualMachine): Base VM to clone.
      vim_datacenter (vim.Datacenter): Datacenter containing 'vim_cluster'.
      vim_cluster (vim.ComputeResource): Cluster hosting 'vim_vm'.
      vim_datastore (vim.Datastore): DS where clone files will reside.
      vm_names (list<strings>): List of names to assign the clones.
      host_names (list<str>): Optional. If provided, a list of host_ids which
        must be the same length as 'vm_names'. When present, 'vm_names[xx]'
        will be cloned to 'host_name[xx]'. If not specified, VMs will be
        cloned to a random host on cluster.
      max_parallel_tasks (int): The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
      linked_clone (bool): Whether or not the clones should be "normal" full
        clones or linked clones.

    Returns:
      List of cloned VMs.

    Raises:
      CurieTestException if fail to complete clones within the timeout.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK(self.__si is not None)
    # Compute the hosts to place the cloned VMs.
    if len(host_names) > 0:
      vim_hosts = self.lookup_hosts(vim_cluster, host_names)
    else:
      vim_hosts = []
      for _ in xrange(len(vm_names)):
        host_idx = random.randint(0, len(vim_cluster.host) - 1)
        vim_host = vim_cluster.host[host_idx]
        vim_hosts.append(vim_host)
    if linked_clone:
      base_snap_name = "clone_base_snap_%s" % uuid4()
      self.snapshot_vms(
        vim_cluster=vim_cluster,
        vm_names=[vim_vm.name],
        snapshot_names=[base_snap_name])
      linked_clone_snapshot = \
        self.find_snapshot_vm(vim_vm, base_snap_name).snapshot
      assert(linked_clone_snapshot is not None)
    else:
      linked_clone_snapshot = None

    # Clone the VMs with bounded parallelism.
    task_desc_list = []
    for xx, (vm_name, vim_host) in enumerate(zip(vm_names, vim_hosts)):
      create_task_func = \
        partial(self.__clone_vm,
                vim_vm, vm_name, vim_datacenter, vim_datastore, vim_host,
                linked_clone_snapshot=linked_clone_snapshot)
      pre_task_msg = "Creating clone %d/%d (%s) of %s" % \
        (xx + 1, len(vm_names), vm_name, vim_vm.name)
      post_task_msg = "Clone %s created" % vm_name
      task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                        post_task_msg=post_task_msg,
                                        create_task_func=create_task_func)
      task_desc_list.append(task_desc)
    clone_vm_results = TaskPoller.execute_parallel_tasks(
      task_descriptors=task_desc_list, max_parallel=max_parallel_tasks,
      timeout_secs=len(vm_names) * 900)

    # TODO (jklein): Harmonize the handling of list/map returns.
    ret = []
    for task_desc in task_desc_list:
      if task_desc.state.status != TaskStatus.kSucceeded:
        raise CurieTestException("VM clone failed: %s" % task_desc)
      ret.append(clone_vm_results[task_desc.task_id].result)

    return ret

  def power_on_vms(self, vim_cluster, vm_names, max_parallel_tasks=None):
    """Powers on VMs and verifies that an IP address is assigned.

    Args:
      vim_cluster: Cluster object containing the vms.
      vm_names: List of VM names to power on.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: VMs fail to acquire an IP address within the
      timeout.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK(self.__si is not None)
    vim_vms = self.lookup_vms(vim_cluster, vm_names)
    # Power on the VMs with bounded parallelism.
    task_desc_list = []
    for xx, vim_vm in enumerate(vim_vms):
      vm_name = vm_names[xx]
      power_state = vim_vm.runtime.powerState
      if power_state != vim.VirtualMachine.PowerState.poweredOn:
        # We don't support VMs that are in a suspended state, so the VM must be
        # powered off.
        CHECK_EQ(power_state, vim.VirtualMachine.PowerState.poweredOff)
        create_task_func = vim_vm.PowerOnVM_Task
        is_task_complete_func = partial(self.__vm_has_ip, vim_vms[xx])
        pre_task_msg = "Powering on VM %d/%d %s" % \
          (xx + 1, len(vm_names), vm_name)
        post_task_msg = "Powered on VM %d/%d %s" % \
          (xx + 1, len(vm_names), vm_name)
        task_desc = VsphereTaskDescriptor(
          pre_task_msg=pre_task_msg, post_task_msg=post_task_msg,
          create_task_func=create_task_func,
          is_complete_func=is_task_complete_func)
        task_desc_list.append(task_desc)
      else:
        log.info("VM %d/%d %s already powered on",
                 xx + 1, len(vm_names), vm_name)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vm_names) * 600)

  def power_off_vms(self, vim_cluster, vm_names, max_parallel_tasks=None):
    """Powers off VMs.

    Args:
      vim_cluster: Cluster object containing the vms.
      vm_names: List of VM names to power on.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: VMs fail to power off within the timeout.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK(self.__si is not None)
    vim_vms = self.lookup_vms(vim_cluster, vm_names)
    # Power off the VMs with bounded parallelism.
    task_desc_list = []
    for xx, (vm_name, vim_vm) in enumerate(zip(vm_names, vim_vms)):
      power_state = vim_vm.runtime.powerState
      if power_state != vim.VirtualMachine.PowerState.poweredOff:
        # We don't support VMs that are in a suspended state, so the VM must be
        # powered on.
        CHECK_EQ(power_state, vim.VirtualMachine.PowerState.poweredOn)
        create_task_func = vim_vm.PowerOffVM_Task
        pre_task_msg = "Powering off VM %d/%d %s" % \
          (xx + 1, len(vm_names), vm_name)
        post_task_msg = "Powered off VM %d/%d %s" % \
          (xx + 1, len(vm_names), vm_name)
        task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                          post_task_msg=post_task_msg,
                                          create_task_func=create_task_func)
        task_desc_list.append(task_desc)
      else:
        log.info("VM %d/%d %s already powered off",
                 xx + 1, len(vm_names), vm_name)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vm_names) * 600)

  def delete_vms(self, vim_cluster, vm_names, ignore_errors=False,
                 max_parallel_tasks=None):
    """Delete VMs.

    Args:
      vim_cluster: Cluster object containing the vms.
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
    CHECK(self.__si is not None)
    if ignore_errors:
      desired_state = None
    else:
      desired_state = TaskStatus.kSucceeded
    vim_vms = self.lookup_vms(vim_cluster, vm_names)
    # Cancel any tasks on the VMs since we're about to destroy the VMs anyway.
    # If we don't do this, the destroy tasks will potentially have to wait for
    # existing tasks on the VMs to complete.
    self.__cancel_entity_tasks(vim_cluster, vm_names)
    # Delete the VMs with bounded parallelism.
    task_desc_list = []
    for xx, (vm_name, vim_vm) in enumerate(zip(vm_names, vim_vms)):
      power_state = vim_vm.runtime.powerState
      # A VM can only be destroyed if it's already powered off.
      if power_state != vim.VirtualMachine.PowerState.poweredOff:
        error_msg = "Cannot destroy VM %s in power state %s" % \
          (vm_name, power_state)
        raise CurieTestException(error_msg)
      create_task_func = vim_vm.Destroy_Task
      pre_task_msg = "Destroying VM %d/%d %s" % \
        (xx + 1, len(vm_names), vm_name)
      post_task_msg = "Destroyed VM %d/%d %s" % \
        (xx + 1, len(vm_names), vm_name)
      task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                        post_task_msg=post_task_msg,
                                        create_task_func=create_task_func,
                                        desired_state=desired_state)
      task_desc_list.append(task_desc)
    # Note: we use a larger per-VM timeout here for destroying since destroying
    # goldimage VMs sometimes seems to take quite a bit longer than normal VMs.
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vm_names) * 600)

  def find_datastore_paths(self, pattern, vim_datastore, recursive=False):
    """
    Find paths in a datastore that matches the provided pattern.

    Args:
      pattern (str): Pattern to match. Example: __curie_goldimage*
      vim_datastore (vim_datastore object): Datastore to search.
      recursive (bool): If true, also search subfolders.

    Returns: (list) of paths in the datastore.
    """
    spec = vim.host.DatastoreBrowser.SearchSpec(
      query=[vim.host.DatastoreBrowser.FolderQuery()],
      matchPattern=pattern)
    if recursive:
      task = vim_datastore.browser.SearchSubFolders(
        "[%s]" % vim_datastore.name, spec)
      WaitForTask(task)
      search_results = task.info.result
    else:
      task = vim_datastore.browser.Search("[%s]" % vim_datastore.name, spec)
      WaitForTask(task)
      search_results = [task.info.result]

    paths = []
    for search_result in search_results:
      for fileinfo in search_result.file:
        paths.append(search_result.folderPath + fileinfo.path)
    return paths

  def delete_datastore_folder_path(self, folder_path, vim_datacenter):
    fm = self.__si.content.fileManager
    delete_path_func = partial(fm.DeleteDatastoreFile_Task,
                               name=folder_path,
                               datacenter=vim_datacenter)

    pre_task_msg = "Deleting '%s'" % folder_path
    post_task_msg = "Deleted '%s'" % folder_path
    task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                      post_task_msg=post_task_msg,
                                      create_task_func=delete_path_func)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc,
                                      max_parallel=1, timeout_secs=600)

  def snapshot_vms(self, vim_cluster, vm_names, snapshot_names,
                   snapshot_descriptions=(), max_parallel_tasks=None):
    """
    For each VM with name 'vm_names[xx]' on the cluster 'vim_cluster', creates
    a snapshot with snapshot name 'snapshot_names[xx]' and optional description
    'snapshot_descriptions[xx]'.

    Args:
      vim_cluster: Cluster object containing the vms.
      vm_names: List of VM names to power on.
      snapshot_descriptions list or tuple of descriptions (optional): If
        non-empty, snapshot descriptions must be specified for all snapshots
        being created.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.

    Raises:
      CurieTestException: Snapshots are not created within the timeout.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    CHECK_EQ(len(vm_names), len(snapshot_names))
    CHECK(len(snapshot_descriptions) == 0 or
          len(snapshot_descriptions) == len(snapshot_names))
    vim_vms = self.lookup_vms(vim_cluster, vm_names)
    # Snapshot the VMs with bounded parallelism.
    task_desc_list = []
    for xx, vm_name in enumerate(vm_names):
      vim_vm = vim_vms[xx]
      if len(snapshot_descriptions) > 0:
        snapshot_description = snapshot_descriptions[xx]
      else:
        snapshot_description = ""
      create_task_func = partial(vim_vm.CreateSnapshot,
                                 name=snapshot_names[xx],
                                 description=snapshot_description,
                                 memory=False,
                                 quiesce=False)
      pre_task_msg = "Creating snapshot of VM %s with name %s" % \
        (vim_vm.name, snapshot_names[xx])
      post_task_msg = "Created snapshot of VM %s with name %s" % \
        (vim_vm.name, snapshot_names[xx])
      task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                        post_task_msg=post_task_msg,
                                        create_task_func=create_task_func)
      task_desc_list.append(task_desc)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vm_names) * 600)

  def find_snapshot_vm(self, vim_vm, snapshot_name):
    """
    Find a snapshot for a VM if present.

    Args:
      vim_vm (vim.VM): vim object for a VM to search snapshot tree.
      snapshot_name (string): Name of the snapshot to try and find.

    Returns:
      The snapshot object if the snapshot is found, otherwise None is returned.
    """
    def __recursively_find_snapshot(snapshots, snapshot_name):
      for snapshot in snapshots:
        if snapshot.name == snapshot_name:
          return snapshot
        elif len(snapshot.childSnapshotList) > 0:
          rval = __recursively_find_snapshot(snapshot.childSnapshotList,
                                             snapshot_name)
          if rval is not None:
            return rval
      return None
    return __recursively_find_snapshot(vim_vm.snapshot.rootSnapshotList,
                                       snapshot_name)

  def migrate_vms(self, vim_vms, vim_hosts, max_parallel_tasks=None):
    """
    Move 'vim_vms' to 'vim_hosts'. (VMotion)

    Args:
      vim_vms (list<vim.VirtualMachine>): vim VMs to move.
      vim_hosts (list<vim.Host>): vim Hosts to move Vim VMs to.
      max_parallel_tasks (int): The number of VMs to migrate in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    task_desc_list = []
    for vim_vm, vim_host in zip(vim_vms, vim_hosts):
      create_task_func = partial(
        vim_vm.MigrateVM_Task, host=vim_host,
        priority=vim.VirtualMachine.MovePriority.highPriority)
      pre_task_msg = "Moving VM: %s to Host: %s" % (vim_vm.name, vim_host.name)
      post_task_msg = "Moved VM: %s to Host: %s" % (vim_vm.name, vim_host.name)
      task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                        post_task_msg=post_task_msg,
                                        create_task_func=create_task_func)
      task_desc_list.append(task_desc)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vim_vms) * 1200)

  def relocate_vms_datastore(
      self, vim_vms, vim_datastores, max_parallel_tasks=None):
    """
    Relocate 'vim_vms' to 'vim_datastores'. (Storage VMotion)

    Args:
      vim_vms (list<vim.VirtualMachine>): vim VMs to move.
      vim_datastores (list<vim.Datastore>): vim Datastores to move Vim VMs to.
      max_parallel_tasks (int): The number of VMs to relocate in parallel. The
      default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
    """
    max_parallel_tasks = self._get_max_parallel_tasks(max_parallel_tasks)
    task_desc_list = []
    for vim_vm, vim_datastore in zip(vim_vms, vim_datastores):
      vim_relocate_spec = vim.vm.RelocateSpec()
      vim_relocate_spec.datastore = vim_datastore
      pre_task_msg = "Moving VM: %s to Datastore: %s" % (
        vim_vm.name, vim_datastore.name)
      post_task_msg = "Moved VM: %s to Datastore: %s" % (
        vim_vm.name, vim_datastore.name)
      create_task_func = partial(
        vim_vm.RelocateVM_Task, spec=vim_relocate_spec,
        priority=vim.VirtualMachine.MovePriority.highPriority)
      task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                        post_task_msg=post_task_msg,
                                        create_task_func=create_task_func)
      task_desc_list.append(task_desc)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                      max_parallel=max_parallel_tasks,
                                      timeout_secs=len(vim_vms) * 1200)

  def disable_ha_vms(self, vim_cluster, vim_vms):
    """
    Disable HA on all the vim_vms in vim_cluster.

    Args:
      vim_cluster (vim.Cluster): Cluster object containing the vms.
      vim_vms: (list<vim.VirtualMachine>): vim VMs on which to disable HA.

    """
    # https://github.com/rreubenur/vmware-pyvmomi-examples/blob/master/disable_HA_on_particular_VM.py
    config_specs = []
    cluster_spec = vim.cluster.ConfigSpecEx()
    for vm in vim_vms:
      das_vm_config_spec = vim.cluster.DasVmConfigSpec()
      das_vm_config_info = vim.cluster.DasVmConfigInfo()
      das_vm_settings = vim.cluster.DasVmSettings()
      das_vm_settings.restartPriority = \
        vim.ClusterDasVmSettingsRestartPriority.disabled
      das_vm_monitor = vim.cluster.VmToolsMonitoringSettings()
      das_vm_monitor.vmMonitoring = \
        vim.cluster.DasConfigInfo.VmMonitoringState.vmMonitoringDisabled
      das_vm_monitor.clusterSettings = False
      das_vm_settings.vmToolsMonitoringSettings = das_vm_monitor
      das_vm_config_info.key = vm
      das_vm_config_info.dasSettings = das_vm_settings
      das_vm_config_spec.info = das_vm_config_info

      # The operation depends on the current setting for the VM. If there is
      # already a setting, it has to be edited, otherwise added. We expect to
      # only add settings since we're creating VMs.
      for config_entry in vim_cluster.configuration.dasVmConfig:
        if config_entry.key == vm:
          das_vm_config_spec.operation = \
            vim.option.ArrayUpdateSpec.Operation.edit
          break
      else:
        das_vm_config_spec.operation = vim.option.ArrayUpdateSpec.Operation.add
      config_specs.append(das_vm_config_spec)

    cluster_spec.dasVmConfigSpec = config_specs
    reconfig_cluster_func = partial(
      vim_cluster.ReconfigureComputeResource_Task,
      spec=cluster_spec,
      modify=True)
    pre_task_msg = "Disabling HA for VMs on cluster %s" % vim_cluster.name
    post_task_msg = "Disabled HA for VMs on cluster %s" % vim_cluster.name
    task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                      post_task_msg=post_task_msg,
                                      create_task_func=reconfig_cluster_func)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc,
                                      max_parallel=1,
                                      timeout_secs=600)

  def disable_drs_vms(self, vim_cluster, vim_vms):
    """
    Disable DRS on all the vim_vms in vim_cluster.

    Args:
      vim_cluster (vim.Cluster): Cluster object containing the vms.
      vim_vms: (list<vim.VirtualMachine>): vim VMs on which to disable DRS.

    Raises:
      CurieTestException if DRS configuration for cluster does not allow for
        per-vm overrides.

    """
    if not vim_cluster.configuration.drsConfig.enableVmBehaviorOverrides:
      raise CurieTestException("Per-VM DRS configuration is not allowed. "
                                "Enable VM overrides in cluster DRS "
                                "configuration or disable DRS.")
    config_specs = []
    cluster_spec = vim.cluster.ConfigSpecEx()
    for vm in vim_vms:
      drs_vm_config_spec = vim.cluster.DrsVmConfigSpec()
      drs_vm_config_info = vim.cluster.DrsVmConfigInfo()
      drs_vm_config_info.key = vm
      drs_vm_config_info.enabled = False
      drs_vm_config_spec.info = drs_vm_config_info
      # The operation depends on the current setting for the VM. If there is
      # already a setting, it has to be edited, otherwise added. We expect to
      # only add settings since we're creating VMs.
      for config_entry in vim_cluster.configuration.drsVmConfig:
        if config_entry.key == vm:
          drs_vm_config_spec.operation = \
            vim.option.ArrayUpdateSpec.Operation.edit
          break
      else:
        drs_vm_config_spec.operation = vim.option.ArrayUpdateSpec.Operation.add
        config_specs.append(drs_vm_config_spec)
    cluster_spec.drsVmConfigSpec = config_specs
    reconfig_cluster_func = partial(
      vim_cluster.ReconfigureComputeResource_Task,
      spec=cluster_spec,
      modify=True)
    pre_task_msg = "Disabling DRS for %d X-Ray VMs on cluster %s" % (
      len(vim_vms), vim_cluster.name)
    post_task_msg = "Disabled DRS for %d X-Ray VMs on cluster %s" % (
      len(vim_vms), vim_cluster.name)
    task_desc = VsphereTaskDescriptor(pre_task_msg=pre_task_msg,
                                      post_task_msg=post_task_msg,
                                      create_task_func=reconfig_cluster_func)
    TaskPoller.execute_parallel_tasks(task_descriptors=task_desc,
                                      max_parallel=1, timeout_secs=600)

  def power_off_hosts(self, vim_cluster, vim_hosts, timeout_secs):
    """
    Power off 'vim_hosts' on 'vim_cluster' via the managing vCenter.

    NB: Although this method waits for the vim tasks to complete, it is
    essentially asynchronous. The vim tasks do not block until the
    host has actually powered off, only until the power off request is
    acknowledged.

    Args:
      vim_cluster (vim.ClusterComputeResource): Cluster to which 'nodes'
        belong.
      vim_hosts (list<vim.HostSystem>): List of hosts to power off.
      timeout_secs (int): Timeout in seconds for all nodes to complete their
        power off tasks.

    Raises:
      CurieTestException (dodged) if all nodes do not complete their power
      off tasks within 'timeout_secs'.
    """
    task_desc_list = []
    for vim_host in vim_hosts:
      task_desc_list.append(VsphereTaskDescriptor(
        pre_task_msg="Shutting down '%s' in cluster '%s' via vCenter" %
        (vim_host.name, vim_cluster.name),
        post_task_msg="Successfully shut down '%s' in cluster '%s' via vCenter"
        % (vim_host.name, vim_cluster.name),
        create_task_func=partial(vim_host.Shutdown, force=True)))

    TaskPoller.execute_parallel_tasks(
      task_descriptors=task_desc_list,
      max_parallel=FLAGS.vsphere_vcenter_max_parallel_tasks,
      timeout_secs=len(vim_hosts) * timeout_secs)

  def disconnect_hosts(self, vim_hosts, timeout_secs=60):
    """
    Disconnect 'vim_hosts' from vCenter.

    Args:
      vim_hosts (list<vim.HostSystem>) vim objects for hosts to reconnect.
      timeout_secs (number): Optional. Timeout for disconnect tasks.
    """
    CHECK_GT(timeout_secs, 0,
             msg="'timeout_secs' must be greater than zero")

    to_disconnect_host_id_set = set([vim_host.name for vim_host in vim_hosts])

    # NB: If host is already disconnected, a disconnect task will fail with a
    # vim.fault.InvalidState error. We'll log a warning and continue, as
    # reconnects may still succeed.
    # NB: If the host state is, e.g., host.runtime.powerState = 'unknown', the
    # Disconnect is expected to fail. However, a Reconnect may still succeed.
    log.info("Disconnecting '%s'", ", ".join(to_disconnect_host_id_set))
    try:
      host_id_task_desc_map = self.__disconnect_hosts(vim_hosts, timeout_secs)
    except CurieTestException:
      log.warning("Disconnect task timed out on '%s'",
                  ", ".join(to_disconnect_host_id_set))
    else:
      for host_id, task_desc in host_id_task_desc_map.items():
        if task_desc.state.status != TaskStatus.kSucceeded:
          log.warning("Disconnect task was unsuccessful on '%s': '%s'. ",
                      task_desc.entity_name, task_desc.state.status)
        else:
          log.info("Successfully disconnected host '%s'", host_id)
          to_disconnect_host_id_set.remove(host_id)

    if not to_disconnect_host_id_set:
      log.info("Successfully disconnected all provided hosts")
    else:
      log.warning("Unable to disconnect '%s'. Reconnect tasks may still "
                  "succeed", ", ".join(to_disconnect_host_id_set))

  def reconnect_hosts(self, vim_hosts, timeout_secs=60):
    """
    Reconnect 'vim_hosts' to vCenter.

    Args:
      vim_hosts (list<vim.HostSystem>) vim objects for hosts to reconnect.
      timeout_secs (number): Optional. Timeout for reconnect tasks.
    """
    CHECK_GT(timeout_secs, 0,
             msg="'attempt_timeout_secs' must be greater than zero")

    to_reconnect_host_id_set = set([vim_host.name for vim_host in vim_hosts])

    try:
      host_id_task_desc_map = self.__reconnect_hosts(vim_hosts, timeout_secs)
    except CurieTestException:
      log.warning("Reconnect task timed out on '%s'",
                  ", ".join(to_reconnect_host_id_set))
    else:
      for host_id, task_desc in host_id_task_desc_map.items():
        if task_desc.state.status != TaskStatus.kSucceeded:
          log.warning("Reconnect task was unsuccessful on '%s': '%s'. ",
                      task_desc.entity_name, task_desc.state.status)
        else:
          log.info("Successfully reconnected host '%s'", host_id)
          to_reconnect_host_id_set.remove(host_id)

    if not to_reconnect_host_id_set:
      log.info("Successfully reconnected all provided hosts")
    else:
      log.error("Unable to reconnect '%s'",
                ", ".join(to_reconnect_host_id_set))

  def reload_hosts(self, vim_hosts):
    """
    Reload 'vim_hosts' in vCenter.

    Args:
      vim_hosts (list<vim.HostSystem>) vim objects for hosts to reconnect.
    """
    for host in vim_hosts:
      log.debug("Reloading host '%s'", host.name)
      # This call returns None, no task object or status is available.
      host.Reload()

  def refresh_datastores(self, vim_cluster):
    """
    Refreshes datastores state for all nodes in 'vim_cluster' in vCenter. This
    is necessary in case changes have been made directly on ESX nodes and the
    ESX nodes fail to synchronize this state with vCenter.

    Args:
      vim_cluster (vim.ClusterComputeResource): vSphere cluster.
    """
    for xx, vim_host in enumerate(vim_cluster.host):
      vim_storage_system = get_optional_vim_attr(vim_host.configManager,
                                                 "storageSystem")
      if vim_storage_system is None:
        error_msg = "Missing storageSystem on node %s" % vim_host.name
        raise CurieTestException(error_msg)
      # Note: RefreshStorageSystem() is asynchronous and that it does not
      # return a vim Task.
      log.info("Asynchronously refreshing datastores on node %d/%d %s on %s",
               xx + 1, len(vim_cluster.host), vim_host.name, vim_cluster.name)
      vim_host.configManager.storageSystem.RefreshStorageSystem()

  def is_ha_enabled(self, vim_cluster):
    """
    Checks whether 'vim_cluster' has HA enabled.

    Returns:
      (bool) True if HA is enabled, else False.

    Args:
      vim_cluster (vim.ClusterComputeResource): vSphere cluster.
    """
    ha_config = vim_cluster.configurationEx.dasConfig
    return get_optional_vim_attr(ha_config, "enabled") == True

  def is_drs_enabled(self, vim_cluster):
    """
    Checks whether 'vim_cluster' has DRS enabled.

    Returns:
      (bool) True if DRS is enabled, else False.

    Args:
      vim_cluster (vim.ClusterComputeResource): vSphere cluster.
    """
    drs_config = vim_cluster.configurationEx.drsConfig
    return get_optional_vim_attr(drs_config, "enabled") == True

  @classmethod
  def get_vsan_version(cls, vim_cluster):
    """Queries host CIM endpoint to determine VSAN vib version.

    Args:
      vim_cluster (vim.ClusterComputeResource): cluster to determine version of
      VSAN running on its nodes.

    Returns:
      (str): VSAN versions for the cluster nodes. If the VSAN version can't
      be determined, returns None
    """
    for vim_host in vim_cluster.host:
      sid = vim_host.AcquireCimServicesTicket()
      cim = WBEMConnection("https://%s" % vim_host.name, (sid, sid),
                           no_verification=True)
      try:
        for ident in cim.EnumerateInstances("CIM_SoftwareIdentity"):
          if ident["ElementName"] == "vsan":
            return ident["VersionString"]
      except Exception as exc:
        raise CurieException(
          CurieError.kInternalError,
          "Unable to query CIM endpoint for VSAN data: %s" % exc)

    return None

  @classmethod
  def get_esx_versions(cls, vim_cluster, nodes=None):
    """
    Determine the versions and build numbers for ESX on the cluster nodes.

    Args:
      vim_cluster (vim.ClusterComputeResource): cluster to determine version of
        ESX running on its nodes.
      nodes (list<CurieSettings.Node>): If provided, a subset of the
        cluster's nodes whose information to return.

    Returns:
      list<(str, str)>: list of tuples containing ESX version and build strings
      for the cluster nodes. If the ESX version and build string for a node
      can't be determined, we use ("", "").
    """
    esx_version_build_pairs = []
    node_id_vim_host_map = dict((vim_host.name, vim_host) for vim_host
                                in vim_cluster.host)
    node_ids = [n.id for n in nodes] if nodes else [h.name
                                                    for h in vim_cluster.host]
    for node_id in node_ids:
      vim_host = node_id_vim_host_map.get(node_id)
      CHECK(vim_host, msg="Unable to locate vim host object for %s" % node_id)
      esx_version_build_pairs.append(
        cls.get_esx_versions_for_vim_host(vim_host))
    log.info("Cluster %s node ESX versions %s",
             vim_cluster.name, esx_version_build_pairs)
    return esx_version_build_pairs

  @classmethod
  def get_esx_versions_for_vim_host(cls, vim_host):
    """
    Returns:
      (str, str) ESX version string, ESX build string for 'vim_host' on
      success, ("", "") if unable to access the 'config' key on the vim_host.
    """
    vim_host_config = get_optional_vim_attr(vim_host, "config")
    if vim_host_config is None:
      log.warning("Could not get config for node %s", vim_host.name)
      return ("", "")
    return (vim_host_config.product.version, vim_host_config.product.build)

  def get_vcenter_version_info(self):
    return self.__si.content.about.version, self.__si.content.about.build

  def _get_max_parallel_tasks(self, arg):
    return FLAGS.vsphere_vcenter_max_parallel_tasks if arg is None else arg

  def __upload_vmdks(self, vim_lease, vmdk_paths):
    """
    Upload the VM's virtual disks with paths 'vmdk_paths' using the URLs from
    'vim_lease'.

    Reference: ImportVApp in the vSphere API documentation.
    """
    # Note: the code in this method is based on diagnostics.py.
    t1 = time.time()
    progress = 0
    while True:
      if vim_lease.state == vim.HttpNfcLease.State.ready:
        device_urls_orig = vim_lease.info.deviceUrl

        # deviceUrl in VSAN 6.7/ESXi 6.7 has extra nvram non disk entry
        # which should not be taken into account to upload vmdks.
        # Example of the output is below:
        #
        # (vim.HttpNfcLease.DeviceUrl) [
        #    (vim.HttpNfcLease.DeviceUrl) {
        #       dynamicType = <unset>,
        #       dynamicProperty = (vmodl.DynamicProperty) [],
        #       key = '/vm-67/VirtualLsiLogicController0:0',
        #       importKey = '/__curie_goldimage_4183265716850586767_ubuntu1604_group_0/VirtualLsiLogicController0:0',
        #       url = 'https://10.60.5.201/nfc/525216e6-797f-a03f-9b45-45cb9ea0f0d9/disk-0.vmdk',
        #       sslThumbprint = 'BE:70:F9:C0:E4:84:D3:BA:EA:C2:DE:86:99:AC:7F:D9:60:7E:A6:09',
        #       disk = true,
        #       targetId = 'disk-0.vmdk',
        #       datastoreKey = 'https://10.60.5.201/nfc/525216e6-797f-a03f-9b45-45cb9ea0f0d9/',
        #       fileSize = <unset>
        #    },
        #    (vim.HttpNfcLease.DeviceUrl) {
        #       dynamicType = <unset>,
        #       dynamicProperty = (vmodl.DynamicProperty) [],
        #       key = '/vm-67/nvram',
        #       importKey = '/__curie_goldimage_4183265716850586767_ubuntu1604_group_0/nvram',
        #       url = 'https://10.60.5.201/nfc/525216e6-797f-a03f-9b45-45cb9ea0f0d9/disk-1.nvram',
        #       sslThumbprint = 'BE:70:F9:C0:E4:84:D3:BA:EA:C2:DE:86:99:AC:7F:D9:60:7E:A6:09',
        #       disk = false,
        #       targetId = <unset>,
        #       datastoreKey = <unset>,
        #       fileSize = <unset>
        #    }
        # ]
        
        device_urls = [x for x in device_urls_orig if x.disk == True]
        CHECK_EQ(len(vmdk_paths), len(device_urls),
                 msg="%s %s" % (vmdk_paths, device_urls))
        for xx in xrange(len(device_urls)):
          progress = int(float(xx)/float(len(device_urls)))
          vmdk_path = vmdk_paths[xx]
          target_url = device_urls[xx].url.replace("*", self.__host)
          log.info("Uploading vmdk %d/%d %s",
                   xx + 1, len(vmdk_paths), vmdk_path)
          upload_thread = threading.Thread(
            target=self.__upload_vmdk, args=(vmdk_path, target_url))
          upload_thread.start()
          upload_t1 = time.time()
          thread_alive = True
          while thread_alive:
            vim_lease.HttpNfcLeaseProgress(progress)
            upload_thread.join(10)
            thread_alive = upload_thread.isAlive()
            upload_t2 = time.time()
            if (upload_t2 - upload_t1) > 3600:
              raise CurieTestException("Timeout uploading vmdk %d/%d %s" %
                                        (xx + 1, len(vmdk_paths), vmdk_path))
        vim_lease.HttpNfcLeaseComplete()
        return
      elif vim_lease.state == vim.HttpNfcLease.State.error:
        raise CurieTestException("Lease connection error on %s: %s" %
                                  (self.__host, vim_lease.error.msg))
      t2 = time.time()
      elapsed_secs = t2 - t1
      if elapsed_secs >= 300:
        raise CurieTestException("Timeout waiting for lease to become ready")
      log.info("Waiting for lease on %s to become ready (%d secs), state %s",
               self.__host, elapsed_secs, vim_lease.state)
      time.sleep(1)

  def __upload_vmdk(self, vmdk_path, target_url):
    """
    Upload a VM virtual disk with path 'vmdk_path' using the target URL
    'target_url'.
    """
    curl_cmd = ("curl -Ss -X POST --insecure -T %s -H 'Content-Type: "
                "application/x-vnd.vmware-streamVmdk' %s" %
                (vmdk_path, target_url))
    proc = subprocess.Popen(curl_cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            close_fds=True)
    stdout, stderr = proc.communicate()
    rv = proc.wait()
    if rv != 0:
      error_msg = "Failed to upload %s to %s: %d, %s, %s" % \
        (vmdk_path, target_url, rv, stdout, stderr)
      raise CurieTestException(error_msg)

  def __clone_vm(self,
                 vim_vm,
                 vm_name,
                 vim_datacenter,
                 vim_datastore,
                 vim_host,
                 linked_clone_snapshot=None):
    """
    Create a clone of the VM 'vim_vm' with name 'vm_name' and place it in the
    VM folder for 'vim_datacenter' and on datastore 'vim_datastore'. Register
    it with host 'vim_host'. Returns the task for the clone operation.
    """
    vim_vm_folder = vim_datacenter.vmFolder
    vim_relocate_spec = vim.vm.RelocateSpec(host=vim_host,
                                            datastore=vim_datastore)
    vim_clone_spec = vim.vm.CloneSpec(location=vim_relocate_spec)
    if linked_clone_snapshot is not None:
      vim_clone_spec.snapshot = linked_clone_snapshot
      vim_relocate_spec.diskMoveType = "createNewChildDiskBacking"
    return vim_vm.Clone(folder=vim_vm_folder,
                        name=vm_name,
                        spec=vim_clone_spec)

  def __cancel_entity_tasks(self, vim_cluster, entity_names):
    """
    Cancel any tasks for the entities (e.g., VMs) on cluster 'vim_cluster' with
    names 'entity_names' and wait for the tasks to complete.
    """
    entity_name_set = set(entity_names)
    tasks = []
    for vim_task in vim_cluster.resourcePool.recentTask:
      try:
        task = VsphereTask.from_vim_task(vim_task)
        if task._task.info.entityName not in entity_name_set:
          continue
        if not task.cancel():
          tasks.append(task)
      except vmodl.fault.ManagedObjectNotFound as exc:
        continue

    # NB: Will raise an appropriate exception on timeout.
    TaskPoller.execute_parallel_tasks(tasks=tasks,
                                      timeout_secs=len(tasks) * 1800)

  def __vm_has_ip(self, vim_vm):
    """
    Returns True if the VM 'vim_vm' has an IPv4 address assigned to it.
    """
    vim_guest = get_optional_vim_attr(vim_vm.summary, "guest")
    if vim_guest is None:
      log.info("VM %s does not have guest information set yet", vim_vm.name)
      return False
    vm_ip = get_optional_vim_attr(vim_guest, "ipAddress")
    if vm_ip is None:
      log.info("VM %s does not have an IP address yet", vim_vm.name)
      return False
    if not CurieUtil.is_ipv4_address(vm_ip):
      log.warning("VM %s currently has a non-IPv4 primary IP %s",
                  vim_vm.name, vm_ip)
      return False
    log.info("VM %s has IP address %s", vim_vm.name, vm_ip)
    return True

  def __disconnect_hosts(self, vim_hosts, timeout_secs, raise_on_error=False):
    """
    Disconnect 'vim_hosts' to force a vCenter refresh.

    Args:
      vim_hosts (list<vim.HostSystem>) vim objects for hosts to reconnect.
      timeout_secs (number): Timeout for disconnect tasks.
      raise_on_error (bool): Optional. If True, raise an exception if any
        tasks enter a terminal state other than 'SUCCEEDED'. (Default False).

    Returns:
      dict<str, VsphereTaskDescriptor> Map of host IDs to descriptors for
        their Disconnect tasks.
    """
    host_id_disconnect_task_desc_map = {}
    # If 'raise_on_error' is False, don't check task state for success.
    desired_state = TaskStatus.kSucceeded if raise_on_error else None
    for vim_host in vim_hosts:
      host_id_disconnect_task_desc_map[vim_host.name] = VsphereTaskDescriptor(
        pre_task_msg="Disconnecting host '%s' via vCenter" % vim_host.name,
        post_task_msg="vCenter disconnect host task for '%s' has exited" %
        vim_host.name,
        create_task_func=vim_host.Disconnect, desired_state=desired_state)

    TaskPoller.execute_parallel_tasks(
      task_descriptors=host_id_disconnect_task_desc_map.values(),
      max_parallel=FLAGS.vsphere_vcenter_max_parallel_tasks,
      timeout_secs=len(vim_hosts) * timeout_secs)

    return host_id_disconnect_task_desc_map

  def __reconnect_hosts(self, vim_hosts, timeout_secs, raise_on_error=False):
    """
    Reconnect 'vim_hosts' to force a vCenter refresh.

    Args:
      vim_hosts (list<vim.HostSystem>) vim objects for hosts to reconnect.
      timeout_secs (number): Timeout for reconnect tasks.
      raise_on_error (bool): Optional. If True, raise an exception if any
        tasks enter a terminal state other than 'SUCCEEDED'. (Default False).

    Returns:
      dict<str, VsphereTaskDescriptor> Map of host IDs to descriptors for
        their Reconnect tasks.
    """
    host_id_reconnect_task_desc_map = {}
    # If 'raise_on_error' is False, don't check task state for success.
    desired_state = TaskStatus.kSucceeded if raise_on_error else None
    for vim_host in vim_hosts:
      host_id_reconnect_task_desc_map[vim_host.name] = VsphereTaskDescriptor(
        pre_task_msg="Reconnecting host '%s' via vCenter" % vim_host.name,
        post_task_msg="Task to reconnect host '%s' via vCenter has exited" %
        vim_host.name,
        create_task_func=vim_host.Reconnect, desired_state=desired_state)

    TaskPoller.execute_parallel_tasks(
      task_descriptors=host_id_reconnect_task_desc_map.values(),
      max_parallel=FLAGS.vsphere_vcenter_max_parallel_tasks,
      timeout_secs=len(vim_hosts) * timeout_secs)

    return host_id_reconnect_task_desc_map
