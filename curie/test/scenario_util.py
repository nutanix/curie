#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import cStringIO
import glob
import logging
import os
import time
from functools import partial

from google.protobuf import message

from curie import curie_test_pb2
from curie.curie_error_pb2 import CurieError
from curie.curie_metrics_pb2 import CurieMetric
from curie.exception import CurieException, CurieTestException
from curie.log import CHECK, CHECK_LE
from curie.metrics_util import MetricsUtil
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.oob_management_util import OobInterfaceType
from curie.os_util import OsUtil
from curie.test import vdbench_util
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class ScenarioUtil(object):
  @staticmethod
  def wait_for_vms_accessible(vms, timeout_secs):
    """
    Wait until all the specified VMs are accessible to run guest OS commands.
    Raises CurieTestException if this takes longer than 'timeout_secs'.
    """
    t1 = time.time()
    t2 = -1
    for xx, vm in enumerate(vms):
      log.info("Waiting for VM %d/%d (%s) to become accessible",
               xx + 1, len(vms), vm.vm_name())
      if t2 >= 0:
        wait_for_timeout_secs = timeout_secs - (t2 - t1)
      else:
        wait_for_timeout_secs = timeout_secs
      if wait_for_timeout_secs < 0:
        error_msg = "Timeout waiting for VMs to become accessible"
        raise CurieTestException(error_msg)
      if vm.vm_ip() is None:
        error_msg = "IP address not set on Vm object %s" % vm.vm_name()
        raise CurieTestException(error_msg)
      if not CurieUtil.is_ipv4_address(vm.vm_ip()):
        error_msg = "Non-IPv4 address %s on VM %s" % (vm.vm_ip(), vm.vm_name())
        raise CurieTestException(error_msg)
      msg = "waiting for VM %s (%s) to become accessible" % \
        (vm.vm_name(), vm.vm_ip())
      if not CurieUtil.wait_for(vm.is_accessible, msg, wait_for_timeout_secs):
        error_msg = "Timeout waiting for VMs to become accessible"
        raise CurieTestException(error_msg)
      log.info("VM %d/%d (%s) is accessible", xx + 1, len(vms), vm.vm_name())
      t2 = time.time()

  @staticmethod
  def prereq_dependency_has_oob_data(metadata):
    """
    Verify that 'metadata' contains necessary info for cluster OoB operations.

    Returns:
      (bool) True if 'metadata' has required OoB data, else False.
    """
    log.debug("Validating that metadata has OoB info")
    if not metadata.cluster_nodes:
      return False
    for node in metadata.cluster_nodes:
      log.debug("Checking node '%s'", node.id)
      if not node.HasField("node_out_of_band_management_info"):
        return False
      oob_info = node.node_out_of_band_management_info
      if oob_info.interface_type == OobInterfaceType.kNone:
        return False
      if not oob_info.HasField("ip_address"):
        return False
      if not oob_info.HasField("vendor"):
        log.warning("BMC vendor not set for node '%s'. OoB operations will "
                    "default to standard IPMI", node.id)
    return True

  @staticmethod
  def prereq_metadata_can_run_failure_scenario(metadata):
    """
    Verifies that 'metadata' contains necessary config for failure tests.
    """
    if not ScenarioUtil.prereq_dependency_has_oob_data(metadata):
      raise CurieTestException(
        "Target '%s' has not been configured for out-of-band power management."
        " Cannot run failure scenario" % metadata.cluster_name)

  @staticmethod
  def prereq_runtime_node_power_check(cluster):
    """
    Confirms that all nodes in 'cluster' are powered on.

    NB: Powered on will not guarantee that the node is ready. There are further
    vendor-specific prereqs which will verify required services, etc.

    Raises:
      CurieTestException if any nodes are not powered on.
    """
    to_power_on_nodes = []
    for node in cluster.nodes():
      log.info("Checking that node '%s' is powered on", node.node_id())
      if not node.is_powered_on():
        to_power_on_nodes.append(node)

    if to_power_on_nodes:
      raise CurieTestException("Node(s) '%s' are powered off" % ", ".join(
        [node.node_id() for node in to_power_on_nodes]))

  @staticmethod
  def prereq_runtime_node_power_fix(cluster):
    """
    Attempt to boot any powered off nodes in 'cluster'. Block until all nodes
    are powered.

    NB: Powered on will not guarantee that the node is ready. There are further
    vendor-specific prereqs which will verify required services, etc.

    Raises:
      CurieTestException on error or timeout.
    """
    to_power_on_nodes = [node for node in cluster.nodes()
                         if not node.is_powered_on()]

    log.info("Attempting to power on the following nodes: %s",
             ", ".join([node.node_id() for node in to_power_on_nodes]))
    for node in to_power_on_nodes:
      node.power_on()

    def nodes_powered_on():
      for node in to_power_on_nodes:
        if not node.is_powered_on():
          return False
      return True

    if not CurieUtil.wait_for(nodes_powered_on,
                               "nodes to be powered on", 600, poll_secs=5):
      raise CurieTestException("Unable to power on nodes prior to test")

  @staticmethod
  def prereq_runtime_cluster_ha_drs_disabled(cluster):
    """
    Confirms that 'cluster' does not have any form of high availability or
    dynamic resource scheduling enabled.

    Raises:
      CurieTestException if any HA or DRS features are enabled on 'cluster'.
    """
    if cluster.is_ha_enabled() or cluster.is_drs_enabled():
      raise CurieTestException(
        "High availability and dynamic resource scheduling services must be "
        "disabled on cluster '%s' prior to test" % cluster.name())

  @staticmethod
  def prereq_runtime_storage_cluster_mgmt_cluster_match(cluster):
    """
    Confirms that 'cluster' comprises the same hosts whether considered as a
    cluster as known to the management software or as a cluster in the
    appropriate sense for 'cluster's storage fabric.

    Raises:
      CurieTestException if there is a mismatch between the different notions
        of a cluster.
    """
    # TODO (jklein): Generalize this along with future refactoring of core
    # Curie entities. For now, the only relevant combination is that checked
    # below.
    cluster_software_info = cluster.metadata().cluster_software_info
    mgmt_info = cluster.metadata().cluster_management_server_info
    if cluster_software_info.HasField("nutanix_info"):
      if not mgmt_info.HasField("vcenter_info"):
        return

      nutanix_node_id_set = set(
        [vm.node_id() for vm in cluster.vms() if vm.is_cvm()])
      vsphere_node_id_set = set(
        [node.node_id() for node in cluster.nodes()])

      mismatched_node_ids = nutanix_node_id_set.symmetric_difference(
        vsphere_node_id_set)

      if not mismatched_node_ids:
        return

      raise CurieTestException(
        "vCenter and Nutanix clusters do not coincide for nodes: %s" %
        ", ".join(mismatched_node_ids))

  @staticmethod
  def prereq_runtime_cluster_is_ready(cluster):
    """
    Confirms that 'cluster' is ready.

    Performs management- and cluster-software specific cluster checks.

    Raises:
      CurieTestException if any nodes are not powered on.
    """
    sync_with_oob = ScenarioUtil.prereq_dependency_has_oob_data(
      cluster.metadata())
    return cluster.check_cluster_ready(sync_with_oob=sync_with_oob)

  @staticmethod
  def prereq_runtime_cluster_is_ready_fix(cluster):
    """
    Attempts to ready cluster.

    Raises:
      CurieTestException on failure.
    """
    if not ScenarioUtil.prereq_dependency_has_oob_data(cluster.metadata()):
      raise CurieTestException(
        "Cannot attempt fix without OoB data for cluster")

    unready_nodes = cluster.get_unready_nodes()
    power_on_nodes = []
    for node in unready_nodes:
      if not node.is_powered_on():
        log.info("Powering on node '%s'", node.node_id())
        node.power_on()
        power_on_nodes.append(node)

    if power_on_nodes and not CurieUtil.wait_for(
      lambda: not cluster.get_powered_off_nodes(
        power_on_nodes, sync_with_oob=True),
      "all nodes to be powered on",
      timeout_secs=1200, poll_secs=5):
      raise CurieTestException("Timed out waiting for nodes to power on")
    log.info("All nodes are now powered on")

    to_power_on_cvms = []
    for cvm in [vm for vm in cluster.vms() if vm.is_cvm()]:
      if not cvm.is_powered_on():
        to_power_on_cvms.append(cvm)
    log.info("Powering on CVMs: %s",
             ", ".join([cvm.vm_name() for cvm in to_power_on_cvms]))
    try:
      cluster.power_on_vms(to_power_on_cvms)
    except Exception as exc:
      raise CurieTestException("Failed to power on CVMs: %s" % exc)
    log.info("Powered on all CVMs")

    if not CurieUtil.wait_for(
        partial(ScenarioUtil.prereq_runtime_cluster_is_ready, cluster),
        "all nodes to be ready", timeout_secs=1200, poll_secs=5):
      raise CurieTestException("Timed out waiting for cluster to be ready")
    log.info("Cluster is now in a ready state")

  @staticmethod
  def prereq_runtime_vm_storage_is_ready(cluster):
    """
    Confirms that curie test VM storage on each node in 'cluster' is
    available.

    Raises:
      CurieTestException if curie test VM storage is unavailable on any node.
    """
    metadata = cluster.metadata()
    if metadata.cluster_hypervisor_info.HasField("esx_info"):
      num_nodes = len(metadata.cluster_nodes)
      CHECK(metadata.cluster_management_server_info.HasField("vcenter_info"))
      vcenter_info = metadata.cluster_management_server_info.vcenter_info
      datastore_name = vcenter_info.vcenter_datastore_name
      # Check that the datastore is visible on all nodes in vCenter.
      log.info("Checking that datastore %s is visible on all %s nodes in "
               "vCenter", datastore_name, cluster.name())
      if not cluster.datastore_visible(datastore_name):
        raise CurieTestException("Datastore %s not visible on all %s nodes "
                                  "in vCenter" %
                                  (datastore_name, cluster.name()))
      log.info("Datastore %s is visible on all %s nodes in vCenter",
               datastore_name, cluster.name())
      cluster_software_info = metadata.cluster_software_info
      if cluster_software_info.HasField("nutanix_info"):
        # On a Nutanix cluster, check that the datastore is also visible on all
        # nodes in Prism.
        log.info("Checking that datastore %s is visible by Prism on all %s "
                 "nodes", datastore_name, cluster.name())
        client = NutanixRestApiClient.from_proto(
          cluster_software_info.nutanix_info)
        host_id_datastore_map = {}
        for item in client.datastores_get():
          host_id_datastore_map.setdefault(item["hostId"], set())
          host_id_datastore_map[item["hostId"]].add(item["datastoreName"])
        CHECK_LE(len(host_id_datastore_map), num_nodes)
        if len(host_id_datastore_map) < num_nodes:
          raise CurieTestException("Datastore %s not visible by Prism on all "
                                    "%s nodes" %
                                    (datastore_name, cluster.name()))
        for host_id in host_id_datastore_map:
          if datastore_name not in host_id_datastore_map[host_id]:
            raise CurieTestException(
              "Datastore %s not visible by Prism on %s node %s" %
              (datastore_name, cluster.name(), host_id))
        log.info("Datastore %s is visible by Prism on all %s nodes",
                 datastore_name, cluster.name())
      elif cluster_software_info.HasField("vsan_info"):
        pass
      elif cluster_software_info.HasField("generic_info"):
        pass
      else:
        raise ValueError("Unknown cluster software info, metadata %s" %
                         metadata)
    elif metadata.cluster_hypervisor_info.HasField("hyperv_info"):
      # TODO (bferlic): More thorough checking here?
      return True
    elif metadata.cluster_hypervisor_info.HasField("ahv_info"):
      # TODO (jklein): More thorough checking here?
      return True
    else:
      raise ValueError("Unknown hypervisor type, metadata %s" % metadata)

  @staticmethod
  def prereq_runtime_vm_storage_is_ready_fix(cluster):
    """
    Attempt to make curie test VM storage available on all nodes.

    Raises:
      CurieTestException on error or timeout.
    """
    metadata = cluster.metadata()
    if metadata.cluster_hypervisor_info.HasField("esx_info"):
      CHECK(metadata.cluster_management_server_info.HasField("vcenter_info"))
      vcenter_info = metadata.cluster_management_server_info.vcenter_info
      datastore_name = vcenter_info.vcenter_datastore_name
      def datastore_visible():
        try:
          ScenarioUtil.prereq_runtime_vm_storage_is_ready(cluster)
          return True
        except CurieTestException:
          pass
      msg = "datastore %s visible on all %s nodes" % \
        (datastore_name, cluster.name())
      # Refresh datastores state on all nodes to try and make the datastore
      # visible from vCenter's perspective.
      log.info("Refreshing datastores on all %s nodes", cluster.name())
      cluster.refresh_datastores()
      if CurieUtil.wait_for(datastore_visible, msg, 60):
        return
      cluster_software_info = metadata.cluster_software_info
      if cluster_software_info.HasField("nutanix_info"):
        client = NutanixRestApiClient.from_proto(
          cluster_software_info.nutanix_info)
        container_name = None
        for item in client.datastores_get():
          if item["datastoreName"] == datastore_name:
            container_name = item["containerName"]
            break
        if container_name is None:
          log.warning("Datastore %s not mounted on any %s nodes, assuming "
                      "container name is the same as the desired datastore "
                      "name", datastore_name, cluster.name())
          # Assume that the desired datastore has the same name as an existing
          # container name.
          container_name = datastore_name
        # Remount the datastore to try and make the datastore visible.
        log.info("Unmounting and mounting datastore %s (container %s) on %s",
                 datastore_name, container_name, cluster.name())
        try:
          client.datastores_delete(datastore_name, verify=True)
        except CurieException, ex:
          if ex.error_code != CurieError.kInvalidParameter:
            raise
          # If Prism views the datastore as unmounted, kInvalidParameter is
          # returned so continue to try and mount the datastore on all nodes.
        client.datastores_create(container_name, datastore_name=datastore_name)
        cluster.refresh_datastores()
        if not CurieUtil.wait_for(datastore_visible, msg, 60):
          raise CurieTestException("Timeout waiting for datastore %s for "
                                    "VM storage to become visible on %s" %
                                    (datastore_name, cluster.name()))
      elif cluster_software_info.HasField("vsan_info"):
        raise CurieTestException("VSAN datastore %s not mounted on all %s "
                                  "nodes" % (datastore_name, cluster.name()))
      elif cluster_software_info.HasField("generic_info"):
        raise CurieTestException("Datastore %s not mounted on all %s nodes"
                                  % (datastore_name, cluster.name()))
      else:
        raise ValueError("Unknown cluster software info, metadata %s" %
                         metadata)
    elif metadata.cluster_hypervisor_info.HasField("hyperv_info"):
      # TODO(ryan.hardin): More thorough checking here?
      return True
    elif metadata.cluster_hypervisor_info.HasField("ahv_info"):
      # TODO(ryan.hardin): More thorough checking here?
      return True
    else:
      raise ValueError("Unknown hypervisor type, metadata %s" % metadata)

  @staticmethod
  def warmup_workload(test, vms, vdb_path, warmup_time_secs, max_jobs_per_node,
                      cmd_description="warmup"):
    """Warm up a workload before running the workload in the run phase.

    After running the warmup workload copy the Vdbench results to the 'test'
    result directory.

    Note:
      This method assumes a uniform workload across all 'vms' provided by the
      'vdb_path' configuration file.

    Args:
      test: reference to test used to generate local results path in a
        consistent manner.
      vms: list of vms to run warmup workload on.
      vdb_path: location of vdbench configuration file on VM.
      warmup_time_secs: number of seconds to run warmup workload.
      max_jobs_per_node: maximum number of jobs to run concurrently on each
        node.
      cmd_description: string used to generate job id. It should only contain
        alpha numeric characters and underscore.
    """
    output_directory = \
      vdbench_util.VdbenchUtil.vdbench_warmup_output_directory(test)
    warmup_cmd = vdbench_util.VdbenchUtil.build_vdbench_cmd(
      test,
      vdb_path,
      output_dir=output_directory,
      elapsed_secs=warmup_time_secs,
      interval_secs=5)
    warmup_timeout_secs = warmup_time_secs + 300
    warmup_jobs = [
      vdbench_util.VdbenchJob(
        vm, warmup_cmd, cmd_description, warmup_timeout_secs)
      for vm in vms]
    vdbench_util.VdbenchUtil.execute_vdbench_jobs(
      warmup_jobs, max_jobs_per_node=max_jobs_per_node)

  @staticmethod
  def vdbench_vm_results_dir(test, vm_name=None, dir_prefix="vdbench_results"):
    """Get the path to the vdbench results directory for a given test.

    Construct a path for storing results that will be formed as:
    <test_dir>/<dir_prefix>, or <test_dir>/<dir_prefix>/<vm_name> if
    the optional argument 'vm_name' is specified.

    Args:
      test: BaseTest instance.
      vm_name: Optional vm_name.
      dir_prefix: String specifying the top level directory for the results.
        This should be used to group together different types of results.
        Prefill, warmup, and run results should be in separate directories.
    Returns:
      A path to the results directory.
    """
    args = [test.test_dir(), dir_prefix]
    if vm_name is not None:
      args.append(vm_name)
    return os.path.join(*args)

  @staticmethod
  def list_vdbench_vm_results_dirs(test):
    """Get a list of paths to the vdbench results directories for a given test.

    If the results directory for this test has not yet been created, an empty
    list is returned.

    Args:
      test: BaseTest instance.

    Returns:
      A list of paths to the results directories.
    """
    results_dir = ScenarioUtil.vdbench_vm_results_dir(test)
    # If the directory does not exist, return early with an empty list.
    if not os.path.exists(results_dir):
      log.debug("Cannot retrieve results subdirectories for test %s: %s does "
                "not exist", test.test_id(), results_dir)
      return []
    subdir_names = os.listdir(results_dir)
    subdir_paths = [os.path.join(results_dir, subdir_name)
                    for subdir_name in subdir_names]
    return [subdir_path for subdir_path in subdir_paths
            if os.path.isdir(subdir_path)]

  @staticmethod
  def get_vdbench_logfile(
      test,
      vm,
      remote_html_path=None):
    """Copy the Vdbench log file for a given test, and return the results.

    If the newest results cannot be copied, or if the VM is inaccessible, the
    most recently retrieved set of results will be returned; If no such result
    exists, an empty result is returned.

    Args:
      test: BaseTest instance.
      vm: VM from which the log file will be copied.
      remote_html_path: Path on the Curie agent where the Vdbench results
        log file is located.

    Returns:
      (datapoints, ioerrors)
        datapoints: list of dicts of vdbench performance records
        ioerrors: List of dicts of ioerrors
    """
    if remote_html_path is None:
      remote_html_path = vdbench_util.VdbenchUtil.vdbench_test_output_path(
        test)
    # Create the results directory, if necessary.
    results_dir = ScenarioUtil.vdbench_vm_results_dir(test, vm.vm_name())
    if not os.path.exists(results_dir):
      os.makedirs(results_dir)
      log.debug("Created %s", results_dir)
    datapoints = []
    ioerrors = []
    local_html_path = os.path.join(results_dir, "logfile.html")
    # If the VM is inaccessible, do not attempt to transfer.
    if not vm.vm_ip() or not vm.is_accessible():
      log.debug("VM: %s is inaccessible", vm.vm_name())
      success = False
    else:
      success = vm.transfer_from(remote_html_path, local_html_path, 300)
    # If the file does not exist, nothing can be added to test_result.
    if not os.path.isfile(local_html_path):
      CHECK(not success, "vm.transfer_from returned success, but %s does not "
                         "exist" % local_html_path)
      log.warning("No previously-collected results available for VM %s",
                  vm.vm_name())
    else:
      if not success:
        log.warning("Failed to fetch new Vdbench data for VM %s; A "
                    "previously-collected sample will be returned",
                    vm.vm_name())
      # Parse the Vdbench results, and insert them into test_result.
      datapoints, ioerrors = vdbench_util.VdbenchUtil.parse_vdbench_logfile(
        local_html_path)
    return datapoints, ioerrors

  @staticmethod
  def prepare_oltp_vm_active_result(active_result_pb, vm_num, result_id):
    """
    Produce a CurieTestResult protobuf with default components filled for a
    result showing whether VMs are active or inactive.

    Args:
      active_result_pb: (curie_test_pb2.CurieTestResult) returned from
        get_active_result_pb.
      vm_num: (int) identifier for a VM
      result_id: (str) the unique ID for this result

    Returns:
      test_result (CurieTestResult)
    """
    active_result_pb.name = "VM %d is performing IO" % vm_num
    active_result_pb.result_id = result_id
    active_result_pb.description = ("The plot shows whether or not the VM %d "
                                    "is performing IO over time." % vm_num)
    active_result_pb.data_2d.x_unit_type = \
      curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
    active_result_pb.data_2d.y_unit_type = \
      curie_test_pb2.CurieTestResult.Data2D.kBoolean
    active_result_pb.data_2d.x_label = "Time"
    active_result_pb.data_2d.y_label = "VM Performing IO"
    return active_result_pb

  @staticmethod
  def prepare_oltp_vm_ioerror_result(errors_result_pb, vm_num, result_id):
    """
    Produce a CurieTestResult protobuf with default components filled for a
    result showing whether VMs have accumulated IO Errors (EIO).

    Args:
      errors_result_pb: (curie_test_pb2.CurieTestResult) returned from
        get_errors_result_pb.
      vm_num: (int) identifier for a VM
      result_id: (str) the unique ID for this result

    Returns:
      test_result (CurieTestResult)
    """
    errors_result_pb.name = ("Cumulative number of IO errors encountered on"
                                 " VM %d" % vm_num)
    errors_result_pb.result_id = result_id
    errors_result_pb.description = ("The plots shows the cumulative number "
                                        "of EIO errors encountered by the "
                                        "VM %d" % vm_num)
    errors_result_pb.data_2d.x_unit_type = \
      curie_test_pb2.CurieTestResult.Data2D.kUnixTimestamp
    errors_result_pb.data_2d.y_unit_type = \
      curie_test_pb2.CurieTestResult.Data2D.kCount
    errors_result_pb.data_2d.x_label = "Time"
    errors_result_pb.data_2d.y_label = "Cumulative EIO Error Count"
    return errors_result_pb

  @staticmethod
  @CurieUtil.log_duration
  def append_cluster_stats(results_map, dir_name):
    """Write cluster results to disk, appending to any that already exist.

    If the output directory is empty, a new subdirectory will be created for
    each node. For each node, a bin (serialized protobuf) file will be created
    for each counter.

    If previously-collected results already exist, the new results will be
    appended to any existing bin files.

    A simple check for duplicate samples is performed, based on the epoch time
    of the last sample in the existing bin file. If a sample to be appended has
    an epoch time less than or equal to the last epoch in the file, it will be
    ignored.

    To read the results files back into a Python object, use
    read_cluster_stats.

    Args:
      results_map (dict): Results from Cluster.collect_performance_stats.
      dir_name (str): Top-level directory in which subdirectories and results
        are written. If it does not exist, it will be created. Can be absolute
        or relative to the current working directory.

    Returns:
      (int) Epoch time of latest appended sample. If no samples were appended,
        returns None.
    """

    max_appended_epoch_time = None
    for node_id in results_map:
      host_results = results_map[node_id]
      if host_results is None:
        # Error message already logged during query call.
        continue
      elif not host_results:
        # Call succeeded, but results list is empty.
        log.warning("No new stats data collected for '%s'", node_id)
        continue
      host_output_dir = os.path.join(dir_name, str(node_id))
      if not os.path.isdir(host_output_dir):
        os.makedirs(host_output_dir)
      for new_metric in host_results:
        # Pull the new timestamps and values out of the new metric.
        counter_name = MetricsUtil.metric_name(new_metric)
        new_t_v_pairs = zip(new_metric.timestamps, new_metric.values)
        # Read the existing metric from disk.
        file_name = ("%s_%s" %
                     (counter_name, new_metric.instance)).replace(".", "_")
        file_path = os.path.join(host_output_dir, "%s.bin" % file_name)
        # Initialize a metric with empty repeated values.
        existing_metric = CurieMetric()
        existing_metric.CopyFrom(new_metric)
        del existing_metric.timestamps[:]
        del existing_metric.values[:]
        if os.path.isfile(file_path):
          try:
            with open(file_path, "r") as f:
              existing_metric.ParseFromString(f.read())
          except (message.DecodeError, IOError):
            log.warning("Failed to decode %s - file will be overwritten",
                        file_path)
          else:
            # Remove duplicates.
            latest_existing_timestamp = -1
            if len(existing_metric.timestamps) > 0:
              latest_existing_timestamp = max(existing_metric.timestamps)
            new_t_v_pairs = [t_v_pair for t_v_pair in new_t_v_pairs
                             if t_v_pair[0] > latest_existing_timestamp]
        if not new_t_v_pairs:
          log.debug("No new %s data available for %s", counter_name, node_id)
          new_timestamps, new_values = [], []
        else:
          new_timestamps, new_values = zip(*new_t_v_pairs)
        existing_metric.timestamps.extend(new_timestamps)
        existing_metric.values.extend(new_values)
        try:
          serialized = existing_metric.SerializeToString()
        except message.EncodeError:
          # This can happen if the protobuf read from disk was invalid, but
          # did not throw a DecodeError when it was parsed.
          log.warning("Failed to serialize appended results to %s - file will "
                      "be overwritten", file_path)
          serialized = new_metric.SerializeToString()
        OsUtil.write_and_rename(file_path, serialized)
        if existing_metric.timestamps:
          max_appended_epoch_time = max(max_appended_epoch_time,
                                        max(existing_metric.timestamps))
    return max_appended_epoch_time

  @staticmethod
  def read_cluster_stats(dir_name):
    """Read cluster results from disk.

    If the output directory is empty, or all bin files are empty, an empty dict
    will be returned. There are no checks for duplicate values.

    Malformed files will be skipped by the parser.

    To write the results object to disk, use append_cluster_stats.

    Args:
      dir_name (str): Top-level directory from which to read the results. Can
        be absolute or relative to the current working directory.
    """
    results_map = {}
    for node_id in os.listdir(dir_name):
      results_map[node_id] = []
      host_output_dir = os.path.join(dir_name, str(node_id))
      bin_file_paths = glob.glob(os.path.join(host_output_dir, "*.bin"))
      for file_path in bin_file_paths:
        existing_metric = CurieMetric()
        with open(file_path, "r") as f:
          try:
            existing_metric.ParseFromString(f.read())
          except message.DecodeError:
            log.warning("Failed to decode %s", file_path)
          else:
            results_map[node_id].append(existing_metric)
    return MetricsUtil.sorted_results_map(results_map)

  @staticmethod
  def results_map_to_csv(results_map, header=True, newline="\n"):
    """Convert a results map to a CSV string.

    Args:
      results_map (dict): Dict of list of curie_metrics_pb2.CurieMetric. Top
        level dict keyed by node ID. List contains one entry per metric.
      header (bool): If True, the first line returned will be a header row.
      newline (str): Newline character.

    Returns:
      (str) CSV-formatted results.
    """
    columns = ("timestamp", "node_id", "metric_name", "instance", "value")
    csv_stringio = cStringIO.StringIO()
    if header:
      csv_stringio.write(",".join(columns) + newline)
    for node_id in results_map:
      host_results = results_map[node_id]
      if not host_results:
        # Error message already logged during append_cluster_stats.
        continue
      for metric in host_results:
        for timestamp, value in zip(metric.timestamps, metric.values):
          csv_stringio.write(",".join([str(timestamp),
                                       str(node_id),
                                       MetricsUtil.metric_name(metric),
                                       str(metric.instance),
                                       str(value)]) + newline)
    # Guarantee that csv_stringio is freed.
    try:
      return csv_stringio.getvalue()
    finally:
      csv_stringio.close()

  @staticmethod
  def create_remote_output_dir(vms, path):
    """Create an output directory for each vm in a list of vms.

    Any existing directory or file at the given path will be deleted and
    overwritten.

    Args:
      vms (list of CurieVM): VMs in which to create the output directory.
      path (str): Absolute path to the remote output directory.

    Raises:
      CurieTestException: If the operation fails.
    """
    rm_cmd_ids = []
    for vm in vms:
      cmd_id = "rm_%s_%d" % (vm.vm_name(), time.time() * 1e6)
      vm.execute_async(cmd_id, "rm -rf %s" % path)
      rm_cmd_ids.append((vm, cmd_id))
    for vm, cmd_id in rm_cmd_ids:
      vm.wait_for_cmd(cmd_id, 30, poll_secs=1)
    mkdir_cmd_ids = []
    for vm in vms:
      cmd_id = "mkdir_%s_%d" % (vm.vm_name(), time.time() * 1e6)
      vm.execute_async(cmd_id, "mkdir -p %s" % path)
      mkdir_cmd_ids.append((vm, cmd_id))
    for vm, cmd_id in mkdir_cmd_ids:
      vm.wait_for_cmd(cmd_id, 30, poll_secs=1)

  @staticmethod
  def wait_for_deadline(scenario, deadline_secs, description, grace_secs=30):
    """Wait until deadline is reached.

    Args:
      scenario (Scenario): Running scenario.
      deadline_secs (int): timestamp to wait for.
      description (str): description for log messages and exceptions.
      grace_secs (int): amount of time allowed to miss deadline by.
    """
    now_secs = int(time.time())
    if not now_secs <= deadline_secs:
      error_message = ("Cannot wait for a deadline that has already passed "
                       "deadline_secs: %d; now_secs: %d" % (
                         deadline_secs, now_secs))
      raise CurieTestException(error_message)
    while now_secs < deadline_secs:
      if scenario.should_stop():
        return
      time.sleep(1)
      now_secs = int(time.time())
    log.debug("deadline_secs: %d; now_secs: %d" % (deadline_secs, now_secs))
    if now_secs <= deadline_secs + grace_secs:
      return
    else:
      error_message = ("Missed %s deadline by %ds with grace of %ds" %
                       (description, now_secs - deadline_secs, grace_secs))
      raise CurieTestException(error_message)
