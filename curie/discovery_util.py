#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Adapted from executables 'discover_clusters', 'discover_nodes'.
#
import itertools
import logging
import re
import socket

# pylint can't correctly process pyVim/pyVmomi
# pylint: disable=no-name-in-module
import sys
from pyVim.connect import SmartConnectNoSSL, Disconnect
from pyVmomi import vim
# pylint: enable=no-name-in-module
from pywbem import WBEMConnection

from curie.acropolis_cluster import AcropolisCluster
from curie.curie_error_pb2 import CurieError
from curie.curie_types_pb2 import LogicalNetwork
from curie.exception import CurieException, CurieTestException
from curie.node import get_power_management_util
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.nutanix_rest_api_client import PrismAPIVersions
from curie.oob_management_util import OobInterfaceType, OobVendor
from curie.util import CurieUtil, get_optional_vim_attr, get_cluster_class
from curie.vsphere_vcenter import VsphereVcenter
from curie.vmm_client import VmmClient

log = logging.getLogger(__name__)


class DiscoveryUtil(object):

  # Map of vendors to a list of regular expressions matching possible
  # manufacturer strings for each vendor.
  VENDOR_ALIAS_RE_MAP = {OobVendor.kSupermicro: [r"super ?micro"],
                         OobVendor.kDell: [r"\bdell(\b| )"]}

  # Prism API v1 /hosts attribute which may be used in determining whether a
  # cluster is running CE.
  CE_HOST_ATTR_KEY = "blockModelName"

  # Corresponding value for 'CE_HOST_ATTR_KEY' on a host which indicates the
  # host is running CE.
  CE_HOST_ATTR_VAL = "CommunityEdition"


  @staticmethod
  def _filter_child_entities(entities, allowed_types):
    """
    Filters list of 'entities' by types 'allowed_types'.

    Args:
      entities (list<vim.ManagedObject>): List of entities to filter.
      allowed_types (type|tuple<type>): Type or types by which to filter.

    Returns:
      (list<vim.ManagedObject>, list<vim.ManagedObject>) Pair of filtered
        entities and ignored entities.
    """
    if not isinstance(allowed_types, tuple):
      allowed_types = (allowed_types, )
    children, ignored = [], []
    for entity in entities:
      if isinstance(entity, allowed_types):
        children.append(entity)
      else:
        ignored.append(entity)

    if ignored:
      output = ["Skipping the following entities with types outside"]
      output.append("'%s'" % ", ".join([t._wsdlName for t in allowed_types]))
      output.append(": %s" % ", ".join(map(
        lambda e: "'%s' (%s)" % (e.name, e.__class__), ignored)))

      log.warning(" ".join(output))

    return children, ignored

  @staticmethod
  def compute_vcenter_cluster_inventory(vim_folder):
    """
    Discovers clusters present in the 'vim.Folder' object 'vim_folder'.

    Args:
      vim_folder (vim.Folder): vCenter folder whose inventory to return.

    Returns:
      (dict): cluster inventory mapping from
        (vim_datacenter_name (str), vim.Datacenter) ->
          (vim_cluster_name (str), vim.ClusterComputeInstance) ->
            (vim_datastore_name (str), vim.Datastore)
    """
    # Note that we save the name of each vim object in the map to avoid
    # redundant vSphere API calls later.
    vim_inventory_map = {}
    vim_folder_dc_list, ignored = DiscoveryUtil._filter_child_entities(
      vim_folder.childEntity, (vim.Datacenter, vim.Folder))

    if not vim_folder_dc_list:
      log.warning("No datacenters or folders present in folder '%s'",
                  vim_folder.name)
      return vim_inventory_map

    for vim_child in vim_folder_dc_list:
      if isinstance(vim_child, vim.Folder):
        vim_inventory_map.update(
          DiscoveryUtil.compute_vcenter_cluster_inventory(vim_child))
        continue

      vim_dc = vim_child
      vim_dc_tuple = (vim_dc.name, vim_dc)
      vim_inventory_map[vim_dc_tuple] = {}

      vim_clusters, ignored = DiscoveryUtil._filter_child_entities(
        vim_dc.hostFolder.childEntity, vim.ClusterComputeResource)

      if not vim_clusters:
        log.warning("No clusters present in datacenter '%s'", vim_dc.name)
        continue

      for vim_cluster in vim_clusters:
        vim_cluster_tuple = (vim_cluster.name, vim_cluster)
        vim_inventory_map[vim_dc_tuple][vim_cluster_tuple] = []
        # Note: it might be useful in the future to filter out local datastores
        # but that will require additional vSphere API calls which will make
        # discovery slower. The user presumably already will recognize the name
        # of the datastore intended to be used with X-Ray.
        for vim_datastore in vim_cluster.datastore:
          vim_datastore_tuple = (vim_datastore.name, vim_datastore)
          vim_inventory_map[vim_dc_tuple][vim_cluster_tuple].append(
            vim_datastore_tuple)
    return vim_inventory_map

  @staticmethod
  def lookup_node_bmc_info(vim_node):
    """
    Looks up BMC data for 'vim_node'.

    Args:
      vim_node (vim.HostSystem): Host whose IPMI data to fetch.

    Returns:
      (str, Node.NodeOutOfBandManagementInfo.Vendor) IPv4 Address and
        vendor for the node's BMC.

    Raises:
      CurieException<kInternalError> (with original trace) on error
    """
    # Catch exceptions while fetching BMC data and re-raise as a
    # CurieException preserving the original stack.
    try:
      # Acquire ticket whose 'sessionId' may be used as username and password
      # when authenticating with CIMOM.
      sid = vim_node.AcquireCimServicesTicket().sessionId

      # According to pyVim documentation, the hostname is guaranteed to be the
      # IP or DNS name for the host.
      cimom_conn = WBEMConnection("https://%s" % vim_node.name, (sid, sid),
                                  no_verification=True)

      ipmi_resp = cimom_conn.EnumerateInstances("OMC_IPMIIPProtocolEndpoint")
      assert len(ipmi_resp) == 1,\
        "Unable to retrieve IPMI data for host '%s'" % vim_node.name

      bmc_fw_resp = cimom_conn.EnumerateInstances("OMC_MCFirmwareIdentity")
      assert bmc_fw_resp, \
        "Unable to retrieve BMC firmware data for host '%s'" % vim_node.name
    except Exception as exc:
      orig_trace = sys.exc_info()[2]
      raise CurieException(
        CurieError.kInternalError,
        "Failed to discover BMC data: %s" % exc), None, orig_trace

    # Catch exceptions while parsing BMC data and re-raise as a CurieException
    # preserving the original stack.
    try:
      ipmi_ip = ipmi_resp[0]["IPv4Address"]

      # If multiple instances are found for MCFirmwareIdentity, ensure that the
      # vendor is consistent.
      vendor_str_set = set()
      for fw_id in bmc_fw_resp:
        vendor_str_set.add(str(fw_id["Manufacturer"]).strip().lower())
      assert len(vendor_str_set) == 1, \
        "Ambiguous BMC vendor. Found vendors: %s" % ", ".join(vendor_str_set)
      vendor_str = vendor_str_set.pop()

      # Try matching 'vendor_str' against regex for known vendors. Fallback to
      # setting 'kUnknownVendor'.
      for vendor, alias_re_list in \
          DiscoveryUtil.VENDOR_ALIAS_RE_MAP.items():
        if any(map(lambda alias_re:
                   re.search(alias_re, vendor_str), alias_re_list)):
          break
      else:
        vendor = OobVendor.kUnknownVendor
    except Exception as exc:
      orig_trace = sys.exc_info()[2]
      raise CurieException(
        CurieError.kInternalError,
        "Failed to process BMC data: %s" % exc), None, orig_trace

    return ipmi_ip, vendor

  #============================================================================
  # Discovery V2
  #============================================================================

  @staticmethod
  def discover_clusters_prism(address, username, password, ret):
    """
    Args:
      address (str): Address of the management server.
      username (str): Name of user of the management server.
      password (str): Password of user of the management server
      ret (DiscoverClustersV2Ret): Return proto to be populated.
    """
    api_client = NutanixRestApiClient(host=address,
                                      api_user=username,
                                      api_password=password,
                                      timeout_secs=10)
    coll = ret.cluster_inventory.cluster_collection_vec.add()
    coll.name = "Prism"

    networks = api_client.networks_get()
    network_proto_list = []
    for net in networks.get("entities", []):
      net_info = LogicalNetwork()
      net_info.id = net["name"]  # Curie expects the network by name
      net_info.name = net["name"]
      network_proto_list.append(net_info)

    containers = api_client.containers_get()
    cluster_uuid_container_map = dict(
      (cid, list(ctrs)) for (cid, ctrs) in
      itertools.groupby(containers.get("entities", []),
                        lambda ctr: ctr["clusterUuid"]))

    clusters = api_client.clusters_get()

    cluster_json_list = clusters.get("entities", [])
    if len(cluster_json_list) == 1:
      # Assume that if we're only discovering one cluster, we are expecting
      # the Prism management address to match the cluster virtual IP.
      # TODO (jklein): This needs to change if we start supporting PC.
      cluster_json = cluster_json_list[0]
      cluster_vip = cluster_json.get("clusterExternalIPAddress")
      if cluster_vip is None:
        raise CurieException(
          CurieError.kInvalidParameter,
          "Cluster '%s' does not appear to be configured with a virtual IP"
          % cluster_json["name"])
      # NB: If the VIP does not match the provided Prism address, it will be
      # corrected during an UpdateAndValidateCluster RPC.

    for cluster_json in cluster_json_list:
      cluster_info = coll.cluster_vec.add()
      cluster_info.id = cluster_json["clusterUuid"]
      cluster_info.name = cluster_json["name"]
      cluster_info.clustering_software.type = \
          cluster_info.clustering_software.kNutanix
      cluster_info.clustering_software.version = cluster_json.get(
        "fullVersion", "")
      cluster_info.management_server.type = \
          cluster_info.management_server.kPrism
      cluster_info.management_server.version = \
          cluster_info.clustering_software.version

      for ctr in cluster_uuid_container_map[cluster_info.id]:
        storage_info = cluster_info.storage_info_vec.add()
        storage_info.name = ctr["name"]
        storage_info.id = ctr["containerUuid"]
        storage_info.kind = storage_info.kNutanixContainer
      cluster_info.network_info_vec.extend(network_proto_list)

  @staticmethod
  def discover_clusters_vcenter(address, username, password, ret):
    """
    Args:
      address (str): Address of the management server.
      username (str): Name of user of the management server.
      password (str): Password of user of the management server
      ret (DiscoverClustersV2Ret): Return proto to be populated.
    """
    conn = None
    try:
      conn = SmartConnectNoSSL(host=address, user=username, pwd=password)
      log.debug("Connected to vCenter %s", address)
      vim_inventory_map = \
          DiscoveryUtil.compute_vcenter_cluster_inventory(conn.content.rootFolder)
      DiscoveryUtil._fill_vcenter_cluster_inventory_v2(
        vim_inventory_map, ret)
      for cluster_collection in ret.cluster_inventory.cluster_collection_vec:
        for cluster in cluster_collection.cluster_vec:
          cluster.management_server.type = cluster.management_server.kVcenter
          cluster.management_server.version = conn.content.about.version
    except socket.error:
      # The plain old socket errors don't indicate which connection.
      raise CurieException(CurieError.kInternalError,
        "Could not connect to vCenter at %s" % address)
    except vim.fault.InvalidLogin:
      # The failures back from vSphere don't provide the best experience.
      raise CurieException(CurieError.kInternalError,
        "Incorrect username or password for vCenter at %s" %address)
    finally:
      if conn is not None:
        Disconnect(conn)

  @staticmethod
  def discover_clusters_vmm(address, username, password, ret):
    """
    Args:
      address (str): Address of the management server.
      username (str): Name of user of the management server.
      password (str): Password of user of the management server
      ret (DiscoverClustersV2Ret): Return proto to be populated.
    """
    vmm_client = VmmClient(address=address, username=username,
                           password=password)
    with vmm_client:
      log.debug("Connected to VMM %s", address)
      clusters = vmm_client.get_clusters()
      library_shares = vmm_client.get_library_shares()
      log.debug(library_shares)  # TODO(ryan.hardin) Remove.

    log.debug(clusters)  # TODO(ryan.hardin) Remove.
    cluster_collection = ret.cluster_inventory.cluster_collection_vec.add()
    cluster_collection.name = "HyperV"
    # TODO(ryan.hardin): Set ClusteringSoftware Type (i.e. kNutanix or kS2D).

    for cluster in clusters:
      cluster_pb = cluster_collection.cluster_vec.add()
      cluster_pb.id = cluster["name"]
      cluster_pb.name = cluster["name"]

      for storage in cluster["storage"]:
        logical_storage_pb = cluster_pb.storage_info_vec.add()
        logical_storage_pb.id = storage["path"]
        logical_storage_pb.name = storage["name"]
        logical_storage_pb.kind = logical_storage_pb.kHypervStorage

      for network in cluster["networks"]:
        logical_network_pb = cluster_pb.network_info_vec.add()
        logical_network_pb.id = network
        logical_network_pb.name = network

      for library_share in library_shares:
        library_share_pb = cluster_pb.library_shares.add()
        library_share_pb.name = library_share.get("name")
        library_share_pb.path = library_share.get("path")
        library_share_pb.server = library_share.get("server")

    # No return value since 'ret' is modified.
    log.debug(ret)  # TODO(ryan.hardin) Remove.
    return None

  @staticmethod
  def _fill_vcenter_cluster_inventory_v2(vim_inventory_map, ret):
    """
    Populate the output protobuf 'ret' with the cluster inventory.

    Args:
      vim_inventory_map (dict): mapping from
        (vim_datacenter_name (str), vim.Datacenter) ->
          (vim_cluster_name (str), vim.ClusterComputeInstance) ->
            (vim_datastore_name (str), vim.Datastore)
      for all clusters managed by the vCenter server.
      ret (DiscoverClustersV2Ret): Output protobuf to populatae.
    """
    ret.cluster_inventory.SetInParent()
    vim_datacenter_tuples = vim_inventory_map.keys()
    vim_datacenter_tuples.sort(key=lambda t: t[0])
    for (dc_name, vim_dc) in vim_datacenter_tuples:
      cluster_collection = ret.cluster_inventory.cluster_collection_vec.add()
      cluster_collection.id = dc_name
      cluster_collection.name = dc_name
      vim_cluster_tuples = vim_inventory_map[(dc_name, vim_dc)].keys()
      vim_cluster_tuples.sort(key=lambda t: t[0])
      for (cluster_name, vim_cluster) in vim_cluster_tuples:
        cluster = cluster_collection.cluster_vec.add()
        cluster.name = cluster_name
        cluster.id = cluster_name
        vim_datastore_tuples = vim_inventory_map[(dc_name, vim_dc)][
          (cluster_name, vim_cluster)]
        vim_datastore_tuples.sort(key=lambda t: t[0])
        ds_type_set = set()
        for ds_name, vim_ds in vim_datastore_tuples:
          storage_info = cluster.storage_info_vec.add()
          storage_info.id = storage_info.name = ds_name
          storage_info.kind = storage_info.kEsxDatastore
          storage_info.type = vim_ds.summary.type
          ds_type_set.add(storage_info.type)
        if len(ds_type_set) == 1 and "vsan" in ds_type_set:
          cluster.clustering_software.type = cluster.clustering_software.kVsan
          # TODO (jklein): Version to be computed by canonical mapping from
          # ESX version.
          cluster.clustering_software.version = ""
        vim_host_set = set(vim_cluster.host)
        for vim_network in vim_dc.network:
          # Only expose networks available on all cluster nodes.
          if vim_host_set.issubset(set(vim_network.host)):
            net_info = cluster.network_info_vec.add()
            net_info.id = net_info.name = vim_network.name

  @staticmethod
  def discover_nodes_prism(arg, ret):
    """
    See 'DiscoveryUtil.handle_nodes_discovery_v2' for info.
    """
    cli = NutanixRestApiClient.from_proto(arg.mgmt_server.conn_params)
    # TODO (jklein): Optimize loops here. Currently OK as nothing is doing
    # discovery on more than one cluster.

    for target_cluster in arg.cluster_collection.cluster_vec:
      cluster_uuid = AcropolisCluster.identifier_to_cluster_uuid(
        cli, target_cluster.name)

      node_coll = ret.node_collection_vec.add()
      node_coll.cluster_id = cluster_uuid

      for host in cli.hosts_get().get("entities", []):
        if host["clusterUuid"] != cluster_uuid:
          continue
        node = node_coll.node_vec.add()
        node.id = host["uuid"]
        # Use UUID as name as this will remain constant. In failure
        # scenarios, hosts revert to an IP address from the name for some
        # reason. See XRAY-276.
        node.name = host["uuid"]

        # Have already validated that either both fields are set or not.
        if (arg.oob_info.conn_params.username and
            arg.oob_info.type != OobInterfaceType.kNone):
          node.oob_info.CopyFrom(arg.oob_info)
          node.oob_info.conn_params.address = host["ipmiAddress"]
          if host.get("bmcModel") in ["X9_ATEN", "X10_ATEN"]:
            node.oob_info.vendor = node.oob_info.kSupermicro
          else:
            node.oob_info.vendor = node.oob_info.kUnknownVendor
        # We only support homogeneous AHV clusters via Prism.
        if host.get("hypervisorType") != "kKvm":
          raise CurieException(
            CurieError.kInvalidParameter,
            "Provided cluster is mixed hypervisor")
        node.hypervisor.type = node.hypervisor.kAhv
        node.hypervisor.version = DiscoveryUtil._get_hyp_version_for_host(host)


  @staticmethod
  def discover_nodes_vcenter(arg, ret):
    """
    See 'DiscoveryUtil.handle_nodes_discovery_v2' for info.
    """
    conn_params = arg.mgmt_server.conn_params
    with VsphereVcenter.from_proto(conn_params) as vcenter:
      vim_dc = vcenter.lookup_datacenter(arg.cluster_collection.name)

      for target_cluster in arg.cluster_collection.cluster_vec:
        vim_cluster = vcenter.lookup_cluster(vim_dc, target_cluster.name)
        node_coll = ret.node_collection_vec.add()
        node_coll.cluster_id = vim_cluster.name
        for vim_node in vim_cluster.host:
          node = node_coll.node_vec.add()
          # According to pyVim documentation, the hostname is guaranteed to be
          # the IP or DNS name for the host.
          node.id = vim_node.name
          node.name = vim_node.name
          node.hypervisor.type = node.hypervisor.kEsx
          node.hypervisor.version = vcenter.get_esx_versions_for_vim_host(
            vim_node)[0]

          # Have already validated that either both fields are set or not.
          if (arg.oob_info.conn_params.username and
              arg.oob_info.type != OobInterfaceType.kNone):
            node.oob_info.CopyFrom(arg.oob_info)
            node.oob_info.conn_params.address, node.oob_info.vendor = \
                DiscoveryUtil.lookup_node_bmc_info(vim_node)

  @staticmethod
  def discover_nodes_vmm(arg, ret):
    """
    See 'DiscoveryUtil.handle_nodes_discovery_v2' for info.
    """
    conn_params = arg.mgmt_server.conn_params
    vmm_client = VmmClient(address=conn_params.address,
                           username=conn_params.username,
                           password=conn_params.password)
    nodes_data = {}
    with vmm_client:
      log.debug("Connected to VMM %s", conn_params.address)
      for target_cluster in arg.cluster_collection.cluster_vec:
        nodes_data[target_cluster.name] = \
          vmm_client.get_nodes(target_cluster.name)

    log.debug(nodes_data)  # TODO(ryan.hardin) Remove.
    for cluster_name, node_list in nodes_data.iteritems():
      node_collection = ret.node_collection_vec.add()
      node_collection.cluster_id = cluster_name

      for node_data in node_list:
        node = node_collection.node_vec.add()
        node.id = node_data["fqdn"]
        node.name = node_data["fqdn"]
        node.hypervisor.type = node.hypervisor.kHyperv
        node.hypervisor.version = node_data["version"]

        # Have already validated that either both fields are set or not.
        if (arg.oob_info.conn_params.username and
            arg.oob_info.type != OobInterfaceType.kNone):
          node.oob_info.CopyFrom(arg.oob_info)
          node.oob_info.conn_params.address = node_data.get("bmc_address", "")
          node.oob_info.vendor = OobVendor.kUnknownVendor
          if not node.oob_info.conn_params.address:
            oob_type_name = OobInterfaceType.InterfaceType.Name(
              arg.oob_info.type)
            raise RuntimeError(
              "Power management type '%s' selected, but no power management "
              "address configured in VMM for node '%s' in cluster '%s'" %
              (oob_type_name[1:], node.name, cluster_name))

    # No return value since 'ret' is modified.
    log.debug(ret)  # TODO(ryan.hardin) Remove.
    return None

  @staticmethod
  def update_cluster_virtual_ip(cluster_pb):
    """
    Updates 'prism_host' to correspond to the cluster virtual IP.

    The 'prism_host' field is set for management and clustering software as
    appropriate for the target hypervisor.

    Returns:
      True if 'prism_host' was updated, else False.

    Raises:
      CurieException<kInvalidParameter>: 'cluster_pb' is a Nutanix cluster but
        does not have a Virtual IP.
    """
    if not cluster_pb.cluster_software_info.HasField("nutanix_info"):
      return False

    prism_proto = None
    ntnx_proto = cluster_pb.cluster_software_info.nutanix_info
    prism_user = ntnx_proto.decrypt_field("prism_user")
    prism_password = ntnx_proto.decrypt_field("prism_password")
    c_uuid = ntnx_proto.cluster_uuid

    if cluster_pb.cluster_management_server_info.HasField("prism_info"):
      prism_proto = cluster_pb.cluster_management_server_info.prism_info
      client = NutanixRestApiClient.from_proto(prism_proto, timeout_secs=10)
      cluster_json = client.clusters_get(cluster_id=c_uuid)
    else:
      cvm_addresses = []
      if cluster_pb.cluster_management_server_info.HasField("vmm_info"):
        vmm_info = cluster_pb.cluster_management_server_info.vmm_info
        vmm_client = VmmClient(address=vmm_info.vmm_server,
                               username=vmm_info.vmm_user,
                               password=vmm_info.vmm_password)
        with vmm_client:
          vms = vmm_client.get_vms(cluster_name=vmm_info.vmm_cluster_name)
          for vm in vms:
            if VmmClient.is_nutanix_cvm(vm):
              if (VmmClient.is_powered_on(vm)):
                log.debug("Found CVM '%s' with IPs: %s", vm["name"], vm["ips"])
                cvm_addresses.extend(vm["ips"])
              else:
                log.debug("Skipping CVM '%s' because it is not powered on.", vm["name"])
      else:
        node_ids = [node.id for node in cluster_pb.cluster_nodes]
        # NB: We currently have an asymmetrical input for Prism credentials
        # depending on whether they're considered as management software or
        # clustering software. In the latter case, which is when 'nutanix_info'
        # is set but not 'prism_info', the user is not asked for a Prism host.
        # In this case, we discover CVMs via vCenter, attempt to connect to Prism
        # on each in sequence until successful, and then query the virtual IP.
        mgmt_info = cluster_pb.cluster_management_server_info.vcenter_info
        with VsphereVcenter.from_proto(mgmt_info) as vcenter:
          vim_dc = vcenter.lookup_datacenter(mgmt_info.vcenter_datacenter_name)
          vim_cluster = vcenter.lookup_cluster(vim_dc,
                                               mgmt_info.vcenter_cluster_name)
          for vim_cvm in (vm for vm in vcenter.lookup_vms(vim_cluster)
                          if vcenter.vim_vm_is_nutanix_cvm(vm)):
            vim_host = get_optional_vim_attr(vim_cvm.runtime, "host")
            if vim_host:
              if vim_host.name in node_ids:
                cvm_address = vcenter.get_vim_vm_ip_address(vim_cvm)
                if cvm_address:
                  log.debug("Found CVM '%s' with address '%s'" %
                            (vim_cvm.name, cvm_address))
                  cvm_addresses.append(cvm_address)
              else:
                log.debug("Skipping CVM '%s'; Host '%s' is not in the "
                          "metadata" % (vim_cvm.name, vim_host.name))
      # We run Nutanix API only against powered on CMVs
      for cvm_address in cvm_addresses:
        client = NutanixRestApiClient(cvm_address, prism_user, prism_password)
        try:
          cluster_json = client.clusters_get(cluster_id=c_uuid, max_retries=3)
        except CurieException:
          log.warning("Unable to query CVM with IP '%s'",
                      cvm_address, exc_info=True)
        else:
          break
      else:
        log.error("Failed to query Prism for cluster metadata on any CVM")
        return False

    if "clusterExternalIPAddress" in cluster_json:
      cluster_name = cluster_json.get("name")
      cluster_vip = cluster_json["clusterExternalIPAddress"]
    elif "entities" in cluster_json:
      cluster_data = cluster_json["entities"][0]
      cluster_name = cluster_data.get("name")
      cluster_vip = cluster_data["clusterExternalIPAddress"]
    else:
      raise CurieException(
        CurieError.kInvalidParameter,
        "Unrecognized response from NutanixRestApiClient.clusters_get")
    if not cluster_vip:
      raise CurieException(
        CurieError.kInvalidParameter,
        "Cluster '%s' does not appear to be configured with a virtual IP "
        "(received '%s')" % (cluster_name, cluster_vip))
    else:
      log.debug("Identified Nutanix cluster virtual IP address: '%s'",
                cluster_vip)
    ntnx_proto.prism_host = cluster_vip
    if prism_proto:
      prism_proto.prism_host = cluster_vip
    return True

  @staticmethod
  def update_cluster_version_info(cluster_pb):
    """
    Args:
      cluster_pb (curie_server_state_pb2.CurieSettings.Cluster): Populated
        cluster proto whose data to update.
    """
    mgmt_info = cluster_pb.cluster_management_server_info
    if mgmt_info.HasField("prism_info"):
      if not (cluster_pb.cluster_software_info.HasField("nutanix_info") and
              cluster_pb.cluster_hypervisor_info.HasField("ahv_info")):
        raise CurieException(
          CurieError.kInvalidParameter,
          "Prism managed clusters require both 'ahv_info' and 'nutanix_info' "
          "to be specified")

      DiscoveryUtil._update_cluster_version_info_prism(cluster_pb)
    elif mgmt_info.HasField("vcenter_info"):
      DiscoveryUtil._update_cluster_version_info_vcenter(cluster_pb)
    elif mgmt_info.HasField("vmm_info"):
      DiscoveryUtil._update_cluster_version_info_vmm(cluster_pb)
    else:
      raise CurieException(CurieError.kInvalidParameter,
                            "Missing management server info")
    return cluster_pb

  @staticmethod
  def validate_oob_config(cluster_pb):
    """
    Validates provided out-of-band management config in 'cluster_pb'.

    Args:
      cluster_pb (curie_server_state_pb2.CurieSettings.Cluster): Populated
        cluster proto whose data to validate.

    Raises: CurieException on any error encountered.
    """
    for node in cluster_pb.cluster_nodes:
      if (node.node_out_of_band_management_info.interface_type
          == node.NodeOutOfBandManagementInfo.kNone):
        continue

      if not CurieUtil.ping_ip(
        node.node_out_of_band_management_info.ip_address):
        raise CurieException(CurieError.kInternalError,
          "Out-of-band management at '%s' is unreachable" %
          node.node_out_of_band_management_info.ip_address)

      oob_util = get_power_management_util(
        node.node_out_of_band_management_info)
      try:
        oob_util.get_chassis_status()
      except CurieException as exc:
        # Not all OoB implementations will explicitly distinguish an
        # authentication error. Tailor error message appropriately.
        if exc.error_code == CurieError.kOobAuthenticationError:
          raise CurieException(
            error_code=exc.error_code,
            error_msg="Out-of-band management authentication failed for node"
                      " %s" % node.id)
        else:
          raise CurieException(
            error_code=exc.error_code,
            error_msg="Failed to query out-of-band management for node %s. "
                      "Please re-enter credentials" % node.id)

  @staticmethod
  def validate_host_connectivity(cluster_pb):
    cluster_cls = get_cluster_class(cluster_pb)
    cluster = cluster_cls(cluster_pb)
    for node in cluster.nodes():
      if not CurieUtil.ping_ip(node.node_ip()):
        raise CurieException(CurieError.kInternalError,
                              "Host %s - %s not reachable." %
                              (node.node_id(), node.node_ip()))

  @staticmethod
  def _update_cluster_version_info_prism(cluster_pb):
    """
    See 'DiscoveryUtil.update_cluster_version_info' for info.
    """
    mgmt_info = cluster_pb.cluster_management_server_info.prism_info
    software_info = cluster_pb.cluster_software_info.nutanix_info
    hyp_info = cluster_pb.cluster_hypervisor_info.ahv_info

    cli = NutanixRestApiClient.from_proto(mgmt_info, timeout_secs=10)

    DiscoveryUtil._update_cluster_version_info_nos(cli, cluster_pb)
    mgmt_info.prism_version = software_info.version

    for host in cli.hosts_get().get("entities", []):
      if host["clusterUuid"] != software_info.cluster_uuid:
        continue

      # We only support homogeneous AHV clusters via Prism.
      if host.get("hypervisorType") != "kKvm":
        raise CurieException(CurieError.kInvalidParameter,
                              "Specified cluster is mixed hypervisor")

      # Strip any "Nutanix " prefix from AHV version strings.
      curr_hyp_version = re.sub(
        "^Nutanix ", "", DiscoveryUtil._get_hyp_version_for_host(host))
      hyp_info.version.extend([curr_hyp_version])

  @staticmethod
  def _update_cluster_version_info_vcenter(cluster_pb):
    """
    See 'DiscoveryUtil.update_cluster_version_info' for info.
    """
    mgmt_info = cluster_pb.cluster_management_server_info.vcenter_info
    hyp_info = cluster_pb.cluster_hypervisor_info.esx_info

    with VsphereVcenter.from_proto(mgmt_info) as vcenter:
      vim_dc = vcenter.lookup_datacenter(mgmt_info.vcenter_datacenter_name)
      vim_cluster = vcenter.lookup_cluster(vim_dc,
                                           mgmt_info.vcenter_cluster_name)
      if vim_cluster is None:
        raise CurieException(CurieError.kInvalidParameter,
                              "Cluster not found in specified vCenter")

      esx_version_pairs = vcenter.get_esx_versions(vim_cluster)
      hyp_info.version.extend(pair[0] for pair in esx_version_pairs)
      hyp_info.build.extend(pair[1] for pair in esx_version_pairs)

      mgmt_info.vcenter_version, mgmt_info.vcenter_build = \
          vcenter.get_vcenter_version_info()

      if cluster_pb.cluster_software_info.HasField("nutanix_info"):
        cvms = [vim_vm for vim_vm in vcenter.lookup_vms(vim_cluster)
                if vcenter.vim_vm_is_nutanix_cvm(vim_vm)]
        if not cvms:
          raise CurieException(
            CurieError.kInvalidParameter,
            "Unable to locate any CVMs on cluster. Is this a Nutanix cluster?")
        for cvm in cvms:
          ip = get_optional_vim_attr(cvm.guest, "ipAddress")
          if ip and CurieUtil.is_ipv4_address(ip):
            break
        else:
          raise CurieException(
            CurieError.kInvalidParameter,
            "Unable to locate any CVMs with IPv4 addresses on cluster")

        software_info = cluster_pb.cluster_software_info.nutanix_info
        cli = NutanixRestApiClient(
          ip,
          software_info.decrypt_field("prism_user"),
          software_info.decrypt_field("prism_password"))
        DiscoveryUtil._update_cluster_version_info_nos(cli, cluster_pb)

  @staticmethod
  def _update_cluster_version_info_vmm(cluster_pb):
    """
    See 'DiscoveryUtil.update_cluster_version_info' for info.
    """
    mgmt_info = cluster_pb.cluster_management_server_info.vmm_info
    hyp_info = cluster_pb.cluster_hypervisor_info.hyperv_info

    vmm_client = VmmClient(address=mgmt_info.vmm_server,
                           username=mgmt_info.vmm_user,
                           password=mgmt_info.vmm_password)
    with vmm_client:
      cluster = vmm_client.get_clusters(
        cluster_name=mgmt_info.vmm_cluster_name)[0]
      mgmt_info.vmm_version = vmm_client.get_vmm_version()
      nodes = vmm_client.get_nodes(mgmt_info.vmm_cluster_name)
      hyp_info.version.extend(node["version"] for node in nodes)
    if cluster_pb.cluster_software_info.HasField("nutanix_info"):
      software_info = cluster_pb.cluster_software_info.nutanix_info
      cli = NutanixRestApiClient(
        software_info.prism_host,
        software_info.decrypt_field("prism_user"),
        software_info.decrypt_field("prism_password"))
      DiscoveryUtil._update_cluster_version_info_nos(cli, cluster_pb)

  @staticmethod
  def _update_cluster_version_info_nos(prism_client, cluster_pb):
    """
    Updates 'cluster_pb' with NOS-specific version data.

    Args:
      prism_client (NutanixRestApiClient): API client with which to query data.
      cluster_pb (curie_server_state_pb2.CurieSettings.Cluster): Cluster to
        populate.

    Raises:
      CurieException<kInternalError> If we are unable to determine the cluster
      UUID.
    """
    software_info = cluster_pb.cluster_software_info.nutanix_info
    metadata = prism_client.get_nutanix_metadata()
    if metadata.cluster_uuid is None:
      raise CurieException(CurieError.kInternalError,
                           "Unable to determine UUID for Nutanix cluster")
    if metadata.version:
      software_info.version = PrismAPIVersions.get_aos_short_version(
        metadata.version)
    software_info.cluster_uuid = metadata.cluster_uuid
    software_info.cluster_incarnation_id = (
      metadata.cluster_incarnation_id or "")

  @staticmethod
  def _get_hyp_version_for_host(host):
    """

    Args:
      host (dict): Host data returned via Prism API.

    Returns:
      (str) full hypervisor version. If 'host' is running CE, the version
        will be updated to include 'CE' in the string.
    """
    hyp_version = host.get("hypervisorFullName", "")
    if not hyp_version:
      raise CurieTestException(
        cause=
        "Cannot get hypervisor name from node: %s."
        % host.get("name", "Unknown"),
        impact=
        "Cannot run discovery on cluster.",
        corrective_action=
        "Please check that all of the nodes in the cluster metadata "
        "are powered on and running in normal state."
      )
    # CE only applicable for AHV clusters and hence we don't need to check
    # for CE in other discovery methods.
    if (host.get(DiscoveryUtil.CE_HOST_ATTR_KEY) ==
        DiscoveryUtil.CE_HOST_ATTR_VAL):
      hyp_version = re.sub(r"^(Nutanix |)", r"\1CE ", hyp_version)

    return hyp_version
