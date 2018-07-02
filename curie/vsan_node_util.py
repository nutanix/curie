#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# VSAN Specific node utilities.
#
# VSAN reference:
#  http://www.vmware.com/files/pdf/products/vsan/VSAN-Troubleshooting-Reference-Manual.pdf

import logging

from pyVmomi import vmodl

from curie.node_util import NodeUtil
from curie.vsphere_vcenter import VsphereVcenter

log = logging.getLogger(__name__)


class VsanNodeUtil(NodeUtil):

  @classmethod
  def _use_handler(cls, node):
    """Returns True if 'node' should use this class as its handler."""
    software_info = node.cluster().metadata().cluster_software_info
    return software_info.HasField("vsan_info")

  def __init__(self, node):
    self.__node = node
    self.__cluster_metadata = self.__node.cluster().metadata()
    self.__vcenter_info = \
      self.__cluster_metadata.cluster_management_server_info.vcenter_info

  def is_ready(self):
    """See 'NodeUtil.is_ready' documentation for further details.

    Confirms node is ready by requesting node's health info from Vsphere
    corresponding to the node.

    Raises:
      CurieTestException: If the VsanNodeUtil's node is not found (passed
                           through from __get_vim_host).
    """
    with VsphereVcenter(
      self.__vcenter_info.vcenter_host,
      self.__vcenter_info.decrypt_field("vcenter_user"),
      self.__vcenter_info.decrypt_field("vcenter_password")) as vcenter:
      vim_host = self.__get_vim_host(vcenter)
      return self.__vim_host_is_ready(vim_host)

  def __get_vim_host(self, vsphere_vcenter):
    """Returns the HostSystem associated with this VsanNodeUtil.

    Args:
      vsphere_vcenter: VsphereVcenter instance. Must already be open.

    Returns:
      HostSystem instance.

    Raises:
      CurieTestException: If the VsanNodeUtil's node is not found.
    """
    vim_datacenter = vsphere_vcenter.lookup_datacenter(
      self.__vcenter_info.vcenter_datacenter_name)
    vim_cluster = vsphere_vcenter.lookup_cluster(
      vim_datacenter,
      self.__vcenter_info.vcenter_cluster_name)
    # Raises CurieTestException if not found.
    vim_host = vsphere_vcenter.lookup_hosts(vim_cluster,
                                            [self.__node.node_id()])[0]
    return vim_host

  def __vim_host_is_ready(self, vim_host):
    """Returns True if the VSAN service on a HostSystem is active and healthy.

    Args:
      vim_host: HostSystem instance.

    Returns:
      True if active and healthy; otherwise, False.

    Note:
      There are three normal roles for a host master, agent, and backup. The
      first two are self explanatory. Backup is the host designated to take
      over if the master fails. See the VSAN Troubleshooting guide linked at
      the top this module for more information.

      The following attributes should always be set by the vSphere api:
        "host_stats"<VsanHostClusterStatus>
          "health"<VsanHostHealthState>
          "nodeStatus"<VsanHostClusterStatusState>
            "state"<VsanHostClusterStatusState>
    """
    try:
      host_status = vim_host.configManager.vsanSystem.QueryHostStatus()
    except vmodl.RuntimeFault:
      log.warning("Error querying VSAN node %s status",
                  self.__node.node_id(), exc_info=True)
      return False
    log.info("node: %s; host_status.nodeState.state == \"%s\"; "
             "host_status.health == \"%s\"", self.__node.node_id(),
             host_status.nodeState.state, host_status.health)
    if (host_status.nodeState.state in ("master", "agent", "backup") and
        host_status.health == "healthy"):
      return True
    else:
      return False
