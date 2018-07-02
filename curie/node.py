#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging
from functools import wraps

from curie.error import CurieError
from curie.exception import CurieException, CurieTestException
from curie.generic_vsphere_node_util import GenericVsphereNodeUtil
from curie.hyperv_node_util import HyperVNodeUtil
from curie.idrac_util import IdracUtil
from curie.ipmi_util import IpmiUtil
from curie.log import CHECK, CHECK_GE
from curie.null_oob_util import NullOobUtil
from curie.nutanix_node_util import NutanixNodeUtil
from curie.oob_management_util import OobInterfaceType, OobVendor
from curie.util import CurieUtil
from curie.vsan_node_util import VsanNodeUtil

log = logging.getLogger(__name__)


def get_node_management_util(cluster_metadata_pb):
  """
  Return the correct subclass of curie.node_util.NodeUtil.

  Args:
    cluster_metadata_pb (curie.curie_server_state_pb2.CurieSettings): The
      metadata for the cluster.

  Returns:
    class: Subclass of curie.node_util.NodeUtil.
  """
  software_info = cluster_metadata_pb.cluster_software_info
  mgmt_info = cluster_metadata_pb.cluster_management_server_info
  if software_info.HasField("nutanix_info"):
    return NutanixNodeUtil
  elif software_info.HasField("vsan_info"):
    return VsanNodeUtil
  elif (software_info.HasField("generic_info") and
        mgmt_info.HasField("vcenter_info")):
    return GenericVsphereNodeUtil
  elif (software_info.HasField("generic_info") and
        mgmt_info.HasField("vmm_info")):
    return HyperVNodeUtil
  else:
    raise ValueError("No node utilities found for cluster with metadata %r" %
                     cluster_metadata_pb)


def get_power_management_util(oob_mgmt_info_pb):
  """
  Return the correct instance of curie.oob_management_util.OobManagementUtil.

  Args:
    oob_mgmt_info_pb (curie.curie_server_state_pb2.CurieSettings.ClusterNode.NodeOutOfBandManagementInfo):
      The metadata for the node's power management controller.

  Returns:
    object: Instance of subclass of
      curie.oob_management_util.OobManagementUtil.
  """
  oob_type = oob_mgmt_info_pb.interface_type
  oob_vendor = oob_mgmt_info_pb.vendor
  if (oob_type == OobInterfaceType.kIpmi and
      oob_vendor in [OobVendor.kUnknownVendor, OobVendor.kSupermicro]):
    return IpmiUtil(
      ip=oob_mgmt_info_pb.ip_address,
      username=oob_mgmt_info_pb.decrypt_field("username"),
      password=oob_mgmt_info_pb.decrypt_field("password"))
  elif oob_type == OobInterfaceType.kIpmi and oob_vendor == OobVendor.kDell:
    return IdracUtil(
      ip=oob_mgmt_info_pb.ip_address,
      username=oob_mgmt_info_pb.decrypt_field("username"),
      password=oob_mgmt_info_pb.decrypt_field("password"))
  elif oob_type == OobInterfaceType.kNone:
    return NullOobUtil()
  else:
    raise ValueError("No power management utilities found for node with "
                     "metadata %r" % oob_mgmt_info_pb)


# Decorator to validate appropriate metadata exists for out-of-band
# management calls.
def oob(functor):
  @wraps(functor)
  def wrapped(self, *args, **kwargs):
    # Verify appropriate metadata is present in cluster config.
    node_metadata = self.metadata()
    if (not node_metadata or
        not node_metadata.HasField("node_out_of_band_management_info")):
      raise CurieTestException(
          "No Out-of-Band management configuration present in cluster "
          "metadata for %s" % str(self))
    oob_config = node_metadata.node_out_of_band_management_info
    if oob_config.interface_type == oob_config.kUnknownInterface:
      raise CurieTestException(
          "Unknown Out-of-Band management interface in cluster metadata "
          "for node '%s'" % self._node_id)
    elif oob_config.interface_type == oob_config.kNone:
      raise CurieTestException(
          "Out-of-Band management interface is set to none in cluster "
          "metadata for %s" % str(self))
    return functor(self, *args, **kwargs)
  return wrapped


class NodePropertyNames(object):
  POWERED_ON = "POWERED_ON"
  POWERED_OFF = "POWERED_OFF"
  UNKNOWN = "UNKNOWN"


class Node(object):

  def __init__(self, cluster, node_id, node_index):
    # Cluster this node is part of.
    self._cluster = cluster

    # Node ID. This node ID need not be unique across all nodes managed by a
    # given management server. However, it must be unique within the set of
    # nodes for the cluster.
    self._node_id = node_id
    CHECK(self._node_id)

    # Node index (corresponds to the index of this node within the node #
    # metadata vector)
    self._node_index = node_index
    CHECK_GE(self._node_index, 0, "Node index must be a natural number")

    # IP address currently associated with this node.
    self._node_ip = None

    self.__node_util = get_node_management_util(cluster.metadata())(self)
    self.__oob_util = get_power_management_util(
      self.metadata().node_out_of_band_management_info)

  def __str__(self):
    return "%s %s (%s)" % (type(self).__name__, self._node_id,
                           self._node_index)

  def __eq__(self, other):
    return (self.cluster() == other.cluster() and
            self.node_id() == other.node_id())

  def __hash__(self):
    return hash(self.cluster()) ^ hash(self.node_id())

  def cluster(self):
    return self._cluster

  def node_id(self):
    return self._node_id

  def node_ip(self):
    return self._node_ip

  def node_index(self):
    return self._node_index

  def metadata(self):
    return self._cluster.metadata().cluster_nodes[self._node_index]

  @classmethod
  def get_management_software_property_name_map(cls):
    """
    Gets map of management software specific values for node attributes.

    Gets map of vendor-specific names for the standardized attributes in
    'NodePropertyNames'.
    """
    raise NotImplementedError("Subclasses must implement this in a "
                              "vendor-specific manner")

  @classmethod
  def get_management_software_value_for_attribute(cls, attr):
    """
    Get management software specific value for 'attr'.
    """
    if getattr(NodePropertyNames, attr) is None:
      raise CurieException(CurieError.kInvalidParameter,
                            "Unknown node property '%s'" % attr)

    return cls.get_management_software_property_name_map()[attr]

  def is_ready(self, check_power_state=True):
    """
    Checks node is in a ready state.

    -- Checks node is powered on (assumed true if check_power_state=False).
    -- Checks relevant services are responsive.

    Args:
      check_power_state (bool): Optional. If False, skip power state checks.

    Returns:
      (bool) True if node is powered on and relevant services are responding,
      else False.
    """
    if check_power_state:
      power_state = self._cluster.get_power_state_for_nodes([self]).get(
          self.node_id())
      log.debug("Management software reports node '%s' in power state '%s'",
                self.node_id(), power_state)
      if power_state != self.get_management_software_value_for_attribute(
          NodePropertyNames.POWERED_ON):
        return False
    else:
      log.debug("Skipping power state check for node '%s'", self._node_id)

    try:
      is_ready = self.__node_util.is_ready()
      if not is_ready:
        log.info("node %s is not ready", self.node_id())
      return is_ready
    except CurieException as ex:
      log.warning("Exception checking if node %s is ready: %s; assuming False",
                  self.node_id(), ex)
      return False

  def sync_power_state(self):
    """Synchronize management software power state and OOB power state.

    Returns:
      dict: Map of Node IDs for 'nodes' to their respective power states.

    Raises:
      CurieTestException:
        If power states cannot be synchronized.
    """
    return self._cluster.sync_power_state_for_nodes([self]).get(self.node_id())

  def power_off_soft(self, timeout_secs=None, async=False):
    """
    Gracefully powers off node.

    Args:
      timeout_secs (int): Timeout in seconds for power off task to complete.
      async (bool): If False, block until power state has changed to "off".

    Raises:
      CurieException on all errors.
    """
    raise NotImplementedError("Subclasses must implement this in a "
                              "vendor-specific manner")

  def is_powered_on_soft(self, sync_with_oob=True):
    """
    Checks if node is powered on from the management software.

    The management software state will lag behind the hardware state.

    If 'sync_with_oob' is True, sync with OOB reported hardware state.

    Args:
      sync_with_oob (bool): Optional. If True, sync with OOB hardware state.

    Returns:
      (bool) True if powered on detected at the management server, False
      otherwise.
    """
    # If sync_with_oob, allow oob check to raise exception immediately when
    # missing OOB metadata.
    if sync_with_oob:
      oob(lambda self: None)(self)
    if sync_with_oob:
      node_id_power_state_map = self._cluster.sync_power_state_for_nodes(
          [self])
    else:
      node_id_power_state_map = self._cluster.get_power_state_for_nodes([self])
    power_state = node_id_power_state_map.get(self.node_id())
    return power_state == self.get_management_software_value_for_attribute(
        NodePropertyNames.POWERED_ON)

  @oob
  def is_powered_on(self):
    """
    Checks if node is currently powered on via OOB management specified in
    the node's metadata.

    Returns:
      (bool) True if node is powered on, else False.
    """
    return self.__oob_util.is_powered_on()

  @oob
  def power_on(self):
    """
    Powers on the node using out-of-band management interface specified in the
    cluster's metadata.

    Raises:
      CurieTestException if no suitable metadata exists, CurieException on
      all other errors.
    """
    log.debug("Powering on node '%s'", self._node_id)
    if not self.__oob_util.power_on():
      raise CurieException(
          CurieError.kInternalError,
          "Failed to power on node '%s'" % self._node_id)

  @oob
  def power_off(self, sync_management_state=True):
    """
    Powers off the node using out-of-band management interface specified in the
    cluster's metadata.

    Args:
      sync_management_state (bool): If true, wait until the management software
      detects the power state is off. This is True by default in order to
      prevent other management server methods that require power to be on from
      failing unexpectedly.

    Raises:
      CurieTestException if no suitable metadata exists, CurieException on
      all other errors.
    """
    log.debug("Powering off node '%s'", self._node_id)
    if not self.__oob_util.power_off():
      raise CurieException(
          CurieError.kInternalError,
          "Failed to power off node '%s'" % self._node_id)
    # If 'sync_management_state', wait until the management server state is
    # synced with the hardware's state.
    if sync_management_state:
      timeout_secs = 40 * 60
      powered_off = CurieUtil.wait_for(
          lambda: not self.is_powered_on_soft(sync_with_oob=True),
          "management server power state to sync to off for node: %s" %
          self.node_id(),
          timeout_secs,
          poll_secs=5)
      if not powered_off:
        raise CurieException(
            CurieError.kInternalError,
            "Failed to sync management server power state after 300s")

  @oob
  def power_cycle(self, async=False):
    """
    Power cycles the node using out-of-band management interface specified
    in the cluster's metadata.

    NB: In order to avoid our status polling racing the node's power status,
      invoke 'power_off' then 'power_on' in the synchronous case rather than
      issuing 'chassis power cycle'.

    Args:
      async (bool): If False, make synchronous calls to 'power_off' and
      'power_on'.

    Raises:
      CurieTestException if no suitable metadata exists, CurieException on
      all other errors.
    """
    if not self.__oob_util.power_cycle(async=async):
      raise CurieException(CurieError.kInternalError,
                            "Failed to power cycle node '%s'" % self._node_id)
