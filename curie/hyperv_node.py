#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#

from curie.exception import CurieException
from curie.node import Node, NodePropertyNames
from curie.util import CurieUtil


class HyperVNode(Node):
  _NODE_PROPERTY_NAMES = {
    NodePropertyNames.POWERED_OFF: "PowerOff",
    NodePropertyNames.POWERED_ON: "Responding",
    NodePropertyNames.UNKNOWN: None
  }

  def __init__(self, cluster, node_id, node_index, node_properties):
    super(HyperVNode, self).__init__(cluster, node_id, node_index)

    self.power_state = node_properties["power_state"]
    self.__fqdn = node_properties["fqdn"]
    self.__overall_state = node_properties["overall_state"]
    ips = node_properties["ips"]

    if not ips:
      raise CurieException("Received empty node ip list in response")

    # vCenter node IDs are either IPv4 addresses or hostnames.
    if CurieUtil.is_ipv4_address(ips[0]):
      self._node_ip = ips[0]
    else:
      self._node_ip = CurieUtil.resolve_hostname(ips[0])

  @classmethod
  def get_management_software_property_name_map(cls):
    """See 'Node.get_node_property_map' for documentation."""
    return cls._NODE_PROPERTY_NAMES

  def power_off_soft(self, timeout_secs=None, async=False):
    """See 'Node.power_off_soft' for documentation."""
    self.cluster().power_off_nodes_soft(
      [self], timeout_secs=timeout_secs, async=async)

  def get_fqdn(self):
    return self.__fqdn
