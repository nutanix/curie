#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

from pyVmomi import vim

from curie.node import Node
from curie.node import NodePropertyNames
from curie.util import CurieUtil


class VsphereNode(Node):

  _NODE_PROPERTY_NAMES = {
    NodePropertyNames.POWERED_OFF: \
      vim.HostSystem.PowerState.poweredOff,
    NodePropertyNames.POWERED_ON: \
      vim.HostSystem.PowerState.poweredOn,
    NodePropertyNames.UNKNOWN: \
      vim.HostSystem.PowerState.unknown
    }

  def __init__(self, cluster, node_id, node_index):
    super(VsphereNode, self).__init__(cluster, node_id, node_index)

    # vCenter node IDs are either IPv4 addresses or hostnames.
    if CurieUtil.is_ipv4_address(node_id):
      self._node_ip = node_id
    else:
      self._node_ip = CurieUtil.resolve_hostname(node_id)

  @classmethod
  def get_management_software_property_name_map(cls):
    """See 'Node.get_node_property_map' for documentation."""
    return cls._NODE_PROPERTY_NAMES

  def power_off_soft(self, timeout_secs=None, async=False):
    """See 'Node.power_off_soft' for documentation."""
    self.cluster().power_off_nodes_soft([self],
                                        timeout_secs=timeout_secs,
                                        async=async)
