#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

from curie.node import Node
from curie.node import NodePropertyNames


class AcropolisNode(Node):

  # TODO (jklein): This is not entirely accurate.
  _NODE_PROPERTY_NAMES = {
    NodePropertyNames.POWERED_OFF: "kNormalDisconnected",
    NodePropertyNames.POWERED_ON: "kNormalConnected",
    NodePropertyNames.UNKNOWN: None
  }

  def __init__(self, cluster, node_id, node_index):
    super(AcropolisNode, self).__init__(cluster, node_id, node_index)
    self.__prism_uuid = None
    self.__node_json = None

  @property
  def _prism_uuid(self):
    if self.__prism_uuid is None:
      self.__prism_uuid = self.cluster().identifier_to_node_uuid(
        self.cluster()._prism_client, self._node_id)
    return self.__prism_uuid

  @property
  def _node_json(self):
    if self.__node_json is None:
      self.__node_json = self.cluster()._prism_client.hosts_get(
        host_id=self._prism_uuid)
    return self.__node_json

  # Cache the CPU capacity of the node.
  @property
  def cpu_capacity_in_hz(self):
    return self._node_json["cpuCapacityInHz"]

  def node_ip(self):
    return self._node_json["hypervisorAddress"]

  @classmethod
  def get_management_software_property_name_map(cls):
    """See 'Node.get_node_property_map' for documentation."""
    return cls._NODE_PROPERTY_NAMES

  def power_off_soft(self, timeout_secs=None, async=False):
    """See 'Node.power_off_soft' for documentation."""
    self.cluster().power_off_nodes_soft([self],
                                        timeout_secs=timeout_secs,
                                        async=async)
