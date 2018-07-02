#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

from curie.node_util import NodeUtil


class GenericVsphereNodeUtil(NodeUtil):

  @classmethod
  def _use_handler(cls, node):
    """Returns True if 'node' should use this class as its handler."""
    software_info = node.cluster().metadata().cluster_software_info
    mgmt_info = node.cluster().metadata().cluster_management_server_info
    return (software_info.HasField("generic_info") and
            mgmt_info.HasField("vcenter_info"))

  def __init__(self, node):
    self.__node = node
    self.__cluster_metadata = self.__node.cluster().metadata()
    self.__vcenter_info = \
      self.__cluster_metadata.cluster_management_server_info.vcenter_info

  def is_ready(self):
    """See 'NodeUtil.is_ready' documentation for further details.

    For a generic vSphere node, check only that vCenter reports the node
    is powered on.
    """
    # Don't sync with oob, as this method is often polled.
    return self.__node.is_powered_on_soft(sync_with_oob=False)
