#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
# Hyper-V Specific node utilities.
#

import logging

from curie.node_util import NodeUtil

log = logging.getLogger(__name__)


class HyperVNodeUtil(NodeUtil):

  def __init__(self, node):
    self.__node = node
    self.__cluster_metadata = self.__node.cluster().metadata()
    self.__vmm_info = \
      self.__cluster_metadata.cluster_management_server_info.vmm_info

  def is_ready(self):
    """See 'NodeUtil.is_ready' documentation for further details.

    For a generic vSphere node, check only that vCenter reports the node
    is powered on.
    """
    # Don't sync with oob, as this method is often polled.
    return self.__node.is_powered_on_soft(sync_with_oob=False)
