#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging

from curie.curie_server_state_pb2 import CurieSettings

log = logging.getLogger(__name__)

# For convenience in accessing Enum members.
OobVendor = CurieSettings.ClusterNode.NodeOutOfBandManagementInfo
OobInterfaceType = CurieSettings.ClusterNode.NodeOutOfBandManagementInfo


class OobManagementUtil(object):
  def get_chassis_status(self):
    raise NotImplementedError("Must be implemented by an OOB handler")

  def power_cycle(self, async=False):
    raise NotImplementedError("Must be implemented by an OOB handler")

  def power_on(self, async=False):
    raise NotImplementedError("Must be implemented by an OOB handler")

  def power_off(self, async=False):
    raise NotImplementedError("Must be implemented by an OOB handler")

  def is_powered_on(self):
    raise NotImplementedError("Must be implemented by an OOB handler")
