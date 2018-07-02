#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
"""
Provides stub Out-of-Band management util for cases with no OoB support.
"""

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.oob_management_util import OobInterfaceType
from curie.oob_management_util import OobManagementUtil


#==============================================================================
# Null OoB Util
#==============================================================================

class NullOobUtil(OobManagementUtil):
  """
  Dummy implementation of 'OobManagementUtil' interface.
  Wraps calls to ipmitool.
  """

  @classmethod
  def _use_handler(cls, oob_type=None, oob_vendor=None):
    return oob_type == OobInterfaceType.kNone

  def __init__(self, *args, **kwargs):
    pass

#==============================================================================
# OobManagementUtil interface
#==============================================================================

  def get_chassis_status(self):
    """
    Returns:
      (dict): Map of IPMI chassis status data.

    Raises:
      CurieException on error.
    """
    raise CurieException(
      CurieError.kInvalidParameter,
      "Attempted to make out-of-band management calls in an environment "
      "which has not been configured to support out-of-band management")

  def power_cycle(self, async=False):
    """
    Power cycles the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, making blocking calls to 'power_off'
      and then 'power_on'.

      If True and node is powered off, performs async 'power_on' call,
      otherwise issues the (async) 'power cycle' command.

    Returns:
      (bool): True on success, else False.
    """
    raise CurieException(
      CurieError.kInvalidParameter,
      "Attempted to make out-of-band management calls in an environment "
      "which has not been configured to support out-of-band management")

  def power_on(self, async=False):
    """
    Powers on the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, block until power state is on.

    Returns:
      (bool): True on success, else False.
    """
    raise CurieException(
      CurieError.kInvalidParameter,
      "Attempted to make out-of-band management calls in an environment "
      "which has not been configured to support out-of-band management")

  def power_off(self, async=False):
    """
    Powers off the node associated with 'self.__flags["host"]'.

    Args:
      async (bool): Optional. If False, block until power state is off.

    Returns:
      (bool): True on success, else False.
    """
    raise CurieException(
      CurieError.kInvalidParameter,
      "Attempted to make out-of-band management calls in an environment "
      "which has not been configured to support out-of-band management")

  def is_powered_on(self):
    """
    Checks whether chassis power state is 'on'.

    Returns:
      (bool) True if powered on, else False.
    """
    raise CurieException(
      CurieError.kInvalidParameter,
      "Attempted to make out-of-band management calls in an environment "
      "which has not been configured to support out-of-band management")
