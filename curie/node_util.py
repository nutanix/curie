#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#


class NodeUtil(object):
  def is_ready(self):
    """
    Performs vendor-specific checks to confirm basic node functionality.

    Must be implemented by subclasses.
    """
    raise NotImplementedError("This must be implemented by a subclass")
