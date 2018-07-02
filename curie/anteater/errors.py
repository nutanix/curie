# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#


class AnteaterError(Exception):
  """ Exception indicating an error occurred during Ansible execution. """
  pass


class UnreachableError(AnteaterError):
  """ Exception indicating an error occurred during Ansible execution. """
  pass


class FailureError(AnteaterError):
  """ Exception indicating an error occurred during Ansible execution. """
  pass
