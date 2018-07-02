#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
from curie.curie_error_pb2 import CurieError


# Exception class for all other uses in curie except for failing a running
# test (CurieTestException should be used in that case).
class CurieException(Exception):
  def __init__(self, error_code, error_msg):
    Exception.__init__(self)
    # Error code (see curie_interface.proto).
    self.error_code = error_code

    # Error message (see curie_interface.proto).
    self.error_msg = error_msg

  def __str__(self):
    return "%s (%s)" % (self.error_msg, CurieError.Type.Name(self.error_code))


# Exception class for failing a test. Raising an exception of this type in a
# running test will cause the test to be failed.
class CurieTestException(RuntimeError):
  pass


class ScenarioStoppedError(CurieTestException):
  pass


class ScenarioTimeoutError(CurieTestException):
  pass
