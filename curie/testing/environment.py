# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
#
# Create test environment by combining information in config file and gflags.
#

import os

top = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", ".."))


def tests_dir():
  """
  Return the absolute path to the tests directory.
  """
  return os.path.join(top, "tests")


def resource_dir():
  """
  Return the absolute path to the tests resource directory.
  """
  return os.path.join(tests_dir(), "resource")


def test_output_dir(test_case):
  """
  Return an absolute path to the test output directory.

  Args:
    test_case (unittest.TestCase): Instance of the test being run.
  """
  return os.path.join(tests_dir(), "output", test_case.id())
