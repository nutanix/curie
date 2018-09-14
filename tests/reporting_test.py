#!/usr/bin/env python
#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# Unit tests for test reports.
import inspect
import os
import unittest

from curie.node_failure_test.node_failure_test import NodeFailureTest
from curie.node_failure_data_loss_test.node_failure_data_loss_test \
  import NodeFailureDataLossTest
from curie.oltp_dss_test.oltp_dss_test import OltpDssTest
from curie.oltp_vdi_test.oltp_vdi_test import OltpVdiTest
from curie.oltp_snapshot_test.oltp_snapshot_test import OltpSnapshotTest
from curie.rolling_upgrade_test.rolling_upgrade_test import RollingUpgradeTest

# Top directory of repository.
TOP = os.path.realpath("%s/../../" %
                       os.path.abspath(
                         os.path.dirname(
                           os.path.abspath(globals()["__file__"]))))

MODULE_NAME = inspect.getmodulename(__file__)


# -----------------------------------------------------------------------------

def module_output_dir():
  if "TESTOUTDIR" in  os.environ:
    return os.environ["TESTOUTDIR"]
  else:
    return os.path.join(TOP, "build", "testoutput", "curie", "pytest",
                        "%s.py" % MODULE_NAME)

class TestTestReport(unittest.TestCase):
  def setUp(self):
    self.__data_dir = os.path.abspath(os.path.join(".",
      "resource/reporting_test"))
    self.__output_dir = module_output_dir()
    if not os.path.exists(self.__output_dir):
      os.makedirs(self.__output_dir)

  def test_ntnx_oltp_dss_report(self):
    test_output_dir = os.path.join(self.__output_dir, "ntnx", "oltp_dss")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "oltp_dss")
    OltpDssTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

  def test_ntnx_oltp_vdi_report(self):
    test_output_dir = os.path.join(self.__output_dir, "ntnx", "oltp_vdi")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "oltp_vdi")
    OltpVdiTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

  def test_ntnx_oltp_snapshot_report(self):
    test_output_dir = os.path.join(self.__output_dir, "ntnx", "oltp_snapshot")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "oltp_snapshot")
    OltpSnapshotTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

  def test_ntnx_node_failure_report(self):
    test_output_dir = os.path.join(self.__output_dir, "ntnx", "node_failure")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "node_failure")
    NodeFailureTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

  def test_ntnx_rolling_upgrade_report(self):
    test_output_dir = os.path.join(
      self.__output_dir, "ntnx", "rolling_upgrade")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "rolling_upgrade")
    RollingUpgradeTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

  def test_ntnx_node_failure_data_loss_report(self):
    test_output_dir = os.path.join(
      self.__output_dir, "ntnx", "node_failure_data_loss")
    if not os.path.exists(test_output_dir):
      os.makedirs(test_output_dir)
    test_dir = os.path.join(self.__data_dir, "ntnx", "node_failure_data_loss")
    NodeFailureDataLossTest.generate_report_testdir(test_dir, test_output_dir)
    self.assertTrue(os.path.exists(
      os.path.join(test_output_dir, "report.html")))

if __name__ == "__main__":
  runner = unittest.TextTestRunner(verbosity=2)
  unittest.main(testRunner=runner)
