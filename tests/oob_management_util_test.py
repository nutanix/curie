#!/usr/bin/env python
#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
#
import logging
import os
import shutil
import sys
import unittest

import gflags
FLAGS = gflags.FLAGS

g_top_dir = "%s/.." % \
  os.path.abspath(os.path.dirname(os.path.abspath(globals()["__file__"])))
sys.path.append("%s/py" % g_top_dir)

from curie.oob_management_util import OobManagementUtil
from curie.curie_server_state_pb2 import CurieSettings
from curie.testing import environment
from curie import log as curie_log

log = logging.getLogger(__name__)

gflags.DEFINE_string(
  "curie_oob_util_test_dir",
  "%s/pytest/output/curie_oob_util_test" % g_top_dir,
  "Test output directory.",
  short_name="o")

gflags.DEFINE_string("curie_oob_util_test_ip",
                     None,
                     "IP address of OoB managment host for this test.")

gflags.DEFINE_string("curie_oob_util_test_username",
                     None,
                     "Username for OoB management to be used in this test.")

gflags.DEFINE_string("curie_oob_util_test_password",
                     None,
                     "Password for OoB management to be used in this test.")

gflags.DEFINE_enum("curie_oob_util_test_vendor",
                   None,
                   ["supermicro", "dell"],
                   "BMC vendor for OoB host to be used in this test.")

class CurieOobHandlerTest(unittest.TestCase):
  """
  Tests a handler implementing required API for OobManagementUtil.
  """
  def get_node_for_test(self):
    # If we have a valid environment, use actual cluster.
    if self.env.valid():
      node = self.env.cluster().nodes()[0]
      node_metadata = self.env.cluster().node_metadata(node.node_id())

      if FLAGS.curie_oob_util_test_vendor == "supermicro":
        node_vendor = \
            node_metadata.node_out_of_band_management_info.kSupermicro
      elif FLAGS.curie_oob_util_test_vendor == "dell":
        node_vendor = \
            node_metadata.node_out_of_band_management_info.kDell
      else:
        # Fail if a new vendor is added to the enum but not updated here.
        self.assert_(False, "Unexpected vendor '%s'" %
                     FLAGS.curie_oob_util_test_vendor)

      node_metadata.node_out_of_band_management_info.vendor = node_vendor
      return node

    # Create dummy node implementing only what's required by
    # OobManagementUtil.
    class DummyNode(object):

      def __init__(self, node_metadata):
        self.__node_metadata = node_metadata

      def metadata(self):
        return self.__node_metadata

    node_metadata = CurieSettings.ClusterNode()
    oob_info = node_metadata.node_out_of_band_management_info
    oob_info.interface_type = oob_info.kIpmi
    oob_info.vendor = FLAGS.curie_oob_util_test_vendor
    oob_info.ip_address = FLAGS.curie_oob_util_test_ip
    oob_info.password = FLAGS.curie_oob_util_test_password
    oob_info.username = FLAGS.curie_oob_util_test_username

    return DummyNode(node_metadata)

  def setUp(self):
    self.env = environment.TestEnvironment.get_test_environment()
    self.node = self.get_node_for_test()

  def test_public_api_read_only(self):
    oob_util = OobManagementUtil(self.node)
    if FLAGS.curie_oob_util_test_vendor == "dell":
      self.assertEqual(oob_util.__class__.__name__,
                       "IdracUtil",
                       "Failed to instantiate correct OoB handler for Dell")
    elif FLAGS.curie_oob_util_test_vendor == "supermicro":
      self.assertEqual(
        oob_util.__class__.__name__,
        "IpmiUtil",
        "Failed to instantiate correct OoB handler for Supermicro")
    ret, cmd = None, None
    for cmd in ["get_chassis_status", "is_powered_on"]:
      log.INFO("Testing command '%s'", cmd)
      ret = getattr(oob_util, cmd)()
      self.assert_(ret is not None, "'%s' failed" % cmd)

if __name__ == "__main__":
  # Until auto-detection is checked-in, need to explicitly set vendor.
  required_flags = ["curie_oob_util_test_vendor"]

  env = environment.TestEnvironment.get_test_environment()
  # If we can't use 'environment', require other essential data via flags.
  if not env.valid():
    required_flags += ["curie_oob_util_test_ip",
                       "curie_oob_util_test_username",
                       "curie_oob_util_test_username"]
  try:
    sys.argv = FLAGS(sys.argv)
  except gflags.FlagsError, ex:
    sys.stderr.write("Error %s\nUsage: %s [FLAGS]\n" % (ex, sys.argv[0]))
    sys.exit(3)

  for flag_name in required_flags:
    if getattr(FLAGS, flag_name) is None:
      sys.stderr.write("ERROR: Flag '%s' is required\n" % flag_name)
      sys.exit(3)

  FLAGS.debug = True
  curie_log.initialize()

  if os.path.exists(FLAGS.curie_oob_util_test_dir):
    shutil.rmtree(FLAGS.curie_oob_util_test_dir)
  os.makedirs(FLAGS.curie_oob_util_test_dir)

  handler_suite = unittest.makeSuite(CurieOobHandlerTest)
  runner = unittest.TextTestRunner()
  runner.run(handler_suite)
