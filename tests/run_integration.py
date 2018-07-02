#!/usr/bin/env python
#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import re
import sys

import gflags
import nose
from nose.plugins.multiprocess import MultiProcess

from curie import curie_server_state_pb2, curie_types_pb2
from curie import curie_test_pb2
from curie import log as curie_log
from curie.proto_util import proto_patch_encryption_support
from curie.testing import environment, util


def cluster_cleanup():
  if not gflags.FLAGS.is_parsed():
    gflags.FLAGS(sys.argv)
  util.cluster_from_json(gflags.FLAGS.cluster_config_path).cleanup()


if __name__ == "__main__":
  gflags.DEFINE_string("path",
                       None,
                       "Test file pattern.",
                       short_name="p")
  gflags.DEFINE_integer("num_procs",
                        6,
                        "Number of parallel test processes to run")
  gflags.DEFINE_string("cluster_config_path",
                       None,
                       "Path to the cluster configuration file.")
  gflags.DEFINE_string("curie_vmdk_goldimages_dir",
                       None,
                       "Path to the Curie worker goldimages")
  gflags.DEFINE_boolean("debug",
                        True,
                        "If True, enable DEBUG log messages.")

  gflags.FLAGS(sys.argv)
  curie_log.initialize(logtostderr=True, debug=gflags.FLAGS.debug)

  proto_patch_encryption_support(curie_server_state_pb2.CurieSettings)
  proto_patch_encryption_support(curie_test_pb2.CurieTestConfiguration)
  proto_patch_encryption_support(curie_types_pb2.ConnectionParams)

  cluster_cleanup()
  try:
    # Replace tags inside parentheses with an empty string, e.g.
    # 'integration/test/steps/test_meta.py (AHV)' becomes
    # 'integration/test/steps/test_meta.py'.
    path = re.sub(r'\s+\(.*\)\s*', r'', gflags.FLAGS.path)
    success = nose.run(argv=["nosetests", path,
                             "-w", environment.tests_dir(),
                             "-v",
                             "--processes=%d" % gflags.FLAGS.num_procs,
                             "--process-timeout", 7200],
                       plugins=[MultiProcess()])
  finally:
    cluster_cleanup()
  sys.exit(not success)
