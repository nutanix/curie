#!/usr/bin/env python
#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import os
import sys
import unittest

import gflags

from curie import log as curie_log

gflags.DEFINE_string("pattern",
                     "test*.py",
                     "Test file pattern.",
                     short_name="p")
gflags.DEFINE_string("start_module",
                     "unit",
                     "Test module or dotted submodule.")
gflags.DEFINE_boolean("debug",
                      True,
                      "If True, enable DEBUG log messages.")


def suite():
  if not gflags.FLAGS.is_parsed():
    gflags.FLAGS(sys.argv)
  curie_log.initialize(logtostderr=False, debug=gflags.FLAGS.debug)
  pytest_directory = os.path.dirname(__file__)
  return unittest.defaultTestLoader.discover(gflags.FLAGS.start_module,
                                             top_level_dir=pytest_directory,
                                             pattern=gflags.FLAGS.pattern)


if __name__ == "__main__":
  gflags.FLAGS(sys.argv)
  success = unittest.TextTestRunner(verbosity=1).run(suite()).wasSuccessful()
  sys.exit(not success)
