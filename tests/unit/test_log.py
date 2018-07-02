#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#

import logging
import unittest

from curie import log as curie_log

# Can't use __name__ since Curie logger is for the "curie" namespace, and
# the tests are in the "tests" namespace.
log = logging.getLogger(".".join(["curie", __name__]))


class TestLog(unittest.TestCase):
  def setUp(self):
    curie_log.initialize()

  def test_log_levels(self):
    log.trace("Trace")
    log.debug("Debug")
    log.info("Info")
    log.warning("Warning")
    log.error("Error")
    log.exception("Exception")
