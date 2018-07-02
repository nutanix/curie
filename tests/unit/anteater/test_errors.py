#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import unittest
from mock import sentinel as sen
from curie.anteater import AnteaterError, FailureError, UnreachableError


class TestErrors(unittest.TestCase):

  def setUp(self):
    pass

  def test_anteatererror(self):
    err = AnteaterError(sen.message)
    self.assertEqual(sen.message, err.message)
    self.assertTrue(isinstance(err, Exception))

  def test_failureerror(self):
    err = FailureError(sen.message)
    self.assertEqual(sen.message, err.message)
    self.assertTrue(isinstance(err, AnteaterError))

  def test_unreachableerror(self):
    err = UnreachableError(sen.message)
    self.assertEqual(sen.message, err.message)
    self.assertTrue(isinstance(err, AnteaterError))
