#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie.steps import _util
from curie.testing import environment
from curie.testing.util import mock_cluster


class TestStepsUtil(unittest.TestCase):
  def setUp(self):
    self.scenario = Scenario(
      cluster=mock_cluster(),
      output_directory=environment.test_output_dir(self))

  def test_get_node_valid(self):
    for index in xrange(self.scenario.cluster.node_count()):
      self.assertEqual(_util.get_node(self.scenario, index),
                       self.scenario.cluster.nodes()[index])

  def test_get_node_out_of_bounds(self):
    with self.assertRaises(CurieTestException) as ar:
      _util.get_node(self.scenario, self.scenario.cluster.node_count())
    self.assertIn("Node slice is out of bounds (cluster contains 4 nodes)",
                  str(ar.exception))
