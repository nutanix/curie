#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest

import mock

from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie.test import steps
from curie.testing import environment


class TestStepsCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  def test_CleanUp_default(self):
    step = steps.cluster.CleanUp(self.scenario)
    step()
    self.cluster.cleanup.assert_called_once_with()

  def test_CleanUp_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.cluster.CleanUp(self.scenario)
    self.assertIsInstance(step, steps.cluster.CleanUp)

  def test_CleanUp_unhandled_exception(self):
    self.cluster.name.return_value = "FakeCluster5000"
    self.cluster.cleanup.side_effect = IOError("Something exploded")
    step = steps.cluster.CleanUp(self.scenario)
    with mock.patch.object(steps.cluster.log,
                           "exception",
                           wraps=steps.cluster.log.exception) as mock_log_exc:
      with self.assertRaises(CurieTestException) as ar:
        step()
    expected_message = ("Cluster cleanup on FakeCluster5000 failed. One or "
                        "more objects may require manual cleanup, such as "
                        "VMs, snapshots, protection domains, etc. Cleanup "
                        "will be reattempted at the beginning of the next "
                        "test.\n\nSomething exploded")
    self.assertEqual(expected_message, str(ar.exception))
    mock_log_exc.assert_called_once()
