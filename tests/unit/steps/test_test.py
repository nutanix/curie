#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest
from itertools import chain, cycle

import mock

from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie import steps
from curie.testing import environment


class TestStepsTest(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  @mock.patch("curie.steps.test.time.sleep")
  def test_Wait_default(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    step = steps.test.Wait(self.scenario, 10)
    with mock.patch.object(step, "elapsed_secs") as mock_elapsed_secs:
      mock_elapsed_secs.side_effect = xrange(30)
      with mock.patch.object(step,
                             "create_annotation",
                             wraps=step.create_annotation) as mock_annotate:
        with mock.patch.object(steps.test.log,
                               "warning",
                               wraps=steps.test.log.warning) as mock_warning:
          slept_secs = step()
          self.assertEqual(slept_secs, 10)
          self.assertEqual(mock_annotate.call_count, 2)
          self.assertEqual(mock_warning.call_count, 0)

  def test_Wait_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.test.Wait(self.scenario, 10)
    self.assertIsInstance(step, steps.test.Wait)

  def test_Wait_negative_duration_secs(self):
    with self.assertRaises(CurieTestException):
      steps.test.Wait(self.scenario, -1)

  @mock.patch("curie.steps.test.time.sleep")
  def test_Wait_should_stop_when_called(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    step = steps.test.Wait(self.scenario, 10)
    with mock.patch.object(step, "elapsed_secs") as mock_elapsed_secs:
      mock_elapsed_secs.side_effect = xrange(30)
      with mock.patch.object(step.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.return_value = True
        with mock.patch.object(step,
                               "create_annotation",
                               wraps=step.create_annotation) as mock_annotate:
          with mock.patch.object(steps.test.log,
                                 "warning",
                                 wraps=steps.test.log.warning) as mock_warning:
            slept_secs = step()
            self.assertEqual(slept_secs, 0)
            self.assertEqual(mock_annotate.call_count, 1)
            self.assertEqual(mock_warning.call_count, 1)

  @mock.patch("curie.steps.test.time.sleep")
  def test_Wait_should_stop_during_run(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    step = steps.test.Wait(self.scenario, 10)
    with mock.patch.object(step, "elapsed_secs") as mock_elapsed_secs:
      mock_elapsed_secs.side_effect = xrange(30)
      with mock.patch.object(step.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.side_effect = chain([False, False], cycle([True]))
        with mock.patch.object(step,
                               "create_annotation",
                               wraps=step.create_annotation) as mock_annotate:
          with mock.patch.object(steps.test.log,
                                 "warning",
                                 wraps=steps.test.log.warning) as mock_warning:
            slept_secs = step()
            self.assertLess(slept_secs, 10)
            self.assertEqual(mock_annotate.call_count, 1)
            self.assertEqual(mock_warning.call_count, 1)
