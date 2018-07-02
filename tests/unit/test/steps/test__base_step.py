#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

import mock

from curie.curie_test_pb2 import CurieTestResult
from curie.scenario import Scenario, Phase
from curie.test import steps
from curie.test.steps._base_step import BaseStep


class TestStepsBaseStep(unittest.TestCase):
  def setUp(self):
    self.scenario = mock.Mock(spec=Scenario)
    self.scenario.phase = Phase.kRun

  def test_BaseStep_call(self):
    step = BaseStep(self.scenario)
    step._run = mock.Mock()
    with mock.patch("curie.test.steps._base_step.time") as time_mock:
      with mock.patch.object(step, "_run") as _run_mock:
        time_mock.time.side_effect = [100, 105]
        step()
        _run_mock.assert_called_once_with()
    self.assertEqual(step.elapsed_secs(), 5)

  def test_BaseStep_call_not_implemented(self):
    step = BaseStep(self.scenario)
    with self.assertRaises(NotImplementedError):
      step()

  def test_BaseStep_annotate_disabled(self):
    step = BaseStep(self.scenario)
    with mock.patch.object(steps._base_step.log,
                           "debug",
                           wraps=steps._base_step.log.debug) as mock_debug:
      step.create_annotation("Watch out!")
      self.assertEqual(mock_debug.call_count, 1)

  def test_BaseStep_annotate_hidden(self):
    step = BaseStep(self.scenario, annotate=True)
    for phase in [Phase.kPreSetup, Phase.kSetup, Phase.kTeardown]:
      self.scenario.phase = phase
      with mock.patch.object(steps._base_step.log,
                             "debug",
                             wraps=steps._base_step.log.debug) as mock_debug:
        step.create_annotation("Watch out!")
        mock_debug.assert_called_once_with(
          "Hidden setup/teardown annotation (%s): %s", step, "Watch out!")

  def test_BaseStep_annotate_enabled(self):
    step = BaseStep(self.scenario, annotate=True)
    annotations = list()
    for index in xrange(5):
      x_annotation = CurieTestResult.Data2D.XAnnotation()
      x_annotation.description = "Annotation %d" % index
      annotations.append(x_annotation)
    self.scenario._annotations = annotations
    for phase in [None, Phase.kRun]:
      self.scenario.phase = phase
      step.create_annotation("Watch out!")
      self.scenario.create_annotation.assert_called_once_with("Watch out!")
      self.scenario.reset_mock()

  def test_BaseStep_elapsed_secs_not_started(self):
    step = BaseStep(self.scenario)
    self.assertEqual(step.elapsed_secs(), None)

  def test_BaseStep_elapsed_secs_started(self):
    step = BaseStep(self.scenario)
    step._BaseStep__start_time_epoch = 100
    with mock.patch("curie.test.steps._base_step.time") as time_mock:
      time_mock.time.return_value = 105
      self.assertEqual(step.elapsed_secs(), 5)

  def test_BaseStep_is_running_not_started(self):
    step = BaseStep(self.scenario)
    self.assertFalse(step.is_running())

  def test_BaseStep_is_running_is_running(self):
    step = BaseStep(self.scenario)
    step._run = lambda: self.assertTrue(step.is_running())
    step()

  def test_BaseStep_is_running_has_finished(self):
    step = BaseStep(self.scenario)
    step._run = lambda: None  # No-op.
    step()
    self.assertFalse(step.is_running())
