#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import time
import unittest

from mock import mock

from curie.exception import CurieTestException
from curie.scenario import Scenario
from curie.scenario_util import ScenarioUtil
from curie.testing.util import mock_cluster


class TestScenarioUtil(unittest.TestCase):
  def setUp(self):
    self.scenario = Scenario(name="Fake Scenario")
    self.scenario.cluster = mock_cluster()

  @mock.patch("curie.scenario_util.time")
  def test_wait_for_deadline(self, time_mock):
    start = time.time()
    time_mock.time.side_effect = lambda: start + time_mock.time.call_count
    time_mock.sleep.return_value = 0
    ret = ScenarioUtil.wait_for_deadline(self.scenario, start + 5,
                                         "5 iterations to pass")
    self.assertIsNone(ret)
    self.assertEqual(6, time_mock.time.call_count)
    time_mock.sleep.assert_has_calls([mock.call(1)] * 5)

  @mock.patch("curie.scenario_util.time")
  def test_wait_for_deadline_already_passed(self, time_mock):
    start = time.time()
    time_mock.time.return_value = start
    with self.assertRaises(CurieTestException) as ar:
      ScenarioUtil.wait_for_deadline(self.scenario, start - 5,
                                     "exception to happen")
    self.assertEqual(str(ar.exception),
                     "Cannot wait for a deadline that has already passed "
                     "deadline_secs: %d; now_secs: %d" % (start - 5, start))
    time_mock.sleep.assert_not_called()

  @mock.patch("curie.scenario_util.time")
  def test_wait_for_deadline_missed_grace_period(self, time_mock):
    start = time.time()
    time_mock.time.side_effect = lambda: (start +
                                          (time_mock.time.call_count - 1) * 60)
    time_mock.sleep.return_value = 0
    with self.assertRaises(CurieTestException) as ar:
      ScenarioUtil.wait_for_deadline(self.scenario, start + 5,
                                     "exception to happen")
    self.assertEqual(str(ar.exception),
                     "Missed exception to happen deadline by 54s with grace "
                     "of 30s")
    time_mock.sleep.assert_has_calls([mock.call(1)] * 1)

  @mock.patch("curie.scenario_util.time")
  def test_wait_for_deadline_should_stop(self, time_mock):
    start = time.time()
    time_mock.time.side_effect = lambda: start + time_mock.time.call_count
    time_mock.sleep.return_value = 0
    with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
      mock_should_stop.side_effect = [False, False, True]
      ret = ScenarioUtil.wait_for_deadline(self.scenario, start + 5,
                                           "5 iterations to pass, only to be "
                                           "interrupted after 2")
    self.assertIsNone(ret)
    self.assertEqual(3, time_mock.time.call_count)
    time_mock.sleep.assert_has_calls([mock.call(1)] * 2)
