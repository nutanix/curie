#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import cPickle as pickle
import errno
import logging
import os
import shutil
import unittest
from itertools import chain, cycle

import mock

from curie import scenario_parser
from curie.acropolis_cluster import AcropolisCluster
from curie.curie_metrics_pb2 import CurieMetric
from curie.curie_test_pb2 import CurieTestResult
from curie.exception import ScenarioStoppedError
from curie.result.cluster_result import ClusterResult
from curie.result.iogen_result import IogenResult
from curie.scenario import Phase, Scenario, Status
from curie.steps._base_step import BaseStep
from curie.steps.cluster import CleanUp
from curie.testing import environment
from curie.testing.util import mock_cluster

log = logging.getLogger(__name__)


class TestScenario(unittest.TestCase):
  def setUp(self):
    path = environment.test_output_dir(self)
    if os.path.isdir(path):
      shutil.rmtree(path)
    os.makedirs(path)
    self.scenario = Scenario(name="Fake Scenario")
    self.scenario.cluster = mock_cluster()
    self.scenario.output_directory = path
    self.mock_setup_step = mock.Mock(spec=BaseStep)
    self.mock_setup_step.description = "Some mock Setup thing"
    self.mock_setup_step.status = Status.NOT_STARTED
    self.mock_setup_step.requirements.return_value = set()
    self.mock_run_step = mock.Mock(spec=BaseStep)
    self.mock_run_step.description = "Some mock Run thing"
    self.mock_run_step.status = Status.NOT_STARTED
    self.mock_run_step.requirements.return_value = set()
    self.scenario.add_step(self.mock_setup_step, Phase.SETUP)
    self.scenario.add_step(self.mock_run_step, Phase.RUN)
    self.scenario._status = Status.EXECUTING

  def test_init(self):
    scenario = Scenario()
    self.assertEqual(scenario.status(), Status.NOT_STARTED)
    self.assertEqual(scenario.duration_secs(), None)

  def test_repr(self):
    scenario = Scenario()
    self.assertEqual(
      "<Scenario(name=None, display_name=None, id=%d, status=<Status.NOT_STARTED: 0>)>" %
      scenario.id,
      repr(scenario))
    scenario = Scenario(name="fake_test")
    self.assertEqual(
      "<Scenario(name='fake_test', display_name='fake_test', id=%d, status=<Status.NOT_STARTED: 0>)>" %
      scenario.id,
      repr(scenario))
    scenario = Scenario(name="fake_test", display_name="Fake Test")
    self.assertEqual(
      "<Scenario(name='fake_test', display_name='Fake Test', id=%d, status=<Status.NOT_STARTED: 0>)>" %
      scenario.id,
      repr(scenario))

  def test_add_step_already_running(self):
    self.scenario._status = Status.EXECUTING
    with self.assertRaises(RuntimeError) as ar:
      self.scenario.add_step(self.mock_setup_step, Phase.SETUP)
    self.assertEqual(
      "Fake Scenario (EXECUTING): Can not add step %r because the scenario "
      "has already been started" % self.mock_setup_step,
      str(ar.exception))

  def test_add_step_phase_not_found(self):
    self.scenario._status = Status.NOT_STARTED
    with self.assertRaises(ValueError) as ar:
      self.scenario.add_step(self.mock_setup_step, None)
    self.assertEqual(
      "Fake Scenario (NOT_STARTED): Can not add step %r because the phase "
      "None is invalid" % self.mock_setup_step,
      str(ar.exception))

  def test_completed_remaining_steps_default(self):
    self.assertEqual([], self.scenario.completed_steps())
    self.assertEqual([(Phase.SETUP, self.mock_setup_step),
                      (Phase.RUN, self.mock_run_step)],
                     self.scenario.remaining_steps())

  def test_completed_remaining_steps_one_succeeded(self):
    self.mock_setup_step.status = Status.SUCCEEDED
    self.assertEqual([(Phase.SETUP, self.mock_setup_step)],
                     self.scenario.completed_steps())
    self.assertEqual([(Phase.RUN, self.mock_run_step)],
                     self.scenario.remaining_steps())

  def test_completed_remaining_steps_all_succeeded(self):
    self.mock_setup_step.status = Status.SUCCEEDED
    self.mock_run_step.status = Status.SUCCEEDED
    self.assertEqual([(Phase.SETUP, self.mock_setup_step),
                      (Phase.RUN, self.mock_run_step)],
                     self.scenario.completed_steps())
    self.assertEqual([], self.scenario.remaining_steps())

  def test_completed_remaining_steps_one_failed(self):
    self.mock_setup_step.status = Status.FAILED
    self.assertEqual([], self.scenario.completed_steps())
    self.assertEqual([(Phase.SETUP, self.mock_setup_step),
                      (Phase.RUN, self.mock_run_step)],
                     self.scenario.remaining_steps())

  def test_progress_no_steps(self):
    scenario = Scenario()
    steps = [step for _, step in scenario.itersteps()]
    self.assertEqual(0, len(steps))
    self.assertEqual(0, scenario.progress())

  def test_progress_not_started(self):
    steps = [step for _, step in self.scenario.itersteps()]
    self.assertEqual(2, len(steps))
    self.assertEqual(0, self.scenario.progress())

  def test_progress_half_finished_success(self):
    self.mock_setup_step.status = Status.SUCCEEDED
    self.mock_run_step.status = Status.EXECUTING
    self.assertEqual(0.5, self.scenario.progress())

  def test_progress_half_finished_failure(self):
    self.mock_setup_step.status = Status.FAILED
    self.mock_run_step.status = Status.NOT_STARTED
    self.assertEqual(0.5, self.scenario.progress())

  def test_progress_all_finished_success(self):
    self.mock_setup_step.status = Status.SUCCEEDED
    self.mock_run_step.status = Status.SUCCEEDED
    self.assertEqual(1, self.scenario.progress())

  def test_progress_all_finished_failure(self):
    self.mock_setup_step.status = Status.SUCCEEDED
    self.mock_run_step.status = Status.FAILED
    self.assertEqual(1, self.scenario.progress())

  def test_duration_secs_both_none(self):
    scenario = Scenario()
    scenario._start_time_secs = None
    scenario._end_time_secs = None
    self.assertIsNone(scenario.duration_secs())

  @mock.patch("curie.scenario.time")
  def test_duration_secs_end_none(self, mock_time):
    scenario = Scenario()
    scenario._end_time_secs = None

    scenario._start_time_secs = 12345
    mock_time.time.return_value = 12346
    self.assertEqual(1, scenario.duration_secs())

  def test_duration_secs_start_none(self):
    scenario = Scenario()
    scenario._start_time_secs = None

    scenario._end_time_secs = 12345
    with self.assertRaises(RuntimeError):
      scenario.duration_secs()

  def test_duration_secs_neither_none(self):
    scenario = Scenario()

    scenario._start_time_secs = 12345
    scenario._end_time_secs = 12346
    self.assertEqual(1, scenario.duration_secs())

  def test_phase_is_new_active_terminal(self):
    new = [
      Status.NOT_STARTED,
    ]
    active = [
      Status.EXECUTING,
      Status.STOPPING,
      Status.FAILING,
    ]
    terminal = [
      Status.SUCCEEDED,
      Status.FAILED,
      Status.STOPPED,
      Status.CANCELED,
      Status.INTERNAL_ERROR,
    ]
    self.assertEqual(new + active + terminal, list(Status))
    for status in new:
      self.assertTrue(status.is_new())
      self.assertFalse(status.is_active())
      self.assertFalse(status.is_terminal())
    for status in active:
      self.assertFalse(status.is_new())
      self.assertTrue(status.is_active())
      self.assertFalse(status.is_terminal())
    for status in terminal:
      self.assertFalse(status.is_new())
      self.assertFalse(status.is_active())
      self.assertTrue(status.is_terminal())

  def test_four_corners_save_state_load_state(self):
    four_corners_directory = os.path.join(
      environment.top, "curie", "yaml", "four_corners_microbenchmark")
    scenario = scenario_parser.from_path(four_corners_directory)
    scenario.cluster = mock_cluster()
    scenario.output_directory = environment.test_output_dir(self)

    scenario.save_state()
    unfrozen_caveman_scenario = scenario.load_state(
      environment.test_output_dir(self))

    self.assertEqual(scenario.id, unfrozen_caveman_scenario.id)
    self.assertEqual(None, unfrozen_caveman_scenario.cluster)
    self.assertEqual(6, len(unfrozen_caveman_scenario.results_map))

  def test_four_corners_save_state_load_state_experimental_metrics(self):
    four_corners_directory = os.path.join(
      environment.top, "curie", "yaml", "four_corners_microbenchmark")
    scenario = scenario_parser.from_path(four_corners_directory,
                                         enable_experimental_metrics=True)
    scenario.cluster = mock_cluster()
    scenario.output_directory = environment.test_output_dir(self)

    scenario.save_state()
    unfrozen_caveman_scenario = scenario.load_state(
      environment.test_output_dir(self))

    self.assertEqual(scenario.id, unfrozen_caveman_scenario.id)
    self.assertEqual(None, unfrozen_caveman_scenario.cluster)
    self.assertEqual(7, len(unfrozen_caveman_scenario.results_map))
    for result in unfrozen_caveman_scenario.results_map.itervalues():
      if isinstance(result, ClusterResult):
        self.assertIsInstance(result.metric, CurieMetric)

  @mock.patch("curie.scenario.os.makedirs")
  def test_save_state_unexpected_oserror(self, mock_makedirs):
    scenario = Scenario()
    scenario.cluster = mock_cluster()
    scenario.output_directory = environment.test_output_dir(self)
    err = OSError()
    err.errno = errno.EIO
    mock_makedirs.side_effect = err
    with self.assertRaises(OSError) as ar:
      scenario.save_state()
    self.assertEqual(errno.EIO, ar.exception.errno)

  @mock.patch("curie.scenario.shutil.copytree")
  @mock.patch("curie.scenario.shutil.copy2")
  @mock.patch("curie.scenario.os.path.isdir")
  @mock.patch("curie.scenario.os.path.isfile")
  @mock.patch("curie.scenario.os.listdir")
  @mock.patch("curie.scenario.open", new_callable=mock.mock_open())
  @mock.patch("curie.scenario.check")
  @mock.patch("curie.scenario.Scenario._cluster_results_update")
  @mock.patch("curie.scenario.Scenario._scenario_results_update")
  def test_start_join_copy_configuration_files(
      self, mock_sru, mock_cru, mock_check, mock_open, m_listdir,
      m_isfile, m_isdir, m_copy2, mock_copytree):
    self.scenario._status = Status.NOT_STARTED
    mock_sru.return_value = 0
    mock_cru.return_value = 0
    m_listdir.return_value = ["file_1", "dir_1", "file_2"]
    m_isfile.side_effect = [True, False, True]
    m_isdir.side_effect = [True]
    self.scenario.source_directory = "/fake/source/directory"
    # Mock this to prevent FATAL from doing bad things during unit test.
    mock_check.prereq_runtime_vm_storage_is_ready.return_value = True

    self.scenario.start()
    self.scenario.join()

    self.mock_setup_step.assert_called_once()
    self.mock_run_step.assert_called_once()
    m_copy2.assert_has_calls([
      mock.call("/fake/source/directory/file_1",
                os.path.join(self.scenario.output_directory, "file_1")),
      mock.call("/fake/source/directory/file_2",
                os.path.join(self.scenario.output_directory, "file_2")),
    ])
    mock_copytree.assert_called_once_with(
      "/fake/source/directory/dir_1",
      os.path.join(self.scenario.output_directory, "dir_1"))
    mock_open.assert_not_called()
    self.assertEqual(Status.SUCCEEDED, self.scenario.status())
    self.assertIsNotNone(self.scenario._start_time_secs)
    self.assertIsNotNone(self.scenario._end_time_secs)

  @mock.patch("curie.scenario.shutil.copytree")
  @mock.patch("curie.scenario.open", new_callable=mock.mock_open())
  @mock.patch("curie.scenario.check")
  @mock.patch("curie.scenario.Scenario._cluster_results_update")
  @mock.patch("curie.scenario.Scenario._scenario_results_update")
  def test_start_join_write_yaml(
      self, mock_sru, mock_cru, mock_check, mock_open, mock_copytree):
    self.scenario._status = Status.NOT_STARTED
    mock_sru.return_value = 0
    mock_cru.return_value = 0
    self.scenario.yaml_str = "NOT A REAL YAML STRING"
    # Mock this to prevent FATAL from doing bad things during unit test.
    mock_check.prereq_runtime_vm_storage_is_ready.return_value = True

    self.scenario.start()
    self.scenario.join()

    self.mock_setup_step.assert_called_once()
    self.mock_run_step.assert_called_once()
    mock_copytree.assert_not_called()
    mock_open.assert_called_once_with(
      os.path.join(self.scenario.output_directory, "test-parsed.yml"), "w")
    fh = mock_open.return_value.__enter__.return_value
    fh.write.assert_called_once_with("NOT A REAL YAML STRING")
    self.assertEqual(Status.SUCCEEDED, self.scenario.status())
    self.assertIsNotNone(self.scenario._start_time_secs)
    self.assertIsNotNone(self.scenario._end_time_secs)

  def test_start_running(self):
    self.scenario._status = Status.EXECUTING

    with self.assertRaises(RuntimeError) as ar:
      self.scenario.start()

    self.assertEqual("Fake Scenario (EXECUTING): Can not start because it "
                     "has already been started",
                     str(ar.exception))
    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.EXECUTING, self.scenario.status())

  def test_start_no_cluster_set(self):
    self.scenario._status = Status.NOT_STARTED
    self.scenario.cluster = None

    with self.assertRaises(RuntimeError) as ar:
      self.scenario.start()

    self.assertEqual("Fake Scenario (NOT_STARTED): Can not start because "
                     "cluster has not been set (perhaps it has been "
                     "pickled/unpickled?)",
                     str(ar.exception))
    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.NOT_STARTED, self.scenario.status())

  def test_start_no_output_directory_set(self):
    self.scenario._status = Status.NOT_STARTED
    self.scenario.output_directory = None

    with self.assertRaises(RuntimeError) as ar:
      self.scenario.start()

    self.assertEqual("Fake Scenario (NOT_STARTED): Can not start because "
                     "output directory has not been set",
                     str(ar.exception))
    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.NOT_STARTED, self.scenario.status())

  def test_start_manually_added_presetup_steps(self):
    self.scenario._status = Status.NOT_STARTED
    mock_step = mock.Mock(spec=BaseStep)
    mock_step.requirements.return_value = set()
    self.scenario.add_step(mock_step, Phase.PRE_SETUP)

    with self.assertRaises(RuntimeError) as ar:
      self.scenario.start()

    self.assertEqual("Fake Scenario (NOT_STARTED): Steps should not be "
                     "manually added to the PreSetup phase",
                     str(ar.exception))
    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.NOT_STARTED, self.scenario.status())

  @mock.patch("curie.scenario.os.makedirs")
  def test_start_unexpected_oserror(self, mock_makedirs):
    self.scenario._status = Status.NOT_STARTED
    err = OSError()
    err.errno = errno.EIO
    mock_makedirs.side_effect = err
    with self.assertRaises(OSError) as ar:
      self.scenario.start()

    self.assertEqual(errno.EIO, ar.exception.errno)
    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.NOT_STARTED, self.scenario.status())

  def test_start_in_terminal_state(self):
    self.scenario._status = Status.STOPPED

    with self.assertRaises(RuntimeError):
      self.scenario.start()

    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.STOPPED, self.scenario.status())
    self.scenario.join()

  def test_stop_new(self):
    self.scenario._status = Status.NOT_STARTED

    self.scenario.stop()

    self.assertEqual(Status.CANCELED, self.scenario.status())

  def test_stop_active(self):
    self.scenario._status = Status.EXECUTING

    self.scenario.stop()

    self.assertEqual(Status.STOPPING, self.scenario.status())

  def test_stop_terminal(self):
    self.scenario._status = Status.SUCCEEDED

    self.scenario.stop()

    self.assertEqual(Status.SUCCEEDED, self.scenario.status())

  def test_stop_stopping(self):
    self.scenario._status = Status.STOPPING

    self.scenario.stop()

    self.assertEqual(Status.STOPPING, self.scenario.status())

  def test_join_not_started(self):
    self.scenario._status = Status.NOT_STARTED
    with self.assertRaises(RuntimeError) as ar:
      self.scenario.join()
    self.assertEqual("Fake Scenario (NOT_STARTED) can not be joined because "
                     "it has not been started", str(ar.exception))

  def test_join_active(self):
    self.scenario._status = Status.EXECUTING
    self.scenario.join()
    self.assertEqual(Status.SUCCEEDED, self.scenario.status())

  def test_join_failing(self):
    self.scenario._status = Status.FAILING
    self.scenario.join()
    self.assertEqual(Status.FAILED, self.scenario.status())

  def test_join_stopping(self):
    self.scenario._status = Status.STOPPING
    self.scenario.join()
    self.assertEqual(Status.STOPPED, self.scenario.status())

  def test_join_terminal(self):
    self.scenario._status = Status.STOPPED
    self.scenario.join()
    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())

  def test_join_all_alive(self):
    mock_thread_0 = mock.Mock()
    mock_thread_1 = mock.Mock()
    mock_thread_0.is_alive.return_value = True
    mock_thread_1.is_alive.return_value = True
    self.scenario._status = Status.EXECUTING
    self.scenario._threads.append(mock_thread_0)
    self.scenario._threads.append(mock_thread_1)

    self.scenario.join()

    mock_thread_0.is_alive.assert_called_once()
    mock_thread_1.is_alive.assert_called_once()
    mock_thread_0.join.assert_called_once_with(timeout=None)
    mock_thread_1.join.assert_called_once_with(timeout=None)

  def test_join_one_alive(self):
    mock_thread_0 = mock.Mock()
    mock_thread_1 = mock.Mock()
    mock_thread_0.is_alive.return_value = True
    mock_thread_1.is_alive.return_value = False
    self.scenario._status = Status.EXECUTING
    self.scenario._threads.append(mock_thread_0)
    self.scenario._threads.append(mock_thread_1)

    self.scenario.join()

    mock_thread_0.is_alive.assert_called_once()
    mock_thread_1.is_alive.assert_called_once()
    mock_thread_0.join.assert_called_once_with(timeout=None)
    mock_thread_1.join.not_called()

  def test_join_pass_through_timeout_parameter(self):
    mock_thread_0 = mock.Mock()
    mock_thread_0.is_alive.return_value = True
    self.scenario._status = Status.EXECUTING
    self.scenario._threads.append(mock_thread_0)

    self.scenario.join(12345)

    mock_thread_0.is_alive.assert_called_once()
    mock_thread_0.join.assert_called_once_with(timeout=12345)

  def test_join_unhandled_exception(self):
    err = RuntimeError("OMGWTFBBQ")
    mock_thread_0 = mock.Mock()
    mock_thread_0.is_alive.return_value = True
    mock_thread_0.join.side_effect = err
    self.scenario._status = Status.EXECUTING
    self.scenario._threads.append(mock_thread_0)

    self.scenario.join(12345)

    mock_thread_0.is_alive.assert_called_once()
    mock_thread_0.join.assert_called_once_with(timeout=12345)
    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())
    self.assertEqual("OMGWTFBBQ", self.scenario.error_message())

  def test_verify_steps(self):
    err = RuntimeError("OMGWTFBBQ")
    self.mock_setup_step.verify.side_effect = err
    failures = self.scenario.verify_steps()
    self.assertEqual(1, len(failures))
    self.assertEqual(self.mock_setup_step, failures[0])

  def test_create_annotation(self):
    self.assertEqual([], self.scenario._annotations)
    self.scenario.create_annotation("Something interesting happened")
    self.assertEqual(1, len(self.scenario._annotations))
    self.assertIsInstance(self.scenario._annotations[0],
                          CurieTestResult.Data2D.XAnnotation)
    self.assertEqual("Something interesting happened",
                     self.scenario._annotations[0].description)

  def test_resource_path(self):
    self.scenario.source_directory = None
    with self.assertRaises(ValueError) as ar:
      self.scenario.resource_path("file.fio")
    self.assertEqual("Cannot find path to resource 'file.fio' because "
                     "source_directory has not been set",
                     str(ar.exception))

  @mock.patch("curie.util.CurieUtil.wait_for_any")
  def test_wait_for_true_not_returned(self, mock_wait_for_any):
    mock_wait_for_any.return_value = [False, False]
    with self.assertRaises(RuntimeError) as ar:
      self.scenario.wait_for(mock.Mock(spec=callable),
                             "Waiting for a fake function",
                             10)
    self.assertEqual("Expected one or more return values to evaluate as True: "
                     "[False, False]", str(ar.exception))

  def test_wait_for_all(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      mock_callable = mock.Mock(side_effect=[False, False, True])
      ret = self.scenario.wait_for_all([mock_callable], "test", 30)
    self.assertEqual(ret, [True])
    self.assertEqual(mock_callable.call_count, 3)

  def test_wait_for_all_custom_interval(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      mock_callable = mock.Mock(side_effect=[False, False, True])
      ret = self.scenario.wait_for_all([mock_callable], "test", 30, 10)
    self.assertEqual(ret, [True])
    self.assertEqual(mock_callable.call_count, 3)
    mock_time.sleep.assert_has_calls([mock.call(10), mock.call(10)])

  def test_wait_for_all_skip_succeeded_default(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      mock_callable_1 = mock.Mock(side_effect=[True, True, True])
      mock_callable_2 = mock.Mock(side_effect=[False, False, True])
      ret = self.scenario.wait_for_all([mock_callable_1, mock_callable_2],
                                       "test", 30)
    self.assertEqual(ret, [True, True])
    self.assertEqual(mock_callable_1.call_count, 1)
    self.assertEqual(mock_callable_2.call_count, 3)

  def test_wait_for_all_skip_succeeded_false(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      mock_callable_1 = mock.Mock(side_effect=[True, False, True])
      mock_callable_2 = mock.Mock(side_effect=[False, False, True])
      ret = self.scenario.wait_for_all([mock_callable_1, mock_callable_2],
                                       "test", 30,
                                       skip_succeeded=False)
    self.assertEqual(ret, [True, True])
    self.assertEqual(mock_callable_1.call_count, 3)
    self.assertEqual(mock_callable_2.call_count, 3)

  def test_wait_for_all_timeout(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      with self.assertRaises(RuntimeError):
        self.scenario.wait_for_all([lambda: False], "test", 1)

  def test_wait_for_all_should_stop(self):
    with mock.patch("curie.scenario.time") as mock_time:
      mock_time.time.side_effect = lambda: mock_time.time.call_count
      mock_time.sleep.return_value = 0
      with mock.patch.object(self.scenario,
                             "should_stop") as mock_should_stop:
        mock_should_stop.side_effect = chain([False], cycle([True]))
        ret = self.scenario.wait_for_all([lambda: False], "test", 30)
    self.assertEqual(ret, None)

  @mock.patch("curie.scenario.check")
  @mock.patch("curie.scenario.nodes")
  def test_initialize_presetup_phase_overwrite(self, mock_nodes, mock_check):
    self.scenario._status = Status.NOT_STARTED
    mock_step = mock.Mock(spec=BaseStep)
    mock_step.requirements.return_value = set()
    self.scenario.add_step(mock_step, Phase.PRE_SETUP)
    self.assertEqual(3, len([_ for _ in self.scenario.itersteps()]))

    # Mock this to prevent FATAL from doing bad things during unit test.
    mock_check.prereq_runtime_vm_storage_is_ready.return_value = True
    self.scenario._initialize_presetup_phase()

    self.assertEqual(2, len([_ for _ in self.scenario.itersteps()]))

    mock_check.OobConfigured.assert_not_called()

  @mock.patch("curie.scenario.check")
  @mock.patch("curie.scenario.nodes")
  def test_initialize_presetup_phase_with_oob(self, mock_nodes, mock_check):
    scenario = Scenario()
    scenario._status = Status.NOT_STARTED
    mock_setup_step = mock.Mock(spec=BaseStep)
    mock_run_step = mock.Mock(spec=BaseStep)
    mock_setup_step.requirements.return_value = set([BaseStep.OOB_CONFIG])
    mock_run_step.requirements.return_value = set()
    scenario.add_step(mock_setup_step, Phase.SETUP)
    scenario.add_step(mock_run_step, Phase.RUN)
    self.assertEqual(2, len([_ for _ in scenario.itersteps()]))

    # Mock this to prevent FATAL from doing bad things during unit test.
    mock_check.prereq_runtime_vm_storage_is_ready.return_value = True
    scenario._initialize_presetup_phase()

    self.assertEqual(3, len([_ for _ in scenario.itersteps()]))

    mock_check.OobConfigured.assert_called_once_with(scenario)

  def test_step_loop(self):
    self.scenario._step_loop()

    self.mock_setup_step.assert_called_once()
    self.mock_run_step.assert_called_once()
    self.assertEqual(Status.EXECUTING, self.scenario.status())
    self.assertIs(self.scenario.error_message(), None)
    self.assertIsNotNone(self.scenario._end_time_secs)

  def test_step_loop_handled_exception(self):
    self.mock_run_step.side_effect = IOError("This error is for you")
    self.scenario._step_loop()

    self.mock_setup_step.assert_called_once()
    self.mock_run_step.assert_called_once()
    self.assertEqual(Status.FAILING, self.scenario.status())
    self.assertEqual("This error is for you", self.scenario.error_message())
    self.assertIsNotNone(self.scenario._end_time_secs)

  @mock.patch("curie.scenario.Scenario._execute_step")
  def test_step_loop_unhandled_exception(self, mock_es):
    mock_es.side_effect = RuntimeError("This error is for you")
    self.scenario._step_loop()

    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())
    self.assertEqual("This error is for you", self.scenario.error_message())
    self.assertIsNotNone(self.scenario._end_time_secs)

  def test_step_loop_stopped(self):
    self.mock_setup_step.side_effect = self.scenario.stop
    self.scenario._step_loop()

    self.mock_setup_step.assert_called_once()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.STOPPING, self.scenario.status())
    self.assertIs(self.scenario.error_message(), None)
    self.assertIsNotNone(self.scenario._end_time_secs)

  def test_step_loop_verification_failed(self):
    err = RuntimeError("OMGWTFBBQ")
    self.mock_setup_step.verify.side_effect = err

    self.scenario._step_loop()

    self.mock_setup_step.assert_not_called()
    self.mock_run_step.assert_not_called()
    self.assertEqual(Status.FAILING, self.scenario.status())
    self.assertEqual("1 step failed to verify: %r" % self.mock_setup_step,
                     self.scenario.error_message())
    self.assertIsNotNone(self.scenario._end_time_secs)

  def test_scenario_results_loop_unhandled_exception(self):
    self.scenario._status = Status.EXECUTING
    err = RuntimeError("OMGWTFBBQ")
    with mock.patch.object(self.scenario,
                           "_scenario_results_update") as mock_sru:
      mock_sru.side_effect = err

      self.scenario._scenario_results_loop(interval_secs=0)

    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())
    self.assertEqual("OMGWTFBBQ", self.scenario.error_message())

  @mock.patch("curie.scenario.os.remove")
  @mock.patch("curie.scenario.os.path.isfile")
  def test_scenario_results_loop_unhandled_exception_remove_target_file(
      self, mock_os_path_isfile, mock_os_remove):
    mock_os_path_isfile.return_value = True
    self.scenario._status = Status.EXECUTING
    self.scenario.prometheus_config_directory = "/a/fake/directory"
    err = RuntimeError("OMGWTFBBQ")
    with mock.patch.object(self.scenario,
                           "_scenario_results_update") as mock_sru:
      mock_sru.side_effect = err

      self.scenario._scenario_results_loop(interval_secs=0)

    mock_os_remove.assert_called_once_with(
      "/a/fake/directory/targets_%s.json" % self.scenario.id)

  @mock.patch("curie.scenario.os.remove")
  @mock.patch("curie.scenario.os.path.isfile")
  def test_scenario_results_loop_unhandled_exception_not_remove_target_file(
      self, mock_os_path_isfile, mock_os_remove):
    mock_os_path_isfile.return_value = False
    self.scenario._status = Status.EXECUTING
    self.scenario.prometheus_config_directory = "/a/fake/directory"
    err = RuntimeError("OMGWTFBBQ")
    with mock.patch.object(self.scenario,
                           "_scenario_results_update") as mock_sru:
      mock_sru.side_effect = err

      self.scenario._scenario_results_loop(interval_secs=0)

    mock_os_remove.assert_not_called()

  def test_cluster_results_loop(self):
    def set_end_time_secs(*_):
      # Allow _cluster_results_update to be called a certain number of times
      # before setting _end_time_secs, stopping the loop.
      if mock_cru.call_count > 1:
        self.scenario._end_time_secs = 1

    self.scenario._status = Status.EXECUTING
    with mock.patch.object(self.scenario,
                           "_cluster_results_update") as mock_cru:
      mock_cru.side_effect = set_end_time_secs

      self.scenario._cluster_results_loop(interval_secs=0)

    self.assertEqual(Status.EXECUTING, self.scenario.status())
    self.assertEqual(2, mock_cru.call_count)

  def test_cluster_results_loop_unhandled_exception(self):
    self.scenario._status = Status.EXECUTING
    err = RuntimeError("OMGWTFBBQ")
    with mock.patch.object(self.scenario,
                           "_cluster_results_update") as mock_cru:
      mock_cru.side_effect = err

      self.scenario._cluster_results_loop()

    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())
    self.assertEqual("OMGWTFBBQ", self.scenario.error_message())
    mock_cru.assert_called_once()

  def test_cluster_results_loop_receives_None(self):
    def set_end_time_secs(*_):
      # Allow _cluster_results_update to be called a certain number of times
      # before setting _end_time_secs, stopping the loop.
      if mock_cru.call_count == 1:
        return 12345
      elif mock_cru.call_count == 2:
        return None
      elif mock_cru.call_count == 3:
        return 12347
      elif mock_cru.call_count > 3:
        self.scenario._end_time_secs = 1

    self.scenario._status = Status.EXECUTING
    with mock.patch.object(self.scenario,
                           "_cluster_results_update") as mock_cru:
      mock_cru.side_effect = set_end_time_secs

      self.scenario._cluster_results_loop(interval_secs=0)

    self.assertEqual(Status.EXECUTING, self.scenario.status())
    self.assertEqual(4, mock_cru.call_count)
    mock_cru.assert_has_calls([mock.call(),
                               mock.call(12345),
                               mock.call(12345),
                               mock.call(12347)])

  def test_execute_step(self):
    mock_step = mock.Mock()

    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_called_once()
    self.assertEqual(Status.EXECUTING, self.scenario.status())

  def test_execute_step_exception_sets_failing_state(self):
    mock_step = mock.Mock()
    self.assertEqual(None, self.scenario.error_message())

    mock_step.side_effect = IOError("This error is for you")
    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_called_once()
    self.assertEqual(Status.FAILING, self.scenario.status())
    self.assertEqual("This error is for you", self.scenario.error_message())

  def test_execute_step_in_new_state_raises_exception(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.NOT_STARTED

    with self.assertRaises(RuntimeError) as ar:
      self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_not_called()
    self.assertIn("executed before Fake Scenario (NOT_STARTED) entered a "
                  "running state", str(ar.exception))
    self.assertEqual(Status.NOT_STARTED, self.scenario.status())

  def test_execute_step_in_terminal_state_raises_exception(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.SUCCEEDED

    with self.assertRaises(RuntimeError) as ar:
      self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_not_called()
    self.assertIn("executed while Fake Scenario (SUCCEEDED) was in a "
                  "terminal state", str(ar.exception))
    self.assertEqual(Status.SUCCEEDED, self.scenario.status())

  def test_execute_step_in_stopping_state_skips_execution(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.STOPPING

    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_not_called()
    self.assertEqual(Status.STOPPING, self.scenario.status())

  def test_execute_step_in_failing_state_skips_execution(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.FAILING

    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_not_called()
    self.assertEqual(Status.FAILING, self.scenario.status())

  def test_execute_step_raises_expected_ScenarioStoppedError(self):
    def set_stopping(*args, **kwargs):
      self.scenario._status = Status.STOPPING
      raise ScenarioStoppedError()

    mock_step = mock.Mock()
    self.scenario._status = Status.EXECUTING
    mock_step.side_effect = set_stopping

    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_called_once()
    self.assertEqual(Status.STOPPING, self.scenario.status())

  def test_execute_step_raises_unexpected_ScenarioStoppedError(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.EXECUTING
    mock_step.side_effect = ScenarioStoppedError

    with self.assertRaises(ScenarioStoppedError):
      self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_called_once()
    self.assertEqual(Status.EXECUTING, self.scenario.status())

  def test_execute_step_in_internal_error_state(self):
    mock_step = mock.Mock()
    self.scenario._status = Status.INTERNAL_ERROR

    self.scenario._execute_step(Phase.RUN, mock_step)

    mock_step.assert_not_called()
    self.assertEqual(Status.INTERNAL_ERROR, self.scenario.status())

  def test_scenario_results_update_uninitialized(self):
    self.scenario._scenario_results_update()

  def test_scenario_results_update(self):
    ctr = CurieTestResult()
    result = mock.Mock(spec=IogenResult)
    result.get_result_pbs.return_value = [ctr]
    self.scenario.phase = Phase.RUN
    self.scenario.results_map = {"node_id": result}
    self.scenario.create_annotation("An important thing happened here")

    self.scenario._scenario_results_update()

    result.get_result_pbs.assert_called_once_with()
    self.assertEqual(ctr, self.scenario.results()[0])
    self.assertEqual(1, len(ctr.data_2d.x_annotations))
    self.assertEqual(1, len(self.scenario.results()[0].data_2d.x_annotations))

    self.scenario.create_annotation("This annotation was added without "
                                    "calling _scenario_results_update()")
    self.assertEqual(ctr, self.scenario.results()[0])
    self.assertEqual(2, len(self.scenario.results()[0].data_2d.x_annotations))

  @mock.patch("curie.scenario.prometheus.scenario_target_config")
  @mock.patch("curie.scenario.OsUtil.write_and_rename")
  def test_scenario_results_update_create_prometheus_target_config(
      self, mock_w_and_r, mock_stc):
    ctr = CurieTestResult()
    result = mock.Mock(spec=IogenResult)
    result.get_result_pbs.return_value = [ctr]
    mock_stc.return_value = ["targets"]
    self.scenario.phase = Phase.RUN
    self.scenario.results_map = {"node_id": result}
    # self.scenario.prometheus_config_directory = "/a/fake/directory"

    self.scenario._scenario_results_update("/fake/directory/targets.json")

    mock_stc.assert_called_once_with(self.scenario)
    mock_w_and_r.assert_called_once_with("/fake/directory/targets.json",
                                         "[\"targets\"]")

  def test_scenario_results_update_setup(self):
    result = mock.Mock(spec=IogenResult)
    self.scenario.phase = Phase.SETUP

    self.scenario._scenario_results_update()

    result.get_result_pbs.assert_not_called()
    self.assertEqual([], self.scenario.results())

  @mock.patch("curie.scenario.log")
  @mock.patch("curie.scenario.prometheus.scenario_target_config")
  @mock.patch("curie.scenario.OsUtil.write_and_rename")
  def test_scenario_results_update_no_netstats_ahv(
      self, mock_w_and_r, mock_stc, mock_log):
    result = ClusterResult.parse(self.scenario, "Cluster Network Bandwidth",
                                 {"metric": "NetReceived.Avg.KilobytesPerSecond",
                                  "aggregate": "sum"})
    result.get_result_pbs = mock.Mock()
    mock_stc.return_value = ["targets"]
    self.scenario.cluster = mock.Mock(spec=AcropolisCluster)
    self.scenario.phase = Phase.RUN
    self.scenario.results_map = {"Cluster Network Bandwidth": result}
    self.scenario._scenario_results_update("/fake/directory/targets.json")

    mock_log.debug.assert_has_calls([
      mock.call("Skipping result update since AHV API "
      "does not return valid results for network statistics")])
    result.get_result_pbs.assert_not_called()

  def test_scenario_results_update_teardown(self):
    result = mock.Mock(spec=IogenResult)
    self.scenario.phase = Phase.TEARDOWN

    self.scenario._scenario_results_update()

    result.get_result_pbs.assert_not_called()
    self.assertEqual([], self.scenario.results())

  @mock.patch("curie.scenario.log")
  def test_scenario_results_update_handle_exception(self, mock_log):
    result = mock.Mock(spec=IogenResult)
    result.get_result_pbs.side_effect = IOError
    self.scenario.results_map = {"node_id": result}
    prev_results_pbs = self.scenario.results()

    self.scenario._scenario_results_update()

    self.assertIs(prev_results_pbs, self.scenario.results())
    mock_log.exception.assert_called_once_with(
      "An exception occurred while updating scenario results. This operation "
      "will be retried.")

  def test_cluster_results_update(self):
    self.scenario.cluster.collect_performance_stats.return_value = {
      "Node 1": [CurieMetric(name=CurieMetric.kCpuUsage,
                              description="Average CPU usage for all cores.",
                              instance="Aggregated",
                              type=CurieMetric.kGauge,
                              consolidation=CurieMetric.kAvg,
                              unit=CurieMetric.kPercent,
                              experimental=True)],
      "Node 2": [CurieMetric(name=CurieMetric.kCpuUsage,
                              description="Average CPU usage for all cores.",
                              instance="Aggregated",
                              type=CurieMetric.kGauge,
                              consolidation=CurieMetric.kAvg,
                              unit=CurieMetric.kPercent,
                              experimental=True)],
    }

    self.scenario._cluster_results_update()

    self.assertTrue(os.path.isdir(os.path.join(self.scenario.output_directory,
                                               "cluster_stats")))
    csv_path = os.path.join(self.scenario.output_directory,
                            "cluster_stats",
                            "cluster_stats.csv")
    self.assertTrue(os.path.isfile(csv_path))

    # Execute again to test the appending code path.
    self.scenario._cluster_results_update()

  @mock.patch("curie.scenario.log")
  def test_cluster_results_update_handle_exception(self, mock_log):
    self.scenario.cluster.collect_performance_stats.side_effect = IOError

    self.scenario._cluster_results_update()

    self.assertFalse(os.path.isdir(os.path.join(self.scenario.output_directory,
                                                "cluster_stats")))
    mock_log.exception.assert_called_once_with(
      "An exception occurred while updating cluster results. This operation "
      "will be retried.")

  def test_pickle_four_corners(self):
    four_corners_directory = os.path.join(
      environment.top, "curie", "yaml", "four_corners_microbenchmark")
    scenario = scenario_parser.from_path(four_corners_directory)
    scenario.cluster = mock_cluster()

    unfrozen_caveman_scenario = pickle.loads(pickle.dumps(scenario))

    self.assertEqual(scenario.id, unfrozen_caveman_scenario.id)
    self.assertEqual(None, unfrozen_caveman_scenario.cluster)
