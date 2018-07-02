#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import unittest
from itertools import chain, cycle

import mock

from curie.charon_agent_interface_pb2 import CmdStatus
from curie.cluster import Cluster
from curie.iogen import iogen
from curie.scenario import Scenario
from curie.vm import Vm
from curie.testing import environment


class TestIOGen(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    self.vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.iogen = iogen.IOGen("test_iogen", self.scenario, "", "test_iogen")

  def test_start_no_runtime(self):
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    with mock.patch.object(self.iogen, "_cmd") as mock_cmd:
      mock_cmd.return_value = "go_make_io_happen"
      self.scenario.should_stop = lambda: False
      self.iogen.start(self.vms)
    for vm in self.vms:
      vm.execute_async.assert_called_once_with(
        self.iogen.get_cmd_id(), "go_make_io_happen", user="root")
    self.assertIsNone(self.iogen._expected_workload_finish_secs)

  @mock.patch("curie.iogen.iogen.time")
  def test_start_sets_expected_finish_time_no_stagger(self, mock_time):
    mock_time.time.return_value = 12345
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    with mock.patch.object(self.iogen, "_cmd") as mock_cmd:
      mock_cmd.return_value = "go_make_io_happen"
      self.scenario.should_stop = lambda: False
      self.iogen.start(self.vms, runtime_secs=900)
    for vm in self.vms:
      vm.execute_async.assert_called_once_with(
        self.iogen.get_cmd_id(), "go_make_io_happen", user="root")
    self.assertEqual(12345 + 900, self.iogen._expected_workload_finish_secs)

  @mock.patch("curie.iogen.iogen.ScenarioUtil.wait_for_deadline")
  @mock.patch("curie.iogen.iogen.time")
  def test_start_sets_expected_finish_time_stagger(self, mock_time, mock_wfd):
    mock_wfd.return_value = 0
    mock_time.time.return_value = 12345
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    with mock.patch.object(self.iogen, "_cmd") as mock_cmd:
      mock_cmd.return_value = "go_make_io_happen"
      self.scenario.should_stop = lambda: False
      self.iogen.start(self.vms, runtime_secs=900, stagger_secs=900)
    for vm in self.vms:
      vm.execute_async.assert_called_once_with(
        self.iogen.get_cmd_id(), "go_make_io_happen", user="root")
    self.assertEqual(12345 + 1800, self.iogen._expected_workload_finish_secs)

  def test_wait_default(self):
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    self.scenario.should_stop = lambda: False
    results = self.iogen.wait(self.vms)
    for result in results:
      self.assertTrue(result.success)

  def test_wait_CmdStatus_kFailed(self):
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kFailed,
                                                 exit_status=1)
    self.scenario.should_stop = lambda: False
    results = self.iogen.wait(self.vms)
    for result in results:
      self.assertFalse(result.success)

  def test_wait_should_stop_before(self):
    self.scenario.should_stop = lambda: True
    with mock.patch("curie.iogen.iogen.IOGen.stop") as mock_stop:
      results = self.iogen.wait(self.vms)
      for result in results:
        self.assertFalse(result.success)
      self.assertEqual(mock_stop.call_count, len(self.vms))

  def test_wait_should_stop_during(self):
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
      mock_should_stop.side_effect = chain([False, False], cycle([True]))
      with mock.patch("curie.iogen.iogen.IOGen.stop") as mock_stop:
        results = self.iogen.wait(self.vms)
        num_success = len([result.success for result in results
                           if result.success])
        num_failure = len([result.success for result in results
                           if not result.success])
        self.assertGreater(num_success, 0)
        self.assertGreater(num_failure, 0)
        self.assertGreater(mock_stop.call_count, 0)

  def test_wait_vm_inaccessible(self):
    for mock_vm in self.vms:
      mock_vm.check_cmd.return_value = CmdStatus(state=CmdStatus.kSucceeded,
                                                 exit_status=0)
    self.scenario.should_stop = lambda: False
    self.vms[0].is_accessible.return_value = False
    with mock.patch.object(iogen.log, "warning",
                           wraps=iogen.log.warning) as mock_warning:
      results = self.iogen.wait(self.vms)
      self.assertFalse(results[0].success)
      for result in results[1:]:
        self.assertTrue(result.success)
      self.assertGreater(mock_warning.call_count, 0)
