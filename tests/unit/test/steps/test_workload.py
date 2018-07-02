#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

import mock

from curie.cluster import Cluster
from curie.exception import CurieTestException
from curie.iogen.iogen import IOGen, ResultResponse
from curie.node import Node
from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.test.workload import Workload
from curie.vm import Vm
from curie.testing import environment


class TestStepsWorkload(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)
    _nodes = [mock.Mock(spec=Node) for _ in xrange(4)]
    for id, node in enumerate(_nodes):
      node.node_id.return_value = id
    self.cluster.nodes.return_value = _nodes
    self.cluster.node_count.return_value = len(_nodes)
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    self.cluster.find_vms_regex.return_value = self.vms
    for vm in self.vms:
      vm.is_accessible.return_value = True
    self.vm_group = VMGroup(self.scenario, "group_0")
    for mock_vm, vm_name in zip(self.vms, self.vm_group.get_vms_names()):
      mock_vm.vm_name.return_value = vm_name
    self.workload = mock.Mock(spec=Workload)
    self.workload.name.return_value = "workload_0"
    self.workload.vm_group.return_value = self.vm_group
    self.scenario.workloads = {self.workload.name(): self.workload}

  @mock.patch("curie.test.steps.workload.PrefillWait")
  def test_PrefillStart_default(self, mock_prefill_wait):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    step = steps.workload.PrefillStart(self.scenario, "workload_0")
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 0)
    self.assertEqual(mock_prefill_wait.call_count, 1)
    self.workload.iogen.assert_not_called()
    self.workload.prefill_iogen.assert_called_once_with()
    mock_prefill_iogen.prepare.assert_called_once_with(self.vms)
    mock_prefill_iogen.start.assert_called_once_with(self.vms,
                                                     runtime_secs=None,
                                                     stagger_secs=None,
                                                     seed=None)

  def test_PrefillStart_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.PrefillStart(self.scenario, "workload_0")
    self.assertIsInstance(step, steps.workload.PrefillStart)

  def test_PrefillStart_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.PrefillStart(self.scenario, "not_a_workload")

  def test_PrefillStart_vm_inaccessible(self):
    self.vms[-1].is_accessible.return_value = False
    step = steps.workload.PrefillStart(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  def test_PrefillStart_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.PrefillStart(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.workload.PrefillWait")
  def test_PrefillStart_async(self, mock_prefill_wait):
    step = steps.workload.PrefillStart(self.scenario, "workload_0", async=True)
    step()
    self.assertEqual(mock_prefill_wait.call_count, 0)

  def test_PrefillWait_default(self):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    mock_prefill_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillWait(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      with mock.patch.object(step, "create_annotation") as mock_annotate:
        step()
        self.assertEqual(mock_annotate.call_count, 0)
    self.workload.iogen.assert_not_called()
    self.workload.prefill_iogen.assert_called_once_with()
    mock_prefill_iogen.wait.assert_called_once_with(self.vms,
                                                    timeout_secs=7200)
    mock_prefill_iogen.fetch_remote_results.assert_called_once_with(self.vms)

  def test_PrefillWait_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.PrefillWait(self.scenario, "workload_0")
    self.assertIsInstance(step, steps.workload.PrefillWait)

  def test_PrefillWait_valid_timeout(self):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.prefill_iogen.return_value = mock_iogen
    mock_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillWait(self.scenario, "workload_0",
                                      timeout_secs=30)
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      step()
    mock_iogen.wait.assert_called_once_with(self.vms, timeout_secs=30)

  def test_PrefillWait_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.PrefillWait(self.scenario, "not_a_workload")

  def test_PrefillWait_results_not_found(self):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    mock_prefill_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillWait(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = False
      with self.assertRaises(CurieTestException):
        step()

  def test_PrefillWait_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.PrefillWait(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  def test_PrefillRun_default(self):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    mock_prefill_iogen.run_max_concurrent_workloads.return_value = [
      ResultResponse(success=True, vm=vm, failure_reason=None)
      for vm in self.vms]
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    mock_prefill_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      step()
    self.workload.prefill_iogen.assert_called_once_with(max_concurrent=8)
    mock_prefill_iogen.run_max_concurrent_workloads.assert_called_once_with(
      self.vms, 8, timeout_secs=7200)

  def test_PrefillRun_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    self.assertIsInstance(step, steps.workload.PrefillRun)

  def test_PrefillRun_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.PrefillRun(self.scenario, "not_a_workload")

  def test_PrefillRun_vm_inaccessible(self):
    self.vms[-1].is_accessible.return_value = False
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  def test_PrefillRun_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  def test_PrefillRun_parameters_concurrent(self):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    mock_prefill_iogen.run_max_concurrent_workloads.return_value = [
      ResultResponse(success=True, vm=vm, failure_reason=None)
      for vm in self.vms]
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    mock_prefill_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillRun(self.scenario, "workload_0",
                                     max_concurrent=16)
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      step()
    mock_prefill_iogen.run_max_concurrent_workloads.assert_called_once_with(
      self.vms, 16, timeout_secs=7200)

  def test_PrefillRun_results_not_found(self):
    mock_prefill_iogen = mock.Mock(spec=IOGen)
    mock_prefill_iogen.run_max_concurrent_workloads.return_value = [
      ResultResponse(success=True, vm=vm, failure_reason=None)
      for vm in self.vms]
    self.workload.prefill_iogen.return_value = mock_prefill_iogen
    mock_prefill_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = False
      with self.assertRaises(CurieTestException):
        step()

  def test_PrefillRun_raise_exception_on_failure(self):
    mock_iogen = mock.Mock(spec=IOGen)
    mock_iogen.run_max_concurrent_workloads.return_value = [
      ResultResponse(success=False, vm=vm, failure_reason="FAILED")
      for vm in self.vms]
    self.workload.prefill_iogen.return_value = mock_iogen
    step = steps.workload.PrefillRun(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.workload.Wait")
  def test_Start_default(self, mock_wait):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.iogen.return_value = mock_iogen
    step = steps.workload.Start(self.scenario, "workload_0", 30)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(mock_wait.call_count, 1)
    self.workload.iogen.assert_called_once_with()
    self.workload.prefill_iogen.assert_not_called()
    mock_iogen.prepare.assert_called_once_with(self.vms)
    mock_iogen.start.assert_called_once_with(self.vms, runtime_secs=30,
                                             stagger_secs=None,
                                             seed=None)

  def test_Start_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.Start(self.scenario, "workload_0", 30)
    self.assertIsInstance(step, steps.workload.Start)

  def test_Start_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.Start(self.scenario, "not_a_workload", 30)

  def test_Start_vm_inaccessible(self):
    self.vms[-1].is_accessible.return_value = False
    step = steps.workload.Start(self.scenario, "workload_0", 30)
    with self.assertRaises(CurieTestException):
      step()

  def test_Start_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.Start(self.scenario, "workload_0", 30)
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.workload.Wait")
  def test_Start_async(self, mock_wait):
    step = steps.workload.Start(self.scenario, "workload_0",
                                runtime_secs=30, async=True)
    step()
    self.assertEqual(mock_wait.call_count, 0)

  def test_Start_async_timeout(self):
    with self.assertRaises(CurieTestException):
      steps.workload.Start(self.scenario, "workload_0",
                           runtime_secs=30, timeout_secs=10, async=True)
      # This should throw an exception saying that timeout is not needed for an
      # async call

  @mock.patch("curie.test.steps.workload.Wait")
  def test_Start_sync_timeout_runtime(self, mock_wait):
    step = steps.workload.Start(self.scenario, "workload_0", runtime_secs=30,
                                timeout_secs=10, async=False)
    step()
    mock_wait.assert_called_once_with(self.scenario, "workload_0",
                                      timeout_secs=10,
                                      annotate=False)

  @mock.patch("curie.test.steps.workload.Wait")
  def test_Start_sync_timeout_noruntime(self, mock_wait):
    step = steps.workload.Start(self.scenario, "workload_0",
                                timeout_secs=10, async=False)
    step()
    mock_wait.assert_called_once_with(self.scenario, "workload_0",
                                      timeout_secs=10,
                                      annotate=False)

  def test_Wait_valid(self):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.iogen.return_value = mock_iogen
    mock_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.Wait(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      with mock.patch.object(step, "create_annotation") as mock_annotate:
        step()
        self.assertEqual(mock_annotate.call_count, 1)
    self.workload.iogen.assert_called_once_with()
    self.workload.prefill_iogen.assert_not_called()
    mock_iogen.wait.assert_called_once_with(self.vms, timeout_secs=None)
    mock_iogen.fetch_remote_results.assert_called_once_with(self.vms)

  def test_Wait_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.Wait(self.scenario, "workload_0")
    self.assertIsInstance(step, steps.workload.Wait)

  def test_Wait_with_timeout(self):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.iogen.return_value = mock_iogen
    mock_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.Wait(self.scenario, "workload_0", timeout_secs=10)
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      with mock.patch.object(step, "create_annotation") as mock_annotate:
        step()
        self.assertEqual(mock_annotate.call_count, 1)
    self.workload.iogen.assert_called_once_with()
    self.workload.prefill_iogen.assert_not_called()
    mock_iogen.wait.assert_called_once_with(self.vms, timeout_secs=10)
    mock_iogen.fetch_remote_results.assert_called_once_with(self.vms)

  def test_Wait_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.Wait(self.scenario, "not_a_workload")

  def test_Wait_results_not_found(self):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.iogen.return_value = mock_iogen
    mock_iogen.get_local_results_path.return_value = "fake_path"
    step = steps.workload.Wait(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = False
      with self.assertRaises(CurieTestException):
        step()

  def test_Wait_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.Wait(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()

  def test_Stop_default(self):
    mock_iogen = mock.Mock(spec=IOGen)
    self.workload.iogen.return_value = mock_iogen
    step = steps.workload.Stop(self.scenario, "workload_0")
    with mock.patch("curie.test.steps.workload."
                    "os.path.exists") as mock_os_path_exists:
      mock_os_path_exists.return_value = True
      with mock.patch.object(step, "create_annotation") as mock_annotate:
        step()
        self.assertEqual(mock_annotate.call_count, 1)
    mock_iogen.stop.assert_called_once_with(self.vms)
    mock_iogen.fetch_remote_results.assert_called_once_with(self.vms)

  def test_Stop_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.workload.Stop(self.scenario, "workload_0")
    self.assertIsInstance(step, steps.workload.Stop)

  def test_Stop_invalid_workload_name(self):
    with self.assertRaises(CurieTestException):
      steps.workload.Stop(self.scenario, "not_a_workload")

  def test_Stop_vms_missing(self):
    self.cluster.find_vms_regex.return_value = []
    step = steps.workload.Stop(self.scenario, "workload_0")
    with self.assertRaises(CurieTestException):
      step()
