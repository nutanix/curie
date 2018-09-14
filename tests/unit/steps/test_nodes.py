#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest
from itertools import chain, cycle

import mock

from curie.curie_server_state_pb2 import CurieSettings
from curie.cluster import Cluster
from curie.exception import CurieTestException, ScenarioTimeoutError
from curie.node import Node
from curie.scenario import Scenario
from curie import steps
from curie.testing import environment


class TestStepsNodes(unittest.TestCase):
  def setUp(self):
    self.cluster = mock.Mock(spec=Cluster)

    cluster_metadata = CurieSettings.Cluster()
    cluster_metadata.cluster_name = "MockCluster"
    self.nodes = [mock.Mock(spec=Node) for _ in xrange(4)]
    for id, node in enumerate(self.nodes):
      node.node_id.return_value = id
      node.node_index.return_value = id
      curr_node = cluster_metadata.cluster_nodes.add()
      curr_node.id = str(id)

    self.cluster.nodes.return_value = self.nodes
    self.cluster.node_count.return_value = len(self.nodes)
    self.cluster.metadata.return_value = cluster_metadata

    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  @mock.patch("curie.steps.nodes.WaitForPowerOff")
  def test_PowerOff_default(self, mock_power_off_wait):
    step = steps.nodes.PowerOff(self.scenario, 0)
    instance = mock_power_off_wait.return_value
    instance.return_value = None
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(self.nodes[0].power_off.call_count, 1)
    for mock_node in self.nodes[1:]:
      mock_node.power_off.assert_not_called()
    mock_power_off_wait.assert_called_once_with(self.scenario, "0", 2400,
                                                annotate=True)
    instance.assert_called_once_with()

  def test_PowerOff_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.nodes.PowerOff(self.scenario, 0)
    self.assertIsInstance(step, steps.nodes.PowerOff)

  @mock.patch("curie.steps.nodes.WaitForPowerOff")
  def test_PowerOff_wait_secs_non_default(self, mock_power_off_wait):
    step = steps.nodes.PowerOff(self.scenario, 0, wait_secs=5000)
    instance = mock_power_off_wait.return_value
    instance._run.return_value = None
    with mock.patch.object(step, "create_annotation") as _:
      step()
    mock_power_off_wait.assert_called_once_with(self.scenario, "0", 5000,
                                                annotate=True)

  @mock.patch("curie.steps.nodes.WaitForPowerOff")
  def test_PowerOff_wait_secs_zero(self, mock_power_off_wait):
    step = steps.nodes.PowerOff(self.scenario, 0, wait_secs=0)
    instance = mock_power_off_wait.return_value
    instance._run.return_value = None
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(self.nodes[0].power_off.call_count, 1)
    mock_power_off_wait.assert_not_called()
    instance._run.assert_not_called()

  def test_PowerOff_node_index_out_of_bounds(self):
    with self.assertRaises(CurieTestException):
      steps.nodes.PowerOff(self.scenario, len(self.nodes))()

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOff_default(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_powered_on_soft.side_effect = chain([True],
                                                         cycle([False]))
    self.nodes[0].is_powered_on.side_effect = chain([True, True],
                                                    cycle([False]))
    step = steps.nodes.WaitForPowerOff(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.return_value = False
        step()
        self.assertGreater(mock_should_stop.call_count, 0)
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertGreater(self.nodes[0].is_powered_on_soft.call_count, 0)
    self.assertGreater(self.nodes[0].is_powered_on.call_count, 0)
    for mock_node in self.nodes[1:]:
      mock_node.is_powered_on_soft.assert_not_called()
      mock_node.is_powered_on.assert_not_called()

  def test_WaitForPowerOff_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.nodes.WaitForPowerOff(self.scenario, 0)
    self.assertIsInstance(step, steps.nodes.WaitForPowerOff)

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOff_already_off(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_powered_on_soft.return_value = False
    self.nodes[0].is_powered_on.return_value = False
    step = steps.nodes.WaitForPowerOff(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertGreater(self.nodes[0].is_powered_on_soft.call_count, 0)
    self.assertGreater(self.nodes[0].is_powered_on.call_count, 0)
    for mock_node in self.nodes[1:]:
      mock_node.is_powered_on_soft.assert_not_called()
      mock_node.is_powered_on.assert_not_called()

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOff_should_stop_before_finished_warning(
      self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_powered_on_soft.return_value = True
    self.nodes[0].is_powered_on.return_value = True
    step = steps.nodes.WaitForPowerOff(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.side_effect = chain([False], cycle([True]))
        with mock.patch.object(steps.nodes.log,
                               "warning",
                               wraps=steps.nodes.log.warning) as mock_warning:
          step()
          self.assertEqual(mock_warning.call_count, 1)
        self.assertGreater(mock_should_stop.call_count, 0)
      self.assertEqual(mock_annotate.call_count, 0)

  @mock.patch("curie.util.time.time")
  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOff_timeout_exception(self, mock_time_sleep,
                                             mock_time_time):
    mock_time_sleep.return_value = 0
    mock_time_time.side_effect = lambda: mock_time_time.call_count * 500
    self.nodes[0].is_powered_on_soft.return_value = True
    self.nodes[0].is_powered_on.return_value = True
    step = steps.nodes.WaitForPowerOff(self.scenario, 0)
    with self.assertRaises(ScenarioTimeoutError):
      step()

  def test_WaitForPowerOff_timeout_exception_zero(self):
    self.nodes[0].is_powered_on_soft.return_value = True
    self.nodes[0].is_powered_on.return_value = True
    step = steps.nodes.WaitForPowerOff(self.scenario, 0, wait_secs=0)
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.steps.nodes.WaitForPowerOn")
  def test_PowerOn_default(self, mock_power_on_wait):
    step = steps.nodes.PowerOn(self.scenario, 0)
    instance = mock_power_on_wait.return_value
    instance.return_value = None
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(self.nodes[0].power_on.call_count, 1)
    for mock_node in self.nodes[1:]:
      mock_node.power_on.assert_not_called()
    mock_power_on_wait.assert_called_once_with(self.scenario, "0", 2400,
                                               annotate=True)
    instance.assert_called_once_with()

  def test_PowerOn_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.nodes.PowerOn(self.scenario, 0)
    self.assertIsInstance(step, steps.nodes.PowerOn)

  @mock.patch("curie.steps.nodes.WaitForPowerOn")
  def test_PowerOn_wait_secs_non_default(self, mock_power_on_wait):
    step = steps.nodes.PowerOn(self.scenario, 0, wait_secs=5000)
    instance = mock_power_on_wait.return_value
    instance._run.return_value = None
    with mock.patch.object(step, "create_annotation") as _:
      step()
    mock_power_on_wait.assert_called_once_with(self.scenario, "0", 5000,
                                               annotate=True)

  @mock.patch("curie.steps.nodes.WaitForPowerOn")
  def test_PowerOn_wait_secs_zero(self, mock_power_on_wait):
    step = steps.nodes.PowerOn(self.scenario, 0, wait_secs=0)
    instance = mock_power_on_wait.return_value
    instance._run.return_value = None
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(self.nodes[0].power_on.call_count, 1)
    mock_power_on_wait.assert_not_called()
    instance._run.assert_not_called()

  def test_PowerOn_node_index_out_of_bounds(self):
    with self.assertRaises(CurieTestException):
      steps.nodes.PowerOn(self.scenario, len(self.nodes))()

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOn_default(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_ready.side_effect = chain([False], cycle([True]))
    step = steps.nodes.WaitForPowerOn(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.return_value = False
        step()
        self.assertGreater(mock_should_stop.call_count, 0)
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertGreater(self.nodes[0].is_ready.call_count, 0)
    for mock_node in self.nodes[1:]:
      mock_node.is_ready.assert_not_called()

  def test_WaitForPowerOn_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.nodes.WaitForPowerOn(self.scenario, 0)
    self.assertIsInstance(step, steps.nodes.WaitForPowerOn)

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOn_already_on(self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_ready.return_value = True
    step = steps.nodes.WaitForPowerOn(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertGreater(self.nodes[0].is_ready.call_count, 0)
    for mock_node in self.nodes[1:]:
      mock_node.is_ready.assert_not_called()

  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOn_should_stop_before_finished_warning(
      self, mock_time_sleep):
    mock_time_sleep.return_value = 0
    self.nodes[0].is_ready.return_value = False
    step = steps.nodes.WaitForPowerOn(self.scenario, 0)
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      with mock.patch.object(self.scenario, "should_stop") as mock_should_stop:
        mock_should_stop.side_effect = chain([False], cycle([True]))
        with mock.patch.object(steps.nodes.log,
                               "warning",
                               wraps=steps.nodes.log.warning) as mock_warning:
          step()
          self.assertEqual(mock_warning.call_count, 1)
        self.assertGreater(mock_should_stop.call_count, 0)
      self.assertEqual(mock_annotate.call_count, 0)

  @mock.patch("curie.util.time.time")
  @mock.patch("curie.util.time.sleep")
  def test_WaitForPowerOn_timeout_exception(self, mock_time_sleep,
                                            mock_time_time):
    mock_time_sleep.return_value = 0
    mock_time_time.side_effect = lambda: mock_time_time.call_count * 500
    self.nodes[0].is_ready.return_value = False
    step = steps.nodes.WaitForPowerOn(self.scenario, 0)
    with self.assertRaises(ScenarioTimeoutError):
      step()

  def test_WaitForPowerOn_timeout_exception_zero(self):
    self.nodes[0].is_ready.return_value = False
    step = steps.nodes.WaitForPowerOn(self.scenario, 0, wait_secs=0)
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.steps.nodes.WaitForPowerOff")
  def test_Shutdown_default(self, mock_wait_for_power_off):
    step = steps.nodes.Shutdown(self.scenario, 0)
    instance = mock_wait_for_power_off.return_value
    instance.return_value = None
    with mock.patch.object(step, "create_annotation") as mock_annotate:
      step()
      self.assertEqual(mock_annotate.call_count, 1)
    self.assertEqual(self.nodes[0].power_off_soft.call_count, 1)
    for mock_node in self.nodes[1:]:
      mock_node.power_off_soft.assert_not_called()
    mock_wait_for_power_off.assert_called_once_with(self.scenario, "0", 2400,
                                                    annotate=True)
    instance.assert_called_once_with()

  def test_Shutdown_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.nodes.Shutdown(self.scenario, 0)
    self.assertIsInstance(step, steps.nodes.Shutdown)
