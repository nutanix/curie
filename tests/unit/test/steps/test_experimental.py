#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import errno
import os
import shutil
import unittest
from itertools import cycle, repeat

import mock
import requests

from curie.cluster import Cluster
from curie.exception import CurieTestException, ScenarioTimeoutError
from curie.node import Node
from curie.scenario import Scenario
from curie.test import steps
from curie.testing import environment
from curie.vm import Vm


class TestStepsExperimental(unittest.TestCase):
  def setUp(self):
    self.cluster_size = 4
    self.cluster = mock.Mock(spec=Cluster)
    self.nodes = [mock.Mock(spec=Node) for _ in xrange(
      self.cluster_size)]
    for id, node in enumerate(self.nodes):
      node.node_id.return_value = id
    self.cluster.nodes.return_value = self.nodes
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))
    self.__delete_output_directory()
    os.makedirs(self.scenario.output_directory)

  def tearDown(self):
    self.__delete_output_directory()

  def __delete_output_directory(self):
    try:
      shutil.rmtree(self.scenario.output_directory)
    except OSError as exc:
      if exc.errno != errno.ENOENT:
        raise


  @mock.patch("curie.test.steps.experimental.requests.get")
  @mock.patch("curie.util.time.sleep")
  def test_WaitOplogEmpty_normal(self,
                                 mock_time_sleep,
                                 mock_requests_get):
    mock_time_sleep.return_value = 0
    # Validate normal operation of WaitOplogEmpty by cycling through a
    # decreasing oplog size for each cvm.
    self.cluster.vms.return_value = self.__create_mock_vms(is_cvm=True)
    side_effects = []
    for oplog_size in ["1000", "100", "0"]:
      response = mock.MagicMock(spec=requests.Response,
                                content=("stargate/vdisk/total/oplog_bytes "
                                         "%s\n" % oplog_size))
      side_effects.extend(repeat(response, self.cluster_size))
    mock_requests_get.side_effect = side_effects
    step = steps.experimental.WaitOplogEmpty(self.scenario)
    with mock.patch("curie.util.time.time") as mock_time:
      mock_time.side_effect = lambda: mock_time.call_count
      step()
    self.assertEqual(mock_requests_get.call_count, self.cluster_size * 3)

  def test_WaitOplogEmpty_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.experimental.WaitOplogEmpty(self.scenario)
    self.assertIsInstance(step, steps.experimental.WaitOplogEmpty)

  @mock.patch("curie.test.steps.experimental.requests.get")
  def test_WaitOplogEmpty_already_empty(self, mock_requests_get):
    # Validate normal operation where oplogs are already empty from the start.
    self.cluster.vms.return_value = self.__create_mock_vms(is_cvm=True)
    response = mock.MagicMock(spec=requests.Response,
                              content="stargate/vdisk/total/oplog_bytes 0\n")
    mock_requests_get.side_effect = repeat(response, self.cluster_size)
    step = steps.experimental.WaitOplogEmpty(self.scenario)
    step()
    self.assertEqual(mock_requests_get.call_count, self.cluster_size)

  @mock.patch("curie.test.steps.experimental.requests.get")
  @mock.patch("curie.util.time.sleep")
  def test_WaitOplogEmpty_timeout(self,
                                  mock_time_sleep,
                                  mock_requests_get):
    mock_time_sleep.return_value = 0
    # Validate that the step raises an exception if the oplog never empties
    # before the desired timeout.
    self.cluster.vms.return_value = self.__create_mock_vms(is_cvm=True)
    mock_requests_get.side_effect = cycle([
      mock.MagicMock(spec=requests.Response,
                     content="stargate/vdisk/total/oplog_bytes 10\n")])
    step = steps.experimental.WaitOplogEmpty(self.scenario, timeout=2)
    with mock.patch("curie.util.time.time") as mock_time:
      mock_time.side_effect = lambda: mock_time.call_count
      self.assertRaises(ScenarioTimeoutError, step)

  @mock.patch("curie.test.steps.experimental.requests.get")
  @mock.patch("curie.util.time.sleep")
  def test_WaitOplogEmpty_cannot_connect_timeout(self,
                                                 mock_time_sleep,
                                                 mock_requests_get):
    mock_time_sleep.return_value = 0
    # Validate that a ConnectionError exception from requests is handled and
    # that if it's indefinite, the timeout manages the issue.
    self.cluster.vms.return_value = self.__create_mock_vms(is_cvm=True)
    mock_requests_get.side_effect = cycle([
      requests.exceptions.ConnectionError])
    step = steps.experimental.WaitOplogEmpty(self.scenario, timeout=2)
    with mock.patch("curie.util.time.time") as mock_time:
      mock_time.side_effect = lambda: mock_time.call_count
      self.assertRaises(ScenarioTimeoutError, step)

  def test_WaitOplogEmpty_no_cvm(self):
    # Validate that no waiting if performed if executed against a cluster
    # without any CVMs.
    self.cluster.vms.return_value = self.__create_mock_vms(is_cvm=False)
    step = steps.experimental.WaitOplogEmpty(self.scenario)
    with mock.patch.object(steps.experimental.log, "warning",
                           wraps=steps.experimental.log.warning) as mock_warning:
      step()
      self.assertEqual(mock_warning.call_count, 1)

  def test_Shell_default(self):
    step = steps.experimental.Shell(self.scenario, "ls")
    output = step()
    self.assertEqual(output, 0)

  def test_Shell_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.experimental.Shell(self.scenario, "ls")
    self.assertIsInstance(step, steps.experimental.Shell)

  def test_Shell_non_zero_return_code(self):
    step = steps.experimental.Shell(self.scenario, "exit 1")
    with self.assertRaises(CurieTestException):
      step()

  @mock.patch("curie.test.steps.experimental.os.getcwd")
  @mock.patch("curie.test.steps.experimental.os.chdir")
  def test_Shell_output_directory_not_set(self, m_chdir, m_getcwd):
    m_getcwd.return_value = "/original/fake/working/directory"
    step = steps.experimental.Shell(self.scenario, "ls")
    output = step()
    self.assertEqual(output, 0)
    m_chdir.assert_has_calls([mock.call("/original/fake/working/directory")])

  @mock.patch("curie.test.steps.experimental.os.getcwd")
  @mock.patch("curie.test.steps.experimental.os.chdir")
  def test_Shell_output_directory_is_set(self, m_chdir, m_getcwd):
    self.scenario.output_directory = "/fake/output/directory"
    m_getcwd.return_value = "/original/fake/working/directory"
    step = steps.experimental.Shell(self.scenario, "ls")
    output = step()
    self.assertEqual(output, 0)
    m_chdir.assert_has_calls([mock.call(self.scenario.output_directory),
                              mock.call("/original/fake/working/directory")])

  def __create_mock_vms(self, is_cvm=False):
    mock_cvms = []
    for num in xrange(self.cluster_size):
      mock_cvm = mock.Mock(spec=Vm)
      mock_cvm.vm_ip.return_value = "%d.%d.%d.%d" % (num, num, num, num)
      mock_cvm.is_cvm.return_value = is_cvm
      mock_cvms.append(mock_cvm)
    return mock_cvms
