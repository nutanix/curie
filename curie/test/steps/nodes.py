#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import time
from multiprocessing.pool import ThreadPool

import _util
from curie.exception import CurieTestException
from curie.exception import ScenarioStoppedError
from curie.test.steps._base_step import BaseStep
from curie.util import CurieUtil

log = logging.getLogger(__name__)


class _PowerOp(BaseStep):
  """Base for any steps involving node power operations.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being powered off.
    wait_secs (int): Maximum amount of time to block until power off is
      confirmed. If wait_secs is less than 0, this does not block.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  @classmethod
  def requirements(cls):
    return super(_PowerOp, cls).requirements().union([BaseStep.OOB_CONFIG, ])

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(_PowerOp, self).__init__(scenario, annotate=annotate)
    self.nodes_slice_str = str(nodes)
    self.wait_secs = wait_secs

    # Exposed via @property 'nodes'.
    self._nodes = None

  def verify(self):
    # NB: This must reference metadata and *not* the actual 'CurieNode'
    # instances to avoid blocking queries to Prism on AHV.
    node_vec = self.scenario.cluster.metadata().cluster_nodes
    try:
      CurieUtil.slice_with_string(node_vec, self.nodes_slice_str)
    except IndexError as exc:
      raise CurieTestException(
        "Node slice is out of bounds (cluster contains %d nodes): %s" %
        (len(node_vec), exc))

  @property
  def nodes(self):
    if self._nodes is None:
      self.nodes = _util.get_nodes(self.scenario, self.nodes_slice_str)
    return self._nodes

  @nodes.setter
  def nodes(self, val):
    if self._nodes is not None:
      raise AttributeError("'%s.nodes' is immutable once set" %
                           type(self).__name__)
    self._nodes = val


class PowerOff(_PowerOp):
  """Immediately power off nodes, and optionally wait to confirm power off.

  This uses the IPMI interface to shut off the power, and does NOT perform a
  clean shutdown.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being powered off.
    wait_secs (int): Maximum amount of time to block until power off is
      confirmed. If wait_secs is less than 0, this does not block.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(PowerOff, self).__init__(
      scenario, nodes, wait_secs=wait_secs, annotate=annotate)
    self.description = "Powering off node(s) %s" % nodes
    # TODO (jklein/ryan.hardin): Can't access this in '__init__'.
    # ", ".join(map(str, self.nodes.keys()))))

  def _run(self):
    """Immediately power off nodes, and optionally wait to confirm power off.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not powered off within wait_secs seconds.
    """
    pool = ThreadPool(len(self.nodes))
    pool.map(self.__power_off, self.nodes)
    if self.wait_secs > 0:
      wait_step = WaitForPowerOff(self.scenario, self.nodes_slice_str,
                                  self.wait_secs, annotate=self._annotate)
      wait_step()

  def __power_off(self, node):
    self.create_annotation("Node %d: Powering off" % node.node_index())
    node.power_off(sync_management_state=False)


class WaitForPowerOff(_PowerOp):
  """Wait for nodes to be powered off.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being powered off.
    wait_secs (int): Maximum amount of time to block until power off is
      confirmed.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(WaitForPowerOff, self).__init__(
      scenario, nodes, wait_secs=wait_secs, annotate=annotate)
    self.description = ("Waiting for node(s) %s to power off" % nodes)

    # TODO (jklein/ryan.hardin): Can't access this in '__init__'.
    # ", ".join(map(str, self.nodes.keys()))))

  def _run(self):
    """Wait for nodes to be powered off.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not powered off within wait_secs seconds.
    """
    start_secs = time.time()
    for node in self.nodes:
      def is_done():
        software_power_on = node.is_powered_on_soft()
        hardware_power_on = node.is_powered_on()
        return not software_power_on and not hardware_power_on

      node.sync_power_state()
      elapsed_secs = time.time() - start_secs
      remaining_secs = self.wait_secs - elapsed_secs
      if remaining_secs > 0:
        try:
          self.scenario.wait_for(
            is_done,
            "node %s to acknowledge power off" % node.node_id(),
            remaining_secs, poll_secs=5)
        except ScenarioStoppedError:
          log.warning("Test stopped before node %d powered off",
                      node.node_index())
          break
        else:
          self.create_annotation("Node %d: Finished powering off" %
                                 node.node_index())
          continue
      else:
        raise CurieTestException("Timeout exceeded after powering off node "
                                  "%s" % node.node_id())


class PowerOn(_PowerOp):
  """Immediately power on nodes, and optionally wait to confirm readiness.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being powered on.
    wait_secs (int): Maximum amount of time to block until the node is ready.
      If wait_secs is less than 0, this does not block.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(PowerOn, self).__init__(
      scenario, nodes, wait_secs=wait_secs, annotate=annotate)
    self.description = ("Powering on node(s) %s" % nodes)

    # TODO (jklein/ryan.hardin): Can't access this in '__init__'.
    # ", ".join(map(str, self.nodes.keys()))))

  def _run(self):
    """Immediately power on nodes, and optionally wait to confirm readiness.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not ready within wait_secs seconds.
    """
    pool = ThreadPool(len(self.nodes))
    pool.map(self.__power_on, self.nodes)
    if self.wait_secs > 0:
      wait_step = WaitForPowerOn(self.scenario, self.nodes_slice_str,
                                 self.wait_secs, annotate=self._annotate)
      wait_step()

  def __power_on(self, node):
    self.create_annotation("Node %d: Powering on" % node.node_index())
    node.power_on()


class WaitForPowerOn(_PowerOp):
  """Wait for nodes to be ready.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being powered on.
    wait_secs (int): Maximum amount of time to block until the node is ready.
      If wait_secs is less than 0, this does not block.
    annotate (bool): If True, annotate key points in the step in the test's
      results.
  """

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(WaitForPowerOn, self).__init__(
      scenario, nodes, wait_secs=wait_secs, annotate=annotate)
    self.description = ("Waiting for node(s) %s to power off" % nodes)

    # TODO (jklein/ryan.hardin): Can't access this in '__init__'.
    # ", ".join(map(str, self.nodes.keys()))))

  def _run(self):
    """Wait for nodes to be ready.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not ready within wait_secs seconds.
    """
    start_secs = time.time()
    for node in self.nodes:
      node.sync_power_state()
      elapsed_secs = time.time() - start_secs
      remaining_secs = self.wait_secs - elapsed_secs
      if remaining_secs > 0:
        try:
          self.scenario.wait_for(node.is_ready,
                                 "%s to be ready" % node.node_id(),
                                 remaining_secs, poll_secs=5)
        except ScenarioStoppedError:
          log.warning("Test stopped before node %d powered on",
                      node.node_index())
          break
        else:
          self.create_annotation(
            "Node %d: Finished powering on" % node.node_index())
          continue
      else:
        raise CurieTestException("Timeout exceeded after powering on node "
                                  "%s" % node.node_id())


class Shutdown(_PowerOp):
  """Shutdown nodes via management software.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    nodes (str or int): Indices of the nodes being shutdown.
    wait_secs (int): Maximum amount of time to block until the node is
      shutdown if greater than 0. If 0, step becomes asynchronous.
    """

  def __init__(self, scenario, nodes, wait_secs=2400, annotate=True):
    super(Shutdown, self).__init__(
      scenario, nodes, wait_secs=wait_secs, annotate=annotate)
    self.description = ("Shutting down node(s) %s" % nodes)

    # TODO (jklein/ryan.hardin): Can't access this in '__init__'.
    # ", ".join(map(str, self.nodes.keys()))))

  def _run(self):
    """Shuts nodes via management software.

    Raises:
      CurieTestException:
        If timeout is reached without shutting down node.
    """
    pool = ThreadPool(len(self.nodes))
    pool.map(self.__shutdown, self.nodes)
    if self.wait_secs > 0:
      wait_step = WaitForPowerOff(self.scenario, self.nodes_slice_str,
                                  self.wait_secs, annotate=self._annotate)
      wait_step()

  def __shutdown(self, node):
    self.create_annotation("Node %d: Shutting down" % node.node_index())
    # Always use async and then use WaitForPowerOff if we want to block.
    node.power_off_soft(timeout_secs=None, async=True)
