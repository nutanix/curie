#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import logging
import time
from multiprocessing.pool import ThreadPool

import _util
from curie.exception import CurieTestException, ScenarioStoppedError
from curie.node import get_power_management_util
from curie.steps._base_step import BaseStep
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

  def verify(self):
    # NB: This must reference metadata and *not* the actual 'CurieNode'
    # instances to avoid blocking queries to Prism on AHV.
    node_vec = self.scenario.cluster.metadata().cluster_nodes
    try:
      CurieUtil.slice_with_string(node_vec, self.nodes_slice_str)
    except IndexError:
      raise CurieTestException(
        cause=
        "Node slice '%s' is out of bounds." % self.nodes_slice_str,
        impact=
        "The scenario '%s' can not be used with cluster '%s' because the %s "
        "step refers to one or more nodes by an index that is either larger "
        "or smaller than the number of nodes in the cluster (%d)." %
        (self.scenario.display_name, self.scenario.cluster.name(), self.name,
         len(node_vec)),
        corrective_action=
        "Please retry the scenario using a cluster with a compatible number "
        "of nodes. If you are the author of the scenario, please check the "
        "syntax of the %s step to make it compatible with clusters of this "
        "size." % self.name)
    except ValueError as exc:
      raise CurieTestException(
        cause=
        "Syntax error for node slice '%s': %s" % (self.nodes_slice_str, exc),
        impact=
        "The scenario '%s' can not be used." % self.scenario.display_name,
        corrective_action=
        "Please check the syntax of the %s step. Python-flavored slice "
        "strings are accepted. Additionally, several slice strings can be "
        "combined using commas. The word 'all' can be used to refer to all "
        "nodes, and the letter 'n' can refer to the number of nodes in the "
        "cluster. Arithmetic operations are also supported using the +, -, *, "
        "and / operators. For example, the following strings are valid:\n"
        "  '0'  # The first node.\n"
        "  '0, 1'  # The first two nodes.\n"
        "  '0:2'  # The first two nodes.\n"
        "  '0:2, n-2:n'  # The first two nodes and the last two nodes.\n"
        "  ':2, n-2:'  # The first two nodes and the last two nodes.\n"
        "  ':n/2'  # The first half of the nodes.\n"
        "  'n/2:'  # The last half of the nodes.\n"
        "  'all'  # All nodes." % self.name)

  def nodes(self):
    return _util.get_nodes(self.scenario, self.nodes_slice_str)

  def node_metadatas(self):
    return CurieUtil.slice_with_string(
      self.scenario.cluster.metadata().cluster_nodes, self.nodes_slice_str).items()

  def node_power_management_utils(self):
    return [
      (index, get_power_management_util(node_metadata.node_out_of_band_management_info))
      for index, node_metadata in self.node_metadatas()]


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
    try:
      self.description = "Powering off node %d" % int(nodes)
    except Exception:
      self.description = "Powering off node(s) %s" % nodes

  def _run(self):
    """Immediately power off nodes, and optionally wait to confirm power off.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not powered off within wait_secs seconds.
    """
    power_management_utils = self.node_power_management_utils()
    pool = ThreadPool(len(power_management_utils))
    pool.map(self.__power_off, power_management_utils)
    if self.wait_secs > 0:
      wait_step = WaitForPowerOff(self.scenario, self.nodes_slice_str,
                                  self.wait_secs, annotate=self._annotate)
      wait_step()

  def __power_off(self, index_power_management_util):
    index, power_management_util = index_power_management_util
    self.create_annotation("Node %d: Sending power-off command to '%s'" %
                           (index, power_management_util.host))
    power_management_util.power_off()


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
    try:
      self.description = "Waiting for node %d to power off" % int(nodes)
    except Exception:
      self.description = "Waiting for node(s) %s to power off" % nodes

  def _run(self):
    """Wait for nodes to be powered off.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not powered off within wait_secs seconds.
    """
    start_secs = time.time()
    for node in self.nodes():
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
        raise CurieTestException(
          cause=
          "Timeout exceeded while verifying that node '%s' has been powered "
          "off (%d seconds)." % (node.node_id(), self.wait_secs),
          impact=
          "Node '%s' may be completely powered on, or may still be in the "
          "process of powering off." % node.node_id(),
          corrective_action=
          "Please check the power state of this node in the cluster "
          "management software. If it is online, please check the function of "
          "the out-of-band power management controller. Please check that no "
          "other nodes are in an unexpected power state.")


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
    try:
      self.description = "Powering on node %d" % int(nodes)
    except Exception:
      self.description = "Powering on node(s) %s" % nodes

  def _run(self):
    """Immediately power on nodes, and optionally wait to confirm readiness.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not ready within wait_secs seconds.
    """
    power_management_utils = self.node_power_management_utils()
    pool = ThreadPool(len(power_management_utils))
    pool.map(self.__power_on, power_management_utils)
    if self.wait_secs > 0:
      wait_step = WaitForPowerOn(self.scenario, self.nodes_slice_str,
                                 self.wait_secs, annotate=self._annotate)
      wait_step()

  def __power_on(self, index_power_management_util):
    index, power_management_util = index_power_management_util
    self.create_annotation("Node %d: Sending power-on command to '%s'" %
                           (index, power_management_util.host))
    power_management_util.power_on()


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
    try:
      self.description = "Waiting for node %d to power on" % int(nodes)
    except Exception:
      self.description = "Waiting for node(s) %s to power on" % nodes

  def _run(self):
    """Wait for nodes to be ready.

    Raises:
      CurieTestException:
        If a node index is out of bounds.
        If the node is not ready within wait_secs seconds.
    """
    def management_api_responding():
      try:
        nodes = self.scenario.cluster.nodes()
        nodes_metadata = self.scenario.cluster.metadata().cluster_nodes
        if len(nodes) == len(nodes_metadata):
          return True
        else:
          log.warning("Nodes list is unexpected length %d (expected %d)" %
                      (len(nodes), len(nodes_metadata)))
          log.debug(nodes)
      except Exception:
        log.warning("Unable to get nodes list", exc_info=True)
      return False

    start_secs = time.time()
    self.scenario.wait_for(management_api_responding,
                           "the management API to become responsive",
                           self.wait_secs, poll_secs=5)
    for node in self.nodes():
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
        raise CurieTestException(
          cause=
          "Timeout exceeded while verifying that node '%s' has been powered "
          "on (%d seconds)." % (node.node_id(), self.wait_secs),
          impact=
          "Node '%s' may be completely powered off, or may still be in the "
          "process of powering on." % node.node_id(),
          corrective_action=
          "Please check the power state of this node in the cluster "
          "management software. If it is offline, please check the function "
          "of the out-of-band power management controller. Please check that "
          "no other nodes are in an unexpected power state.")


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
    try:
      self.description = "Shutting down node %d" % int(nodes)
    except Exception:
      self.description = "Shutting down node(s) %s" % nodes

  def _run(self):
    """Shuts nodes via management software.

    Raises:
      CurieTestException:
        If timeout is reached without shutting down node.
    """
    pool = ThreadPool(len(self.nodes()))
    pool.map(self.__shutdown, self.nodes())
    if self.wait_secs > 0:
      wait_step = WaitForPowerOff(self.scenario, self.nodes_slice_str,
                                  self.wait_secs, annotate=self._annotate)
      wait_step()

  def __shutdown(self, node):
    self.create_annotation("Node %d: Shutting down" % node.node_index())
    # Always use async and then use WaitForPowerOff if we want to block.
    node.power_off_soft(timeout_secs=None, async=True)
