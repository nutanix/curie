#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# Note: The members of this module are intended to be lower-level utilities
# used within the steps submodule only. These utilities are not intended to be
# used directly as steps inside a Scenario.
#
import os

from curie.exception import CurieTestException
from curie.util import CurieUtil


def get_node(scenario, node_index):
  """Get a Node for a given node index.

  Args:
    scenario (Scenario): Scenario that owns the node.
    node_index (int): Index of the desired node.

  Returns:
    Node: Matching node.

  Raises:
    CurieTestException:
      If node_index is out of bounds.
  """
  return get_nodes(scenario, str(node_index))[0]


def get_nodes(scenario, slice_string):
  """Get a list of CurieClusterNodes for a given node index slice string.

  Args:
    scenario (Scenario): Scenario that owns the node.
    slice_string (str): String containing indices of the desired nodes.

  Returns:
    (list): CurieClusterNodes in an order matching that specified by
      'slice_string' .

  Raises:
    CurieTestException:
      If slice_string is out of bounds.
  """
  nodes = scenario.cluster.nodes()
  try:
    return CurieUtil.slice_with_string(nodes, slice_string).values()
  except IndexError as err:
    raise CurieTestException(
      "Node slice is out of bounds (cluster contains %d nodes): %s" %
      (len(nodes), err))


def remote_output_directory():
  # TODO(ryan.hardin): Change to /home/nutanix/data/curie/output?
  return os.path.join(os.sep, "home", "nutanix", "output")
