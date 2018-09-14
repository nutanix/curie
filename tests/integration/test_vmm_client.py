#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import json
import logging
import unittest

import gflags

from curie.vmm_client import VmmClient

log = logging.getLogger(__name__)


class TestVmmClient(unittest.TestCase):
  def setUp(self):
    with open(gflags.FLAGS.cluster_config_path, "r") as f:
      self.config = json.load(f)
    self.type = self.config["manager.type"].lower()
    if self.type != "scvmm":
      raise ValueError("Unsupported manager.type '%s'" % self.type)

  def test_get_clusters(self):
    vmm_client = VmmClient(address=self.config["scvmm.address"],
                           username=self.config["scvmm.username"],
                           password=self.config["scvmm.password"])
    with vmm_client:
      clusters = vmm_client.get_clusters(self.config["scvmm.cluster"])
    self.assertIsInstance(clusters, list)
    self.assertEqual(len(clusters), 1)
    cluster = clusters[0]
    self.assertEqual(set(cluster.keys()),
                     {"name", "type", "networks", "storage"})
    self.assertEqual(cluster["type"], "HyperV")
    self.assertIsInstance(cluster["networks"], list)
    self.assertIsInstance(cluster["storage"], list)
    for storage in cluster["storage"]:
      self.assertIsInstance(storage, dict)
      self.assertEqual(set(storage.keys()), {"name", "path"})
      self.assertIsInstance(storage["name"], basestring)
      self.assertIsInstance(storage["path"], basestring)

  def test_get_nodes(self):
    vmm_client = VmmClient(address=self.config["scvmm.address"],
                           username=self.config["scvmm.username"],
                           password=self.config["scvmm.password"])
    with vmm_client:
      nodes = vmm_client.get_nodes(self.config["scvmm.cluster"])
    self.assertIsInstance(nodes, list)
    self.assertEqual(len(nodes), len(self.config["nodes"]))
    for node in nodes:
      self.assertEqual(set(node.keys()),
                       {"name", "id", "fqdn", "ips", "state", "version",
                        "bmc_address", "bmc_port", "overall_state"})
