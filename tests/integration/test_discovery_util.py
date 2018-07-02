#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import json
import logging
import unittest
import uuid

import gflags

from curie.curie_interface_pb2 import DiscoverClustersV2Ret
from curie.curie_interface_pb2 import DiscoverNodesV2Arg, DiscoverNodesV2Ret
from curie.discovery_util import DiscoveryUtil

log = logging.getLogger(__name__)


class TestCurieDiscoveryUtil(unittest.TestCase):
  def setUp(self):
    with open(gflags.FLAGS.cluster_config_path, "r") as f:
      self.config = json.load(f)
    self.type = self.config["manager.type"].lower()
    if self.type not in ["prism", "vcenter", "scvmm"]:
      raise ValueError("Unsupported manager.type '%s'" % self.type)

  def test_discover_clusters_vcenter(self):
    if self.type != "vcenter":
      raise unittest.SkipTest("Test requires a vCenter cluster (found '%s')" %
                              self.type)

    resp_pb = DiscoverClustersV2Ret()
    DiscoveryUtil.discover_clusters_vcenter(
      address=self.config["vcenter.address"],
      username=self.config["vcenter.username"],
      password=self.config["vcenter.password"],
      ret=resp_pb)
    inventory = self._DiscoverClustersV2Ret_to_inventory(resp_pb)

    for cluster_id, cluster in inventory["clusters"].iteritems():
      if cluster["name"] == self.config["cluster.name"]:
        break
    else:
      self.fail("Cluster with name '%s' not found in discovery response" %
                self.config["cluster.name"])

    datacenter_id = cluster["datacenter"]
    datacenter = inventory["datacenters"][datacenter_id]
    self.assertIn(self.config["cluster.name"], datacenter["clusters"])

    for network_id in cluster["networks"]:
      network = inventory["networks"][network_id]
      if network["name"] == self.config["vcenter.network"]:
        self.assertEqual(network["cluster"], self.config["cluster.name"])
        self.assertEqual(network["datacenter"],
                         self.config["vcenter.datacenter"])
        break
    else:
      self.fail("Network with name '%s' not found with discovered cluster" %
                self.config["vcenter.network"])

    for container_id in cluster["containers"]:
      container = inventory["containers"][container_id]
      if container["name"] == self.config["vcenter.datastore"]:
        self.assertEqual(container["cluster"], self.config["cluster.name"])
        self.assertEqual(container["datacenter"],
                         self.config["vcenter.datacenter"])
        break
    else:
      self.fail("Datastore with name '%s' not found with discovered cluster" %
                self.config["vcenter.datastore"])

  def test_discover_clusters_prism(self):
    if self.type != "prism":
      raise unittest.SkipTest("Test requires a Prism cluster (found '%s')" %
                              self.type)

    resp_pb = DiscoverClustersV2Ret()
    DiscoveryUtil.discover_clusters_prism(
      address=self.config["prism.address"],
      username=self.config["prism.username"],
      password=self.config["prism.password"],
      ret=resp_pb)
    inventory = self._DiscoverClustersV2Ret_to_inventory(resp_pb)

    # Verify that the cluster name is found in the discovery
    for cluster_id, cluster in inventory["clusters"].iteritems():
      if cluster["name"] == self.config["cluster.name"]:
        break
    else:
      self.fail("Cluster with name '%s' not found in discovery response" %
                self.config["cluster.name"])

    # Verify that the desired network is found in discovery
    for network_id in cluster["networks"]:
      network = inventory["networks"][network_id]
      if network["name"] == self.config["prism.network"]:
        break
    else:
      self.fail("Network with name '%s' not found with discovered cluster" %
                self.config["prism.network"])

    # Verify that the desired container is found in discovery
    for container_id in cluster["containers"]:
      container = inventory["containers"][container_id]
      if container["name"] == self.config["prism.container"]:
        break
    else:
      self.fail("Container with name '%s' not found with discovered cluster" %
                self.config["prism.container"])
    datacenter = inventory["datacenters"][cluster["datacenter"]]
    self.assertEqual("Prism", datacenter["name"])

  def test_discover_clusters_vmm(self):
    if self.type != "scvmm":
      raise unittest.SkipTest("Test requires a VMM cluster (found '%s')" %
                              self.type)

    resp_pb = DiscoverClustersV2Ret()
    DiscoveryUtil.discover_clusters_vmm(
      address=self.config["scvmm.address"],
      username=self.config["scvmm.username"],
      password=self.config["scvmm.password"],
      ret=resp_pb)
    inventory = self._DiscoverClustersV2Ret_to_inventory(resp_pb)

    for cluster_id, cluster in inventory["clusters"].iteritems():
      if cluster["name"] == self.config["scvmm.cluster"]:
        break
    else:
      self.fail("Cluster with name '%s' not found in discovery response" %
                self.config["scvmm.cluster"])

    for network_id in cluster["networks"]:
      network = inventory["networks"][network_id]
      if network["name"] == self.config["scvmm.network"]:
        break
    else:
      self.fail("Network with name '%s' not found with discovered cluster" %
                self.config["scvmm.network"])
    for container_id in cluster["containers"]:
      if container_id == self.config["scvmm.share_path"]:
        break
    else:
      self.fail("Share with path '%s' not found with discovered cluster" %
                self.config["scvmm.share_path"])
    datacenter = inventory["datacenters"][cluster["datacenter"]]
    self.assertEqual("HyperV", datacenter["name"])

  def test_discover_nodes_vcenter(self):
    if self.type != "vcenter":
      raise unittest.SkipTest("Test requires a vCenter cluster (found '%s')" %
                              self.type)

    req_pb = DiscoverNodesV2Arg()
    if self.config.get("oob.type") == "ipmi":
      req_pb.oob_info.type = req_pb.oob_info.kIpmi
    else:
      self.assertEqual(req_pb.oob_info.type, req_pb.oob_info.kUnknownInterface)

    req_pb.oob_info.conn_params.username = self.config["ipmi.username"]
    req_pb.oob_info.conn_params.password = self.config["ipmi.password"]
    req_pb.mgmt_server.type = req_pb.mgmt_server.kVcenter
    req_pb.mgmt_server.conn_params.address = self.config["vcenter.address"]
    req_pb.mgmt_server.conn_params.username = self.config["vcenter.username"]
    req_pb.mgmt_server.conn_params.password = self.config["vcenter.password"]

    req_pb.cluster_collection.name = self.config["vcenter.datacenter"]
    cluster_pb = req_pb.cluster_collection.cluster_vec.add()
    cluster_pb.name = self.config["vcenter.cluster"]

    resp_pb = DiscoverNodesV2Ret()
    DiscoveryUtil.discover_nodes_vcenter(req_pb, resp_pb)

    self.assertEqual(len(resp_pb.node_collection_vec), 1)
    discovered_nodes = resp_pb.node_collection_vec[0].node_vec

    self.assertEqual(len(discovered_nodes), len(self.config["nodes"]))
    for discovered_node, expected_node in zip(discovered_nodes,
                                              self.config["nodes"]):
      self.assertEqual(discovered_node.hypervisor.type,
                       discovered_node.hypervisor.kEsx)
      self.assertNotIn(discovered_node.hypervisor.version, [None, ""])
      self.assertEqual(discovered_node.oob_info.conn_params.address,
                       expected_node["ipmi_addr"])
      if self.config.get("oob.type") == "ipmi":
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kIpmi)
      else:
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kUnknownInterface)
      if self.config.get("ipmi.vendor") == "supermicro":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kSupermicro)
      elif self.config.get("ipmi.vendor") == "dell":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kDell)
      else:
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kUnknownVendor)

  def test_discover_nodes_prism(self):
    if self.type != "prism":
      raise unittest.SkipTest("Test requires a Prism cluster (found '%s')" %
                              self.type)

    req_pb = DiscoverNodesV2Arg()
    if self.config.get("oob.type") == "ipmi":
      req_pb.oob_info.type = req_pb.oob_info.kIpmi
    else:
      self.assertEqual(req_pb.oob_info.type, req_pb.oob_info.kUnknownInterface)

    req_pb.oob_info.conn_params.username = self.config["ipmi.username"]
    req_pb.oob_info.conn_params.password = self.config["ipmi.password"]
    req_pb.mgmt_server.type = req_pb.mgmt_server.kPrism
    req_pb.mgmt_server.conn_params.address = self.config["prism.address"]
    req_pb.mgmt_server.conn_params.username = self.config["prism.username"]
    req_pb.mgmt_server.conn_params.password = self.config["prism.password"]

    req_pb.cluster_collection.name = "Prism"
    cluster_pb = req_pb.cluster_collection.cluster_vec.add()
    cluster_pb.name = self.config["prism.cluster"]

    resp_pb = DiscoverNodesV2Ret()
    DiscoveryUtil.discover_nodes_prism(req_pb, resp_pb)

    self.assertEqual(len(resp_pb.node_collection_vec), 1)
    discovered_nodes = resp_pb.node_collection_vec[0].node_vec

    self.assertEqual(len(discovered_nodes), len(self.config["nodes"]))
    for discovered_node, expected_node in zip(discovered_nodes,
                                              self.config["nodes"]):
      self.assertEqual(discovered_node.hypervisor.type,
                       discovered_node.hypervisor.kAhv)
      self.assertNotIn(discovered_node.hypervisor.version, [None, ""])
      self.assertEqual(discovered_node.oob_info.conn_params.address,
                       expected_node["ipmi_addr"])
      if self.config.get("oob.type") == "ipmi":
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kIpmi)
      else:
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kUnknownInterface)
      if self.config.get("ipmi.vendor") == "supermicro":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kSupermicro)
      elif self.config.get("ipmi.vendor") == "dell":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kDell)
      else:
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kUnknownVendor)

  def test_discover_nodes_vmm(self):
    if self.type != "scvmm":
      raise unittest.SkipTest("Test requires a VMM cluster (found '%s')" %
                              self.type)

    req_pb = DiscoverNodesV2Arg()
    if self.config.get("oob.type") == "ipmi":
      req_pb.oob_info.type = req_pb.oob_info.kIpmi
    else:
      self.assertEqual(req_pb.oob_info.type, req_pb.oob_info.kUnknownInterface)

    req_pb.oob_info.conn_params.username = self.config["ipmi.username"]
    req_pb.oob_info.conn_params.password = self.config["ipmi.password"]
    req_pb.mgmt_server.type = req_pb.mgmt_server.kPrism
    req_pb.mgmt_server.conn_params.address = self.config["scvmm.address"]
    req_pb.mgmt_server.conn_params.username = self.config["scvmm.username"]
    req_pb.mgmt_server.conn_params.password = self.config["scvmm.password"]

    req_pb.cluster_collection.name = "HyperV"
    cluster_pb = req_pb.cluster_collection.cluster_vec.add()
    cluster_pb.name = self.config["scvmm.cluster"]

    resp_pb = DiscoverNodesV2Ret()
    DiscoveryUtil.discover_nodes_vmm(req_pb, resp_pb)

    self.assertEqual(len(resp_pb.node_collection_vec), 1)
    discovered_nodes = resp_pb.node_collection_vec[0].node_vec

    self.assertEqual(len(discovered_nodes), len(self.config["nodes"]))
    for discovered_node, expected_node in zip(discovered_nodes,
                                              self.config["nodes"]):
      self.assertEqual(discovered_node.hypervisor.type,
                       discovered_node.hypervisor.kHyperv)
      self.assertNotIn(discovered_node.hypervisor.version, [None, ""])
      self.assertEqual(discovered_node.oob_info.conn_params.address,
                       expected_node["ipmi_addr"])
      if self.config.get("oob.type") == "ipmi":
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kIpmi)
      else:
        self.assertEqual(discovered_node.oob_info.type,
                         discovered_node.oob_info.kUnknownInterface)
      if self.config.get("ipmi.vendor") == "supermicro":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kSupermicro)
      elif self.config.get("ipmi.vendor") == "dell":
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kDell)
      else:
        self.assertEqual(discovered_node.oob_info.vendor,
                         discovered_node.oob_info.kUnknownVendor)


  @classmethod
  def _DiscoverClustersV2Ret_to_inventory(cls, response_pb):
    """
    Args:
      response_pb (DiscoverClustersV2Ret): Populated response arg.

    Returns:
      (dict) Discovered information in the format specified by the X-Ray API.

    Raises:
        CodexStatus: There was a problem executing the discovery.

    See 'Curie.discover_prism_clusters' or
    'Curie.discover_vsphere_clusters' for addition details.
    """
    inventory = {
      "datacenters": {}, "clusters": {}, "containers": {}, "networks": {},
      "library_servers": {}, "library_shares": {}}
    for datacenter_pb in response_pb.cluster_inventory.cluster_collection_vec:
      clusters = []
      for cluster_pb in datacenter_pb.cluster_vec:
        clusters.append(cluster_pb.id)
        containers = []
        networks = []
        library_servers = []
        library_shares = []
        for container_pb in cluster_pb.storage_info_vec:
          containers.append(container_pb.id)
          inventory["containers"][container_pb.id] = {
            "name": container_pb.name,
            "datacenter": datacenter_pb.id,
            "cluster": cluster_pb.id
            }
        for network_pb in cluster_pb.network_info_vec:
          networks.append(network_pb.id)
          inventory["networks"][network_pb.id] = {
            "name": network_pb.name,
            "datacenter": datacenter_pb.id,
            "cluster": cluster_pb.id
            }
        for library_share_pb in cluster_pb.library_shares:
          library_shares.append(library_share_pb.path)
          inventory["library_shares"][library_share_pb.path] = {
            "datacenter": datacenter_pb.id,
            # TODO(ryan.hardin): The UI uses the 'name' field to populate the
            # library_server_share_path in the target config. Since this must
            # be fully qualified, we use 'path' as the name here. Ideally, the
            # UI would allow the user to choose by the .name field and then
            # send the corresponding .path field.
            "name": library_share_pb.path,
            "path": library_share_pb.path,
            }
          library_server = inventory["library_servers"].setdefault(
            library_share_pb.server, {"name": library_share_pb.server,
                                      "shares": []})
          if library_share_pb.path not in library_server["shares"]:
            library_server["shares"].append(library_share_pb.path)
          if library_share_pb.server not in library_servers:
            library_servers.append(library_share_pb.server)
        inventory["clusters"][cluster_pb.id] = {
          "name": cluster_pb.name,
          "datacenter": datacenter_pb.id,
          "containers": containers,
          "networks": networks,
          "library_servers": library_servers,
          "library_shares": library_shares,
          }
      inventory["datacenters"][datacenter_pb.id] = {
        "name": datacenter_pb.name,
        "clusters": clusters
        }
    return inventory
