# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import json
import sys
import threading
import time
import unittest

import gflags
import mock

from curie import curie_server_state_pb2
from curie.acropolis_cluster import AcropolisCluster
from curie.acropolis_node import AcropolisNode
from curie.curie_error_pb2 import CurieError
from curie.curie_metrics_pb2 import CurieMetric
from curie.exception import CurieTestException, CurieException
from curie.nutanix_rest_api_client import NutanixMetadata
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.oob_management_util import OobInterfaceType
from curie.proto_util import proto_patch_encryption_support


class TestAcropolisCluster(unittest.TestCase):

  class mock_hosts_get(object):
    def __init__(self, host_json_list):
      self._hosts = host_json_list

    def __call__(
      self, host_name=None, host_id=None, host_ip=None, projection=None):
      _filter = None
      if host_name:
        _filter = lambda h: h["name"] == host_name
      elif host_id:
        _filter = lambda h: h["uuid"] == host_id
      elif host_ip:
        _filter = lambda h: h.get("ipAddresses", [None])[0] == host_ip

      if _filter is None:
        return {"entities": self._hosts}

      ret = filter(_filter, self._hosts)
      if ret:
        return ret[0]
      raise Exception("Failed to lookup host")

  def setUp(self):
    gflags.FLAGS(sys.argv)
    proto_patch_encryption_support(curie_server_state_pb2.CurieSettings)
    curie_settings = curie_server_state_pb2.CurieSettings()
    self.cluster_metadata = curie_settings.Cluster()
    self.cluster_metadata.cluster_name = "Fake AHV Cluster"
    self.cluster_metadata.cluster_hypervisor_info.ahv_info.SetInParent()
    cluster_nodes_count = 4
    for index in xrange(cluster_nodes_count):
      curr_node = self.cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_node_%d" % index
      curr_node.node_out_of_band_management_info.interface_type = OobInterfaceType.kNone
    self.cluster_metadata.cluster_software_info.nutanix_info.SetInParent()
    prism_info = \
      self.cluster_metadata.cluster_management_server_info.prism_info
    prism_info.prism_host = "fake-prism-host.fake.address"
    prism_info.prism_cluster_id = "fake-cluster-id"
    prism_info.prism_host = "fake-prism-host.fake.address"
    prism_info.prism_cluster_id = "fake-cluster-id"

  def tearDown(self):
    pass

  def _get_node_json(self, name="MockHost", uuid="2-a-b-c", ip="1.1.1.1"):
    return {
      "uuid": uuid,
      "ipAddresses": [ip],
      "name": name
    }

  def _get_curie_test_vm_json(self, name="MockVM", uuid="1-a-b-c",
                              is_cvm=False, host_id=1, ip="1.2.3.4"):
    return {
      "controllerVm": is_cvm,
      "hostUuid": host_id,
      "ipAddresses": [ip],
      "vmName": "__curie_test_12345_%s" % name,
      "uuid": uuid
    }

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_update_metadata(self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get
    m_prism_client.hosts_get.return_value = {
      "entities": [
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_0"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_1"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_2"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_3"
        },
      ]
    }

    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["fake_node_uuid_0",
                            "fake_node_uuid_1",
                            "fake_node_uuid_2",
                            "fake_node_uuid_3",
                            CurieException(CurieError.kInvalidParameter,
                                           "Unable to locate host.")]
      cluster.update_metadata(False)

    for index, node_metadata in enumerate(cluster.metadata().cluster_nodes):
      self.assertEqual("fake_node_uuid_%d" % index, node_metadata.id)

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_update_metadata_if_cluster_contains_extra_nodes(
      self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get
    m_prism_client.hosts_get.return_value = {
      "entities": [
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_0"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_1"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_2"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_3"
        },
      ]
    }

    extra_node = self.cluster_metadata.cluster_nodes.add()
    extra_node.id = "fake_node_extra"

    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["fake_node_uuid_0",
                            "fake_node_uuid_1",
                            "fake_node_uuid_2",
                            "fake_node_uuid_3",
                            CurieException(CurieError.kInvalidParameter,
                                           "Unable to locate host.")]
      with self.assertRaises(CurieTestException) as ar:
        cluster.update_metadata(False)

    self.assertIn(
      "Cause: Node with ID 'fake_node_extra' is in the Curie cluster "
      "metadata, but not found in the AHV cluster.\n"
      "\n"
      "Impact: The cluster configuration is invalid.\n"
      "\n"
      "Corrective Action: Please check that all of the nodes in the Curie "
      "cluster metadata are part of the AHV cluster. For example, if the "
      "cluster configuration has four nodes, please check that all four nodes "
      "are present in the AHV cluster.\n"
      "\n"
      "Traceback (most recent call last):", str(ar.exception))

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_update_metadata_if_cluster_contains_fewer_nodes(
      self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get
    m_prism_client.hosts_get.return_value = {
      "entities": [
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_0"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_1"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_2"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_3"
        },
      ]
    }

    del self.cluster_metadata.cluster_nodes[-1]  # Remove the last item.

    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["fake_node_uuid_0",
                            "fake_node_uuid_1",
                            "fake_node_uuid_2",
                            "fake_node_uuid_3",
                            CurieException(CurieError.kInvalidParameter,
                                           "Unable to locate host.")]
      cluster.update_metadata(False)

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_update_metadata_version(self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    m_prism_client.hosts_get.return_value = {
      "entities": [
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_0",
          "hypervisorAddress": "1.1.1.0",
          "serviceVMExternalIP": "2.2.2.0",
          "name": "MockEntity",
          "numCpuSockets": 2,
          "numCpuCores": 32,
          "numCpuThreads": 64,
          "cpuFrequencyInHz": int(3e9),
          "memoryCapacityInBytes": int(32e9),
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_1",
          "hypervisorAddress": "1.1.1.1",
          "serviceVMExternalIP": "2.2.2.1",
          "name": "MockEntity",
          "numCpuSockets": 2,
          "numCpuCores": 32,
          "numCpuThreads": 64,
          "cpuFrequencyInHz": int(3e9),
          "memoryCapacityInBytes": int(32e9),
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_2",
          "hypervisorAddress": "1.1.1.2",
          "serviceVMExternalIP": "2.2.2.2",
          "name": "MockEntity",
          "numCpuSockets": 2,
          "numCpuCores": 32,
          "numCpuThreads": 64,
          "cpuFrequencyInHz": int(3e9),
          "memoryCapacityInBytes": int(32e9),
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_3",
          "hypervisorAddress": "1.1.1.3",
          "serviceVMExternalIP": "2.2.2.3",
          "name": "MockEntity",
          "numCpuSockets": 2,
          "numCpuCores": 32,
          "numCpuThreads": 64,
          "cpuFrequencyInHz": int(3e9),
          "memoryCapacityInBytes": int(32e9),
        },
      ]
    }
    nm = NutanixMetadata()
    nm.version = "el6-release-euphrates-5.0.2-stable-9d20638eb2ba1d3f84f213d5976fbcd412630c6d"
    m_prism_client.get_nutanix_metadata.return_value = nm

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get
    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["fake_node_uuid_0",
                            "fake_node_uuid_1",
                            "fake_node_uuid_2",
                            "fake_node_uuid_3",
                            CurieException(CurieError.kInvalidParameter,
                                           "Unable to locate host.")]
      cluster.update_metadata(include_reporting_fields=True)
    self.assertEquals(
      nm.version,
      cluster._metadata.cluster_software_info.nutanix_info.version)

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_update_metadata_contains_correct_nodes(self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get

    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["this", "that", "the other", "and the last one"]
      self.assertEqual(4, len(cluster.nodes()))

  def test_nodes(self):
    cluster_nodes = self.cluster_metadata.cluster_nodes
    del cluster_nodes[:]
    for index in range(1, 5):
      node = cluster_nodes.add()
      node.id = "aaaaaaaa-aaaa-aaaa-0001-00000000000%d" % index
      oob_info = node.node_out_of_band_management_info
      oob_info.interface_type = oob_info.kIpmi

    cluster = AcropolisCluster(self.cluster_metadata)
    hosts_get_data = {
      "metadata": {},
      "entities": [
        {
          "serviceVMId": "aaaaaaaa-aaaa-aaaa-0000-000000000001",
          "uuid": "aaaaaaaa-aaaa-aaaa-0001-000000000001",
          "name": "RTP-Test-14-1",
          "serviceVMExternalIP": "10.60.4.71",
          "hypervisorAddress": "10.60.5.71",
          "controllerVmBackplaneIp": "10.60.4.71",
          "managementServerName": "10.60.5.71",
          "ipmiAddress": "10.60.2.71",
          "hypervisorState": "kAcropolisNormal",
          "state": "NORMAL",
          "clusterUuid": "aaaaaaaa-aaaa-aaaa-0002-000000000000",
          "stats": {},
          "usageStats": {},
        },
        {
          "serviceVMId": "aaaaaaaa-aaaa-aaaa-0000-000000000002",
          "uuid": "aaaaaaaa-aaaa-aaaa-0001-000000000002",
          "name": "RTP-Test-14-2",
          "serviceVMExternalIP": "10.60.4.72",
          "hypervisorAddress": "10.60.5.72",
          "controllerVmBackplaneIp": "10.60.4.72",
          "managementServerName": "10.60.5.72",
          "ipmiAddress": "10.60.2.72",
          "hypervisorState": "kAcropolisNormal",
          "state": "NORMAL",
          "clusterUuid": "aaaaaaaa-aaaa-aaaa-0002-000000000000",
          "stats": {},
          "usageStats": {},
        },
        {
          "serviceVMId": "aaaaaaaa-aaaa-aaaa-0000-000000000003",
          "uuid": "aaaaaaaa-aaaa-aaaa-0001-000000000003",
          "name": "RTP-Test-14-3",
          "serviceVMExternalIP": "10.60.4.73",
          "hypervisorAddress": "10.60.5.73",
          "controllerVmBackplaneIp": "10.60.4.73",
          "managementServerName": "10.60.5.73",
          "ipmiAddress": "10.60.2.73",
          "hypervisorState": "kAcropolisNormal",
          "state": "NORMAL",
          "clusterUuid": "aaaaaaaa-aaaa-aaaa-0002-000000000000",
          "stats": {},
          "usageStats": {},
        },
        {
          "serviceVMId": "aaaaaaaa-aaaa-aaaa-0000-000000000004",
          "uuid": "aaaaaaaa-aaaa-aaaa-0001-000000000004",
          "name": "RTP-Test-14-4",
          "serviceVMExternalIP": "10.60.4.74",
          "hypervisorAddress": "10.60.5.74",
          "controllerVmBackplaneIp": "10.60.4.74",
          "managementServerName": "10.60.5.74",
          "ipmiAddress": "10.60.2.74",
          "hypervisorState": "kAcropolisNormal",
          "state": "NORMAL",
          "clusterUuid": "aaaaaaaa-aaaa-aaaa-0002-000000000000",
          "stats": {},
          "usageStats": {},
        },
      ]
    }

    def fake_hosts_get_by_id(host_id, *args, **kwargs):
      for host in hosts_get_data["entities"]:
        if host["uuid"] == host_id:
          return host
      raise RuntimeError("Host '%s' not found" % host_id)

    with mock.patch("curie.nutanix_rest_api_client.requests.Session.get") as m_get, \
         mock.patch("curie.nutanix_rest_api_client.NutanixRestApiClient.hosts_get_by_id", wraps=fake_hosts_get_by_id) as m_hosts_get_by_id:
      m_response = mock.Mock()
      m_response.status_code = 200
      m_response.content = json.dumps(hosts_get_data)
      m_response.json.return_value = hosts_get_data
      m_get.return_value = m_response

      nodes = cluster.nodes()

      for index, (node, entity) in enumerate(zip(nodes, hosts_get_data["entities"])):
        self.assertIsInstance(node, AcropolisNode)
        self.assertEqual(index, node.node_index())
        self.assertEqual(entity["uuid"], node.node_id())
        self.assertEqual(entity["hypervisorAddress"], node.node_ip())

  @mock.patch("curie.nutanix_cluster_dp_mixin.NutanixRestApiClient")
  def test_nodes_if_cluster_contains_fewer_nodes(self, m_NutanixRestApiClient):
    m_prism_client = mock.MagicMock(spec=NutanixRestApiClient)
    m_NutanixRestApiClient.from_proto.return_value = m_prism_client

    def fake_clusters_get(**kwargs):
      cluster_data = {"clusterUuid": "fake-cluster-id"}
      if kwargs.get("cluster_id"):
        return cluster_data
      else:
        return {"entities": [cluster_data]}

    m_prism_client.clusters_get.side_effect = fake_clusters_get
    m_prism_client.hosts_get.return_value = {
      "entities": [
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_0"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_1"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_2"
        },
        {
          "clusterUuid": "fake-cluster-id",
          "uuid": "fake_node_uuid_3"
        },
      ]
    }

    del self.cluster_metadata.cluster_nodes[-1]  # Remove the last item.
    cluster = AcropolisCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "identifier_to_node_uuid") as m_itnu:
      m_itnu.side_effect = ["fake_node_uuid_0",
                            "fake_node_uuid_1",
                            "fake_node_uuid_2",
                            "fake_node_uuid_3"]
      self.assertEqual(3, len(cluster.nodes()))

  @mock.patch.object(NutanixRestApiClient, "hosts_stats_get_by_id")
  def test_collect_performance_stats(self, mock_hosts_stats):
    mock_hosts_stats.return_value = {
        "statsSpecificResponses": [
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_memory_usage_ppm",
            "values": [260433, 260433]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_transmitted_bytes",
            "values": [0, 0]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_cpu_usage_ppm",
            "values": [50357, 50357]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_received_bytes",
            "values": [0, 0]
          }
        ]
      }
    cluster = AcropolisCluster(self.cluster_metadata)
    nodes = [mock.Mock(spec=AcropolisNode) for _ in xrange(4)]
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.cpu_capacity_in_hz = 12345
    with mock.patch.object(cluster, "nodes") as mock_nodes:
      mock_nodes.return_value = nodes
      results_map = cluster.collect_performance_stats()
    self.assertEqual(results_map.keys(), sorted(results_map.keys()))
    for node_id in results_map:
      self.assertEqual(len(results_map[node_id]),
                       len(cluster.metrics()))

  @mock.patch.object(NutanixRestApiClient, "hosts_stats_get_by_id")
  def test_collect_performance_stats_unsupported_metric(self,
                                                        mock_hosts_stats):
    mock_hosts_stats.return_value = {
        "statsSpecificResponses": [
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_memory_usage_ppm",
            "values": [12345, 12345]
          }
        ]
      }
    cluster = AcropolisCluster(self.cluster_metadata)
    nodes = [mock.Mock(spec=AcropolisNode) for _ in xrange(4)]
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.cpu_capacity_in_hz = 12345
    with mock.patch.object(cluster, "nodes") as mock_nodes:
      mock_nodes.return_value = nodes
      with mock.patch.object(cluster, "metrics") as mock_metrics:
        mock_metrics.return_value = [
          CurieMetric(name=CurieMetric.kDatastoreRead,
                       description="This should not be supported.",
                       instance="*",
                       type=CurieMetric.kGauge,
                       consolidation=CurieMetric.kAvg,
                       unit=CurieMetric.kKilobytes,
                       rate=CurieMetric.kPerSecond)]
        with self.assertRaises(CurieTestException):
          cluster.collect_performance_stats()

  @mock.patch.object(NutanixRestApiClient, "hosts_stats_get_by_id")
  def test_collect_performance_stats_none_value(self, mock_hosts_stats):
    mock_hosts_stats.return_value = {
        "statsSpecificResponses": [
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_memory_usage_ppm",
            "values": [260433, None, 260433]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_transmitted_bytes",
            "values": [0, None, 0]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_cpu_usage_ppm",
            "values": [50357, None, 50357]
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_received_bytes",
            "values": [0, None, 0]
          }
        ]
      }
    cluster = AcropolisCluster(self.cluster_metadata)
    nodes = [mock.Mock(spec=AcropolisNode) for _ in xrange(4)]
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.cpu_capacity_in_hz = 12345
    with mock.patch.object(cluster, "nodes") as mock_nodes:
      mock_nodes.return_value = nodes
      results_map = cluster.collect_performance_stats()
    self.assertEqual(results_map.keys(), sorted(results_map.keys()))
    for node_id in results_map:
      self.assertEqual(len(results_map[node_id]),
                       len(cluster.metrics()))

  @mock.patch.object(NutanixRestApiClient, "hosts_stats_get_by_id")
  def test_collect_performance_stats_empty(self, mock_hosts_stats):
    mock_hosts_stats.return_value = {
        "statsSpecificResponses": [
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_memory_usage_ppm",
            "values": []
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_transmitted_bytes",
            "values": []
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_cpu_usage_ppm",
            "values": []
          },
          {
            "successful": True,
            "message": None,
            "startTimeInUsecs": 1476739262143200,
            "intervalInSecs": 20,
            "metric": "hypervisor_num_received_bytes",
            "values": []
          }
        ]
      }
    cluster = AcropolisCluster(self.cluster_metadata)
    nodes = [mock.Mock(spec=AcropolisNode) for _ in xrange(4)]
    for id, node in enumerate(nodes):
      node.node_id.return_value = id
      node.cpu_capacity_in_hz = 12345
    with mock.patch.object(cluster, "nodes") as mock_nodes:
      mock_nodes.return_value = nodes
      results_map = cluster.collect_performance_stats()
    self.assertEqual(results_map.keys(), sorted(results_map.keys()))
    for node_id in results_map:
      self.assertEqual(len(results_map[node_id]),
                       len(cluster.metrics()))

  # TODO (jklein): Look into a better way of handling this test rather than
  # using a 'time.sleep' call.
  @mock.patch("curie.nutanix_rest_api_client.NutanixRestApiClient.hosts_get")
  def test_access_to_rest_client_does_not_deadlock(self, mock_hosts_get):
    entity = {"uuid": "uuid", "hypervisorAddress": "1.1.1.1",
              "serviceVMExternalIP": "2.2.2.2", "name": "MockEntity"}
    mock_hosts_get.side_effect = [{"entities": [entity]}] + [entity] * 100

    @classmethod
    def _delay(*args, **kwargs):
      time.sleep(1)
      return NutanixRestApiClient("host", "user", "pass")

    NutanixRestApiClient.get_client_for_ips = _delay

    metadata = curie_server_state_pb2.CurieSettings.Cluster()
    metadata.CopyFrom(self.cluster_metadata)
    for ii in xrange(4):
      node = metadata.cluster_nodes.add()
      node.id = "1.1.1.%s" % ii

    cluster = AcropolisCluster(metadata)

    threads = [
      threading.Thread(
        target=cluster.identifier_to_node_uuid,
        args=(cluster._prism_client, str(ii),)) for ii in xrange(5)]

    [thread.start() for thread in threads]
    [thread.join() for thread in threads]

  @mock.MagicMock(NutanixRestApiClient)
  def test_resolve_node_by_ip_no_conflicts(self, mock_rest_api):
    mock_cli = mock_rest_api()
    metadata = curie_server_state_pb2.CurieSettings.Cluster()
    metadata.CopyFrom(self.cluster_metadata)
    host_json_list = []
    for ii in xrange(4):
      curr_json = self._get_node_json(
        name="1.1.1.%s" % ii,
        uuid="21234567-abcd-bcde-cdef-123456789ab%s" % ii,
        ip="1.1.1.%s" % ii)
      node = metadata.cluster_nodes.add()
      node.id = curr_json["uuid"]
      host_json_list.append(curr_json)
    mock_cli.hosts_get = self.mock_hosts_get(host_json_list)


    cluster = AcropolisCluster(metadata)
    ident_to_uuid = cluster.identifier_to_node_uuid

    host = host_json_list[0]

    self.assertEqual(ident_to_uuid(mock_cli, host["ipAddresses"][0]),
                     host["uuid"])
    self.assertEqual(ident_to_uuid(mock_cli, host["name"]), host["uuid"])
    self.assertEqual(ident_to_uuid(mock_cli, host["uuid"]), host["uuid"])

  @mock.MagicMock(NutanixRestApiClient)
  def test_cleanup_with_vm_power_op_failures(self, mock_rest_api):
    mgmt_info = self.cluster_metadata.cluster_management_server_info
    cluster_uuid = mgmt_info.prism_info.prism_cluster_id

    mock_cli = mock_rest_api()
    mock_cluster_json = {"clusterUuid": cluster_uuid, "name": "MockCluster"}
    def mock_clusters_get(**kwargs):
      if kwargs:
        return mock_cluster_json
      return {"entities": [mock_cluster_json]}
    mock_cli.clusters_get = mock_clusters_get

    cluster = AcropolisCluster(self.cluster_metadata)
    cluster.cleanup_nutanix_state = lambda self, test_ids: True
    cluster.cleanup_images = lambda self, test_ids: True

    vm_json_list = []
    for ii in xrange(5):
      curr_json = self._get_curie_test_vm_json(
        name="MockVM-%s" % ii, uuid="1-a-b-c-%s" % ii, is_cvm=False,
        host_id=ii % 4, ip="1.2.%s.1" % ii)

      cluster._AcropolisCluster__vm_uuid_host_uuid_map[
        curr_json["uuid"]] = curr_json["hostUuid"]
      vm_json_list.append(curr_json)
    mock_cli.vms_get.return_value = {"entities": vm_json_list}

    def mock_rest_call(vm_ids, *args):
      return dict((vm_id, None if ii % 2 else True)
                  for (ii, vm_id) in enumerate(vm_ids))
    mock_cli.vms_power_op = mock_cli.vms_delete = mock_rest_call

    with self.assertRaises(CurieTestException):
      cluster.cleanup()

  def test_power_off_nodes_soft(self):
    cluster = AcropolisCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.genesis_prepare_node_for_shutdown.return_value = True
    cluster._prism_client.genesis_shutdown_hypervisor.return_value = True

    nodes = [mock.Mock() for _ in xrange(4)]
    for index, node in enumerate(nodes):
      node.node_id.return_value = str(index)

    with mock.patch.object(cluster, "get_power_state_for_nodes") as m_gpsfn:
      m_gpsfn.return_value = {"0": "kNormalConnected",
                              "1": "kNormalConnected",
                              "2": "kNormalConnected",
                              "3": "kNormalConnected"}
      cluster.power_off_nodes_soft(nodes)

    cluster._prism_client.genesis_prepare_node_for_shutdown.assert_has_calls(
      [mock.call("0"), mock.call("1"), mock.call("2"), mock.call("3")]
    )
    cluster._prism_client.genesis_shutdown_hypervisor.assert_has_calls(
      [mock.call("0"), mock.call("1"), mock.call("2"), mock.call("3")]
    )
    cluster._prism_client.genesis_clear_shutdown_token.assert_called_once_with()

  def test_power_off_nodes_soft_clears_shutdown_token_prepare_fails(self):
    cluster = AcropolisCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.genesis_prepare_node_for_shutdown.side_effect = \
      [True, True, False, False]
    cluster._prism_client.genesis_shutdown_hypervisor.return_value = True

    nodes = [mock.Mock() for _ in xrange(4)]
    for index, node in enumerate(nodes):
      node.node_id.return_value = str(index)

    with mock.patch.object(cluster, "get_power_state_for_nodes") as m_gpsfn:
      m_gpsfn.return_value = {"0": "kNormalConnected",
                              "1": "kNormalConnected",
                              "2": "kNormalConnected",
                              "3": "kNormalConnected"}
      with self.assertRaises(CurieTestException) as ar:
        cluster.power_off_nodes_soft(nodes)

    self.assertEqual(str(ar.exception), "Failed to power off nodes")
    cluster._prism_client.genesis_prepare_node_for_shutdown.assert_has_calls(
      [mock.call("0"), mock.call("1"), mock.call("2")]
    )
    cluster._prism_client.genesis_shutdown_hypervisor.assert_has_calls(
      [mock.call("0"), mock.call("1")]
    )
    cluster._prism_client.genesis_clear_shutdown_token.assert_called_once_with()

  def test_power_off_nodes_soft_clears_shutdown_token_shutdown_fails(self):
    cluster = AcropolisCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.genesis_prepare_node_for_shutdown.return_value = True
    cluster._prism_client.genesis_shutdown_hypervisor.side_effect =  \
      [True, True, False, False]

    nodes = [mock.Mock() for _ in xrange(4)]
    for index, node in enumerate(nodes):
      node.node_id.return_value = str(index)

    with mock.patch.object(cluster, "get_power_state_for_nodes") as m_gpsfn:
      m_gpsfn.return_value = {"0": "kNormalConnected",
                              "1": "kNormalConnected",
                              "2": "kNormalConnected",
                              "3": "kNormalConnected"}
      with self.assertRaises(CurieTestException) as ar:
        cluster.power_off_nodes_soft(nodes)

    self.assertEqual(str(ar.exception), "Failed to power off nodes")
    cluster._prism_client.genesis_prepare_node_for_shutdown.assert_has_calls(
      [mock.call("0"), mock.call("1")]
    )
    cluster._prism_client.genesis_shutdown_hypervisor.assert_has_calls(
      [mock.call("0"), mock.call("1")]
    )
    cluster._prism_client.genesis_clear_shutdown_token.assert_called_once_with()
