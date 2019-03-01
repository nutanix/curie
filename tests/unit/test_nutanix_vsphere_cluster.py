# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

from mock import mock

from curie import curie_server_state_pb2
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.nutanix_vsphere_cluster import NutanixVsphereCluster
from curie.oob_management_util import OobInterfaceType
from curie.proto_util import proto_patch_encryption_support
from curie.vsphere_vm import VsphereVm


class TestAcropolisCluster(unittest.TestCase):

  def setUp(self):
    proto_patch_encryption_support(curie_server_state_pb2.CurieSettings)
    curie_settings = curie_server_state_pb2.CurieSettings()
    self.cluster_metadata = curie_settings.Cluster()
    self.cluster_metadata.cluster_name = "Fake Nutanix/vSphere Cluster"
    self.cluster_metadata.cluster_hypervisor_info.esx_info.SetInParent()
    cluster_nodes_count = 4
    for index in xrange(cluster_nodes_count):
      curr_node = self.cluster_metadata.cluster_nodes.add()
      curr_node.id = "fake_node_%d" % index
      curr_node.node_out_of_band_management_info.interface_type = OobInterfaceType.kNone
    self.cluster_metadata.cluster_software_info.nutanix_info.SetInParent()
    vcenter_info = \
      self.cluster_metadata.cluster_management_server_info.vcenter_info
    vcenter_info.vcenter_host = "fake-vcenter-host.fake.address"
    vcenter_info.vcenter_user = "fake-user"
    vcenter_info.vcenter_password = "fake-password"
    vcenter_info.vcenter_datacenter_name = "fake-datacenter-name"
    vcenter_info.vcenter_cluster_name = "fake-cluster-name"
    vcenter_info.vcenter_datastore_name = "fake-datastore-name"

  def test_snapshot_vms_not_protected(self):
    cluster = NutanixVsphereCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.datastores_applicable_to_cluster.return_value = True
    cluster._prism_client.datastores_get.return_value = [
      {"datastoreName": "fake-datastore-name",
       "containerName": "fake_container_0"},
    ]
    cluster._prism_client.protection_domains_get.return_value = []

    m_vms = [mock.MagicMock(spec=VsphereVm) for _ in xrange(4)]
    for index, m_vm in enumerate(m_vms):
      m_vm.vm_name.return_value = "mock_vm_%d" % index
    cluster.snapshot_vms(m_vms)

    cluster._prism_client.protection_domains_create.assert_called_once_with(
      "None-0")
    cluster._prism_client.protection_domains_protect_vms.assert_called_once_with(
      "None-0", ["mock_vm_0", "mock_vm_1", "mock_vm_2", "mock_vm_3"])
    cluster._prism_client.protection_domains_oob_schedules.assert_called_once_with(
      "None-0")
    cluster._prism_client.reset_mock()

    cluster.snapshot_vms(m_vms)

    cluster._prism_client.protection_domains_create.assert_not_called()
    cluster._prism_client.protection_domains_protect_vms.assert_not_called()
    cluster._prism_client.protection_domains_oob_schedules.assert_called_once_with(
      "None-0")

  def test_snapshot_vms_protected_by_metro(self):
    cluster = NutanixVsphereCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.datastores_applicable_to_cluster.return_value = True
    cluster._prism_client.datastores_get.return_value = [
      {"datastoreName": "fake-datastore-name",
       "containerName": "fake_container_0"},
    ]
    cluster._prism_client.protection_domains_get.return_value = [
      {
        "name": "fake-metro-pd-0",
        "metroAvail": {
          "container": "fake_container_0",
          "role": "active",
          "status": "enabled",
        },
      },
    ]

    cluster.snapshot_vms([mock.MagicMock(spec=VsphereVm) for _ in xrange(4)])

    cluster._prism_client.protection_domains_create.assert_not_called()
    cluster._prism_client.protection_domains_protect_vms.assert_not_called()
    cluster._prism_client.protection_domains_oob_schedules.assert_called_once_with(
      "fake-metro-pd-0")

  def test_snapshot_vms_metro_disabled_create_new_pd(self):
    cluster = NutanixVsphereCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.datastores_applicable_to_cluster.return_value = True
    cluster._prism_client.datastores_get.return_value = [
      {"datastoreName": "fake-datastore-name",
       "containerName": "fake_container_0"},
    ]
    cluster._prism_client.protection_domains_get.return_value = [
      {
        "name": "fake-metro-pd-0",
        "metroAvail": {
          "container": "fake_container_0",
          "role": "active",
          "status": "disabled",
        },
      },
    ]

    m_vms = [mock.MagicMock(spec=VsphereVm) for _ in xrange(4)]
    for index, m_vm in enumerate(m_vms):
      m_vm.vm_name.return_value = "mock_vm_%d" % index
    cluster.snapshot_vms(m_vms)

    cluster._prism_client.protection_domains_create.assert_called_once_with(
      "None-0")
    cluster._prism_client.protection_domains_protect_vms.assert_called_once_with(
      "None-0", ["mock_vm_0", "mock_vm_1", "mock_vm_2", "mock_vm_3"])
    cluster._prism_client.protection_domains_oob_schedules.assert_called_once_with(
      "None-0")

  def test_snapshot_vms_non_metro_pd_already_exists(self):
    cluster = NutanixVsphereCluster(self.cluster_metadata)
    cluster._prism_client = mock.Mock(spec=NutanixRestApiClient)
    cluster._prism_client.datastores_applicable_to_cluster.return_value = True
    cluster._prism_client.datastores_get.return_value = [
      {"datastoreName": "fake-datastore-name",
       "containerName": "fake_container_0"},
    ]
    cluster._prism_client.protection_domains_get.return_value = [
      {
        "name": "fake-metro-pd-0",
        "metroAvail": None,
      },
    ]

    m_vms = [mock.MagicMock(spec=VsphereVm) for _ in xrange(4)]
    for index, m_vm in enumerate(m_vms):
      m_vm.vm_name.return_value = "mock_vm_%d" % index
    cluster.snapshot_vms(m_vms)

    cluster._prism_client.protection_domains_create.assert_called_once_with(
      "None-0")
    cluster._prism_client.protection_domains_protect_vms.assert_called_once_with(
      "None-0", ["mock_vm_0", "mock_vm_1", "mock_vm_2", "mock_vm_3"])
    cluster._prism_client.protection_domains_oob_schedules.assert_called_once_with(
      "None-0")
