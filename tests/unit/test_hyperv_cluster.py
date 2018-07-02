#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import time
import unittest

import mock

from curie.curie_server_state_pb2 import CurieSettings
from curie.exception import CurieTestException
from curie.hyperv_cluster import HyperVCluster
from curie.hyperv_node import HyperVNode
from curie.node import Node
from curie.vm import Vm
from curie.vmm_client import VmmClientException


class TestHyperVCluster(unittest.TestCase):
  def setUp(self):
    self.cluster_metadata = CurieSettings.Cluster()
    self.cluster_metadata.cluster_name = "Fake Cluster"
    self.cluster_metadata.cluster_hypervisor_info.hyperv_info.SetInParent()
    vmm_info = self.cluster_metadata.cluster_management_server_info.vmm_info
    vmm_info.vmm_server = "fake_vmm_server_address"
    vmm_info.vmm_user = "fake_vmm_username"
    vmm_info.vmm_password = "fake_vmm_password"
    vmm_info.vmm_host = "fake_host_address"
    vmm_info.vmm_host_user = "fake_host_username"
    vmm_info.vmm_host_password = "fake_host_password"
    vmm_info.vmm_library_server_share_path = "\\\\fake\\library\\share\\path"
    vmm_info.vmm_cluster_name = "fake_cluster"
    vmm_info.vmm_share_name = "fake_share_name"
    vmm_info.vmm_share_path = "\\\\fake\\path\\to\\fake_share_name"
    vmm_info.vmm_network_name = "fake_network"

  @mock.patch("curie.hyperv_cluster.VmmClient")
  def test_get_power_state_for_vms(self, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    for index, m_vm in enumerate(vms):
      m_vm.vm_id.return_value = "fake_vm_%d" % index

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.is_logged_in = False
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_vm_1",
        "name": "Fake VM 1",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_1",
      },
      {
        "id": "fake_vm_2",
        "name": "Fake VM 2 (Some other VM we don't care about)",
        "status": "PowerOff",
        "ips": ["169.254.1.3"],
        "node_id": "fake_node_1",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    ret_data = cluster.get_power_state_for_vms(vms)

    m_vmm_client.get_vms.assert_called_once_with("fake_cluster")
    self.assertEqual(ret_data,
                     {"fake_vm_0": "Running",
                      "fake_vm_1": "PowerOff"})

  @mock.patch("curie.hyperv_cluster.VmmClient")
  def test_get_power_state_for_vms_invalid_ids(self, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[1].vm_id.return_value = "fake_vm_OH_NO_THIS_ID_IS_NOT_GOOD"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_vm_1",
        "name": "Fake VM 1",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_1",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.get_power_state_for_vms(vms)

    self.assertEqual(
      str(ar.exception),
      "Expected VM ID(s) missing from 'get_vms' response: "
      "fake_vm_OH_NO_THIS_ID_IS_NOT_GOOD")

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_import_vm(self, m_time_sleep, m_VmmClient, m_GoldImageManager,
                     m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 1234]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # upload_image
      [{"task_id": "0", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"}],
      # update_library
      [{"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # create_vm
      [{"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "3", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_imported_vm",
        "name": "Fake Imported VM",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    vm = cluster.import_vm("/fake/goldimages/directory", "fake_goldimage_name",
                           "Fake Imported VM", node_id="fake_node_0")

    self.assertEqual(vm.vm_name(), "Fake Imported VM")
    self.assertEqual(vm.vm_id(), "fake_imported_vm")
    target_dir = "fake_cluster" + "\\" + vm.vm_name()
    m_vmm_client.upload_image.assert_called_once_with(
      ["http://1.2.3.4/goldimages/fake_goldimage_name.vhdx.zip"],
      goldimage_target_dir=target_dir,
      transfer_type="bits")
    m_vmm_client.create_vm_template.assert_called_once_with(
      "fake_cluster", "Fake Imported VM", "fake_node_0",
      "\\\\fake\\library\\share\\path\\fake_goldimage_name\\"
      "fake_goldimage_name.vhdx",
      "\\\\fake\\path\\to\\fake_share_name",
      "fake_network")
    m_vmm_client.update_library.assert_called_once_with(
      "\\\\fake\\library\\share\\path\\fake_goldimage_name\\"
      "fake_goldimage_name.vhdx")
    m_vmm_client.create_vm.assert_called_once_with(
      "fake_cluster", None, [{"vm_name": "Fake Imported VM",
                              "node_id": "fake_node_0"}],
      "\\\\fake\\path\\to\\fake_share_name", [16, 16, 16, 16, 16, 16])

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.HypervTaskPoller")
  def test_import_vm_created_not_found(
      self, m_HypervTaskPoller, m_VmmClient, m_GoldImageManager, m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 5985]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.import_vm("/fake/goldimages/directory", "fake_goldimage_name",
                        "Fake Imported VM", node_id="fake_node_0")

    self.assertEqual(str(ar.exception),
                     "Imported VM 'Fake Imported VM' not found in self.vms()")
    self.assertEqual(m_HypervTaskPoller.execute_parallel_tasks.call_count, 3)

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_create_vm(
      self, m_time_sleep, m_VmmClient, m_GoldImageManager, m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 1234]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # upload_image
      [{"task_id": "0", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"}],
      # update_library
      [{"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # create_vm
      [{"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "3", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_imported_vm",
        "name": "Fake Imported VM",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    vm = cluster.create_vm("/fake/goldimages/directory", "fake_goldimage_name",
                           "Fake Imported VM", node_id="fake_node_0")

    self.assertEqual(vm.vm_name(), "Fake Imported VM")
    self.assertEqual(vm.vm_id(), "fake_imported_vm")
    target_dir = "fake_cluster" + "\\" + vm.vm_name()
    m_vmm_client.upload_image.assert_called_once_with(
      ["http://1.2.3.4/goldimages/fake_goldimage_name.vhdx.zip"],
      goldimage_target_dir=target_dir,
      transfer_type="bits")
    m_vmm_client.create_vm_template.assert_called_once_with(
      "fake_cluster", "Fake Imported VM", "fake_node_0",
      "\\\\fake\\library\\share\\path\\fake_cluster\\Fake Imported VM\\"
      "fake_goldimage_name.vhdx",
      "\\\\fake\\path\\to\\fake_share_name",
      "fake_network", 1, 1024)
    m_vmm_client.update_library.assert_called_once_with(
      "\\\\fake\\library\\share\\path\\fake_cluster\\Fake Imported VM\\"
      "fake_goldimage_name.vhdx")
    m_vmm_client.create_vm.assert_called_once_with(
      "fake_cluster", "Fake Imported VM", [{"vm_name": "Fake Imported VM",
                                            "node_id": "fake_node_0"}],
      "\\\\fake\\path\\to\\fake_share_name", ())

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_create_vm_duplicate(
      self, m_time_sleep, m_VmmClient, m_GoldImageManager, m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 1234]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # upload_image
      [{"task_id": "0", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"}],
      # update_library
      [{"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # create_vm
      [{"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "3", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_imported_vm",
        "name": "Fake Imported VM",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_imported_vm",
        "name": "Fake Imported VM",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.create_vm("/fake/goldimages/directory", "fake_goldimage_name",
                        "Fake Imported VM", node_id="fake_node_0")
    self.assertEqual(str(ar.exception),
                     "Duplicate VM 'Fake Imported VM' found in self.vms()")

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.HypervTaskPoller")
  def test_create_vm_created_not_found(
      self, m_HypervTaskPoller, m_VmmClient, m_GoldImageManager, m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 1234]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]

    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.create_vm("/fake/goldimages/directory", "fake_goldimage_name",
                        "Fake Imported VM", node_id="fake_node_0")

    self.assertEqual(str(ar.exception),
                     "Created VM 'Fake Imported VM' not found in self.vms()")
    self.assertEqual(m_HypervTaskPoller.execute_parallel_tasks.call_count, 3)

  @mock.patch("curie.hyperv_cluster.socket.socket")
  @mock.patch("curie.hyperv_cluster.GoldImageManager")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_create_vm_parameters(
      self, m_time_sleep, m_VmmClient, m_GoldImageManager, m_socket):
    m_sock = m_socket.return_value
    m_sock.getsockname.return_value = ["1.2.3.4", 1234]
    m_GoldImageManager.get_goldimage_filename.side_effect = [
      "fake_goldimage_name.vhdx.zip", "fake_goldimage_name.vhdx"]

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # upload_image
      [{"task_id": "0", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"}],
      # update_library
      [{"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # create_vm
      [{"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "3", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.upload_image.__name__ = "upload_image"
    m_vmm_client.upload_image.return_value = [
      {"task_id": "0", "task_type": "vmm"}]
    m_vmm_client.update_library.__name__ = "update_library"
    m_vmm_client.update_library.return_value = [
      {"task_id": "1", "task_type": "vmm"}]
    m_vmm_client.create_vm_template.__name__ = "create_vm_template"
    m_vmm_client.create_vm_template.return_value = [
      {"task_id": "2", "task_type": "vmm"}]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.return_value = [
      {"task_id": "3", "task_type": "vmm"}]
    m_vmm_client.vms_delete.__name__ = "vms_delete"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_imported_vm",
        "name": "Fake Imported VM",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    vm = cluster.create_vm("/fake/goldimages/directory", "fake_goldimage_name",
                           "Fake Imported VM", node_id="fake_node_0",
                           vcpus=3, ram_mb=1234, data_disks=[1, 2, 3, 4])

    self.assertEqual(vm.vm_name(), "Fake Imported VM")
    self.assertEqual(vm.vm_id(), "fake_imported_vm")
    target_dir = "fake_cluster" + "\\" + vm.vm_name()
    m_vmm_client.upload_image.assert_called_once_with(
      ["http://1.2.3.4/goldimages/fake_goldimage_name.vhdx.zip"],
      goldimage_target_dir=target_dir, transfer_type="bits")
    m_vmm_client.create_vm_template.assert_called_once_with(
      "fake_cluster", "Fake Imported VM", "fake_node_0",
      "\\\\fake\\library\\share\\path\\fake_cluster\\Fake Imported VM\\"
      "fake_goldimage_name.vhdx",
      "\\\\fake\\path\\to\\fake_share_name",
      "fake_network", 3, 1234)
    m_vmm_client.update_library.assert_called_once_with(
      "\\\\fake\\library\\share\\path\\fake_cluster\\Fake Imported VM\\"
      "fake_goldimage_name.vhdx")
    m_vmm_client.create_vm.assert_called_once_with(
      "fake_cluster", "Fake Imported VM", [{"vm_name": "Fake Imported VM",
                                            "node_id": "fake_node_0"}],
      "\\\\fake\\path\\to\\fake_share_name", [1, 2, 3, 4])

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_delete_vms(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    for index, m_vm in enumerate(vms):
      m_vm.vm_id.return_value = "fake_vm_%d" % index

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_delete.__name__ = "vms_delete"
    m_vmm_client.vms_delete.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.delete_vms(vms)

    m_vmm_client.vms_delete.assert_has_calls([
      mock.call("fake_cluster", ["fake_vm_0"]),
      mock.call("fake_cluster", ["fake_vm_1"]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  def test_delete_vms_exception_raises_CurieTestException(
      self, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 0.1
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    for index, m_vm in enumerate(vms):
      m_vm.vm_id.return_value = "fake_vm_%d" % index

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vms_delete.__name__ = "vms_delete"
    m_vmm_client.vms_delete.side_effect = VmmClientException("Boom")

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.delete_vms(vms)

    self.assertEqual(str(ar.exception),
                     "Unhandled exception occurred in HypervTaskPoller while "
                     "waiting for tasks: Boom")

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  def test_delete_vms_raises_exception_on_empty_task_list(
      self, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 0.1
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    for index, m_vm in enumerate(vms):
      m_vm.vm_id.return_value = "fake_vm_%d" % index

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vms_delete.__name__ = "vms_delete"
    m_vmm_client.vms_delete.return_value = []

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.delete_vms(vms)

    self.assertEqual(str(ar.exception),
                     "Unhandled exception occurred in HypervTaskPoller while "
                     "waiting for tasks: Expected exactly 1 task in response "
                     "- got []")

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  @mock.patch("curie.task.time.sleep")
  def test_migrate_vms(self, m_time_sleep, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 5
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    # Both VMs start on Node 0, and will be moved to Nodes 1 and 2.
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_0"

    nodes = [mock.Mock(spec=Node) for _ in xrange(2)]
    nodes[0].node_id.return_value = "fake_node_1"
    nodes[1].node_id.return_value = "fake_node_2"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # set_possible_owners_for_vms tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # migrate_vm tasks
      [{"task_id": "2", "task_type": "vmm", "state": "running"},
       {"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "2", "task_type": "vmm", "state": "completed"},
       {"task_id": "3", "task_type": "vmm", "state": "completed"}],
      # set_possible_owners_for_vms tasks
      [{"task_id": "4", "task_type": "vmm", "state": "running"},
       {"task_id": "5", "task_type": "vmm", "state": "running"}],
      [{"task_id": "4", "task_type": "vmm", "state": "completed"},
       {"task_id": "5", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_possible_owners_for_vms.__name__ = \
      "vms_set_possible_owners_for_vms"
    m_vmm_client.vms_set_possible_owners_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "4", "task_type": "vmm"}],
      [{"task_id": "5", "task_type": "vmm"}],
    ]
    m_vmm_client.migrate_vm.__name__ = "migrate_vm"
    m_vmm_client.migrate_vm.side_effect = [
      [{"task_id": "2", "task_type": "vmm"}],
      [{"task_id": "3", "task_type": "vmm"}],
    ]
    cluster = HyperVCluster(self.cluster_metadata)
    cluster.migrate_vms(vms, nodes)

    m_vmm_client.vms_set_possible_owners_for_vms.assert_has_calls([
      mock.call("fake_cluster", [
        {"id": "fake_vm_0",
         "possible_owners": ["fake_node_0", "fake_node_1"]},
      ]),
      mock.call("fake_cluster", [
        {"id": "fake_vm_1",
         "possible_owners": ["fake_node_0", "fake_node_2"]},
      ]),
    ], any_order=True)
    m_vmm_client.vms_set_possible_owners_for_vms.assert_has_calls([
      mock.call("fake_cluster", [
        {"id": "fake_vm_1", "possible_owners": ["fake_node_2"]},
      ]),
      mock.call("fake_cluster", [
        {"id": "fake_vm_0", "possible_owners": ["fake_node_1"]},
      ]),
    ], any_order=True)
    m_vmm_client.migrate_vm.assert_has_calls([
      mock.call("fake_cluster", [
        {"vm_id": "fake_vm_1", "node_id": "fake_node_2"}],
        "\\\\fake\\path\\to\\fake_share_name"),
      mock.call("fake_cluster", [
        {"vm_id": "fake_vm_0", "node_id": "fake_node_1"}],
        "\\\\fake\\path\\to\\fake_share_name"),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  def test_migrate_vms_raises_exception_on_empty_task_list(
      self, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 2
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    # Both VMs start on Node 0, and will be moved to Nodes 1 and 2.
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_0"

    nodes = [mock.Mock(spec=Node) for _ in xrange(2)]
    nodes[0].node_id.return_value = "fake_node_1"
    nodes[1].node_id.return_value = "fake_node_2"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # set_possible_owners_for_vms tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_possible_owners_for_vms.__name__ = \
      "vms_set_possible_owners_for_vms"
    m_vmm_client.vms_set_possible_owners_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]
    m_vmm_client.migrate_vm.__name__ = "migrate_vm"
    m_vmm_client.migrate_vm.return_value = []

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.migrate_vms(vms, nodes)

    self.assertEqual(str(ar.exception),
                     "Unhandled exception occurred in HypervTaskPoller while "
                     "waiting for tasks: Expected exactly 1 task in response "
                     "- got []")

  def test_collect_performance_stats(self):
    # TODO(ryan.hardin) Implement this.
    pass

  @mock.patch("curie.hyperv_cluster.VmmClient")
  def test_is_ha_enabled_true(self, m_VmmClient):
    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
      {
        "id": "fake_vm_1",
        "name": "Fake VM 1",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": True
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    response = cluster.is_ha_enabled()

    self.assertEqual(response, True)
    m_vmm_client.get_vms.assert_called_once_with("fake_cluster")

  @mock.patch("curie.hyperv_cluster.VmmClient")
  def test_is_ha_enabled_false(self, m_VmmClient):
    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
      {
        "id": "fake_vm_1",
        "name": "Fake VM 1",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    response = cluster.is_ha_enabled()

    self.assertEqual(response, False)
    m_vmm_client.get_vms.assert_called_once_with("fake_cluster")

  def test_is_drs_enabled(self):
    cluster = HyperVCluster(self.cluster_metadata)
    self.assertEqual(cluster.is_drs_enabled(), False)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_disable_ha_vms(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_1"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_possible_owners_for_vms tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_possible_owners_for_vms.__name__ = \
      "vms_set_possible_owners_for_vms"
    m_vmm_client.vms_set_possible_owners_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.disable_ha_vms(vms)

    m_vmm_client.vms_set_possible_owners_for_vms.assert_has_calls([
      mock.call("fake_cluster", [{"id": "fake_vm_0",
                                  "possible_owners": ["fake_node_0"]}]),
      mock.call("fake_cluster", [{"id": "fake_vm_1",
                                  "possible_owners": ["fake_node_1"]}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_disable_ha_vms_raises_exception_on_empty_task_list(
      self, m_time_sleep, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 0.1
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_1"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_possible_owners_for_vms tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_possible_owners_for_vms.__name__ = \
      "vms_set_possible_owners_for_vms"
    m_vmm_client.vms_set_possible_owners_for_vms.return_value = []

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.disable_ha_vms(vms)

    self.assertEqual(str(ar.exception),
                     "Unhandled exception occurred in HypervTaskPoller while "
                     "waiting for tasks: Expected exactly 1 task in response "
                     "- got []")

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_snapshot_vms(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_1"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_snapshot tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_snapshot.__name__ = "vms_set_snapshot"
    m_vmm_client.vms_set_snapshot.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.snapshot_vms(vms, "fake_snapshot_0")

    m_vmm_client.vms_set_snapshot.assert_has_calls([
      mock.call(
        "fake_cluster", [{"vm_id": "fake_vm_0", "name": "fake_snapshot_0"}]),
      mock.call(
        "fake_cluster", [{"vm_id": "fake_vm_1", "name": "fake_snapshot_0"}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_snapshot_vms_description(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_1"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_snapshot tasks
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_snapshot.__name__ = "vms_set_snapshot"
    m_vmm_client.vms_set_snapshot.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.snapshot_vms(vms, "fake_snapshot_0", "Oh, snap!")

    m_vmm_client.vms_set_snapshot.assert_has_calls([
      mock.call(
        "fake_cluster", [{"vm_id": "fake_vm_0", "name": "fake_snapshot_0",
                          "description": "Oh, snap!"}]),
      mock.call(
        "fake_cluster", [{"vm_id": "fake_vm_1", "name": "fake_snapshot_0",
                          "description": "Oh, snap!"}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.time.sleep")
  def test_power_on_vms(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(3)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].is_powered_on.return_value = False
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].is_powered_on.return_value = True  # Do not expect this in list.
    vms[2].vm_id.return_value = "fake_vm_2"
    vms[2].is_powered_on.return_value = False

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_power_state_for_vms
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"},
       {"task_id": "2", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"},
       {"task_id": "2", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_power_state_for_vms.__name__ = \
      "vms_set_power_state_for_vms"
    m_vmm_client.vms_set_power_state_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]

    for vm in vms:
      vm.is_accessible.return_value = True

    cluster = HyperVCluster(self.cluster_metadata)
    with mock.patch.object(cluster, "vms") as m_vms:
      m_vms.return_value = vms
      cluster.power_on_vms(vms)

    m_vmm_client.vms_set_power_state_for_vms.assert_has_calls([
      mock.call("fake_cluster", [{"vm_id": "fake_vm_0", "power_state": "on"}]),
      mock.call("fake_cluster", [{"vm_id": "fake_vm_2", "power_state": "on"}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.time")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.HypervTaskPoller")
  def test_power_on_vms_timeout(
      self,  m_HypervTaskPoller, m_VmmClient, m_time):

    # Fake all tasks as done
    m_HypervTaskPoller.execute_parallel_tasks.return_value = {}

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vms_set_power_state_for_vms.__name__ = \
      "vms_set_power_state_for_vms"
    m_vmm_client.vms_set_power_state_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]
    m_time.time.side_effect = lambda: m_time.time.call_count * 100
    m_time.sleep.return_value = 0

    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "Stopped",
        "ips": [],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
      {
        "id": "fake_vm_1",
        "name": "Fake VM 1",
        "status": "Stopped",
        "ips": [],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
      {
        "id": "fake_vm_2",
        "name": "Fake VM 1",
        "status": "Stopped",
        "ips": [],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
    ]
    m_vmm_client.refresh_vms.__name__ = "refresh_vms"
    m_vmm_client.refresh_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]


    cluster = HyperVCluster(self.cluster_metadata)
    vms = cluster.vms()

    with self.assertRaises(CurieTestException) as ar:
      cluster.power_on_vms(vms)
    self.assertEqual(str(ar.exception),
                     "Timed out waiting for all VMs to become accessible "
                     "within 900 seconds")
    self.assertGreater(m_time.sleep.call_count, 0)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.TaskPoller.get_deadline_secs")
  @mock.patch("curie.task.time.sleep")
  def test_power_on_vms_raises_exception_on_empty_task_list(
      self, m_time_sleep, m_get_deadline_secs, m_VmmClient):
    m_get_deadline_secs.side_effect = lambda x: time.time() + 0.1
    vms = [mock.Mock(spec=Vm) for _ in xrange(3)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].is_powered_on.return_value = False
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].is_powered_on.return_value = False
    vms[2].vm_id.return_value = "fake_vm_2"
    vms[2].is_powered_on.return_value = False

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vms_set_power_state_for_vms.__name__ = \
      "vms_set_power_state_for_vms"
    m_vmm_client.vms_set_power_state_for_vms.return_value = []

    cluster = HyperVCluster(self.cluster_metadata)
    with self.assertRaises(CurieTestException) as ar:
      cluster.power_on_vms(vms)

    self.assertEqual(str(ar.exception),
                     "Unhandled exception occurred in HypervTaskPoller while "
                     "waiting for tasks: Expected exactly 1 task in response "
                     "- got []")

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_power_off_vms(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(3)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].is_powered_on.return_value = True
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].is_powered_on.return_value = False  # Do not expect this in list.
    vms[2].vm_id.return_value = "fake_vm_2"
    vms[2].is_powered_on.return_value = True

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_power_state_for_vms
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"},
       {"task_id": "2", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"},
       {"task_id": "2", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_power_state_for_vms.__name__ = \
      "vms_set_power_state_for_vms"
    m_vmm_client.vms_set_power_state_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.power_off_vms(vms)

    m_vmm_client.vms_set_power_state_for_vms.assert_has_calls([
      mock.call("fake_cluster", [{"vm_id": "fake_vm_0", "power_state": "off"}]
      ),
      mock.call("fake_cluster", [{"vm_id": "fake_vm_2", "power_state": "off"}]
      ),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.HyperVNode", spec=True)
  def test_nodes(self, m_HyperVNode, m_VmmClient):
    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.get_nodes.__name__ = "get_nodes"
    m_vmm_client.get_nodes.return_value = [
      {
        "id": "fake_node_0",
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "ips": ["169.254.1.0"],
        "state": "Running",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
      {
        "id": "fake_node_1",
        "name": "Fake Node 1",
        "fqdn": "fake_node_1",
        "ips": ["169.254.1.1"],
        "state": "Running",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    curr_node0 = self.cluster_metadata.cluster_nodes.add()
    curr_node0.id = "fake_node_0"
    curr_node1 = self.cluster_metadata.cluster_nodes.add()
    curr_node1.id = "fake_node_1"

    nodes = cluster.nodes()

    m_vmm_client.get_nodes.assert_called_once_with("fake_cluster", None)
    self.assertIsInstance(nodes[0], HyperVNode)
    m_HyperVNode.assert_has_calls([
      mock.call(cluster, "fake_node_0", 0, {
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "ips": ["169.254.1.0"],
        "power_state": "Running",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      }),
      mock.call(cluster, "fake_node_1", 1, {
        "name": "Fake Node 1",
        "fqdn": "fake_node_1",
        "ips": ["169.254.1.1"],
        "power_state": "Running",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      }),
    ])

  @mock.patch("curie.hyperv_cluster.time")
  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.hyperv_cluster.HyperVNode", spec=True)
  def test_power_off_nodes_soft(
      self, m_HyperVNode, m_VmmClient, m_time):
    nodes = [mock.Mock(spec=HyperVNode) for _ in xrange(3)]
    nodes[0].node_id.return_value = "fake_node_0"
    nodes[0].get_fqdn.return_value = "fake_node_0_fqdn"
    nodes[0].is_powered_on_soft.return_value = True
    nodes[1].node_id.return_value = "fake_node_1"
    nodes[1].get_fqdn.return_value = "fake_node_1_fqdn"
    nodes[1].is_powered_on_soft.return_value = False  # Don't expect in list.
    nodes[2].node_id.return_value = "fake_node_2"
    nodes[2].get_fqdn.return_value = "fake_node_2_fqdn"
    nodes[2].is_powered_on_soft.return_value = True

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # nodes_power_state
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"},
       {"task_id": "2", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"},
       {"task_id": "2", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.nodes_power_state.__name__ = "nodes_power_state"
    m_vmm_client.nodes_power_state.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]
    m_vmm_client.get_nodes.return_value = [
      {
        "id": "fake_node_0",
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "ips": ["169.254.1.0"],
        "state": "PowerOff",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
      {
        "id": "fake_node_1",
        "name": "Fake Node 1",
        "fqdn": "fake_node_1",
        "ips": ["169.254.1.1"],
        "state": "PowerOff",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
      {
        "id": "fake_node_2",
        "name": "Fake Node 2",
        "fqdn": "fake_node_2",
        "ips": ["169.254.1.2"],
        "state": "PowerOff",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
    ]
    m_time.time.side_effect = lambda: m_time.time.call_count * 100
    m_time.sleep.return_value = 0

    cluster = HyperVCluster(self.cluster_metadata)
    curr_node0 = self.cluster_metadata.cluster_nodes.add()
    curr_node0.id = "fake_node_0"
    curr_node1 = self.cluster_metadata.cluster_nodes.add()
    curr_node1.id = "fake_node_1"
    curr_node2 = self.cluster_metadata.cluster_nodes.add()
    curr_node2.id = "fake_node_2"

    cluster.power_off_nodes_soft(nodes)

    m_vmm_client.nodes_power_state.assert_has_calls([
      mock.call("fake_cluster", [{"id": "fake_node_0",
                                  "fqdn": "fake_node_0_fqdn"}]),
      mock.call("fake_cluster", [{"id": "fake_node_2",
                                  "fqdn": "fake_node_2_fqdn"}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_relocate_vms_datastore(self, m_time_sleep, m_VmmClient):
    vms = [mock.Mock(spec=Vm) for _ in xrange(2)]
    vms[0].vm_id.return_value = "fake_vm_0"
    vms[0].node_id.return_value = "fake_node_0"
    vms[1].vm_id.return_value = "fake_vm_1"
    vms[1].node_id.return_value = "fake_node_1"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # migrate_vm_datastore
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.migrate_vm_datastore.__name__ = "migrate_vm_datastore"
    m_vmm_client.migrate_vm_datastore.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.relocate_vms_datastore(vms,
                                   ["fake_datastore_0", "fake_datastore_1"])

    m_vmm_client.migrate_vm_datastore.assert_has_calls([
      mock.call("fake_cluster", [{"vm_id": "fake_vm_0",
                                  "datastore_name": "fake_datastore_0"}]),
      mock.call("fake_cluster", [{"vm_id": "fake_vm_1",
                                  "datastore_name":"fake_datastore_1"}]),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_clone_vms_from_template(self, m_time_sleep, m_VmmClient):
    vm = mock.Mock(spec=Vm)
    from curie.name_util import CURIE_GOLDIMAGE_VM_NAME_PREFIX
    vm.vm_id.return_value = "%s_fake_source_vm" % CURIE_GOLDIMAGE_VM_NAME_PREFIX
    vm.node_id.return_value = "fake_node_0"
    vm.vm_name.return_value = "%s_fake_source_vm" % CURIE_GOLDIMAGE_VM_NAME_PREFIX
    vm.is_powered_on.return_value = False

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # create_vm
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.0"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_source_vm_0",
        "name": "Fake Source VM 0",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_cloned_vm_0",
        "name": "Fake Cloned VM 0",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_cloned_vm_1",
        "name": "Fake Cloned VM 1",
        "status": "PowerOff",
        "ips": ["169.254.1.3"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    vms = cluster.clone_vms(vm, ["Fake Cloned VM 0", "Fake Cloned VM 1"],
                            node_ids=["fake_node_0", "fake_node_0"])
    self.assertEqual(vms[0].vm_name(), "Fake Cloned VM 0")
    self.assertEqual(vms[0].vm_id(), "fake_cloned_vm_0")
    self.assertEqual(vms[1].vm_name(), "Fake Cloned VM 1")
    self.assertEqual(vms[1].vm_id(), "fake_cloned_vm_1")

    m_vmm_client.clone_vm.assert_not_called()
    m_vmm_client.convert_to_template.assert_not_called()
    m_vmm_client.create_vm.assert_has_calls([
      mock.call("fake_cluster", "__curie_goldimage_fake_source_vm",
                [{"vm_name": "Fake Cloned VM 0", "node_id": "fake_node_0"}],
                "\\\\fake\\path\\to\\fake_share_name", None),
      mock.call("fake_cluster", "__curie_goldimage_fake_source_vm",
                [{"vm_name": "Fake Cloned VM 1", "node_id": "fake_node_0"}],
                "\\\\fake\\path\\to\\fake_share_name", None),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_clone_vms_from_other_vm(self, m_time_sleep, m_VmmClient):
    vm = mock.Mock(spec=Vm)
    vm.vm_id.return_value = "fake_source_vm_0"
    vm.node_id.return_value = "fake_node_0"
    vm.vm_name.return_value = "Fake Source VM 0"
    vm.is_powered_on.return_value = False

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # clone_vm
      [{"task_id": "0", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"}],
      # convert_to_template
      [{"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # create_vm
      [{"task_id": "2", "task_type": "vmm", "state": "running"},
       {"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "2", "task_type": "vmm", "state": "completed"},
       {"task_id": "3", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.nodes_power_state.__name__ = "nodes_power_state"
    m_vmm_client.clone_vm.__name__ = "clone_vm"
    m_vmm_client.clone_vm.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
    ]
    m_vmm_client.convert_to_template.__name__ = "convert_to_template"
    m_vmm_client.convert_to_template.side_effect = [
      [{"task_id": "1", "task_type": "vmm"}],
    ]
    m_vmm_client.create_vm.__name__ = "create_vm"
    m_vmm_client.create_vm.side_effect = [
      [{"task_id": "2", "task_type": "vmm"}],
      [{"task_id": "3", "task_type": "vmm"}],
    ]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "fake_some_other_vm",
        "name": "Fake Some Other VM",
        "status": "PowerOff",
        "ips": ["169.254.1.0"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_source_vm_0",
        "name": "Fake Source VM 0",
        "status": "PowerOff",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_cloned_vm_0",
        "name": "Fake Cloned VM 0",
        "status": "PowerOff",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
      {
        "id": "fake_cloned_vm_1",
        "name": "Fake Cloned VM 1",
        "status": "PowerOff",
        "ips": ["169.254.1.3"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    vms = cluster.clone_vms(vm, ["Fake Cloned VM 0", "Fake Cloned VM 1"],
                            node_ids=["fake_node_0", "fake_node_0"])
    self.assertEqual(vms[0].vm_name(), "Fake Cloned VM 0")
    self.assertEqual(vms[0].vm_id(), "fake_cloned_vm_0")
    self.assertEqual(vms[1].vm_name(), "Fake Cloned VM 1")
    self.assertEqual(vms[1].vm_id(), "fake_cloned_vm_1")

    m_vmm_client.clone_vm.assert_called_once_with(
      "fake_cluster", "fake_source_vm_0", "Fake Source VM 0",
      "\\\\fake\\path\\to\\fake_share_name"
    )
    m_vmm_client.convert_to_template.assert_called_once_with(
      "fake_cluster", "Fake Source VM 0"
    )
    m_vmm_client.create_vm.assert_has_calls([
      mock.call("fake_cluster", "Fake Source VM 0",
                [{"vm_name": "Fake Cloned VM 0", "node_id": "fake_node_0"}],
                "\\\\fake\\path\\to\\fake_share_name", None),
      mock.call("fake_cluster", "Fake Source VM 0",
                [{"vm_name": "Fake Cloned VM 1", "node_id": "fake_node_0"}],
                "\\\\fake\\path\\to\\fake_share_name", None),
    ], any_order=True)

  @mock.patch("curie.hyperv_cluster.VmmClient")
  def test_sync_power_state_for_nodes_equal_to_get_power_state_for_nodes(
      self, m_VmmClient):
    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.get_nodes.__name__ = "get_nodes"
    m_vmm_client.get_nodes.return_value = [
      {
        "id": "fake_node_0",
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "ips": ["169.254.1.0"],
        "state": "Responding",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
      {
        "id": "fake_node_1",
        "name": "Fake Node 1",
        "fqdn": "fake_node_1",
        "ips": ["169.254.1.1"],
        "state": "Responding",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
      {
        "id": "fake_node_2",
        "name": "Fake Node 2",
        "fqdn": "fake_node_2",
        "ips": ["169.254.1.2"],
        "state": "PowerOff",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
    ]

    nodes = [mock.Mock(spec=HyperVNode) for _ in xrange(3)]
    for node in nodes:
      node.get_management_software_property_name_map.return_value = \
        HyperVNode._NODE_PROPERTY_NAMES
    nodes[0].node_id.return_value = "fake_node_0"
    nodes[0].get_fqdn.return_value = "fake_node_0_fqdn"
    nodes[0].power_state = "Responding"
    nodes[1].node_id.return_value = "fake_node_1"
    nodes[1].get_fqdn.return_value = "fake_node_1_fqdn"
    nodes[1].power_state = "Responding"
    nodes[2].node_id.return_value = "fake_node_2"
    nodes[2].get_fqdn.return_value = "fake_node_2_fqdn"
    nodes[2].power_state = "PowerOff"

    cluster = HyperVCluster(self.cluster_metadata)
    self.assertEqual(cluster.sync_power_state_for_nodes(nodes),
                     {"fake_node_0": "Responding",
                      "fake_node_1": "Responding",
                      "fake_node_2": "PowerOff",
                      })
    self.assertEqual(cluster.sync_power_state_for_nodes(nodes),
                     cluster.get_power_state_for_nodes(nodes))

  @mock.patch("curie.hyperv_cluster.VmmClient")
  @mock.patch("curie.task.time.sleep")
  def test_cleanup(self, m_time_sleep, m_VmmClient):
    nodes = [mock.Mock(spec=HyperVNode) for _ in xrange(3)]
    for index, node in enumerate(nodes):
      node.get_management_software_property_name_map.return_value = \
        HyperVNode._NODE_PROPERTY_NAMES
      node.node_id.return_value = "fake_node_%d" % index
      node.get_fqdn.return_value = "fake_node_%d_fqdn" % index
      node.power_state = "Responding"

    m_vmm_client = m_VmmClient.return_value.__enter__.return_value
    m_vmm_client.vm_get_job_status.side_effect = [
      # vms_set_power_state_for_vms
      [{"task_id": "0", "task_type": "vmm", "state": "running"},
       {"task_id": "1", "task_type": "vmm", "state": "running"}],
      [{"task_id": "0", "task_type": "vmm", "state": "completed"},
       {"task_id": "1", "task_type": "vmm", "state": "completed"}],
      # vms_delete
      [{"task_id": "2", "task_type": "vmm", "state": "running"},
       {"task_id": "3", "task_type": "vmm", "state": "running"}],
      [{"task_id": "2", "task_type": "vmm", "state": "completed"},
       {"task_id": "3", "task_type": "vmm", "state": "completed"}],
      # clean_vmm
      [{"task_id": "4", "task_type": "vmm", "state": "running"}],
      [{"task_id": "4", "task_type": "vmm", "state": "completed"}],
      # clean_library_server
      [{"task_id": "5", "task_type": "vmm", "state": "running"}],
      [{"task_id": "5", "task_type": "vmm", "state": "completed"}],
    ]
    m_vmm_client.vms_set_power_state_for_vms.__name__ = \
      "vms_set_power_state_for_vms"
    m_vmm_client.vms_set_power_state_for_vms.side_effect = [
      [{"task_id": "0", "task_type": "vmm"}],
      [{"task_id": "1", "task_type": "vmm"}],
    ]
    m_vmm_client.vms_delete.__name__ = "vms_delete"
    m_vmm_client.vms_delete.side_effect = [
      [{"task_id": "2", "task_type": "vmm"}],
      [{"task_id": "3", "task_type": "vmm"}],
    ]
    m_vmm_client.clean_vmm.__name__ = "clean_vmm"
    m_vmm_client.clean_vmm.side_effect = [
      [{"task_id": "4", "task_type": "vmm"}],
    ]
    m_vmm_client.clean_library_server.__name__ = "clean_library_server"
    m_vmm_client.clean_library_server.side_effect = [
      [{"task_id": "5", "task_type": "vmm"}],
    ]
    m_vmm_client.get_vms.__name__ = "get_vms"
    m_vmm_client.get_vms.return_value = [
      {
        "id": "__curie_0_fake_vm_to_be_cleaned_0",
        "name": "__curie_0_fake_vm_to_be_cleaned_0",
        "status": "Running",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
      },
      {
        "id": "__curie_0_fake_vm_to_be_cleaned_1",
        "name": "__curie_0_fake_vm_to_be_cleaned_1",
        "status": "Running",
        "ips": ["169.254.1.2"],
        "node_id": "fake_node_0",
      },
      {
        "id": "innocent_bystander_0",
        "name": "I was created manually by the user... please don't delete me",
        "status": "Running",
        "ips": ["169.254.1.3"],
        "node_id": "fake_node_0",
      },
    ]

    cluster = HyperVCluster(self.cluster_metadata)
    cluster.cleanup()

    m_vmm_client.vms_set_power_state_for_vms.assert_has_calls([
      mock.call(
      "fake_cluster", [
        {"vm_id": "__curie_0_fake_vm_to_be_cleaned_0", "power_state": "off"},
      ]),
      mock.call(
      "fake_cluster", [
        {"vm_id": "__curie_0_fake_vm_to_be_cleaned_1", "power_state": "off"},
      ]),
    ], any_order=True)
    m_vmm_client.vms_delete.assert_has_calls([
      mock.call(
      "fake_cluster", ["__curie_0_fake_vm_to_be_cleaned_1"]),
      mock.call(
      "fake_cluster", ["__curie_0_fake_vm_to_be_cleaned_0"]),
    ], any_order=True)
    m_vmm_client.clean_library_server.assert_called_once_with(
      "fake_cluster", "__curie")
