#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import json
import unittest

import mock

from curie.vmm_client import VmmClient


class TestVmmClient(unittest.TestCase):
  def setUp(self):
    self.vmm_client = VmmClient("fake_hostname", "fake_username",
                                "fake_password")

  def test_init_set_host_defaults(self):
    vmm_client = VmmClient("fake_hostname", "fake_username", "fake_password")
    self.assertEqual(vmm_client.address, "fake_hostname")
    self.assertEqual(vmm_client.username, "fake_username")
    self.assertEqual(vmm_client.password, "fake_password")
    self.assertEqual(vmm_client.host_address, "fake_hostname")
    self.assertEqual(vmm_client.host_username, "fake_username")
    self.assertEqual(vmm_client.host_password, "fake_password")
    self.assertEqual(vmm_client.library_server_username, "fake_username")
    self.assertEqual(vmm_client.library_server_password, "fake_password")
    self.assertIsNone(vmm_client.library_server_share_path)
    self.assertIsNone(vmm_client.library_server_address)

  def test_init_set_host_override(self):
    vmm_client = VmmClient(
      "fake_hostname", "fake_username", "fake_password",
      host_address="fake_host_hostname",
      host_username="fake_host_username",
      host_password="fake_host_password",
      library_server_address="fake_library_server_hostname",
      library_server_username="fake_library_server_username",
      library_server_password="fake_library_server_password")
    self.assertEqual(vmm_client.address, "fake_hostname")
    self.assertEqual(vmm_client.username, "fake_username")
    self.assertEqual(vmm_client.password, "fake_password")
    self.assertEqual(vmm_client.host_address, "fake_host_hostname")
    self.assertEqual(vmm_client.host_username, "fake_host_username")
    self.assertEqual(vmm_client.host_password, "fake_host_password")
    self.assertEqual(vmm_client.library_server_address,
                     "fake_library_server_hostname")
    self.assertEqual(vmm_client.library_server_username,
                     "fake_library_server_username")
    self.assertEqual(vmm_client.library_server_password,
                     "fake_library_server_password")
    self.assertIsNone(vmm_client.library_server_share_path)

  def test_init_library_server(self):
    vmm_client = VmmClient("fake_hostname", "fake_user", "fake_password",
                           library_server_address="fake_library_server",
                           library_server_share_path="fake_library_path")
    self.assertEqual(vmm_client.library_server_username, "fake_user")
    self.assertEqual(vmm_client.library_server_password, "fake_password")
    self.assertEqual(vmm_client.library_server_address, "fake_library_server")
    self.assertEqual(vmm_client.library_server_share_path, "fake_library_path")

  def test_is_nutanix_cvm_false(self):
    vm = {"name": "NOT-A-CVM"}
    self.assertFalse(VmmClient.is_nutanix_cvm(vm))

  def test_is_nutanix_cvm_true(self):
    vm = {"name": "NTNX-12345678-A-CVM"}
    self.assertTrue(VmmClient.is_nutanix_cvm(vm))

  def test_get_clusters(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_clusters()

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmHypervCluster", json_params="{}")

  def test_get_clusters_cluster_name_cluster_type(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_clusters(cluster_name="Fake Cluster",
                                   cluster_type="HyperV")

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmHypervCluster",
      json_params="{\"name\": \"Fake Cluster\", \"type\": \"HyperV\"}")

  def test_get_library_shares(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_library_shares()

    m_ps_client.execute.assert_called_once_with("Get-VmmLibraryShares")

  def test_get_vms(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_vms(cluster_name="Fake Cluster")

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmVM", cluster_name="Fake Cluster", json_params="[]", num_retries=10)

  def test_get_vms_matching_ids(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_vms(cluster_name="Fake Cluster",
                              vm_input_list=[{"id": "0"}, {"id": "1"}])

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmVM", cluster_name="Fake Cluster",
      json_params="[{\"id\": \"0\"}, {\"id\": \"1\"}]", num_retries=10)

  def test_refresh_vms(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.refresh_vms(cluster_name="Fake Cluster")

    m_ps_client.execute.assert_called_once_with(
      "Read-VmmVM", cluster_name="Fake Cluster", json_params="[]")

  def test_refresh_vms_matching_ids(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.refresh_vms(cluster_name="Fake Cluster",
                                  vm_input_list=[{"id": "0"}, {"id": "1"}])

    m_ps_client.execute.assert_called_once_with(
      "Read-VmmVM", cluster_name="Fake Cluster",
      json_params="[{\"id\": \"0\"}, {\"id\": \"1\"}]")

  def test_get_nodes(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_nodes(cluster_name="Fake Cluster")

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmHypervClusterNode", cluster_name="Fake Cluster",
      json_params="[]", num_retries=10)

  def test_get_nodes_matching_ids(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.get_nodes(cluster_name="Fake Cluster",
                                nodes=[{"id": "0"}, {"id": "1"}])

    m_ps_client.execute.assert_called_once_with(
      "Get-VmmHypervClusterNode", cluster_name="Fake Cluster",
      json_params="[{\"id\": \"0\"}, {\"id\": \"1\"}]", num_retries=10)

  def test_nodes_power_state(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.nodes_power_state(cluster_name="Fake Cluster")

    m_ps_client.execute_async.assert_called_once_with(
      "Set-VmmHypervClusterNodeShutdown", cluster_name="Fake Cluster",
      json_params="[]")

  def test_nodes_power_state_matching_ids(self):
    self.vmm_client.host_username = "fake_host_username"
    self.vmm_client.host_password = "fake_host_password"
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.nodes_power_state(cluster_name="Fake Cluster",
                                        nodes=[{"id": "0"}, {"id": "1"}])

    m_ps_client.execute_async.assert_called_once_with(
      "Set-VmmHypervClusterNodeShutdown", cluster_name="Fake Cluster",
      json_params="[{\"id\": \"0\", "
                  "\"password\": \"fake_host_password\", "
                  "\"username\": \"fake_host_username\"}, "
                  "{\"id\": \"1\", "
                  "\"password\": \"fake_host_password\", "
                  "\"username\": \"fake_host_username\"}]")

  def test_vms_set_power_state_for_vms(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vms_set_power_state_for_vms(
        cluster_name="Fake Cluster",
        task_req_list=[{"vm_id": "0", "power_state": "off"},
                       {"vm_id": "1", "power_state": "off"}])

    m_ps_client.execute.assert_called_once_with(
      "Set-VmmVMPowerState", cluster_name="Fake Cluster",
      json_params="[{\"power_state\": \"off\", \"vm_id\": \"0\"}, "
                  "{\"power_state\": \"off\", \"vm_id\": \"1\"}]")

  def test_vms_set_possible_owners_for_vms(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vms_set_possible_owners_for_vms(
        cluster_name="Fake Cluster",
        task_req_list=[{"vm_id": "0", "possible_owners": ["0", "1"]},
                       {"vm_id": "1", "possible_owners": ["0", "1"]}])

    m_ps_client.execute.assert_called_once_with(
      "Set-VmmVMPossibleOwners", cluster_name="Fake Cluster",
      json_params="[{\"possible_owners\": [\"0\", \"1\"], \"vm_id\": \"0\"}, "
                  "{\"possible_owners\": [\"0\", \"1\"], \"vm_id\": \"1\"}]")

  def test_vms_set_snapshot(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vms_set_snapshot(
        cluster_name="Fake Cluster",
        task_req_list=[{"vm_id": "0", "name": "snapshot_0",
                        "description": "Snapshot 0"},
                       {"vm_id": "1", "name": "snapshot_1",
                        "description": "Snapshot 1"}])

    m_ps_client.execute.assert_called_once_with(
      "Set-VmmVMSnapshot", cluster_name="Fake Cluster",
      json_params="[{\"description\": \"Snapshot 0\", "
                  "\"name\": \"snapshot_0\", \"vm_id\": \"0\"}, "
                  "{\"description\": \"Snapshot 1\", "
                  "\"name\": \"snapshot_1\", \"vm_id\": \"1\"}]")

  def test_vm_get_job_status_vmm_tasks(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vm_get_job_status(
        task_id_list=[{"task_type": "vmm", "task_id": "0"},
                      {"task_type": "vmm", "task_id": "1"}])

    m_ps_client.execute.assert_called_once_with(
      "Get-Task",
      json_params="[{\"task_id\": \"0\", \"task_type\": \"vmm\"}, "
                  "{\"task_id\": \"1\", \"task_type\": \"vmm\"}]", num_retries=10)

  def test_vm_get_job_status_ps_tasks(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      mock_ps_cmd_0 = mock.Mock()
      mock_ps_cmd_1 = mock.Mock()
      m_ps_client.poll.side_effect = [mock_ps_cmd_0, mock_ps_cmd_1]
      self.vmm_client.vm_get_job_status(
        task_id_list=[{"task_type": "ps", "task_id": "0"},
                      {"task_type": "ps", "task_id": "1"}])

    m_ps_client.poll.assert_has_calls([mock.call("0"), mock.call("1")])
    mock_ps_cmd_0.as_ps_task.assert_called_once_with()
    mock_ps_cmd_1.as_ps_task.assert_called_once_with()

  def test_vm_get_job_status_unknown_task_type(self):
    with self.assertRaises(ValueError) as ar:
      self.vmm_client.vm_get_job_status(
        task_id_list=[{"task_type": "arduous", "task_id": "0"}])

    self.assertEqual("Unknown task type 'arduous'", str(ar.exception))

  def test_vm_stop_job_vmm_tasks(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vm_stop_job(
        task_id_list=[{"task_type": "vmm", "task_id": "0"},
                      {"task_type": "vmm", "task_id": "1"}])

    m_ps_client.execute.assert_called_once_with(
      "Stop-Task",
      json_params="[{\"task_id\": \"0\", \"task_type\": \"vmm\"}, "
                  "{\"task_id\": \"1\", \"task_type\": \"vmm\"}]")

  def test_vm_stop_job_ps_tasks(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      mock_ps_cmd_0 = mock.Mock()
      mock_ps_cmd_1 = mock.Mock()
      m_ps_client.poll.side_effect = [mock_ps_cmd_0, mock_ps_cmd_1]
      self.vmm_client.vm_stop_job(
        task_id_list=[{"task_type": "ps", "task_id": "0"},
                      {"task_type": "ps", "task_id": "1"}])

    m_ps_client.poll.assert_has_calls([mock.call("0"), mock.call("1")])
    mock_ps_cmd_0.terminate.assert_called_once_with()
    mock_ps_cmd_1.terminate.assert_called_once_with()
    mock_ps_cmd_0.as_ps_task.assert_called_once_with()
    mock_ps_cmd_1.as_ps_task.assert_called_once_with()

  def test_vm_stop_job_unknown_task_type(self):
    with self.assertRaises(ValueError) as ar:
      self.vmm_client.vm_stop_job(
        task_id_list=[{"task_type": "arduous", "task_id": "0"}])

    self.assertEqual("Unknown task type 'arduous'", str(ar.exception))

  def test_vms_delete_default(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vms_delete(cluster_name="Fake Cluster",
                                 vm_ids=["0", "1"])

    m_ps_client.execute.assert_called_once_with(
      "Remove-VmmVM", cluster_name="Fake Cluster",
      json_params="{\"force_delete\": false, \"vm_ids\": [\"0\", \"1\"]}")

  def test_vms_delete_force_delete_true(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.vms_delete(cluster_name="Fake Cluster",
                                 vm_ids=["0", "1"], force_delete=True)

    m_ps_client.execute.assert_called_once_with(
      "Remove-VmmVM", cluster_name="Fake Cluster",
      json_params="{\"force_delete\": true, \"vm_ids\": [\"0\", \"1\"]}")

  def test_create_vm_template(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.create_vm_template(
        "fake_cluster", "fake_vm_template", "host_id_0",
        "/fake/goldimages/path", "/fake/datastore/path", "fake_network")

    m_ps_client.execute.assert_called_once_with(
      "Install-VmmVMTemplate", cluster_name="fake_cluster",
      json_params=json.dumps({
        "vm_name": "fake_vm_template",
        "vm_host_id": "host_id_0",
        "goldimage_disk_path": "/fake/goldimages/path",
        "vm_datastore_path": "/fake/datastore/path",
        "vmm_network_name": "fake_network",
        "vcpus": 1,
        "ram_mb": 1024
      }, sort_keys=True))

  def test_create_vm(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.create_vm(
        "fake_cluster", "fake_vm_template",
        [{"vm_name": "fake_vm_0", "node_id": "0"},
         {"vm_name": "fake_vm_1", "node_id": "1"}],
        "/fake/datastore/path", None, None)

    m_ps_client.execute.assert_called_once_with(
      "New-VmmVM", cluster_name="fake_cluster",
      json_params=json.dumps({
        "vm_template_name": "fake_vm_template",
        "vm_host_map": [{"vm_name": "fake_vm_0", "node_id": "0"},
                        {"vm_name": "fake_vm_1", "node_id": "1"}],
        "vm_datastore_path": "/fake/datastore/path",
        "data_disks": None,
        "differencing_disks_path": None
      }, sort_keys=True))

  def test_clone_vm(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.clone_vm(
        "fake_cluster", "fake_vm_id_0", "fake_new_cloned_vm",
        "/fake/datastore/path")

    m_ps_client.execute.assert_called_once_with(
      "New-VmmVMClone", cluster_name="fake_cluster",
      json_params=json.dumps({
        "base_vm_id": "fake_vm_id_0",
        "vm_name": "fake_new_cloned_vm",
        "vm_datastore_path": "/fake/datastore/path"
      }, sort_keys=True))

  def test_upload_image(self):
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    self.vmm_client.library_server_address = "fake_library_server"
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.upload_image(
        ["/fake/goldimage/path/0", "/fake/goldimage/path/1"],
        "/fake/goldimage/target/directory", "fake_disk_name")

    m_ps_client.execute_async.assert_called_once_with(
      "Install-VmmDiskImage", overwriteFiles=False,
      json_params=json.dumps({
        "vmm_library_server_share": "/fake/library/share/path",
        "vmm_library_server_user": "fake_username",
        "vmm_library_server_password": "fake_password",
        "vmm_library_server": "fake_library_server",
        "goldimage_disk_list": ["/fake/goldimage/path/0",
                                "/fake/goldimage/path/1"],
        "goldimage_target_dir": "/fake/goldimage/target/directory",
        "disk_name": "fake_disk_name",
        "transfer_type": None
      }, sort_keys=True)
    )

  def test_convert_to_template(self):
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.convert_to_template(cluster_name="Fake Cluster",
                                          template_name="fake_template")

    m_ps_client.execute.assert_called_once_with(
      "ConvertTo-Template", cluster_name="Fake Cluster",
      json_params="{\"template_name\": \"fake_template\", "
                  "\"vmm_library_server_share\": \"/fake/library/share/path\"}")

  def test_migrate_vm(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.migrate_vm(
        "fake_cluster",
        [{"vm_name": "fake_vm_0", "node_id": "0"},
         {"vm_name": "fake_vm_1", "node_id": "1"}],
        "/fake/datastore/path")

    m_ps_client.execute.assert_called_once_with(
      "Move-VmmVM", cluster_name="fake_cluster",
      json_params=json.dumps({
        "vm_host_map": [{"vm_name": "fake_vm_0", "node_id": "0"},
                        {"vm_name": "fake_vm_1", "node_id": "1"}],
        "vm_datastore_path": "/fake/datastore/path",
      }, sort_keys=True))

  def test_migrate_vm_datastore(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.migrate_vm_datastore(
        "fake_cluster",
        [{"vm_id": "fake_vm_id_0", "datastore_name": "fake_datastore_0"},
         {"vm_id": "fake_vm_id_1", "datastore_name": "fake_datastore_1"}])

    m_ps_client.execute.assert_called_once_with(
      "Move-VmmVMDatastore", cluster_name="fake_cluster",
      json_params=json.dumps({
        "vm_datastore_map": [
          {"vm_id": "fake_vm_id_0", "datastore_name": "fake_datastore_0"},
          {"vm_id": "fake_vm_id_1", "datastore_name": "fake_datastore_1"}],
      }, sort_keys=True))

  def test_clean_vmm(self):
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.clean_vmm("fake_cluster", "fake_target_dir", "fake_datastore_path", "fake_vm_")

    m_ps_client.execute_async.assert_called_once_with(
      "Remove-ClusterVmmObjects", cluster_name="fake_cluster",
      json_params=json.dumps({
        "vmm_library_server_share": "/fake/library/share/path",
        "target_dir": "fake_target_dir",
        "vm_datastore_path": "fake_datastore_path",
        "vm_name_prefix": "fake_vm_"
      }, sort_keys=True))

  def test_clean_library_server(self):
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    self.vmm_client.library_server_address = "fake_library_server"
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.clean_library_server("/fake/target/directory",
                                           "fake_vm_")

    m_ps_client.execute_async.assert_called_once_with(
      "Remove-VmmDiskImages",
      json_params=json.dumps({
        "vmm_library_server_share": "/fake/library/share/path",
        "vmm_library_server_user": "fake_username",
        "vmm_library_server_password": "fake_password",
        "vmm_library_server": "fake_library_server",
        "target_dir": "/fake/target/directory",
        "vm_name_prefix": "fake_vm_"
      }, sort_keys=True))

  def test_update_library(self):
    with mock.patch.object(self.vmm_client, "ps_client") as m_ps_client:
      self.vmm_client.update_library("/fake/goldimages/path")

    m_ps_client.execute_async.assert_called_once_with(
      "Update-Library",
      json_params="{\"goldimage_disk_path\": \"/fake/goldimages/path\"}", num_retries=3)
