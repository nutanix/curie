#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import json
import unittest

import mock
import requests

from curie.vmm_client import VmmClient, VmmClientException
from curie.vmm_client import log as vmm_client_log


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

  def test_enter_exit(self):
    with mock.patch("curie.vmm_client.VmmClient.login") as m_login, \
         mock.patch("curie.vmm_client.VmmClient.logout") as m_logout:
      m_login.assert_not_called()
      m_logout.assert_not_called()
      with self.vmm_client:
        pass
      m_login.assert_called_once_with()
      m_logout.assert_called_once_with()

  def test_is_nutanix_cvm_false(self):
    vm = {"name": "NOT-A-CVM"}
    self.assertFalse(VmmClient.is_nutanix_cvm(vm))

  def test_is_nutanix_cvm_true(self):
    vm = {"name": "NTNX-12345678-A-CVM"}
    self.assertTrue(VmmClient.is_nutanix_cvm(vm))

  @mock.patch("curie.vmm_client.requests")
  @mock.patch("curie.vmm_client.log", wraps=vmm_client_log)
  def test_get_debug_kwargs(self, m_log, m_requests):
    response = mock.Mock(spec=requests.Response)
    response.content = "The response from the 'get' method"
    response.json.return_value = "The response from the 'get' method"
    response.status_code = 200
    m_requests.get.func_name = "get"
    m_requests.get.return_value = response

    ret = self.vmm_client.get("fake/path/to/resource", some_kwarg="Kwarg?")

    m_requests.get.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/fake/path/to/resource",
      some_kwarg="Kwarg?")
    self.assertEqual(ret, response)
    m_log.warning.assert_not_called()
    self.assertEqual(m_log.debug.call_count, 2)
    m_log.debug.assert_has_calls([
      mock.call("%s %s %r", "GET", "fake/path/to/resource",
                {"some_kwarg": "Kwarg?"}),
      mock.call("RESPONSE '%s' (%d): %r", "fake/path/to/resource", 200,
                "The response from the 'get' method"),
    ])

  @mock.patch("curie.vmm_client.requests")
  @mock.patch("curie.vmm_client.log", wraps=vmm_client_log)
  def test_get_debug_kwargs_empty(self, m_log, m_requests):
    response = mock.Mock(spec=requests.Response)
    response.content = "The response from the 'get' method"
    response.json.return_value = "The response from the 'get' method"
    response.status_code = 200
    m_requests.get.func_name = "get"
    m_requests.get.return_value = response

    ret = self.vmm_client.get("fake/path/to/resource")

    m_requests.get.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/fake/path/to/resource")
    self.assertEqual(ret, response)
    m_log.warning.assert_not_called()
    self.assertEqual(m_log.debug.call_count, 2)
    m_log.debug.assert_has_calls([
      mock.call("%s %s %r", "GET", "fake/path/to/resource", {}),
      mock.call("RESPONSE '%s' (%d): %r", "fake/path/to/resource", 200,
                "The response from the 'get' method"),
    ])

  @mock.patch("curie.vmm_client.requests")
  @mock.patch("curie.vmm_client.VmmClient.logout")
  @mock.patch("curie.vmm_client.VmmClient.login")
  @mock.patch("curie.vmm_client.log", wraps=vmm_client_log)
  def test_requests_methods(self, m_log, m_login, m_logout, m_requests):
    for method in ["get", "options", "head", "post", "put", "patch", "delete"]:
      response = mock.Mock(spec=requests.Response)
      response.json.return_value = "The response from the '%s' method" % method
      response.status_code = 200
      mock_func = getattr(m_requests, method)
      mock_func.func_name = method
      mock_func.return_value = response
      func = getattr(self.vmm_client, method)
      ret = func("fake/path/to/resource", some_kwarg="Kwarg?")
      getattr(m_requests, method).assert_called_once_with(
        "http://hyperv-ps-server:8888/v1/vmm/"
        "fake/path/to/resource", some_kwarg="Kwarg?")
      self.assertEqual(ret, response)
      m_logout.assert_not_called()
      m_login.assert_not_called()
      m_log.warning.assert_not_called()

      m_log.reset_mock()
      m_login.reset_mock()
      m_logout.reset_mock()
      m_requests.reset_mock()

  @mock.patch("curie.vmm_client.requests")
  @mock.patch("curie.vmm_client.VmmClient.logout")
  @mock.patch("curie.vmm_client.VmmClient.login")
  @mock.patch("curie.vmm_client.log", wraps=vmm_client_log)
  def test_requests_reconnection(self, m_log, m_login, m_logout, m_requests):
    for method in ["get", "options", "head", "post", "put", "patch", "delete"]:
      response = mock.Mock(spec=requests.Response)
      response.json.return_value = "I can't find your session!"
      response.status_code = 410
      mock_func = getattr(m_requests, method)
      mock_func.func_name = method
      mock_func.return_value = response
      func = getattr(self.vmm_client, method)
      ret = func("fake/path/to/resource", some_kwarg="Kwarg?")
      getattr(m_requests, method).assert_has_calls(
        [mock.call("http://hyperv-ps-server:8888/v1/vmm/fake/path/to/resource",
                   some_kwarg="Kwarg?")] * 2)
      self.assertEqual(ret, response)
      m_logout.assert_called_once_with()
      m_login.assert_called_once_with()
      m_log.warning.assert_called_once_with(
        "Exceeded maximum reconnection attempts (%d) in vmm_client.__request",
        1)

      m_log.reset_mock()
      m_login.reset_mock()
      m_logout.reset_mock()
      m_requests.reset_mock()

  @mock.patch("curie.vmm_client.requests")
  @mock.patch("curie.vmm_client.log", wraps=vmm_client_log)
  def test_login_successful(self, m_log, m_requests):
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({"session_id": 12345})
    m_requests.post.return_value = response
    m_requests.post.func_name = "post"

    self.vmm_client.login()

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/logon",
      json={"host_address": "fake_hostname",
            "host_user": "fake_username",
            "host_password": "fake_password",
            "vmm_server_address": "fake_hostname",
            "vmm_username": "fake_username",
            "vmm_password": "fake_password"},
      timeout=90.0)
    self.assertTrue(self.vmm_client.is_logged_in)
    self.assertEqual(self.vmm_client._session_id, "12345")
    m_log.debug.assert_any_call(
      "%s %s %r", "POST", "logon", {"json": {
        "vmm_username": "fake_username",
        "host_user": "fake_username",
        "host_password": "<REDACTED>",
        "host_address": "fake_hostname",
        "vmm_password": "<REDACTED>",
        "vmm_server_address": "fake_hostname"
      }, "timeout": 90.0})

  @mock.patch("curie.vmm_client.requests")
  def test_login_already_logged_in(self, m_requests):
    self.vmm_client._session_id = "12345"
    self.assertTrue(self.vmm_client.is_logged_in)

    self.vmm_client.login()

    m_requests.post.assert_not_called()
    self.assertTrue(self.vmm_client.is_logged_in)
    self.assertEqual(self.vmm_client._session_id, "12345")

  @mock.patch("curie.test.steps.test.time.sleep")
  @mock.patch("curie.vmm_client.requests")
  def test_login_failure_authentication_error(self, m_requests, m_time_sleep):
    response = requests.Response()
    response.status_code = 401
    response._content = json.dumps([
      {
        "reason": "AUTHENTICATION_ERROR",
        "message": "Those credentials are no good!",
        "details": "/",
      }
    ])
    m_requests.post.return_value = response

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.login()

    self.assertEqual(str(ar.exception),
                     "Error while logging in to host 'fake_hostname' and VMM "
                     "server 'fake_hostname' (AUTHENTICATION_ERROR): Those "
                     "credentials are no good!")
    call = mock.call(
      "http://hyperv-ps-server:8888/v1/vmm/logon",
      json={"host_address": "fake_hostname",
            "host_user": "fake_username",
            "host_password": "fake_password",
            "vmm_server_address": "fake_hostname",
            "vmm_username": "fake_username",
            "vmm_password": "fake_password"},
      timeout=90.0)
    m_requests.post.assert_has_calls([call])
    m_time_sleep.assert_not_called()

  @mock.patch("curie.vmm_client.time.sleep")
  @mock.patch("curie.vmm_client.requests")
  def test_login_failure_internal_error_retries(self, m_requests,
                                                m_time_sleep):
    response = requests.Response()
    response.status_code = 500
    response._content = json.dumps([
      {
        "reason": "INTERNAL_ERROR",
        "message": "Something really bad happened",
        "details": "/",
      }
    ])
    m_requests.post.return_value = response

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.login()

    self.assertEqual(str(ar.exception),
                     "Could not log in to host 'fake_hostname' and VMM server "
                     "'fake_hostname'")
    m_time_sleep.assert_has_calls([mock.call(1)] * 5)

  @mock.patch("curie.vmm_client.requests")
  def test_logout_while_logged_in(self, m_requests):
    login_response = requests.Response()
    login_response.status_code = 200
    login_response._content = json.dumps({"session_id": 12345})
    logout_response = requests.Response()
    logout_response.status_code = 200
    logout_response._content = json.dumps({"status": "ok"})
    m_requests.post.side_effect = [login_response, logout_response]
    self.vmm_client.login()

    m_requests.reset_mock()
    self.vmm_client.logout()

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/logout", timeout=90.0)
    self.assertFalse(self.vmm_client.is_logged_in)

  @mock.patch("curie.vmm_client.requests")
  def test_logout_internal_error(self, m_requests):
    login_response = requests.Response()
    login_response.status_code = 200
    login_response._content = json.dumps({"session_id": 12345})
    logout_response = requests.Response()
    logout_response.status_code = 500
    logout_response._content = json.dumps([
      {
        "reason": "INTERNAL_ERROR",
        "message": "Something really bad happened",
        "details": "/",
      }
    ])
    m_requests.post.side_effect = [login_response, logout_response]
    self.vmm_client.login()

    m_requests.reset_mock()
    self.vmm_client.logout()

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/logout", timeout=90.0)
    self.assertFalse(self.vmm_client.is_logged_in)

  @mock.patch("curie.vmm_client.requests")
  def test_logout_while_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)
    self.vmm_client.logout()
    m_requests.post.assert_not_called()
    self.assertFalse(self.vmm_client.is_logged_in)

  @mock.patch("curie.vmm_client.requests")
  def test_get_clusters_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.get_clusters()

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_get_clusters(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {"name": "Fake Cluster",
       "storage": {
         "name": "Share-1",
         "path": "//fake/path/to/share-1"
       },
       "networks": [
         "Network-1"
       ],
       }
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_clusters()

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/clusters/list", json={}, timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_clusters_cluster_name_cluster_type(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps("Return value not important here")
    m_requests.post.return_value = resp

    self.vmm_client.get_clusters(cluster_name="Fake Cluster",
                                 cluster_type="HyperV")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/clusters/list",
      json={"name": "Fake Cluster", "type": "HyperV"},
      timeout=90.0)

  @mock.patch("curie.vmm_client.requests")
  def test_get_clusters_error_code_raise_exception_no_decode(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.url = "http://fake-server:8888/v1/vmm/1234/clusters/list"
    resp.status_code = 401
    resp.reason = "Fake reason"
    resp._content = "This test string is not JSON-decodable"
    m_requests.post.return_value = resp

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.get_clusters()

    self.assertIn("401 Client Error: Fake reason for url: "
                  "http://fake-server:8888/v1/vmm/1234/clusters/list",
                  str(ar.exception))

  @mock.patch("curie.vmm_client.requests")
  def test_get_clusters_error_code_raise_exception_decode(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.url = "http://fake-server:8888/v1/vmm/1234/clusters/list"
    resp.status_code = 401
    resp.reason = "Fake reason"
    resp._content = json.dumps([{"message": "Fake message from PS server",
                                 "details": "The fake details from PS"}])
    m_requests.post.return_value = resp

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.get_clusters()

    self.assertIn("401 Client Error: Fake reason for url: "
                  "http://fake-server:8888/v1/vmm/1234/clusters/list: "
                  "Fake message from PS server",
                  str(ar.exception))

  @mock.patch("curie.vmm_client.requests")
  def test_get_library_shares_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.get_library_shares()

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_get_library_shares(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {"name": "Share-1",
       "path": "//fake/path/to/share-1"
       }
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_library_shares()

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/library_shares/list", json={}, timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_library_shares_error_code_raises_exception(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.url = "http://fake-server:8888/v1/vmm/1234/library_shares/list"
    resp.status_code = 401
    resp.reason = "Fake reason"
    resp._content = json.dumps([{"message": "Fake message from PS server",
                                 "details": "The fake details from PS"}])
    m_requests.post.return_value = resp

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.get_library_shares()

    self.assertIn("401 Client Error: Fake reason for url: "
                  "http://fake-server:8888/v1/vmm/1234/library_shares/list: "
                  "Fake message from PS server",
                  str(ar.exception))

  @mock.patch("curie.vmm_client.requests")
  def test_get_vms_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.get_vms("fake_cluster")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_get_vms(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "ok",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_vms("fake_cluster")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/list?refresh=false", json=None, timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_vms_matching_ids(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {
        "id": "fake_vm_0",
        "name": "Fake VM 0",
        "status": "ok",
        "ips": ["169.254.1.1"],
        "node_id": "fake_node_0",
        "is_dynamic_optimization_available": False
      },
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_vms("fake_cluster",
                                        vm_input_list=[{"id": "0"}, {"id": "1"}])

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/list?refresh=false",
      json=[{"id": "0"}, {"id": "1"}],
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_vms_error_code_raises_exception(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.url = "http://fake-server:8888/v1/vmm/1234/cluster/fake_cluster/" \
               "vms/list"
    resp.status_code = 401
    resp.reason = "Fake reason"
    resp._content = json.dumps([{"message": "Fake message from PS server",
                                 "details": "The fake details from PS"}])
    m_requests.post.return_value = resp

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.get_vms("fake_cluster")

    self.assertIn("401 Client Error: Fake reason for url: "
                  "http://fake-server:8888/v1/vmm/1234/cluster/fake_cluster/"
                  "vms/list: Fake message from PS server",
                  str(ar.exception))

  @mock.patch("curie.vmm_client.requests")
  def test_get_nodes_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.get_nodes("fake_cluster")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_get_nodes(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {
        "id": "fake_node_0",
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "state": "on",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_nodes("fake_cluster")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/nodes/list", json=None, timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_nodes_matching_ids(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [
      {
        "id": "fake_node_0",
        "name": "Fake Node 0",
        "fqdn": "fake_node_0",
        "state": "on",
        "overall_state": "ok",
        "version": "fake_version_string_1234"
      },
    ]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.get_nodes("fake_cluster",
                                          nodes=[{"id": "0"}, {"id": "1"}])

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/nodes/list",
      json=[{"id": "0"}, {"id": "1"}],
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_get_nodes_error_code_raises_exception(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.url = "http://fake-server:8888/v1/vmm/1234/cluster/fake_cluster/" \
               "nodes/list"
    resp.status_code = 401
    resp.reason = "Fake reason"
    resp._content = json.dumps([{"message": "Fake message from PS server",
                                 "details": "The fake details from PS"}])
    m_requests.post.return_value = resp

    with self.assertRaises(VmmClientException) as ar:
      self.vmm_client.get_nodes("fake_cluster")

    self.assertIn("401 Client Error: Fake reason for url: "
                  "http://fake-server:8888/v1/vmm/1234/cluster/fake_cluster/"
                  "nodes/list: Fake message from PS server",
                  str(ar.exception))

  @mock.patch("curie.vmm_client.requests")
  def test_nodes_power_state_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.nodes_power_state("fake_cluster", nodes=[])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_nodes_power_state(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.nodes_power_state(
      "fake_cluster", nodes=[{"id": "fake_node_0", "fqdn": "fake_node_0_fqdn"},
                             {"id": "fake_node_1", "fqdn": "fake_node_1_fqdn"}])

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/nodes/shutdown",
      json=[
        {"username": "fake_username", "password": "fake_password",
         "id": "fake_node_0", "fqdn": "fake_node_0_fqdn"},
        {"username": "fake_username", "password": "fake_password",
         "id": "fake_node_1", "fqdn": "fake_node_1_fqdn"},
      ],
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_power_state_for_vms_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vms_set_power_state_for_vms("fake_cluster", [])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_power_state_for_vms(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    vm_actions = [{"vm_id": "0", "power_state": "off"},
                  {"vm_id": "1", "power_state": "off"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.vms_set_power_state_for_vms(
      "fake_cluster", vm_actions)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/power_state",
      json=vm_actions,
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_possible_owners_for_vms_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vms_set_possible_owners_for_vms("fake_cluster", [])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_possible_owners_for_vms(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    vm_actions = [{"vm_id": "0", "possible_owners": ["0", "1"]},
                  {"vm_id": "1", "possible_owners": ["0", "1"]}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.vms_set_possible_owners_for_vms(
      "fake_cluster", vm_actions)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/placement",
      json=vm_actions,
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_snapshot_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vms_set_snapshot("fake_cluster", [])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vms_set_snapshot(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    vm_actions = [{"vm_id": "0", "name": "snapshot_0",
                   "description": "Snapshot 0"},
                  {"vm_id": "1", "name": "snapshot_1",
                   "description": "Snapshot 1"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.vms_set_snapshot(
      "fake_cluster", vm_actions)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/snapshot",
      json=vm_actions,
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vm_get_job_status_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vm_get_job_status([])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vm_get_job_status(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    task_list = [{"task_id": "0", "task_type": "vmm"},
                 {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.vm_get_job_status(task_list)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/task",
      json=task_list,
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vm_stop_job_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vm_stop_job([])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vm_stop_job(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    task_list = [{"task_id": "0", "task_type": "vmm"},
                 {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.put.return_value = resp

    resp_data = self.vmm_client.vm_stop_job(task_list)

    m_requests.put.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/task",
      json=task_list,
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_vms_delete_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.vms_delete("fake_cluster", [])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_vms_delete(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    vm_id_list = ["0", "1"]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.vms_delete("fake_cluster", vm_id_list)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/delete",
      json={"force_delete": False, "vm_ids": vm_id_list},
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_create_vm_template_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.create_vm_template("fake_cluster", "fake_vm_name",
                                         "host_id_0", "/fake/goldimages/path",
                                         "/fake/datastore/path",
                                         "fake_network")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_create_vm_template(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    resp = requests.Response()
    resp.status_code = 200
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.create_vm_template(
      "fake_cluster", "fake_vm_template", "host_id_0", "/fake/goldimages/path",
      "/fake/datastore/path", "fake_network")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/create_vm_template",
      json={
        "vm_name": "fake_vm_template",
        "vm_host_id": "host_id_0",
        "goldimage_disk_path": "/fake/goldimages/path",
        "vm_datastore_path": "/fake/datastore/path",
        "vmm_network_name": "fake_network",
        "vcpus": 1,
        "ram_mb": 1024
      },
      timeout=90.0)
    self.assertEqual(resp_data.status_code, 200)

  @mock.patch("curie.vmm_client.requests")
  def test_create_vm_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.create_vm("fake_cluster", "fake_vm_template",
                                [], "/fake/datastore/path", None)

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_create_vm(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.create_vm(
      "fake_cluster", "fake_vm_template",
      [{"vm_name": "fake_vm_0", "node_id": "0"},
       {"vm_name": "fake_vm_1", "node_id": "1"}],
      "/fake/datastore/path", None)

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/create",
      json={
        "vm_template_name": "fake_vm_template",
        "vm_host_map": [{"vm_name": "fake_vm_0", "node_id": "0"},
                        {"vm_name": "fake_vm_1", "node_id": "1"}],
        "vm_datastore_path": "/fake/datastore/path",
        "data_disks": None
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_clone_vm_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.clone_vm("fake_cluster", "fake_vm_id_0",
                               "fake_new_cloned_vm", "/fake/datastore/path")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_clone_vm(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.clone_vm(
      "fake_cluster", "fake_vm_id_0", "fake_new_cloned_vm",
      "/fake/datastore/path")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/clone",
      json={
        "base_vm_id": "fake_vm_id_0",
        "vm_name": "fake_new_cloned_vm",
        "vm_datastore_path": "/fake/datastore/path"},
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_upload_image_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.upload_image([], "/fake/goldimage/target/directory")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_upload_image(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    self.vmm_client.library_server_address = "fake_library_server"
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.upload_image(
      ["/fake/goldimage/path/0", "/fake/goldimage/path/1"],
      "/fake/goldimage/target/directory")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/image_upload",
      json={
        "vmm_library_server_share": "/fake/library/share/path",
        "vmm_library_server_user": "fake_username",
        "vmm_library_server_password": "fake_password",
        "vmm_library_server": "fake_library_server",
        "goldimage_disk_list": ["/fake/goldimage/path/0",
                                "/fake/goldimage/path/1"],
        "goldimage_target_dir": "/fake/goldimage/target/directory",
        "transfer_type": None
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_convert_to_template_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.convert_to_template("fake_cluster", "fake_template")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_convert_to_template(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.convert_to_template("fake_cluster",
                                                    "fake_template")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/convert_to_template",
      json={
        "vmm_library_server_share": "/fake/library/share/path",
        "template_name": "fake_template",
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_migrate_vm_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.migrate_vm("fake_cluster", [], "/fake/datastore/path")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_migrate_vm(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.migrate_vm(
      "fake_cluster",
      [{"vm_name": "fake_vm_0", "node_id": "0"},
       {"vm_name": "fake_vm_1", "node_id": "1"}],
      "/fake/datastore/path")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/migrate",
      json={
        "vm_host_map": [{"vm_name": "fake_vm_0", "node_id": "0"},
                        {"vm_name": "fake_vm_1", "node_id": "1"}],
        "vm_datastore_path": "/fake/datastore/path",
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_migrate_vm_datastore_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.migrate_vm_datastore("fake_cluster", [])

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_migrate_vm_datastore(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.migrate_vm_datastore(
      "fake_cluster",
      [{"vm_id": "fake_vm_id_0", "datastore_name": "fake_datastore_0"},
       {"vm_id": "fake_vm_id_1", "datastore_name": "fake_datastore_1"}])

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/vms/migrate_datastore",
      json={
        "vm_datastore_map": [
          {"vm_id": "fake_vm_id_0", "datastore_name": "fake_datastore_0"},
          {"vm_id": "fake_vm_id_1", "datastore_name": "fake_datastore_1"}],
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_clean_vmm_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.clean_vmm("fake_cluster", "fake_target_dir", "fake_vm_")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_clean_vmm_datastore(self, m_requests):
    self.vmm_client._session_id = 12345
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.clean_vmm("fake_cluster", "fake_target_dir", "fake_vm_")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/cluster/fake_cluster/cleanup",
      json={"vmm_library_server_share": "/fake/library/share/path",
            "target_dir": "fake_target_dir",
            "vm_name_prefix": "fake_vm_"},
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_clean_library_server_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.clean_library_server("/fake/target/directory",
                                           "fake_vm_")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_clean_library_server(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    self.vmm_client.library_server_share_path = "/fake/library/share/path"
    self.vmm_client.library_server_address = "fake_library_server"
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.clean_library_server("/fake/target/directory",
                                                     "fake_vm_")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/library_server_cleanup",
      json={
        "vmm_library_server_share": "/fake/library/share/path",
        "vmm_library_server_user": "fake_username",
        "vmm_library_server_password": "fake_password",
        "vmm_library_server": "fake_library_server",
        "target_dir": "/fake/target/directory",
        "vm_name_prefix": "fake_vm_"
      },
      timeout=90.0)
    self.assertEqual(resp_data, expected)

  @mock.patch("curie.vmm_client.requests")
  def test_update_library_not_logged_in(self, m_requests):
    self.assertFalse(self.vmm_client.is_logged_in)

    with self.assertRaises(AssertionError) as ar:
      self.vmm_client.update_library("/fake/goldimages/path")

    self.assertEqual(str(ar.exception), "Must be logged in")

  @mock.patch("curie.vmm_client.requests")
  def test_update_library(self, m_requests):
    self.vmm_client._session_id = 12345
    self.assertTrue(self.vmm_client.is_logged_in)
    expected = [{"task_id": "0", "task_type": "vmm"},
                {"task_id": "1", "task_type": "vmm"}]
    resp = requests.Response()
    resp.status_code = 200
    resp._content = json.dumps(expected)
    m_requests.post.return_value = resp

    resp_data = self.vmm_client.update_library("/fake/goldimages/path")

    m_requests.post.assert_called_once_with(
      "http://hyperv-ps-server:8888/v1/vmm/"
      "12345/update_library",
      json={"goldimage_disk_path": "/fake/goldimages/path"},
      timeout=90.0)
    self.assertEqual(resp_data, expected)
