#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import json
import unittest

import mock
from requests.exceptions import ConnectionError

from curie.exception import CurieException
from curie.nutanix_rest_api_client import NutanixMetadata
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.nutanix_rest_api_client import PrismAPIVersions
from curie.nutanix_rest_api_client import REST_API_MAX_RETRIES


class TestPrismAPIVersions(unittest.TestCase):
  VALID_AOS_VERSION = ("el7.3-release-euphrates-5.5-stable-"
                       "cbd9acfdebb3c7fd70ff2b4c4061515daadb50b1")
  VALID_AOS_VERSION_OUTDATED = (
    "el6-release-congo-3.0-stable-ee1b1aab1ac3a630694d9fd45ac1c6b91c1d3dd5")
  VALID_AOS_SHORT_VERSION = "5.5"
  VALID_AOS_SHORT_VERSION_OUTDATED = "3.0"

  VALID_AOS_CE_VERSION = ("el6-release-ce-2017.02.23-stable-"
                          "4d4e6fd77654bcfee2893f571830a031cb7517d4")
  VALID_AOS_CE_VERSION_OUTDATED = ("el6-release-ce-2015.02.23-stable-"
                                   "4d4e6fd77654bcfee2893f571830a031cb7517d4")
  VALID_AOS_CE_SHORT_VERSION = "2017.02.23"
  VALID_AOS_CE_SHORT_VERSION_OUTDATED = "2012.02.23"

  def test_prism_api_version(self):
    self.assertTrue(PrismAPIVersions.is_applicable(PrismAPIVersions.PRISM_V1,
                    "ANY_VERSION_HERE"))

  def test_valid_aos_version(self):
    valid = PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1,
                                           self.VALID_AOS_VERSION)
    self.assertTrue(valid)

    valid = PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1,
                                           self.VALID_AOS_SHORT_VERSION)
    self.assertTrue(valid)

  def test_valid_ce_version(self):
    valid = PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1,
                                           self.VALID_AOS_CE_VERSION)
    self.assertTrue(valid)

  def test_genesis_version_parse_failure(self):
    valid = PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1,
                                           "releasece-201702.23-stable")
    self.assertFalse(valid)

  def test_ce_version_too_low(self):
    valid = PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1,
                                           self.VALID_AOS_CE_VERSION_OUTDATED)
    self.assertFalse(valid)

  def test_aos_version_too_low(self):
    valid = PrismAPIVersions.is_applicable(
      PrismAPIVersions.GENESIS_V1, self.VALID_AOS_VERSION_OUTDATED)
    self.assertFalse(valid)

  def test_invalid_api(self):
    self.assertFalse(PrismAPIVersions.is_applicable("BadAPI", "DoesntMatter"))

  def test_get_aos_short_version(self):
    for version_string in [self.VALID_AOS_VERSION,
                           self.VALID_AOS_SHORT_VERSION]:
      short_version = PrismAPIVersions.get_aos_short_version(version_string)
      self.assertEqual(short_version, self.VALID_AOS_SHORT_VERSION)

    short_version = PrismAPIVersions.get_aos_short_version("Invalid Version")
    self.assertEqual(short_version, "Unknown")


@mock.patch.multiple("curie.nutanix_rest_api_client.TimeoutSession",
                     post=mock.DEFAULT, get=mock.DEFAULT)
@mock.patch.multiple("curie.util.CurieUtil", ping_ip=mock.DEFAULT)
class TestNutanixRestApiClient(unittest.TestCase):

  _DUMMY_RPC_RET = {
    "value": json.dumps({
      ".return": {
        "lockdown_mode": False, "state": "OK", "retry": False, "svms": []}
    })
  }

  _DUMMY_RPC_PROXY_ERROR_RET = {
    "value": json.dumps({
      ".warning": "Dummy RPC proxy warning",
      ".error": "Dummy RPC proxy error",
      ".exception": "Dummy RPC proxy exception",
    })
  }

  # TODO (jklein): If any tests use it, fill in actual dummy data.
  _DUMMY_CLUSTERS_GET_RET = {"entities": []}

  # Dummy host IPs
  HOSTS = ["1.1.1.1", "1.1.1.2", "1.1.1.3"]

  # Dummy host URL prefixes.
  HOST_URL_PREFIXES = ["https://%s:9440/" % h for h in HOSTS]

  # Currently hard-coded max retries for genesis RPCs.
  GENESIS_MAX_RETRIES = 5

  @classmethod
  def _make_conn_error_for_host(cls, host):
    return ConnectionError("Mock Conn Error for '%s'" % host)

  def setUp(self):
    # Patch get_nutanix_metadata to return a version that is compatible with
    # Genesis RPCs. We don't care about the cluster UUID/incarnation ID.
    NutanixRestApiClient.get_nutanix_metadata = mock.Mock(
      return_value=NutanixMetadata(
        version="el6-release-danube-4.6-stable-"
        "ee1b1aab1ac3a630694d9fd45ac1c6b91c1d3dd5"))

    patch_time = mock.patch("curie.nutanix_rest_api_client.time")
    self.mock_time = patch_time.start()
    self.addCleanup(patch_time.stop)
    self.mock_time.sleep.return_value = 0

  def _verify_url_in_call_args(self, expected_list, mock_method):
    count = len(expected_list)
    # Verify calls cycled through available hosts.
    for ii, call in enumerate(mock_method.call_args_list):
      self.assert_(call[0][0].startswith(expected_list[ii % count]))

  @mock.patch("requests.Response")
  def test_retry_success_genesis(self, resp, **kwargs):
    post = kwargs["post"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    resp.json.return_value = self._DUMMY_RPC_RET
    resp.status_code = 200
    post.side_effect = [
      self._make_conn_error_for_host(self.HOSTS[0])] * 3 + [resp]

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")

    # Call genesis RPC method.
    cli.genesis_cluster_status()

    # Verify calls cycled through available hosts.
    self._verify_url_in_call_args([self.HOST_URL_PREFIXES[0], ], post)

  @mock.patch("requests.Response")
  def test_retry_on_proxy_error_genesis_success(self, resp, **kwargs):
    post = kwargs["post"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    resp.json.side_effect = (
      [self._DUMMY_RPC_PROXY_ERROR_RET] * 3 + [self._DUMMY_RPC_RET])
    resp.status_code = 200
    post.side_effect = [resp] * 4

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")

    # Call genesis RPC method.
    cli.genesis_cluster_status()

    # Verify calls cycled through available hosts.
    self._verify_url_in_call_args([self.HOST_URL_PREFIXES[0], ], post)

  def test_retry_failure_genesis(self, **kwargs):
    post = kwargs["post"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    side_effect = [self._make_conn_error_for_host(
        self.HOSTS[0])] * self.GENESIS_MAX_RETRIES
    side_effect.append(ConnectionError("Last error, should trigger exception"))
    post.side_effect = side_effect

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")

    # Call standard REST API method.
    with self.assertRaises(CurieException):
      ret = cli.genesis_cluster_status()

    # Verify calls cycled through available hosts.
    self._verify_url_in_call_args([self.HOST_URL_PREFIXES[0], ], post)

  @mock.patch("requests.Response")
  def test_retry_on_proxy_error_genesis_failure(self, resp, **kwargs):
    post = kwargs["post"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    resp.json.return_value = self._DUMMY_RPC_PROXY_ERROR_RET
    resp.status_code = 200
    post.side_effect = [resp] * (self.GENESIS_MAX_RETRIES + 1)

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")

    # Call genesis RPC method.
    with self.assertRaises(CurieException):
      cli.genesis_cluster_status()

  @mock.patch("requests.Response", create=True)
  def test_retry_success(self, resp, **kwargs):
    get = kwargs["get"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    resp.status_code = 200
    resp.json.return_value = self._DUMMY_CLUSTERS_GET_RET

    get.side_effect = [
        self._make_conn_error_for_host(self.HOSTS[0])] * 2 + [resp]

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    # Call standard REST API method.
    cli.clusters_get()

    # Verify calls cycled through available hosts.
    self._verify_url_in_call_args([self.HOST_URL_PREFIXES[0], ], get)

  def test_retry_failure(self, **kwargs):
    get = kwargs["get"]
    ping_ip = kwargs["ping_ip"]
    ping_ip.return_value = True
    side_effect = [
        self._make_conn_error_for_host(self.HOSTS[0])] * REST_API_MAX_RETRIES
    side_effect.append(ConnectionError("Last error, should raise exception"))
    get.side_effect = side_effect

    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    # Call standard REST API method.
    with self.assertRaises(CurieException):
      cli.clusters_get()

    # Verify calls cycled through available hosts.
    self._verify_url_in_call_args([self.HOST_URL_PREFIXES[0], ], get)

  def test_vms_get_invalid_both_vm_ip_and_vm_name(self, **_):
    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    with self.assertRaises(CurieException):
      cli.vms_get(vm_ip="123.45.67.89", vm_name="fake_vm_name")

  def test_vms_get_no_retries(self, **_):
    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    expected = {"entities": [{"uuid": "uuid_0"},
                             {"uuid": "uuid_1"},
                             ]}
    with mock.patch.object(cli, "_NutanixRestApiClient__get") as mock_cli_get:
      mock_cli_get.return_value = expected
      self.assertDictEqual(expected, cli.vms_get(vm_ip="123.45.67.89"))

  def test_vms_get_drop_duplicates_default(self, **_):
    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    expected = {"entities": [{"uuid": "uuid_0"},
                             {"uuid": "uuid_0"},
                             ]}
    with mock.patch.object(cli, "_NutanixRestApiClient__get") as mock_cli_get:
      mock_cli_get.return_value = expected
      response = cli.vms_get(vm_ip="123.45.67.89")
    self.assertEqual(response,
                     {"entities": [{"uuid": "uuid_0"}]})

  def test_vms_get_drop_duplicates_false(self, **_):
    cli = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    expected = {"entities": [{"uuid": "uuid_0"},
                             {"uuid": "uuid_0"},
                             ]}
    with mock.patch.object(cli, "_NutanixRestApiClient__get") as mock_cli_get:
      mock_cli_get.return_value = expected
      response = cli.vms_get(vm_ip="123.45.67.89", drop_duplicates=False)
    self.assertEqual(response, expected)

  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_nutanix_metadata")
  def test_get_pc_uuid_header_under_5_5(self,
                                        get_nutanix_metadata, **kwargs):

    get_nutanix_metadata.return_value = NutanixMetadata(
        version="el6-release-danube-4.6-stable-"
        "ee1b1aab1ac3a630694d9fd45ac1c6b91c1d3dd5")
    client = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    header = client._get_pc_uuid_header()
    self.assertEqual(header, {})

  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_nutanix_metadata")
  def test_get_pc_uuid_header_over_5_8(self,
                                       get_nutanix_metadata, **kwargs):
    get_nutanix_metadata.return_value = NutanixMetadata(
      version="el7.3-release-euphrates-5.8-stable-"
              "4c26d1af153833c54b67536fb0a4044e6e8c1b07")
    client = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    header = client._get_pc_uuid_header()
    self.assertEqual(header, {})

  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_pc_uuid")
  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_nutanix_metadata")
  def test_get_pc_uuid_header_5_5(self, get_nutanix_metadata, get_pc_uuid,
                                  **kwargs):
    get_nutanix_metadata.return_value = NutanixMetadata(
      version="el7.3-release-euphrates-5.5.0.5-stable-"
              "0f7655edaa04231239690f59cfb9fce39377ef89")
    get_pc_uuid.return_value = "UUID-UUID-UUID-UUID-UUID"
    client = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    header = client._get_pc_uuid_header()
    self.assertEqual(header, {"X-NTNX-PC-UUID": "UUID-UUID-UUID-UUID-UUID"})

  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_pc_uuid")
  @mock.patch(
    "curie.nutanix_rest_api_client.NutanixRestApiClient.get_nutanix_metadata")
  def test_get_pc_uuid_header_5_5_unsupported_version(self,
                                                      get_nutanix_metadata,
                                                      get_pc_uuid,
                                                      **kwargs):
    get_nutanix_metadata.return_value = NutanixMetadata(
      version="el7.3-release-euphrates-5.5-stable-"
              "cbd9acfdebb3c7fd70ff2b4c4061515daadb50b1")
    get_pc_uuid.return_value = "UUID-UUID-UUID-UUID-UUID"
    # Get version
    client = NutanixRestApiClient(self.HOSTS[0], "user", "password")
    with self.assertRaises(CurieException) as ar:
      client._get_pc_uuid_header()
      self.assertEqual(str(ar.exception), (
        "Cluster is running a 5.5 version which is incompatible with X-Ray "
        "when connected to Prism Central. Please upgrade AOS to 5.5.0.2 or "
        "newer, or disconnect Prism Central."))

