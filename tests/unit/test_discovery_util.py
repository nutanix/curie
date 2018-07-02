#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
#
# pylint: disable=pointless-statement
import unittest
import uuid

import mock

from curie.curie_error_pb2 import CurieError
from curie.curie_server_state_pb2 import CurieSettings
from curie.discovery_util import DiscoveryUtil
from curie.exception import CurieException
from curie.ipmi_util import IpmiUtil
from curie.proto_util import proto_patch_encryption_support
from curie.util import CurieUtil
from curie.vmm_client import VmmClient
from curie.nutanix_rest_api_client import NutanixMetadata
from curie.nutanix_rest_api_client import NutanixRestApiClient


class TestCurieDiscoveryUtil(unittest.TestCase):

  def setUp(self):
    self.fq_disc_util_name = "curie.discovery_util.DiscoveryUtil"

    self._no_oob_node_proto = CurieSettings.ClusterNode()
    oob_info = self._no_oob_node_proto.node_out_of_band_management_info
    oob_info.interface_type = oob_info.kNone

    self._ipmi_node_proto = CurieSettings.ClusterNode()
    oob_info = self._ipmi_node_proto.node_out_of_band_management_info
    oob_info.interface_type = oob_info.kIpmi
    oob_info.ip_address = "1.2.3.4"
    oob_info.username = "username"
    oob_info.password = "password"

  def test_dispatch(self):
    cluster_pb = proto_patch_encryption_support(CurieSettings.Cluster)()
    mgmt_info = cluster_pb.cluster_management_server_info
    software_info = cluster_pb.cluster_software_info
    hyp_info = cluster_pb.cluster_hypervisor_info

    mgmt_info.prism_info.SetInParent()
    with self.assertRaises(CurieException):
      DiscoveryUtil.update_cluster_version_info(cluster_pb)
    software_info.nutanix_info.SetInParent()
    with self.assertRaises(CurieException):
      DiscoveryUtil.update_cluster_version_info(cluster_pb)
    hyp_info.ahv_info.SetInParent()

    fq_update_prism = (
      "%s._update_cluster_version_info_prism" % self.fq_disc_util_name)
    with mock.patch(fq_update_prism) as mock_prism:
      DiscoveryUtil.update_cluster_version_info(cluster_pb)
      mock_prism.assert_called_once_with(cluster_pb)

    mgmt_info.Clear()
    software_info.Clear()
    hyp_info.Clear()

    mgmt_info.vcenter_info.SetInParent()
    fq_update_vcenter = (
      "%s._update_cluster_version_info_vcenter" % self.fq_disc_util_name)
    with mock.patch(fq_update_vcenter) as mock_vcenter:
      DiscoveryUtil.update_cluster_version_info(cluster_pb)
      mock_vcenter.assert_called_once_with(cluster_pb)

    mgmt_info.Clear()

    mgmt_info.vmm_info.SetInParent()
    fq_update_vmm = (
      "%s._update_cluster_version_info_vmm" % self.fq_disc_util_name)
    with mock.patch(fq_update_vmm) as mock_vmm:
      DiscoveryUtil.update_cluster_version_info(cluster_pb)
      mock_vmm.assert_called_once_with(cluster_pb)

    mgmt_info.Clear()

    with self.assertRaises(CurieException):
      DiscoveryUtil.update_cluster_version_info(cluster_pb)

    fq_update_vip = (
      "%s.update_cluster_virtual_ip" % self.fq_disc_util_name)
    with mock.patch(fq_update_vip) as mock_vip:
      DiscoveryUtil.update_cluster_virtual_ip(cluster_pb)
      mock_vip.assert_called_once_with(cluster_pb)

  @mock.patch.object(IpmiUtil, "get_chassis_status")
  @mock.patch.object(CurieUtil, "ping_ip")
  def test_validate_oob_config(self, mock_ping, mock_status):
    proto_patch_encryption_support(CurieSettings)

    cluster_pb = CurieSettings.Cluster()
    for ii in xrange(4):
      node_pb = cluster_pb.cluster_nodes.add()
      node_pb.CopyFrom(self._no_oob_node_proto)
      node_pb.id = str(ii)

    DiscoveryUtil.validate_oob_config(cluster_pb)
    self.assertEqual(mock_ping.call_count, 0)
    self.assertEqual(mock_status.call_count, 0)

    cluster_pb = CurieSettings.Cluster()
    for ii in xrange(4):
      node_pb = cluster_pb.cluster_nodes.add()
      node_pb.CopyFrom(self._ipmi_node_proto)
      node_pb.id = str(ii)

    mock_ping.return_value = True
    DiscoveryUtil.validate_oob_config(cluster_pb)
    self.assertEqual(mock_ping.call_count, len(cluster_pb.cluster_nodes))
    self.assertEqual(mock_status.call_count, len(cluster_pb.cluster_nodes))

    mock_ping.reset_mock()
    mock_status.reset_mock()

    mock_ping.side_effect = [True, False, True, True]
    with self.assertRaises(CurieException):
      DiscoveryUtil.validate_oob_config(cluster_pb)
    # We expect that the first ping succeeds and then the second fails. There
    # should be an exception after the second ping attempt. If ping fails, the
    # expectations is then that the chassis status won't be called.
    self.assertEqual(mock_ping.call_count, 2)
    self.assertEqual(mock_status.call_count, 1)

    mock_ping.reset_mock()
    mock_status.reset_mock()

    mock_ping.return_value = True
    mock_ping.side_effect = None
    mock_status.side_effect = [
      {},
      CurieException(CurieError.kOobAuthenticationError, "AuthError"),
      {},
      CurieException(CurieError.kInternalError, "SomeOtherError")
    ]
    with self.assertRaises(CurieException):
      DiscoveryUtil.validate_oob_config(cluster_pb)
    self.assertEqual(mock_ping.call_count, 2)
    self.assertEqual(mock_status.call_count, 2)

  def test__get_hyp_version_for_host(self):
    host = {"hypervisorFullName": "Nutanix 20170726.42",
            DiscoveryUtil.CE_HOST_ATTR_KEY:
              DiscoveryUtil.CE_HOST_ATTR_VAL
            }
    self.assertEqual(
      DiscoveryUtil._get_hyp_version_for_host(host),
      "Nutanix CE 20170726.42")

    host["hypervisorFullName"] = "20170726.42"

    self.assertEqual(
      DiscoveryUtil._get_hyp_version_for_host(host),
      "CE 20170726.42")

    host["hypervisorFullName"] = "20170726.42"

    host[DiscoveryUtil.CE_HOST_ATTR_KEY] = ""
    self.assertEqual(
      DiscoveryUtil._get_hyp_version_for_host(host),
      "20170726.42")
    host["hypervisorFullName"] = "Nutanix %s" % host["hypervisorFullName"]
    self.assertEqual(
      DiscoveryUtil._get_hyp_version_for_host(host),
      "Nutanix 20170726.42")

  @mock.patch("curie.discovery_util.NutanixRestApiClient")
  @mock.patch("curie.discovery_util.VmmClient")
  def test__update_cluster_version_info_vmm(self, m_VmmClient, n_NtnxApiCli):
    cluster_pb = proto_patch_encryption_support(CurieSettings.Cluster)()
    mgmt_info = cluster_pb.cluster_management_server_info
    mgmt_info.vmm_info.SetInParent()
    software_info = cluster_pb.cluster_software_info
    software_info.nutanix_info.SetInParent()

    m_vmm_cli = m_VmmClient.return_value
    m_vmm_cli.get_nodes.return_value = [
      {
        "ips": ["1.2.3.4"],
        "fqdn": "node1.somewhere.blah",
        "name": "node1.somewhere.blah",
        "id": "157bbf6f-010b-41c6-938b-2a3dc3fae7ca",
        "bmc_port": "623",
        "bmc_address": "1.2.3.5",
        "overall_state": "OK",
        "state": "Responding",
        "version": "10.0.14393.351"
      }, {
        "ips": ["2.3.4.5"],
        "fqdn": "node2.somewhere.blah",
        "name": "node2.somewhere.blah",
        "id": "4657f9f7-4027-4fc4-bc90-04c16188438d",
        "bmc_port": "623",
        "bmc_address": "2.3.4.6",
        "overall_state": "OK",
        "state": "Responding",
        "version": "10.0.14393.351"
      }, {
        "ips": ["3.4.5.6"],
        "fqdn": "node3.somewhere.blah",
        "name": "node3.somewhere.blah",
        "id": "a4b928cf-2d16-43a1-9139-f98d4cbd55d6",
        "bmc_port": "623",
        "bmc_address": "3.4.5.7",
        "overall_state": "OK",
        "state": "Responding",
        "version": "10.0.14393.351"
      }
    ]
    m_ntnx_api = n_NtnxApiCli.return_value
    cluster_inc_id = 12345
    cluster_uuid = str(uuid.uuid4())
    cluster_version = "el6-release-euphrates-5.0.2-stable-9d20638eb2ba1d3f84f213d5976fbcd412630c6d"
    m_ntnx_api.get_nutanix_metadata.return_value = NutanixMetadata(
      version=cluster_version, cluster_uuid=cluster_uuid,
      cluster_incarnation_id=cluster_inc_id)

    DiscoveryUtil.update_cluster_version_info(cluster_pb)

    self.assertEqual(cluster_pb.cluster_software_info.nutanix_info.version,
                     "5.0.2")
    self.assertEqual(
      cluster_pb.cluster_management_server_info.vmm_info.vmm_version,
      "Unknown")
    self.assertEqual(cluster_pb.cluster_hypervisor_info.hyperv_info.version,
                     ["10.0.14393.351", "10.0.14393.351", "10.0.14393.351"])

