#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
import logging

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.log import CHECK
from curie.node_util import NodeUtil
from curie.nutanix_rest_api_client import NutanixRestApiClient

log = logging.getLogger(__name__)


class NutanixNodeUtil(NodeUtil):

  @classmethod
  def _use_handler(cls, node):
    """Returns True if 'node' should use this class as its handler."""
    # pylint: disable=arguments-differ
    software_info = node.cluster().metadata().cluster_software_info
    return software_info.HasField("nutanix_info")

  def __init__(self, node, rest_api_timeout_secs=60):
    # Full cluster metadata proto.
    self.__cluster_metadata = node.cluster().metadata()

    # Node object for which this util is used.
    self.__node = node

    software_info = self.__cluster_metadata.cluster_software_info
    CHECK(software_info.HasField("nutanix_info"))

    # NutanixRestApiClient instance to use.
    self.__api = NutanixRestApiClient.from_proto(
      software_info.nutanix_info, timeout_secs=rest_api_timeout_secs)

  def is_ready(self):
    """
    See 'NodeUtil.is_ready' documentation for further details.

    Confirms node is ready by requesting node's health info from Prism via
    SVM corresponding to the node.
    """
    try:
      return self.__is_ready()
    except CurieException:
      log.warning("Exception checking if node %s is ready returning False",
                  self.__node.node_id(), exc_info=True)
      return False

  def __is_ready(self):
    """
    See public method 'is_ready' documentation for further details.
    """
    host = self.__api.hosts_get(
      host_ip=self.__node.node_ip(), projection="HEALTH")
    state = host.get("state").strip().upper()
    log.debug("Host '%s' in state: %s", self.__node.node_id(), state)
    if state != "NORMAL":
      log.warning("Host '%s' reports abnormal health state: %s",
                  self.__node.node_id(), state)
      return False
    ret = self.__api.vms_get(cvm_only=True)
    if not ret["entities"]:
      raise CurieException(CurieError.kClusterApiError,
                            "No CVM entities found")
    cvm_ip = host["serviceVMExternalIP"]
    found_ip = False
    for cvm_dto in ret["entities"]:
      for ip_address in cvm_dto["ipAddresses"]:
        if ip_address.startswith(cvm_ip):
          found_ip = True
          log.debug("found entity for cvm: %s", cvm_ip)
          break
      if found_ip:
        break
    else:
      raise CurieException(CurieError.kClusterApiError,
                            "No entity for CVM: %s" % cvm_ip)

    # Pylint doesn't understand for/else raising if cvm_dto is undefined.
    # pylint: disable=undefined-loop-variable
    log.info("CVM %s power state %s", cvm_ip, cvm_dto["powerState"])
    if cvm_dto["powerState"].strip().lower() != "on":
      return False
    try:
      status = self.__api.genesis_node_services_status()
      # Check overall node status. If it's determined we only care about a
      # subset of services, we can alternatively iterate through the list
      # 'services' to check PIDs and error messages per-service.
      # State may be a CSV list such as "Up, Zeus Leader".
      return "up" in [val.strip() for
                      val in status["state"].strip().lower().split(",")]
    except Exception:
      # If Prism is not up and running, the above will raise an exception
      # when failing to successfully connect and issue the RPC.
      log.warning("Failed to query Genesis on node '%s'",
                  self.__node.node_id(), exc_info=True)
      return False
