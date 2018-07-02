#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#

import logging

from curie.nutanix_rest_api_client import NutanixRestApiClient

log = logging.getLogger(__name__)


class NutanixClusterDPMixin(object):
  def __init__(self, cluster_metadata):
    super(NutanixClusterDPMixin, self).__init__(cluster_metadata)
    # A map for keeping track of the snapshot count for a set of VMs.
    # Key = pd_name, Value = snapshot count
    self.__snapshot_count = {}

    # A map for keeping track of pd_name to a set of VMs.
    # Key = vm_id_set, Value = pd_name
    self.__pd_name_map = {}

    # Prism client for issuing REST API calls.
    self._prism_client = NutanixRestApiClient.from_proto(
      cluster_metadata.cluster_software_info.nutanix_info)

  def create_protection_domain(self, pd_name):
    """
    Create a protection domain.
    """
    self._prism_client.protection_domains_create(pd_name)

  def protect_vms_protection_domain(self, pd_name, vms):
    """
    Protect the VMs with the protection domain.
    """
    self._prism_client.protection_domains_protect_vms(
      pd_name, [vm.vm_name() for vm in vms])

  def snapshot_pd(self, pd_name):
    self._prism_client.protection_domains_oob_schedules(pd_name)

  def snapshot_vms(self, vms, tag=None):
    # Use the snapshot tag as the root of the pd_name to help tag the snapshots
    # in case additional inspection is required on the cluster.
    vm_id_set = self._get_vm_id_set(vms)
    pd_name = self.__pd_name_map.get(vm_id_set, None)
    if not pd_name:
      # Protection domain does not exist for the vms in the set.
      # Even if there is one VM difference, a new PD is created.
      pd_name = "%s-%d" % (tag, len(self.__pd_name_map))
      log.info("Creating new PD: %s", pd_name)
      self.__pd_name_map[vm_id_set] = pd_name
      self.create_protection_domain(pd_name)
      self.protect_vms_protection_domain(pd_name, vms)
    snapshot_num = self.__snapshot_count.get(pd_name, 0)
    self.snapshot_pd(pd_name)
    self.__snapshot_count[pd_name] = snapshot_num + 1
