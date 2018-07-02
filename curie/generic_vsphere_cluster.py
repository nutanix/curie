#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#

import gflags

from curie.vsphere_cluster import VsphereCluster

FLAGS = gflags.FLAGS


class GenericVsphereCluster(VsphereCluster):
  def __init__(self, cluster_metadata):
    super(GenericVsphereCluster, self).__init__(cluster_metadata)
    # A map for keeping track of the snapshot count for a set of VMs.
    # Key = vm_id_set, Value = snapshot count
    self.__snapshot_count = {}

  # TODO (jklein): Argument mismatch with method in parent class.
  def snapshot_vms(self, vms, tag=None, max_parallel_tasks=None):
    """
    For each VM with name 'vm_names[xx]' on the cluster 'vim_cluster', creates
    a snapshot with snapshot name 'snapshot_names[xx]' and optional description
    'snapshot_descriptions[xx]'.

    Args
      vms list of CurieVMs: List of VMs to create snapshots for.
      snapshot_names list of strings: Names for snapshot which must be the same
        length as 'vms'.
      snapshot_descriptions List of strings: List of escriptions for each
        snapshot corresponding to 'vms' and 'snapshot_names'. If provided it
        must be the same length as 'vms'.
      max_parallel_tasks int: The number of VMs to power on in parallel. The
        default value is FLAGS.vsphere_vcenter_max_parallel_tasks.
    """
    if max_parallel_tasks is None:
      max_parallel_tasks = FLAGS.vsphere_vcenter_max_parallel_tasks
    snapshot_count = self.__get_snapshot_count(vms)
    snapshot_name = "%s_snap_%d" % (tag, snapshot_count)
    with self._open_vcenter_connection() as vcenter:
      vim_cluster = self._lookup_vim_cluster(vcenter)
      vcenter.snapshot_vms(vim_cluster,
                           [vm.vm_name() for vm in vms],
                           [snapshot_name for vm in vms],
                           snapshot_descriptions=(),
                           max_parallel_tasks=max_parallel_tasks)
    self.__set_snapshot_count(vms, snapshot_count + 1)

  def __set_snapshot_count(self, vms, count):
    vm_id_set = self._get_vm_id_set(vms)
    self.__snapshot_count[vm_id_set] = count

  def __get_snapshot_count(self, vms):
    vm_id_set = self._get_vm_id_set(vms)
    return self.__snapshot_count.get(vm_id_set, 0)
