#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import threading
import time

from curie.acropolis_cluster import AcropolisCluster
from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.scenario import Scenario, Phase, Status
from curie.steps import check, cluster, vm_group
from curie.vm_group import VMGroup
from curie.vsphere_cluster import VsphereCluster

log = logging.getLogger(__name__)


class ClusterCheck(Scenario):
  def __init__(self, cluster_obj, **kwargs):
    super(ClusterCheck, self).__init__(
      cluster=cluster_obj, name="cluster_check", display_name="Cluster Check",
      **kwargs)
    self.estimated_runtime = 600
    nodes = cluster_obj.nodes()
    # Pre-check VMs are default Ubuntu VMs, one per node.
    vmgroup = VMGroup(self, "Cluster Check", template="ubuntu1604",
                      nodes=nodes, count_per_node=1)
    self.vm_groups = {"Cluster Check": vmgroup}

    include_vsphere_checks = isinstance(self.cluster, VsphereCluster)
    include_nutanix_checks = (isinstance(self.cluster, AcropolisCluster) or
                              isinstance(self.cluster, NutanixClusterDPMixin))

    # Cluster management.
    if include_vsphere_checks:
      self.add_step(check.VCenterConnectivity(self), Phase.RUN)
    if include_nutanix_checks:
      self.add_step(check.PrismConnectivity(self), Phase.RUN)
      for node in nodes:
        self.add_step(check.PrismHostInSameCluster(
          self, node_index=node.node_index()), Phase.RUN)

    # Node stuff.
    for node in nodes:
      self.add_step(check.HypervisorConnectivity(
        self, node_index=node.node_index()), Phase.RUN)
    if include_vsphere_checks:
      vcenter_info = self.cluster.metadata().cluster_management_server_info.vcenter_info
      datastore_name = vcenter_info.vcenter_datastore_name
      for node in nodes:
        self.add_step(check.VSphereDatastoreVisible(
          self, datastore_name=datastore_name, node_index=node.node_index()),
          Phase.RUN)
        if include_nutanix_checks:
          self.add_step(check.PrismDatastoreVisible(
            self, datastore_name=datastore_name, node_index=node.node_index()),
            Phase.RUN)

    # Power management, if applicable.
    for node_index, node in enumerate(nodes):
      if node.power_management_is_configured():
        self.add_step(check.NodePowerManagementConnectivity(self, node_index),
                      Phase.RUN)

    # Worker VM deployment.
    self.add_step(cluster.CleanUp(self), Phase.RUN)
    self.add_step(vm_group.CloneFromTemplate(
      self, vm_group_name="Cluster Check"), Phase.RUN)
    self.add_step(vm_group.PowerOn(self, vm_group_name="Cluster Check"),
                  Phase.RUN)
    self.add_step(cluster.CleanUp(self), Phase.RUN)

    self._expand_meta_steps()

  def start(self):
    """
    Begin running the cluster check.

    This method returns immediately.

    Raises:
      RuntimeError: If the check has already been started, or no cluster is
        set.
    """
    with self._lock:
      if not self._status.is_new():
        raise RuntimeError("%s: Can not start because it has already been "
                           "started" % self)
      self._status = Status.EXECUTING
      self._start_time_secs = time.time()
    self._threads.append(threading.Thread(target=self._step_loop,
                                          name="step_loop"))
    for thread in self._threads:
      thread.start()
    log.info("%s has started", self)
