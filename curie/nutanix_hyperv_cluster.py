#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#

from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.hyperv_cluster import HyperVCluster


class NutanixHypervCluster(NutanixClusterDPMixin, HyperVCluster):
  def __init__(self, cluster_metadata):
    super(NutanixHypervCluster, self).__init__(cluster_metadata)
