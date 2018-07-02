#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#

from curie.nutanix_cluster_dp_mixin import NutanixClusterDPMixin
from curie.vsphere_cluster import VsphereCluster


class NutanixVsphereCluster(NutanixClusterDPMixin, VsphereCluster):
  def __init__(self, cluster_metadata):
    super(NutanixVsphereCluster, self).__init__(cluster_metadata)
