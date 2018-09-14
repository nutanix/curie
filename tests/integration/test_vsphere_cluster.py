#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import unittest

import gflags

from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.testing import environment
from curie.testing import util
from curie.vsphere_cluster import VsphereCluster
from curie.vsphere_vm import VsphereVm

log = logging.getLogger(__name__)


class TestVsphereCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.cluster.update_metadata(False)
    assert isinstance(self.cluster, VsphereCluster), \
           "This test must run on a vCenter cluster"
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

  def tearDown(self):
    self.scenario.cluster.cleanup()

  def test_create_vm(self):
    vm_name = NameUtil.goldimage_vm_name(self.scenario, "ubuntu1604")
    datastore_name = self.cluster._vcenter_info.vcenter_datastore_name
    node_id = self.cluster.nodes()[0].node_id()
    vm = self.cluster.create_vm(self.scenario.goldimages_directory,
                                "ubuntu1604", vm_name,
                                vcpus=1, ram_mb=1024, node_id=node_id,
                                datastore_name=datastore_name,
                                data_disks=[10, 20, 30])
    vms = self.cluster.find_vms([vm_name])
    assert len(vms) == 1, "Too many VMs found for %s" % vm_name
    assert vms[0].vm_name() == vm_name, "VM found %s wasn't %s" % (
      vms[0].vm_name(), vm_name)
    assert isinstance(vms[0], VsphereVm), ("VM is %s instead of VsphereVm"
                                           % str(type(vms[0])))
    paths = self._get_datastore_paths("__curie_goldimage*")
    assert len(paths) > 0, "Goldimage paths were not found in datastore. %s" % paths
    self.scenario.cluster.cleanup()
    paths = self._get_datastore_paths("__curie_goldimage*")
    assert len(paths) == 0, \
      "Goldimage paths were found in datastore after cleanup."

  def _get_datastore_paths(self, pattern):
    with self.scenario.cluster._open_vcenter_connection() as vcenter:
      vim_datacenter, vim_cluster, vim_datastore = \
        self.scenario.cluster._lookup_vim_objects(vcenter)
      return vcenter.find_datastore_paths(pattern, vim_datastore)
