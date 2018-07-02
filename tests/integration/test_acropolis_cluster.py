#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import logging
import unittest

import gflags

from curie.acropolis_cluster import AcropolisCluster
from curie.acropolis_vm import AcropolisVm
from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.testing import environment
from curie.testing import util

log = logging.getLogger(__name__)


class TestAcropolisCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    assert isinstance(self.cluster, AcropolisCluster), \
           "This test must run on an AHV cluster"
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)

  def tearDown(self):
    self.scenario.cluster.cleanup()

  def test_create_vm(self):
    expected_goldimage_name = NameUtil.goldimage_vmdisk_name(
      "ubuntu1604-x86_64", "os")
    vm_name = NameUtil.goldimage_vm_name(self.scenario, "ubuntu1604")

    datastore_name = self.cluster._mgmt_server_info.prism_container_id
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
    assert isinstance(vms[0], AcropolisVm), ("VM is %s instead of AcropolisVm"
                                             % str(type(vms[0])))

    found_images = self.__get_image_names()
    assert expected_goldimage_name in found_images, \
      "Goldimage disk wasn't found in image service."

    self.scenario.cluster.cleanup()

    vms = self.cluster.find_vms([vm_name])
    found_vms = [vm.vm_name() for vm in vms if vm is not None]
    assert vm_name not in found_vms, "VM %s was still found after cleanup"
    found_images = self.__get_image_names()
    assert expected_goldimage_name not in found_images, \
      "Goldimage disk wasn't found in image service."

  def __get_image_names(self):
    images = self.scenario.cluster._prism_client.images_get()
    image_names = [entity["name"] for entity in images["entities"]]
    return image_names

