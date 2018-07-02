#
#  Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import os
import random
import string
import time
import unittest

import gflags

from curie.exception import CurieTestException
from curie.name_util import NameUtil
from curie.scenario import Scenario
from curie.test import steps
from curie.test.vm_group import VMGroup
from curie.test.workload import Workload
from curie.testing import environment, util


class TestIntegrationStepsWorkload(unittest.TestCase):
  _multiprocess_can_split_ = True

  def setUp(self):
    self.cluster = util.cluster_from_json(gflags.FLAGS.cluster_config_path)
    self.group_name = "".join(
      [random.choice(string.printable)
       for _ in xrange(VMGroup.MAX_NAME_LENGTH)])
    self.workload_name = "".join(
      [random.choice(string.printable) for _ in xrange(40)])
    self.scenario = Scenario(
      cluster=self.cluster,
      source_directory=os.path.join(environment.resource_dir(), "fio"),
      output_directory=environment.test_output_dir(self),
      goldimages_directory=gflags.FLAGS.curie_vmdk_goldimages_dir)
    self.valid_fio_path = "oltp.fio"
    self.invalid_fio_path = "not-a-file.bogus"

  def tearDown(self):
    test_vms, _ = NameUtil.filter_test_vms(self.cluster.vms(),
                                           [self.scenario.id])
    self.cluster.power_off_vms(test_vms)
    self.cluster.delete_vms(test_vms)

  def test_prefill(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.workload.PrefillStart(self.scenario, self.workload_name)()

  def test_prefill_async(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.workload.PrefillStart(self.scenario, self.workload_name,
                                async=True)()
    steps.workload.PrefillWait(self.scenario, self.workload_name)()

  def test_prefill_invalid_fio_path(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.invalid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    with self.assertRaises(CurieTestException):
      steps.workload.PrefillStart(self.scenario, self.workload_name)()

  def test_PrefillRun_default(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.workload.PrefillRun(self.scenario, self.workload_name)()

  def test_PrefillRun_invalid_fio_path(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.invalid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    with self.assertRaises(CurieTestException):
      steps.workload.PrefillRun(self.scenario, self.workload_name)()

  def test_start_default(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.workload.Start(self.scenario, self.workload_name, 30)()

  def test_start_async(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    steps.workload.Start(self.scenario, self.workload_name, 30, async=True)()
    steps.workload.Wait(self.scenario, self.workload_name)()

  def test_invalid_fio_path(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.invalid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    with self.assertRaises(CurieTestException):
      steps.workload.Start(self.scenario, self.workload_name, 30)()

  def test_wait_after_finish(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    duration_secs = 30
    steps.workload.Start(self.scenario, self.workload_name, duration_secs)()
    steps.workload.Wait(self.scenario, self.workload_name)()

  def test_wait_after_stop_test(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    duration_secs = 120
    # Set should_stop to return False for 15 seconds, and True after that.
    self.scenario.should_stop = util.return_until(False, True,
                                                  duration_secs / 8)
    start_secs = time.time()
    steps.workload.Start(self.scenario, self.workload_name, duration_secs)()
    total_secs = time.time() - start_secs
    self.assertTrue(total_secs < duration_secs)

  def test_Stop(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    duration_secs = 600
    start_secs = time.time()
    steps.workload.Start(self.scenario, self.workload_name, duration_secs,
                         async=True)()
    time.sleep(30)
    steps.workload.Stop(self.scenario, self.workload_name, duration_secs)()
    total_secs = time.time() - start_secs
    self.assertTrue(total_secs < duration_secs)

  def test_Stop_inaccessible(self):
    vmgroup = VMGroup(self.scenario, self.group_name, template="ubuntu1604",
                      template_type="DISK", count_per_cluster=1,
                      data_disks=[16, 16, 16, 16, 16, 16])
    workload = Workload(test=self.scenario, name=self.workload_name,
                        vm_group=vmgroup, generator="fio",
                        config_file=self.valid_fio_path)
    self.scenario.vm_groups = {self.group_name: vmgroup}
    self.scenario.workloads = {self.workload_name: workload}
    steps.vm_group.CloneFromTemplate(self.scenario, self.group_name)()
    steps.vm_group.PowerOn(self.scenario, self.group_name)()
    duration_secs = 600
    start_secs = time.time()
    steps.workload.Start(self.scenario, self.workload_name, duration_secs,
                         async=True)()
    time.sleep(30)
    steps.vm_group.PowerOff(self.scenario, self.group_name)()
    steps.workload.Stop(self.scenario, self.workload_name, duration_secs)()
    total_secs = time.time() - start_secs
    self.assertTrue(total_secs < duration_secs)
