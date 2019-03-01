#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#

import logging
import sys
import unittest

import gflags
import mock

from curie.name_util import NameUtil, CURIE_GOLDIMAGE_VM_DISK_PREFIX, \
  CURIE_GOLDIMAGE_VM_NAME_PREFIX


class TestNameUtil(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass

  def test_goldimage_vmdisk_name(self):
    vdisk_name = NameUtil.goldimage_vmdisk_name("imagename", "disk0")
    expected = "%s_imagename_disk0" % CURIE_GOLDIMAGE_VM_DISK_PREFIX
    self.assertEqual(vdisk_name, expected)

  def test_goldimage_vm_name(self):
    mock_test = mock.MagicMock()
    mock_test.test_id.return_value = 1234
    vm_name = NameUtil.goldimage_vm_name(mock_test, "ubuntu1604")
    expected = "%s_1234_ubuntu1604" % CURIE_GOLDIMAGE_VM_NAME_PREFIX
    self.assertEqual(vm_name, expected)

  def test_is_hyperv_cvm_vm(self):
    self.assertTrue(NameUtil.is_hyperv_cvm_vm("NTNX-18SM6F380322-D-CVM"))
    self.assertTrue(NameUtil.is_hyperv_cvm_vm("NTNX-ZZZZZZZZZZZZ-ZZZ-CVM"))
    self.assertTrue(NameUtil.is_hyperv_cvm_vm("NTNX-0-A-CVM"))
    self.assertFalse(NameUtil.is_hyperv_cvm_vm("NTNX-18SM6F380322-D"))
    self.assertFalse(NameUtil.is_hyperv_cvm_vm("18SM6F380322-D-CVM"))
    self.assertFalse(NameUtil.is_hyperv_cvm_vm(""))
    self.assertFalse(NameUtil.is_hyperv_cvm_vm("NTNX-WHATEVER"))
