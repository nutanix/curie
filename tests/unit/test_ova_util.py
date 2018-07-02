#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# pylint: disable=pointless-statement
import multiprocessing
import os
import unittest
from multiprocessing.synchronize import Semaphore

import mock

from curie import ova_util
from curie.ova_util import CurieOva, CurieOvf
from curie.punit_parser import PUnit


class _TestCurieOvaUtilBase(unittest.TestCase):
  SAMPLE_OVA_PATH = os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../resource/test_ova_util/metis.ova"))

  def setUp(self):
    self.ova = CurieOva(self.SAMPLE_OVA_PATH)


class TestCurieOvaUtil(_TestCurieOvaUtilBase):
  def test_load(self):
    # Only test that setUp doesn't raise an exception loading the OVA.
    pass

  @mock.patch("curie.ova_util.os.path.exists")
  def test_construct_CurieOvf_path_not_exist(self, mock_os_path_exists):
    mock_os_path_exists.return_value = False
    with self.assertRaises(RuntimeError) as ar:
      CurieOvf("")
    self.assertEqual("Must run 'make parsers'", str(ar.exception))

  def test_parse(self):
    ovf = self.ova.parse()
    self.assertIsInstance(ovf, ova_util._OvfEnvelope)
    self.assertIsInstance(ovf.disk_section, ova_util._OvfDiskSection)
    self.assertIsInstance(ovf.network_section,
                          ova_util._OvfNetworkSection)
    self.assertIsInstance(ovf.references, ova_util._OvfReferences)
    self.assertIsInstance(ovf.virtual_system,
                          ova_util._OvfVirtualSystem)

  def test_disk_section(self):
    disk_section = self.ova.parse().disk_section
    self.assertIsInstance(disk_section, ova_util._OvfDiskSection)
    self.assertEqual(disk_section.info, "Virtual disk information")
    self.assertEqual(disk_section.description, "")

    disks = disk_section.disk
    self.assertIsInstance(disks, list)
    self.assertEqual(len(disks), 1)

    disk = disks[0]
    self.assertIsInstance(disk, ova_util._OvfDisk)
    self.assertEqual(disk.capacity, "16")
    self.assertIsInstance(disk.units, PUnit)
    self.assertEqual(disk.units.multiplier, 2**30)
    self.assertEqual(set(disk.units.unit_map.keys()), set(["byte"]))
    self.assertEqual(disk.units.unit_map["byte"], 1)
    self.assertEqual(disk.id, "vmdisk1")
    self.assertEqual(disk.file_ref, "file1")
    self.assertEqual(disk.populated_size, "1394737152")

  def test_network_section(self):
    network_section = self.ova.parse().network_section
    self.assertIsInstance(network_section, ova_util._OvfNetworkSection)
    self.assertEqual(network_section.info, "The list of logical networks")
    self.assertEqual(network_section.description, "")

    networks = network_section.network
    self.assertIsInstance(networks, list)
    self.assertEqual(len(networks), 1)

    network = networks[0]
    self.assertIsInstance(network, ova_util._OvfNetwork)
    self.assertEqual(network.name, "VM Network")
    self.assertEqual(network.description, "The VM Network network")

  def test_references(self):
    references = self.ova.parse().references
    self.assertIsInstance(references, ova_util._OvfReferences)

    files = references.file
    self.assertIsInstance(files, list)
    self.assertEqual(len(files), 1)

    f0 = files[0]
    self.assertIsInstance(f0, ova_util._OvfFile)
    self.assertEqual(f0.href, "metis-1.6.1-release-disk1.vmdk")
    self.assertEqual(f0.id, "file1")
    self.assertEqual(f0.size, "906")

  def test_virtual_system(self):
    virtual_system = self.ova.parse().virtual_system
    self.assertIsInstance(virtual_system, ova_util._OvfVirtualSystem)
    self.assertEqual(virtual_system.info, "A virtual machine")
    self.assertEqual(virtual_system.id, "Metis")
    self.assertEqual(virtual_system.name, "Metis")

    os_section = virtual_system.os_section
    self.assertIsInstance(os_section, ova_util._OvfOsSection)
    self.assertEqual(os_section.id, "107")
    self.assertEqual(os_section.type, "centos64Guest")
    self.assertEqual(os_section.description, "CentOS 4/5/6/7 (64-bit)")

    vhw_section = virtual_system.virtual_hardware_section
    self.assertIsInstance(vhw_section,
                          ova_util._OvfVirtualHardwareSection)
    self.assertEqual(vhw_section.info, "Virtual hardware requirements")

    vhw_system = vhw_section.system
    self.assertIsInstance(vhw_system, ova_util._OvfSystem)
    self.assertEqual(vhw_system.name, "Virtual Hardware Family")
    self.assertEqual(vhw_system.instance_id, "0")
    self.assertEqual(vhw_system.system_id, "Metis")
    self.assertEqual(vhw_system.type, "vmx-08")

    items = vhw_section.item
    self.assertIsInstance(items, list)
    self.assertEqual(len(items), 11)

    # Verify all Items have expected attributes accessible.
    for item in items:
      item.id
      item.name
      item.type
      item.subtype
      item.description
      item.count
      item.units
      item.parent
      item.address
      item.address_on_parent

    num_vcpus, gb_ram = items[:2]

    # Check parsing of Processor item, including PUnit parsing.
    self.assertEqual(num_vcpus.type, "Processor")
    self.assertIsInstance(num_vcpus, ova_util._OvfItemProcessor)
    self.assertIsInstance(num_vcpus.units, PUnit)
    self.assertEqual(num_vcpus.units.multiplier, 10**6)
    self.assertEqual(set(num_vcpus.units.unit_map.keys()), set(["hertz"]))
    self.assertEqual(num_vcpus.units.unit_map["hertz"], 1)

    # Check parsing of Memory item, including PUnit parsing.
    self.assertIsInstance(gb_ram, ova_util._OvfItemMemory)
    self.assertEqual(gb_ram.type, "Memory")
    self.assertIsInstance(gb_ram.units, PUnit)

    self.assertEqual(gb_ram.units.multiplier, 2**20)
    self.assertEqual(set(gb_ram.units.unit_map.keys()), set(["byte"]))
    self.assertEqual(gb_ram.units.unit_map["byte"], 1)
