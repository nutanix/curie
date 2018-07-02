#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import inspect
import json
import logging
import os
import struct
import tarfile
import time
from functools import wraps

import pkg_resources
from argparse import Namespace
from lxml import etree

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.mof_util import generate_enum_definitions
from curie.os_util import FileLock
from curie.punit_parser import PUnit
from curie.vm import VmDescriptor

log = logging.getLogger(__name__)


# TODO (jklein): HACKY, probably not needed. If not needed will remove.
# Otherwise cleanup.

class VMDK4(struct.Struct):

  # VMDK headers are little endian.
  __FORMAT_STR__ = "<IIIQQQQIQQQ?ccccH433x"
  __MAGIC_NUMBER__ = 0x564d444b

  SECTOR_BYTES = 512

  def __init__(self, path):
    struct.Struct.__init__(self, self.__FORMAT_STR__)
    self.path = path

    with open(self.path, "rb") as fh:
      self.header_tuple = self.unpack(fh.read(self.size))

      (magic, version, flags, capacity_sectors, grain_sectors,
       descriptor_offset, descriptor_sectors, grain_table_entry_count,
       rgd_offset, gd_offset, overhead, unclean, endln, non_endln,
       double_endln1, double_endln2, compress_algorithm) = self.header_tuple

      fh.seek(self.SECTOR_BYTES * descriptor_offset)
      self.descriptor = fh.read(
          self.SECTOR_BYTES * descriptor_sectors).rstrip(chr(0))

    lines = filter(lambda l: l and not l.startswith("#"),
                   self.descriptor.splitlines())

    self.version, self.cid, self.parent_cid, self.create_type = lines[0:4]
    self.extents = []
    ii = 4
    # VMDK lists extend decriptions, followed by Disk Data Base (ddb) entries.
    while not lines[ii].startswith("ddb"):
      self.extents.append(lines[ii])
      ii += 1

  def is_empty(self):
    if len(self.extents) > 1:
      return False

    extent_info = self.extents[0].split()
    if len(extent_info) > 4:
      return False

    access, num_sectors, extent_type, fname = extent_info
    if extent_type == "SPARSE" and int(num_sectors) == self.header_tuple[3]:
      return True

    return False


# TODO (jklein): Formalize this.

def _get_mof_json_path():
  try:
    path = pkg_resources.resource_filename("curie", "mof_enum_defs.json")
  except:
    path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                        "mof_enum_defs.json"))

  return path

def _generate_mof_enum_defs(abs_path, timeout_secs=60):
  # Attempt to acquire an associated lockfile.
  dirname, fname = os.path.split(abs_path)
  file_lock = FileLock(os.path.join(dirname, ".%s.lock" % fname))

  if not file_lock.acquire():
    log.error("Unable to acquire lockfile, waiting up to '%s' seconds for "
              "owner to generate definitions", timeout_secs)
    # Wait max 'timeout_secs' for the lock owner to generate the file.
    for ii in range(timeout_secs):
      time.sleep(1)
      if os.path.exists(abs_path):
        log.info("Successfully located generated file %d of %d secs elapsed",
                 ii + 1, timeout_secs)
        return
      log.info("Waiting for lock owner to generate file. %d of %d secs "
               "elapsed", ii + 1, timeout_secs)
    raise Exception("Unable to acquire lockfile, timed out waiting for lock "
                    "owner to generate definitions")
  else:
    log.info("Successfully acquired file lock %s", file_lock._path)
    try:
      cls_prop_map = {"CIM_ResourceAllocationSettingData": ["ResourceType",
                                                            "MappingBehavior"]}
      generate_enum_definitions(abs_path, cls_prop_map)
    finally:
      file_lock.release()
      log.info("Released file lock %s", file_lock._path)

  if not os.path.exists(_get_mof_json_path()):
    raise Exception("Error, failed to locate generated .mof enum "
                    "definitions at '%s'" % abs_path)

def _lookup_resource_type(text):
  path = _get_mof_json_path()
  assert os.path.exists(path), "Must run 'make parsers'"
  with open(path) as fh:
    defs = json.loads(fh.read())

  return defs["CIM_ResourceAllocationSettingData"]["ResourceType"][text]

# TODO (jklein): See about auto-generating these with a metaclass and the CIM
# schema rather than explicitly defining methods and decorating them.

# TODO (jklein): Use schema to automatically detect PUnit fields and add in
# appropriate transform.

class _ovf_element_property(object):
  def __init__(self, fget, key):
    self.fget = fget
    self.name = key
    self.__doc__ = fget.__doc__

  def __get__(self, obj, objtype=None):
    if obj is None:
      return self
    return self.fget(obj)

  def __set__(self, obj, val):
    raise AttributeError("Cannot set read-only property on %s" % obj)


def attrib_property(key=None, ns_alias=None, transform=None):
  def _attrib_property(functor):
    # Ideally would have 'nonlocal' keyword, but it's not part of 2.6
    _nonlocal = Namespace(
        key=functor.func_name if key is None else key, ns=None)

    @wraps(functor)
    def wrapper(self):
      # pylint seems to fail at understanding how argparse.Namespace works.
      # pylint: disable=no-member
      _nonlocal.ns = self.nsmap.get(ns_alias) if ns_alias else self.NAMESPACE
      ret = self.attrib.get("{%s}%s" % (_nonlocal.ns, _nonlocal.key))
      if transform is not None:
        ret = transform(ret)

      return ret
    return property(wrapper)
  return _attrib_property


def child_property(key, transform=None, required=False, multiple=False):
  def _child_property(functor):
    @wraps(functor)
    def wrapper(self):
      val = self.xpath("./ovf:%s" % key, namespaces={"ovf": self.NAMESPACE})
      if required and not val:
        raise AttributeError("Unable to locate required attribute '%s'" % key)
      if multiple:
        return map(transform, val) if transform else val
      if len(val) > 1:
        raise AttributeError(
            "Multiple elements not expected for attribute '%s'" % key)

      if not val:
        return ""
      return transform(val[0]) if transform else val[0]

    return _ovf_element_property(wrapper, key)
  return _child_property


def child_text_property(key, transform=None):
  if transform is None:
    _transform = lambda elt: elt.text
  else:
    _transform = lambda elt: transform(elt.text)
  return child_property(key, transform=_transform)


def repeated_child_property(key, transform=None):
  return child_property(key, transform=transform, multiple=True)

#==============================================================================
# Internal classes
#==============================================================================

class _OvfElementMeta(type):
  _CLS_MAP = {}
  def __new__(mcs, name, bases, dct):
    if "TAG" not in dct:
      dct["TAG"] = None
    cls = super(_OvfElementMeta, mcs).__new__(mcs, name, bases, dct)
    mcs._CLS_MAP[name] = cls
    super(_OvfElementMeta, cls).__init__(name, bases, dct)
    return cls

# TODO (jklein): Clean this up, along with cleaning up the metaclass.

class _OvfXmlClassLookup(etree.PythonElementClassLookup):
  _TAG_FIELD_VAL_CLASS_MAP = {}

  @classmethod
  def register_class_specialization(cls, target_values, target_class):
    if not isinstance(target_values, (list, tuple)):
      target_values = [target_values, ]
    def _register_class_specialization(functor):
      cls_name = inspect.getouterframes(inspect.currentframe())[1][3]
      cls._TAG_FIELD_VAL_CLASS_MAP.setdefault(cls_name, {})
      cls._TAG_FIELD_VAL_CLASS_MAP[cls_name].setdefault(functor.name, {})
      curr_map = cls._TAG_FIELD_VAL_CLASS_MAP[cls_name][functor.name]
      for val in target_values:
        curr_map[val] = target_class
      return functor
    return _register_class_specialization

  def __init__(self):
    self.tag_field_val_class_map = {}
    for key, val in self._TAG_FIELD_VAL_CLASS_MAP.items():
      tag = _OvfElementMeta._CLS_MAP[key].TAG
      if tag is None:
        continue
      self.tag_field_val_class_map[tag] = val

    self._tag_fq_tag_map = {}
    for tag in ["Item", ]:
      self._tag_fq_tag_map[tag] = "{%s}%s" % (_OvfElement.NAMESPACE, tag)
    for tag in ["ResourceType", ]:
      self._tag_fq_tag_map[tag] = "{%s}%s" % (_OvfItem.NAMESPACE, tag)

  def fq_tag(self, tag):
    return self._tag_fq_tag_map[tag]

  def lookup(self, _doc, elt):
    tag = elt.tag.split("}")[-1]
    if tag in self.tag_field_val_class_map:
      for field, val_cls_map in self.tag_field_val_class_map[tag].items():
        for c_elt in elt.iterchildren():
          if c_elt.tag == self.fq_tag(field):
            if c_elt.text in val_cls_map:
              return _OvfElementMeta._CLS_MAP[val_cls_map[c_elt.text]]
    return None

# 'lxml.etree' API explicitly forbids implementing an __init__ method.
# pylint: disable=no-init

class _OvfElement(etree.ElementBase):
  __metaclass__ = _OvfElementMeta
  NAMESPACE = "http://schemas.dmtf.org/ovf/envelope/1"


class _OvfSection(_OvfElement):
  @child_text_property("Info")
  def info(self):
    pass

  @child_text_property("Description")
  def description(self):
    pass


class _OvfEnvelope(_OvfElement):
  TAG = "Envelope"

  @child_property("DiskSection")
  def disk_section(self):
    pass

  @child_property("NetworkSection")
  def network_section(self):
    pass

  @child_property("References")
  def references(self):
    pass

  @child_property("VirtualSystem")
  def virtual_system(self):
    pass

  #============================================================================
  # Public utility methods
  #============================================================================

  def get_disk_file_map(self):
    """
    Returns:
      (dict<_OvfDisk, _OvfFile>): Map of disks to corresponding file refs, or
        None if disk is not backed by an existing file.
    """
    id_file_ref_map = dict((f.id, f) for f in self.references.file)
    ret = {}
    for disk in self.disk_section.disk:
      if not disk.file_ref:
        assert not disk.populated_size, "Non-empty disk lacks a 'file_ref'"
        ret[disk] = None
      ret[disk] = id_file_ref_map[disk.file_ref]
    return ret

  def to_curie_vm_desc(self):
    """
    Returns:
      (VmDescriptor, list<str>, list<(str, _OvfDisk)>) OVF VM parameters
        translated into a 'VmDescriptor', list of networks to which NICs
        should be attached, and pairs of (HD name, _OvfDisk).
    """
    disk_map = dict((d.id, d) for d in self.disk_section.disk)
    vm_desc = VmDescriptor(self.virtual_system.name)
    vm_nics = []
    vm_disks = []
    for item in self.virtual_system.virtual_hardware_section.item:
      if item.type == "Processor":
        vm_desc.num_vcpus = int(item.count)
      elif item.type == "Memory":
        assert set(item.units.unit_map.keys()) == set(["byte"])
        vm_desc.memory_mb = (int(item.count) * item.units.multiplier) / (2**20)
      elif item.type in ["Ethernet Adapter", "Other Network Adapter"]:
        vm_nics.append(item.connection)
      elif item.type in ["Disk Drive"]:
        vm_disks.append((item.name,
                         disk_map[item.host_resource.split("/")[-1]]))

    return vm_desc, vm_nics, vm_disks


class _OvfReferences(_OvfElement):
  TAG = "References"
  @repeated_child_property("File")
  def file(self):
    pass


class _OvfFile(_OvfElement):
  TAG = "File"
  @attrib_property()
  def href(self):
    pass

  @attrib_property()
  def id(self):
    pass

  @attrib_property()
  def size(self):
    pass


class _OvfDiskSection(_OvfSection):
  TAG = "DiskSection"

  @repeated_child_property("Disk")
  def disk(self):
    pass


class _OvfDisk(_OvfElement):
  TAG = "Disk"
  @attrib_property()
  def capacity(self):
    pass

  @attrib_property("capacityAllocationUnits", transform=PUnit.from_string)
  def units(self):
    pass

  @attrib_property("diskId")
  def id(self):
    pass

  @attrib_property("fileRef")
  def file_ref(self):
    pass

  @attrib_property()
  def format(self):
    pass

  @attrib_property("populatedSize")
  def populated_size(self):
    pass


class _OvfNetworkSection(_OvfSection):
  TAG = "NetworkSection"
  @repeated_child_property("Network")
  def network(self):
    pass


class _OvfNetwork(_OvfElement):
  TAG = "Network"
  @attrib_property()
  def name(self):
    pass

  @child_text_property("Description")
  def description(self):
    pass


class _OvfVirtualSystem(_OvfElement):
  TAG = "VirtualSystem"
  @attrib_property()
  def id(self):
    pass

  #@VmDescriptor.register_field("name")
  @child_text_property("Name")
  def name(self):
    pass

  @child_text_property("Info")
  def info(self):
    pass

  @child_property("OperatingSystemSection")
  def os_section(self):
    pass

  @child_property("VirtualHardwareSection")
  def virtual_hardware_section(self):
    pass


class _OvfOsSection(_OvfSection):
  TAG = "OperatingSystemSection"
  @attrib_property("id")
  def id(self):
    pass

  @attrib_property("osType", ns_alias="vmw")
  def type(self):
    pass

  @child_text_property("Info")
  def info(self):
    pass

  @child_text_property("Description")
  def description(self):
    pass


class _OvfVirtualHardwareSection(_OvfSection):
  TAG = "VirtualHardwareSection"
  @child_property("System")
  def system(self):
    pass

  @repeated_child_property("Item")
  def item(self):
    pass


class _OvfSystem(_OvfElement):
  TAG = "System"
  NAMESPACE = ("http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
               "CIM_VirtualSystemSettingData")

  @child_text_property("ElementName")
  def name(self):
    pass

  @child_text_property("InstanceID")
  def instance_id(self):
    pass

  @child_text_property("VirtualSystemIdentifier")
  def system_id(self):
    pass

  @child_text_property("VirtualSystemType")
  def type(self):
    pass


class _OvfItem(_OvfElement):
  TAG = "Item"
  NAMESPACE = ("http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"
               "CIM_ResourceAllocationSettingData")

  @child_text_property("ElementName")
  def name(self):
    pass

  @child_text_property("InstanceID")
  def id(self):
    pass

  @_OvfXmlClassLookup.register_class_specialization("3", "_OvfItemProcessor")
  @_OvfXmlClassLookup.register_class_specialization("4", "_OvfItemMemory")
  @_OvfXmlClassLookup.register_class_specialization(["10", "11"],
                                                    "_OvfItemNetwork")
  @_OvfXmlClassLookup.register_class_specialization("17", "_OvfItemDisk")
  @child_text_property("ResourceType", transform=_lookup_resource_type)
  def type(self):
    pass

  @child_text_property("ResourceSubType")
  def subtype(self):
    pass

  @child_text_property("Description")
  def description(self):
    pass

  # @VmDescriptor.register_field(
  #   "num_vcpus", filter=lambda item: item.type == "Processor")
  # @VmDescriptor.register_field(
  #   "mb_ram", filter=lambda item: item.type == "Memory")
  @child_text_property("VirtualQuantity")
  def count(self):
    pass

  @child_text_property("AllocationUnits", transform=PUnit.from_string)
  def units(self):
    pass

  @child_text_property("Parent")
  def parent(self):
    pass

  @child_text_property("Address")
  def address(self):
    pass

  @child_text_property("AddressOnParent")
  def address_on_parent(self):
    pass


class _OvfItemProcessor(_OvfItem):
  pass


class _OvfItemMemory(_OvfItem):
  pass


class _OvfItemNetwork(_OvfItem):
  @child_text_property("Connection")
  def connection(self):
    pass


class _OvfItemDisk(_OvfItem):
  @child_text_property("HostResource")
  def host_resource(self):
    pass

# pylint: enable=no-init

#==============================================================================
# Public classes
#==============================================================================

class CurieOvf(object):
  @staticmethod
  def _recurse_class_tree(curr_cls, ns_update_map):
    for subcls in curr_cls.__subclasses__():
      if subcls.TAG is not None:
        ns_update_map[subcls.TAG] = subcls
      ns_update_map.update(
          CurieOvf._recurse_class_tree(subcls, ns_update_map))
    return ns_update_map

  @classmethod
  def parse_file(cls, abs_path):
    with open(abs_path) as fh:
      raw_xml = fh.read()

    return cls(raw_xml)

  def __new__(cls, raw_xml):
    mof_json_path = _get_mof_json_path()
    if not os.path.exists(mof_json_path):
      raise RuntimeError("Must run 'make parsers'")

    ns_class_lookup = etree.ElementNamespaceClassLookup()
    _ovf_envelope_ns = ns_class_lookup.get_namespace(_OvfElement.NAMESPACE)
    _ovf_envelope_ns.update(cls._recurse_class_tree(_OvfElement,
                                                    {None: _OvfElement}))

    ovf_class_lookup = _OvfXmlClassLookup()
    # pylint: disable=no-member
    ovf_class_lookup.set_fallback(ns_class_lookup)
    # pylint: enable=no-member
    parser = etree.XMLParser(
        attribute_defaults=False, dtd_validation=False, ns_clean=True,
        schema=None, remove_comments=True, remove_pis=True,
        remove_blank_text=True)
    parser.set_element_class_lookup(ovf_class_lookup)

    return etree.XML(raw_xml, parser=parser)


class CurieOva(object):
  def __init__(self, abs_path):
    if not os.path.exists(abs_path):
      raise IOError("File '%s' does not exist")
    if not tarfile.is_tarfile(abs_path):
      raise ValueError("File '%s' is not a valid tar file")

    self._tarfile = tarfile.TarFile(abs_path)
    ovfs = [n for n in self._tarfile.getnames() if n.endswith(".ovf")]
    if len(ovfs) != 1:
      self._tarfile.close()
      raise CurieException(CurieError.kInvalidParameter,
                            "Invalid OVA '%s'. Unable to read contained OVF"
                            % abs_path)
    self._ovf_name = ovfs[0]

  # TODO (jklein): Finish implementing this, as well as finishing code for
  # compiling the CIM .mof files.

  def parse(self):
    """
    Parse contained OVF.
    """
    ovf_fh = self.get_file_handle(self._ovf_name)
    raw_xml = ovf_fh.read()
    ovf_fh.close()
    return CurieOvf(raw_xml)

  def get_file_info(self, name):
    return self._tarfile.getmember(name)

  def get_file_handle(self, name):
    return self._tarfile.extractfile(name)

  def close(self):
    self._tarfile.close()
