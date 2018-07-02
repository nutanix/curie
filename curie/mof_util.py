#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Basic utility for generating value to display name mappings for Enum types
# defined in the CIM spec.
#

import json
import logging
import os

# pylint: disable=no-name-in-module
from pywbem import mof_compiler
# pylint: enable=no-name-in-module

from curie.os_util import OsUtil
from curie.log import patch_trace

log = logging.getLogger(__name__)
patch_trace()

CURIE_ROOT_DIR = os.path.abspath(os.path.join(__file__, "..", ".."))
CIM_SCHEMA_ROOT = os.path.join(CURIE_ROOT_DIR, "cim")
CIM_SCHEMA_FNAME = "cim_schema_2.33.0.mof"


class _DummyWbemConnection(object):
  """
  Dummy WBEMConnection class to collect parsed descriptors from .mof files.
  """
  __INITIALIZED__ = False

  default_namespace = ""

  CLASS_MAP = {}

  def SetQualifier(self, qualifier):
    log.trace("SetQualifier: %s\t\t%s: %s", qualifier, qualifier.type, qualifier.value)

  def EnumerateQualifiers(self):
    log.trace("EnumerateQualifiers")

  def CreateClass(self, cls):
    log.trace("CreateClass: %s" % cls)
    self.CLASS_MAP[cls.classname] = cls


def _lookup_class(cls_name):
  if not _DummyWbemConnection.__INITIALIZED__:
    _DummyWbemConnection.__INITIALIZED__ = True
    comp = mof_compiler.MOFCompiler(_DummyWbemConnection(), [CIM_SCHEMA_ROOT])
    comp.compile_file(os.path.join(CIM_SCHEMA_ROOT, CIM_SCHEMA_FNAME), "")

  cls = _DummyWbemConnection.CLASS_MAP.get(cls_name, None)
  if cls is None:
    return cls

  # Fill in inherited attributes
  scls_name = cls.superclass
  while scls_name:
    scls = _DummyWbemConnection.CLASS_MAP[scls_name]
    cls.properties.update(scls.properties)
    cls.qualifiers.update(scls.qualifiers)
    scls_name = scls.superclass

  return cls


def _generate_enum_map(cls_name, prop_name):
  obj = _lookup_class(cls_name)
  if not obj:
    return None
  obj = obj.properties.get(prop_name)
  if not obj:
    return None

  return dict(zip(obj.qualifiers["ValueMap"].value,
                  obj.qualifiers["Values"].value))


def generate_enum_definitions(path, cls_prop_map):
  ret = {}
  for cls_name, cls_prop_list in cls_prop_map.items():
    ret[cls_name] = {}
    for prop_name in cls_prop_list:
      ret[cls_name][prop_name] = _generate_enum_map(cls_name, prop_name)

  OsUtil.write_and_rename(path, json.dumps(ret))

if __name__ == "__main__":
  cls_prop_map = {"CIM_ResourceAllocationSettingData": ["ResourceType",
                                                        "MappingBehavior"]}
  generate_enum_definitions(os.path.join(os.path.dirname(__file__),
                                         "mof_enum_defs.json"),
                            cls_prop_map)
