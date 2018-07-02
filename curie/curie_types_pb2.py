# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: curie_types.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import curie_extensions_pb2 as curie__extensions__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='curie_types.proto',
  package='nutanix.curie',
  syntax='proto2',
  serialized_pb=_b('\n\x11\x63urie_types.proto\x12\rnutanix.curie\x1a\x16\x63urie_extensions.proto\"S\n\x10\x43onnectionParams\x12\x16\n\x08username\x18\x01 \x01(\tB\x04\x80\x88\'\x01\x12\x16\n\x08password\x18\x02 \x01(\tB\x04\x80\x88\'\x01\x12\x0f\n\x07\x61\x64\x64ress\x18\x03 \x01(\t\"\xa6\x03\n\x12OutOfBandInterface\x12\x34\n\x04type\x18\x01 \x01(\x0e\x32&.nutanix.curie.OutOfBandInterface.Type\x12\x38\n\x06vendor\x18\x02 \x01(\x0e\x32(.nutanix.curie.OutOfBandInterface.Vendor\x12\x34\n\x0b\x63onn_params\x18\x03 \x01(\x0b\x32\x1f.nutanix.curie.ConnectionParams\x12X\n\x17interface_specific_info\x18\x04 \x01(\x0b\x32\x37.nutanix.curie.OutOfBandInterface.InterfaceSpecificInfo\x1a\x17\n\x15InterfaceSpecificInfo\"=\n\x04Type\x12\x15\n\x11kUnknownInterface\x10\x00\x12\t\n\x05kIpmi\x10\x01\x12\x08\n\x04kPdu\x10\x02\x12\t\n\x05kNone\x10\x03\"8\n\x06Vendor\x12\x12\n\x0ekUnknownVendor\x10\x00\x12\x0f\n\x0bkSupermicro\x10\x01\x12\t\n\x05kDell\x10\x02\"\xe1\x01\n\x10ManagementServer\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x34\n\x0b\x63onn_params\x18\x03 \x01(\x0b\x32\x1f.nutanix.curie.ConnectionParams\x12\x32\n\x04type\x18\x04 \x01(\x0e\x32$.nutanix.curie.ManagementServer.Type\x12\x0f\n\x07version\x18\x05 \x01(\t\"8\n\x04Type\x12\x0c\n\x08kUnknown\x10\x00\x12\x0c\n\x08kVcenter\x10\x01\x12\n\n\x06kPrism\x10\x02\x12\x08\n\x04kVmm\x10\x03\"\x82\x01\n\nHypervisor\x12,\n\x04type\x18\x01 \x01(\x0e\x32\x1e.nutanix.curie.Hypervisor.Type\x12\x0f\n\x07version\x18\x02 \x01(\t\"5\n\x04Type\x12\x0c\n\x08kUnknown\x10\x00\x12\x08\n\x04kEsx\x10\x01\x12\x08\n\x04kAhv\x10\x02\x12\x0b\n\x07kHyperv\x10\x03\"\x94\x01\n\x12\x43lusteringSoftware\x12\x34\n\x04type\x18\x01 \x01(\x0e\x32&.nutanix.curie.ClusteringSoftware.Type\x12\x0f\n\x07version\x18\x02 \x01(\t\"7\n\x04Type\x12\x0c\n\x08kUnknown\x10\x00\x12\x0c\n\x08kNutanix\x10\x01\x12\t\n\x05kVsan\x10\x02\x12\x08\n\x04kS2D\x10\x03\"\xbc\x01\n\x0eVirtualMachine\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x34\n\x0b\x63onn_params\x18\x03 \x01(\x0b\x32\x1f.nutanix.curie.ConnectionParams\x12\x30\n\x04type\x18\x04 \x01(\x0e\x32\".nutanix.curie.VirtualMachine.Type\"(\n\x04Type\x12\x0c\n\x08kUnknown\x10\x00\x12\x08\n\x04kCvm\x10\x01\x12\x08\n\x04kUvm\x10\x02\"\x8b\x01\n\x0b\x43lusterNode\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x33\n\x08oob_info\x18\x03 \x01(\x0b\x32!.nutanix.curie.OutOfBandInterface\x12-\n\nhypervisor\x18\x04 \x01(\x0b\x32\x19.nutanix.curie.Hypervisor\"\xfc\x02\n\x07\x43luster\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x34\n\x0b\x63onn_params\x18\x03 \x01(\x0b\x32\x1f.nutanix.curie.ConnectionParams\x12\x37\n\x10storage_info_vec\x18\x04 \x03(\x0b\x32\x1d.nutanix.curie.LogicalStorage\x12\x37\n\x10network_info_vec\x18\x05 \x03(\x0b\x32\x1d.nutanix.curie.LogicalNetwork\x12>\n\x13\x63lustering_software\x18\x06 \x01(\x0b\x32!.nutanix.curie.ClusteringSoftware\x12:\n\x11management_server\x18\x07 \x01(\x0b\x32\x1f.nutanix.curie.ManagementServer\x12\x33\n\x0elibrary_shares\x18\x08 \x03(\x0b\x32\x1b.nutanix.curie.LibraryShare\"Z\n\x11\x43lusterCollection\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x0b\x63luster_vec\x18\x03 \x03(\x0b\x32\x16.nutanix.curie.Cluster\"*\n\x0eLogicalNetwork\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\":\n\x0cLibraryShare\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04path\x18\x02 \x01(\t\x12\x0e\n\x06server\x18\x03 \x01(\t\"\xbe\x01\n\x0eLogicalStorage\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\x30\n\x04kind\x18\x04 \x01(\x0e\x32\".nutanix.curie.LogicalStorage.Kind\x12\x0c\n\x04type\x18\x03 \x01(\t\"R\n\x04Kind\x12\x0c\n\x08kUnknown\x10\x00\x12\x11\n\rkEsxDatastore\x10\x01\x12\x15\n\x11kNutanixContainer\x10\x02\x12\x12\n\x0ekHypervStorage\x10\x03')
  ,
  dependencies=[curie__extensions__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_OUTOFBANDINTERFACE_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.OutOfBandInterface.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknownInterface', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kIpmi', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kPdu', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kNone', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=449,
  serialized_end=510,
)
_sym_db.RegisterEnumDescriptor(_OUTOFBANDINTERFACE_TYPE)

_OUTOFBANDINTERFACE_VENDOR = _descriptor.EnumDescriptor(
  name='Vendor',
  full_name='nutanix.curie.OutOfBandInterface.Vendor',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknownVendor', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kSupermicro', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kDell', index=2, number=2,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=512,
  serialized_end=568,
)
_sym_db.RegisterEnumDescriptor(_OUTOFBANDINTERFACE_VENDOR)

_MANAGEMENTSERVER_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.ManagementServer.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kVcenter', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kPrism', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kVmm', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=740,
  serialized_end=796,
)
_sym_db.RegisterEnumDescriptor(_MANAGEMENTSERVER_TYPE)

_HYPERVISOR_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.Hypervisor.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kEsx', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kAhv', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kHyperv', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=876,
  serialized_end=929,
)
_sym_db.RegisterEnumDescriptor(_HYPERVISOR_TYPE)

_CLUSTERINGSOFTWARE_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.ClusteringSoftware.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kNutanix', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kVsan', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kS2D', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1025,
  serialized_end=1080,
)
_sym_db.RegisterEnumDescriptor(_CLUSTERINGSOFTWARE_TYPE)

_VIRTUALMACHINE_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.VirtualMachine.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kCvm', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kUvm', index=2, number=2,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1231,
  serialized_end=1271,
)
_sym_db.RegisterEnumDescriptor(_VIRTUALMACHINE_TYPE)

_LOGICALSTORAGE_KIND = _descriptor.EnumDescriptor(
  name='Kind',
  full_name='nutanix.curie.LogicalStorage.Kind',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kEsxDatastore', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kNutanixContainer', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kHypervStorage', index=3, number=3,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=2103,
  serialized_end=2185,
)
_sym_db.RegisterEnumDescriptor(_LOGICALSTORAGE_KIND)


_CONNECTIONPARAMS = _descriptor.Descriptor(
  name='ConnectionParams',
  full_name='nutanix.curie.ConnectionParams',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='username', full_name='nutanix.curie.ConnectionParams.username', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=_descriptor._ParseOptions(descriptor_pb2.FieldOptions(), _b('\200\210\'\001'))),
    _descriptor.FieldDescriptor(
      name='password', full_name='nutanix.curie.ConnectionParams.password', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=_descriptor._ParseOptions(descriptor_pb2.FieldOptions(), _b('\200\210\'\001'))),
    _descriptor.FieldDescriptor(
      name='address', full_name='nutanix.curie.ConnectionParams.address', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=60,
  serialized_end=143,
)


_OUTOFBANDINTERFACE_INTERFACESPECIFICINFO = _descriptor.Descriptor(
  name='InterfaceSpecificInfo',
  full_name='nutanix.curie.OutOfBandInterface.InterfaceSpecificInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=424,
  serialized_end=447,
)

_OUTOFBANDINTERFACE = _descriptor.Descriptor(
  name='OutOfBandInterface',
  full_name='nutanix.curie.OutOfBandInterface',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.OutOfBandInterface.type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='vendor', full_name='nutanix.curie.OutOfBandInterface.vendor', index=1,
      number=2, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='conn_params', full_name='nutanix.curie.OutOfBandInterface.conn_params', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='interface_specific_info', full_name='nutanix.curie.OutOfBandInterface.interface_specific_info', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_OUTOFBANDINTERFACE_INTERFACESPECIFICINFO, ],
  enum_types=[
    _OUTOFBANDINTERFACE_TYPE,
    _OUTOFBANDINTERFACE_VENDOR,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=146,
  serialized_end=568,
)


_MANAGEMENTSERVER = _descriptor.Descriptor(
  name='ManagementServer',
  full_name='nutanix.curie.ManagementServer',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.ManagementServer.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.ManagementServer.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='conn_params', full_name='nutanix.curie.ManagementServer.conn_params', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.ManagementServer.type', index=3,
      number=4, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='nutanix.curie.ManagementServer.version', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _MANAGEMENTSERVER_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=571,
  serialized_end=796,
)


_HYPERVISOR = _descriptor.Descriptor(
  name='Hypervisor',
  full_name='nutanix.curie.Hypervisor',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.Hypervisor.type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='nutanix.curie.Hypervisor.version', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _HYPERVISOR_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=799,
  serialized_end=929,
)


_CLUSTERINGSOFTWARE = _descriptor.Descriptor(
  name='ClusteringSoftware',
  full_name='nutanix.curie.ClusteringSoftware',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.ClusteringSoftware.type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='nutanix.curie.ClusteringSoftware.version', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _CLUSTERINGSOFTWARE_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=932,
  serialized_end=1080,
)


_VIRTUALMACHINE = _descriptor.Descriptor(
  name='VirtualMachine',
  full_name='nutanix.curie.VirtualMachine',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.VirtualMachine.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.VirtualMachine.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='conn_params', full_name='nutanix.curie.VirtualMachine.conn_params', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.VirtualMachine.type', index=3,
      number=4, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _VIRTUALMACHINE_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1083,
  serialized_end=1271,
)


_CLUSTERNODE = _descriptor.Descriptor(
  name='ClusterNode',
  full_name='nutanix.curie.ClusterNode',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.ClusterNode.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.ClusterNode.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='oob_info', full_name='nutanix.curie.ClusterNode.oob_info', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='hypervisor', full_name='nutanix.curie.ClusterNode.hypervisor', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1274,
  serialized_end=1413,
)


_CLUSTER = _descriptor.Descriptor(
  name='Cluster',
  full_name='nutanix.curie.Cluster',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.Cluster.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.Cluster.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='conn_params', full_name='nutanix.curie.Cluster.conn_params', index=2,
      number=3, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='storage_info_vec', full_name='nutanix.curie.Cluster.storage_info_vec', index=3,
      number=4, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='network_info_vec', full_name='nutanix.curie.Cluster.network_info_vec', index=4,
      number=5, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='clustering_software', full_name='nutanix.curie.Cluster.clustering_software', index=5,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='management_server', full_name='nutanix.curie.Cluster.management_server', index=6,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='library_shares', full_name='nutanix.curie.Cluster.library_shares', index=7,
      number=8, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1416,
  serialized_end=1796,
)


_CLUSTERCOLLECTION = _descriptor.Descriptor(
  name='ClusterCollection',
  full_name='nutanix.curie.ClusterCollection',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.ClusterCollection.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.ClusterCollection.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='cluster_vec', full_name='nutanix.curie.ClusterCollection.cluster_vec', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1798,
  serialized_end=1888,
)


_LOGICALNETWORK = _descriptor.Descriptor(
  name='LogicalNetwork',
  full_name='nutanix.curie.LogicalNetwork',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.LogicalNetwork.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.LogicalNetwork.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1890,
  serialized_end=1932,
)


_LIBRARYSHARE = _descriptor.Descriptor(
  name='LibraryShare',
  full_name='nutanix.curie.LibraryShare',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.LibraryShare.name', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='path', full_name='nutanix.curie.LibraryShare.path', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='server', full_name='nutanix.curie.LibraryShare.server', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1934,
  serialized_end=1992,
)


_LOGICALSTORAGE = _descriptor.Descriptor(
  name='LogicalStorage',
  full_name='nutanix.curie.LogicalStorage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='id', full_name='nutanix.curie.LogicalStorage.id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='name', full_name='nutanix.curie.LogicalStorage.name', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='kind', full_name='nutanix.curie.LogicalStorage.kind', index=2,
      number=4, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.LogicalStorage.type', index=3,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _LOGICALSTORAGE_KIND,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1995,
  serialized_end=2185,
)

_OUTOFBANDINTERFACE_INTERFACESPECIFICINFO.containing_type = _OUTOFBANDINTERFACE
_OUTOFBANDINTERFACE.fields_by_name['type'].enum_type = _OUTOFBANDINTERFACE_TYPE
_OUTOFBANDINTERFACE.fields_by_name['vendor'].enum_type = _OUTOFBANDINTERFACE_VENDOR
_OUTOFBANDINTERFACE.fields_by_name['conn_params'].message_type = _CONNECTIONPARAMS
_OUTOFBANDINTERFACE.fields_by_name['interface_specific_info'].message_type = _OUTOFBANDINTERFACE_INTERFACESPECIFICINFO
_OUTOFBANDINTERFACE_TYPE.containing_type = _OUTOFBANDINTERFACE
_OUTOFBANDINTERFACE_VENDOR.containing_type = _OUTOFBANDINTERFACE
_MANAGEMENTSERVER.fields_by_name['conn_params'].message_type = _CONNECTIONPARAMS
_MANAGEMENTSERVER.fields_by_name['type'].enum_type = _MANAGEMENTSERVER_TYPE
_MANAGEMENTSERVER_TYPE.containing_type = _MANAGEMENTSERVER
_HYPERVISOR.fields_by_name['type'].enum_type = _HYPERVISOR_TYPE
_HYPERVISOR_TYPE.containing_type = _HYPERVISOR
_CLUSTERINGSOFTWARE.fields_by_name['type'].enum_type = _CLUSTERINGSOFTWARE_TYPE
_CLUSTERINGSOFTWARE_TYPE.containing_type = _CLUSTERINGSOFTWARE
_VIRTUALMACHINE.fields_by_name['conn_params'].message_type = _CONNECTIONPARAMS
_VIRTUALMACHINE.fields_by_name['type'].enum_type = _VIRTUALMACHINE_TYPE
_VIRTUALMACHINE_TYPE.containing_type = _VIRTUALMACHINE
_CLUSTERNODE.fields_by_name['oob_info'].message_type = _OUTOFBANDINTERFACE
_CLUSTERNODE.fields_by_name['hypervisor'].message_type = _HYPERVISOR
_CLUSTER.fields_by_name['conn_params'].message_type = _CONNECTIONPARAMS
_CLUSTER.fields_by_name['storage_info_vec'].message_type = _LOGICALSTORAGE
_CLUSTER.fields_by_name['network_info_vec'].message_type = _LOGICALNETWORK
_CLUSTER.fields_by_name['clustering_software'].message_type = _CLUSTERINGSOFTWARE
_CLUSTER.fields_by_name['management_server'].message_type = _MANAGEMENTSERVER
_CLUSTER.fields_by_name['library_shares'].message_type = _LIBRARYSHARE
_CLUSTERCOLLECTION.fields_by_name['cluster_vec'].message_type = _CLUSTER
_LOGICALSTORAGE.fields_by_name['kind'].enum_type = _LOGICALSTORAGE_KIND
_LOGICALSTORAGE_KIND.containing_type = _LOGICALSTORAGE
DESCRIPTOR.message_types_by_name['ConnectionParams'] = _CONNECTIONPARAMS
DESCRIPTOR.message_types_by_name['OutOfBandInterface'] = _OUTOFBANDINTERFACE
DESCRIPTOR.message_types_by_name['ManagementServer'] = _MANAGEMENTSERVER
DESCRIPTOR.message_types_by_name['Hypervisor'] = _HYPERVISOR
DESCRIPTOR.message_types_by_name['ClusteringSoftware'] = _CLUSTERINGSOFTWARE
DESCRIPTOR.message_types_by_name['VirtualMachine'] = _VIRTUALMACHINE
DESCRIPTOR.message_types_by_name['ClusterNode'] = _CLUSTERNODE
DESCRIPTOR.message_types_by_name['Cluster'] = _CLUSTER
DESCRIPTOR.message_types_by_name['ClusterCollection'] = _CLUSTERCOLLECTION
DESCRIPTOR.message_types_by_name['LogicalNetwork'] = _LOGICALNETWORK
DESCRIPTOR.message_types_by_name['LibraryShare'] = _LIBRARYSHARE
DESCRIPTOR.message_types_by_name['LogicalStorage'] = _LOGICALSTORAGE

ConnectionParams = _reflection.GeneratedProtocolMessageType('ConnectionParams', (_message.Message,), dict(
  DESCRIPTOR = _CONNECTIONPARAMS,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ConnectionParams)
  ))
_sym_db.RegisterMessage(ConnectionParams)

OutOfBandInterface = _reflection.GeneratedProtocolMessageType('OutOfBandInterface', (_message.Message,), dict(

  InterfaceSpecificInfo = _reflection.GeneratedProtocolMessageType('InterfaceSpecificInfo', (_message.Message,), dict(
    DESCRIPTOR = _OUTOFBANDINTERFACE_INTERFACESPECIFICINFO,
    __module__ = 'curie_types_pb2'
    # @@protoc_insertion_point(class_scope:nutanix.curie.OutOfBandInterface.InterfaceSpecificInfo)
    ))
  ,
  DESCRIPTOR = _OUTOFBANDINTERFACE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.OutOfBandInterface)
  ))
_sym_db.RegisterMessage(OutOfBandInterface)
_sym_db.RegisterMessage(OutOfBandInterface.InterfaceSpecificInfo)

ManagementServer = _reflection.GeneratedProtocolMessageType('ManagementServer', (_message.Message,), dict(
  DESCRIPTOR = _MANAGEMENTSERVER,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ManagementServer)
  ))
_sym_db.RegisterMessage(ManagementServer)

Hypervisor = _reflection.GeneratedProtocolMessageType('Hypervisor', (_message.Message,), dict(
  DESCRIPTOR = _HYPERVISOR,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.Hypervisor)
  ))
_sym_db.RegisterMessage(Hypervisor)

ClusteringSoftware = _reflection.GeneratedProtocolMessageType('ClusteringSoftware', (_message.Message,), dict(
  DESCRIPTOR = _CLUSTERINGSOFTWARE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ClusteringSoftware)
  ))
_sym_db.RegisterMessage(ClusteringSoftware)

VirtualMachine = _reflection.GeneratedProtocolMessageType('VirtualMachine', (_message.Message,), dict(
  DESCRIPTOR = _VIRTUALMACHINE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.VirtualMachine)
  ))
_sym_db.RegisterMessage(VirtualMachine)

ClusterNode = _reflection.GeneratedProtocolMessageType('ClusterNode', (_message.Message,), dict(
  DESCRIPTOR = _CLUSTERNODE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ClusterNode)
  ))
_sym_db.RegisterMessage(ClusterNode)

Cluster = _reflection.GeneratedProtocolMessageType('Cluster', (_message.Message,), dict(
  DESCRIPTOR = _CLUSTER,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.Cluster)
  ))
_sym_db.RegisterMessage(Cluster)

ClusterCollection = _reflection.GeneratedProtocolMessageType('ClusterCollection', (_message.Message,), dict(
  DESCRIPTOR = _CLUSTERCOLLECTION,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ClusterCollection)
  ))
_sym_db.RegisterMessage(ClusterCollection)

LogicalNetwork = _reflection.GeneratedProtocolMessageType('LogicalNetwork', (_message.Message,), dict(
  DESCRIPTOR = _LOGICALNETWORK,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.LogicalNetwork)
  ))
_sym_db.RegisterMessage(LogicalNetwork)

LibraryShare = _reflection.GeneratedProtocolMessageType('LibraryShare', (_message.Message,), dict(
  DESCRIPTOR = _LIBRARYSHARE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.LibraryShare)
  ))
_sym_db.RegisterMessage(LibraryShare)

LogicalStorage = _reflection.GeneratedProtocolMessageType('LogicalStorage', (_message.Message,), dict(
  DESCRIPTOR = _LOGICALSTORAGE,
  __module__ = 'curie_types_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.LogicalStorage)
  ))
_sym_db.RegisterMessage(LogicalStorage)


_CONNECTIONPARAMS.fields_by_name['username'].has_options = True
_CONNECTIONPARAMS.fields_by_name['username']._options = _descriptor._ParseOptions(descriptor_pb2.FieldOptions(), _b('\200\210\'\001'))
_CONNECTIONPARAMS.fields_by_name['password'].has_options = True
_CONNECTIONPARAMS.fields_by_name['password']._options = _descriptor._ParseOptions(descriptor_pb2.FieldOptions(), _b('\200\210\'\001'))
# @@protoc_insertion_point(module_scope)
