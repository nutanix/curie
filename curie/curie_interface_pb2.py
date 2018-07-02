# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: curie_interface.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import service as _service
from google.protobuf import service_reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import curie_extensions_pb2 as curie__extensions__pb2
import curie_server_state_pb2 as curie__server__state__pb2
import curie_test_pb2 as curie__test__pb2
import curie_types_pb2 as curie__types__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='curie_interface.proto',
  package='nutanix.curie',
  syntax='proto2',
  serialized_pb=_b('\n\x15\x63urie_interface.proto\x12\rnutanix.curie\x1a\x16\x63urie_extensions.proto\x1a\x18\x63urie_server_state.proto\x1a\x10\x63urie_test.proto\x1a\x11\x63urie_types.proto\"J\n\x16ScenarioValidationInfo\x12\x0c\n\x04path\x18\x01 \x02(\t\x12\x10\n\x08is_valid\x18\x02 \x02(\x08\x12\x10\n\x08messages\x18\x03 \x03(\t\"=\n\x12ServerStatusGetArg\x12\'\n\x18include_persistent_state\x18\x01 \x01(\x08:\x05\x66\x61lse\"q\n\x12ServerStatusGetRet\x12+\n\x06status\x18\x01 \x01(\x0b\x32\x1b.nutanix.curie.ServerStatus\x12.\n\x05state\x18\x02 \x01(\x0b\x32\x1f.nutanix.curie.CurieServerState\" \n\x0fTestValidateArg\x12\r\n\x05paths\x18\x01 \x03(\t\"Q\n\x0fTestValidateRet\x12>\n\x0fvalidation_info\x18\x01 \x03(\x0b\x32%.nutanix.curie.ScenarioValidationInfo\"\x10\n\x0eTestCatalogArg\"N\n\x0eTestCatalogRet\x12<\n\x12test_metadata_list\x18\x01 \x03(\x0b\x32 .nutanix.curie.CurieTestMetadata\" \n\rTestStatusArg\x12\x0f\n\x07test_id\x18\x01 \x02(\x03\"v\n\rTestStatusRet\x12/\n\ttest_info\x18\x01 \x02(\x0b\x32\x1c.nutanix.curie.CurieTestInfo\x12\x34\n\x0ctest_results\x18\x02 \x03(\x0b\x32\x1e.nutanix.curie.CurieTestResult\"M\n\x15\x44iscoverClustersV2Arg\x12\x34\n\x0bmgmt_server\x18\x01 \x01(\x0b\x32\x1f.nutanix.curie.ManagementServer\"\xbf\x01\n\x15\x44iscoverClustersV2Ret\x12P\n\x11\x63luster_inventory\x18\x01 \x02(\x0b\x32\x35.nutanix.curie.DiscoverClustersV2Ret.ClusterInventory\x1aT\n\x10\x43lusterInventory\x12@\n\x16\x63luster_collection_vec\x18\x01 \x03(\x0b\x32 .nutanix.curie.ClusterCollection\"\xbd\x01\n\x12\x44iscoverNodesV2Arg\x12\x34\n\x0bmgmt_server\x18\x01 \x01(\x0b\x32\x1f.nutanix.curie.ManagementServer\x12<\n\x12\x63luster_collection\x18\x02 \x01(\x0b\x32 .nutanix.curie.ClusterCollection\x12\x33\n\x08oob_info\x18\x03 \x01(\x0b\x32!.nutanix.curie.OutOfBandInterface\"\xb7\x01\n\x12\x44iscoverNodesV2Ret\x12M\n\x13node_collection_vec\x18\x02 \x03(\x0b\x32\x30.nutanix.curie.DiscoverNodesV2Ret.NodeCollection\x1aR\n\x0eNodeCollection\x12\x12\n\ncluster_id\x18\x01 \x01(\t\x12,\n\x08node_vec\x18\x02 \x03(\x0b\x32\x1a.nutanix.curie.ClusterNode\"T\n\x1bUpdateAndValidateClusterArg\x12\x35\n\x07\x63luster\x18\x01 \x01(\x0b\x32$.nutanix.curie.CurieSettings.Cluster\"\xf5\x02\n\x1bUpdateAndValidateClusterRet\x12\x35\n\x07\x63luster\x18\x01 \x01(\x0b\x32$.nutanix.curie.CurieSettings.Cluster\x12\\\n\x16validation_message_vec\x18\x03 \x03(\x0b\x32<.nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage\x1a\xc0\x01\n\x11ValidationMessage\x12O\n\x04type\x18\x01 \x01(\x0e\x32\x41.nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage.Type\x12\x0b\n\x03msg\x18\x02 \x01(\t\x12\x0f\n\x07\x64\x65tails\x18\x03 \x01(\t\"<\n\x04Type\x12\x0c\n\x08kUnknown\x10\x01\x12\x0c\n\x08kUpdated\x10\x02\x12\x0c\n\x08kWarning\x10\x03\x12\n\n\x06kError\x10\x04\x32\xad\x05\n\x0b\x43urieRpcSvc\x12W\n\x0fServerStatusGet\x12!.nutanix.curie.ServerStatusGetArg\x1a!.nutanix.curie.ServerStatusGetRet\x12U\n\x0cTestValidate\x12\x1e.nutanix.curie.TestValidateArg\x1a\x1e.nutanix.curie.TestValidateRet\"\x05\xc0\xe4\x1d\x90N\x12K\n\x0bTestCatalog\x12\x1d.nutanix.curie.TestCatalogArg\x1a\x1d.nutanix.curie.TestCatalogRet\x12O\n\nTestStatus\x12\x1c.nutanix.curie.TestStatusArg\x1a\x1c.nutanix.curie.TestStatusRet\"\x05\xc0\xe4\x1d\xe8\x07\x12h\n\x12\x44iscoverClustersV2\x12$.nutanix.curie.DiscoverClustersV2Arg\x1a$.nutanix.curie.DiscoverClustersV2Ret\"\x06\xc0\xe4\x1d\xe0\xa7\x12\x12_\n\x0f\x44iscoverNodesV2\x12!.nutanix.curie.DiscoverNodesV2Arg\x1a!.nutanix.curie.DiscoverNodesV2Ret\"\x06\xc0\xe4\x1d\xe0\xa7\x12\x12z\n\x18UpdateAndValidateCluster\x12*.nutanix.curie.UpdateAndValidateClusterArg\x1a*.nutanix.curie.UpdateAndValidateClusterRet\"\x06\xc0\xe4\x1d\xe0\xa7\x12\x1a\t\xc0\xf3\x18\xf4\x03\xc8\xf3\x18\x00\x42\x03\x90\x01\x01')
  ,
  dependencies=[curie__extensions__pb2.DESCRIPTOR,curie__server__state__pb2.DESCRIPTOR,curie__test__pb2.DESCRIPTOR,curie__types__pb2.DESCRIPTOR,])
_sym_db.RegisterFileDescriptor(DESCRIPTOR)



_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='kUnknown', index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kUpdated', index=1, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kWarning', index=2, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='kError', index=3, number=4,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1801,
  serialized_end=1861,
)
_sym_db.RegisterEnumDescriptor(_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE_TYPE)


_SCENARIOVALIDATIONINFO = _descriptor.Descriptor(
  name='ScenarioValidationInfo',
  full_name='nutanix.curie.ScenarioValidationInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='path', full_name='nutanix.curie.ScenarioValidationInfo.path', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='is_valid', full_name='nutanix.curie.ScenarioValidationInfo.is_valid', index=1,
      number=2, type=8, cpp_type=7, label=2,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='messages', full_name='nutanix.curie.ScenarioValidationInfo.messages', index=2,
      number=3, type=9, cpp_type=9, label=3,
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
  serialized_start=127,
  serialized_end=201,
)


_SERVERSTATUSGETARG = _descriptor.Descriptor(
  name='ServerStatusGetArg',
  full_name='nutanix.curie.ServerStatusGetArg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='include_persistent_state', full_name='nutanix.curie.ServerStatusGetArg.include_persistent_state', index=0,
      number=1, type=8, cpp_type=7, label=1,
      has_default_value=True, default_value=False,
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
  serialized_start=203,
  serialized_end=264,
)


_SERVERSTATUSGETRET = _descriptor.Descriptor(
  name='ServerStatusGetRet',
  full_name='nutanix.curie.ServerStatusGetRet',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='status', full_name='nutanix.curie.ServerStatusGetRet.status', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='state', full_name='nutanix.curie.ServerStatusGetRet.state', index=1,
      number=2, type=11, cpp_type=10, label=1,
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
  serialized_start=266,
  serialized_end=379,
)


_TESTVALIDATEARG = _descriptor.Descriptor(
  name='TestValidateArg',
  full_name='nutanix.curie.TestValidateArg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='paths', full_name='nutanix.curie.TestValidateArg.paths', index=0,
      number=1, type=9, cpp_type=9, label=3,
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
  serialized_start=381,
  serialized_end=413,
)


_TESTVALIDATERET = _descriptor.Descriptor(
  name='TestValidateRet',
  full_name='nutanix.curie.TestValidateRet',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='validation_info', full_name='nutanix.curie.TestValidateRet.validation_info', index=0,
      number=1, type=11, cpp_type=10, label=3,
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
  serialized_start=415,
  serialized_end=496,
)


_TESTCATALOGARG = _descriptor.Descriptor(
  name='TestCatalogArg',
  full_name='nutanix.curie.TestCatalogArg',
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
  serialized_start=498,
  serialized_end=514,
)


_TESTCATALOGRET = _descriptor.Descriptor(
  name='TestCatalogRet',
  full_name='nutanix.curie.TestCatalogRet',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='test_metadata_list', full_name='nutanix.curie.TestCatalogRet.test_metadata_list', index=0,
      number=1, type=11, cpp_type=10, label=3,
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
  serialized_start=516,
  serialized_end=594,
)


_TESTSTATUSARG = _descriptor.Descriptor(
  name='TestStatusArg',
  full_name='nutanix.curie.TestStatusArg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='test_id', full_name='nutanix.curie.TestStatusArg.test_id', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
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
  serialized_start=596,
  serialized_end=628,
)


_TESTSTATUSRET = _descriptor.Descriptor(
  name='TestStatusRet',
  full_name='nutanix.curie.TestStatusRet',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='test_info', full_name='nutanix.curie.TestStatusRet.test_info', index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='test_results', full_name='nutanix.curie.TestStatusRet.test_results', index=1,
      number=2, type=11, cpp_type=10, label=3,
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
  serialized_start=630,
  serialized_end=748,
)


_DISCOVERCLUSTERSV2ARG = _descriptor.Descriptor(
  name='DiscoverClustersV2Arg',
  full_name='nutanix.curie.DiscoverClustersV2Arg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='mgmt_server', full_name='nutanix.curie.DiscoverClustersV2Arg.mgmt_server', index=0,
      number=1, type=11, cpp_type=10, label=1,
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
  serialized_start=750,
  serialized_end=827,
)


_DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY = _descriptor.Descriptor(
  name='ClusterInventory',
  full_name='nutanix.curie.DiscoverClustersV2Ret.ClusterInventory',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cluster_collection_vec', full_name='nutanix.curie.DiscoverClustersV2Ret.ClusterInventory.cluster_collection_vec', index=0,
      number=1, type=11, cpp_type=10, label=3,
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
  serialized_start=937,
  serialized_end=1021,
)

_DISCOVERCLUSTERSV2RET = _descriptor.Descriptor(
  name='DiscoverClustersV2Ret',
  full_name='nutanix.curie.DiscoverClustersV2Ret',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cluster_inventory', full_name='nutanix.curie.DiscoverClustersV2Ret.cluster_inventory', index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=830,
  serialized_end=1021,
)


_DISCOVERNODESV2ARG = _descriptor.Descriptor(
  name='DiscoverNodesV2Arg',
  full_name='nutanix.curie.DiscoverNodesV2Arg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='mgmt_server', full_name='nutanix.curie.DiscoverNodesV2Arg.mgmt_server', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='cluster_collection', full_name='nutanix.curie.DiscoverNodesV2Arg.cluster_collection', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='oob_info', full_name='nutanix.curie.DiscoverNodesV2Arg.oob_info', index=2,
      number=3, type=11, cpp_type=10, label=1,
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
  serialized_start=1024,
  serialized_end=1213,
)


_DISCOVERNODESV2RET_NODECOLLECTION = _descriptor.Descriptor(
  name='NodeCollection',
  full_name='nutanix.curie.DiscoverNodesV2Ret.NodeCollection',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cluster_id', full_name='nutanix.curie.DiscoverNodesV2Ret.NodeCollection.cluster_id', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='node_vec', full_name='nutanix.curie.DiscoverNodesV2Ret.NodeCollection.node_vec', index=1,
      number=2, type=11, cpp_type=10, label=3,
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
  serialized_start=1317,
  serialized_end=1399,
)

_DISCOVERNODESV2RET = _descriptor.Descriptor(
  name='DiscoverNodesV2Ret',
  full_name='nutanix.curie.DiscoverNodesV2Ret',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='node_collection_vec', full_name='nutanix.curie.DiscoverNodesV2Ret.node_collection_vec', index=0,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_DISCOVERNODESV2RET_NODECOLLECTION, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1216,
  serialized_end=1399,
)


_UPDATEANDVALIDATECLUSTERARG = _descriptor.Descriptor(
  name='UpdateAndValidateClusterArg',
  full_name='nutanix.curie.UpdateAndValidateClusterArg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cluster', full_name='nutanix.curie.UpdateAndValidateClusterArg.cluster', index=0,
      number=1, type=11, cpp_type=10, label=1,
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
  serialized_start=1401,
  serialized_end=1485,
)


_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE = _descriptor.Descriptor(
  name='ValidationMessage',
  full_name='nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage.type', index=0,
      number=1, type=14, cpp_type=8, label=1,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='msg', full_name='nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage.msg', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='details', full_name='nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage.details', index=2,
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
    _UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1669,
  serialized_end=1861,
)

_UPDATEANDVALIDATECLUSTERRET = _descriptor.Descriptor(
  name='UpdateAndValidateClusterRet',
  full_name='nutanix.curie.UpdateAndValidateClusterRet',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='cluster', full_name='nutanix.curie.UpdateAndValidateClusterRet.cluster', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='validation_message_vec', full_name='nutanix.curie.UpdateAndValidateClusterRet.validation_message_vec', index=1,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=1488,
  serialized_end=1861,
)

_SERVERSTATUSGETRET.fields_by_name['status'].message_type = curie__server__state__pb2._SERVERSTATUS
_SERVERSTATUSGETRET.fields_by_name['state'].message_type = curie__server__state__pb2._CURIESERVERSTATE
_TESTVALIDATERET.fields_by_name['validation_info'].message_type = _SCENARIOVALIDATIONINFO
_TESTCATALOGRET.fields_by_name['test_metadata_list'].message_type = curie__test__pb2._CURIETESTMETADATA
_TESTSTATUSRET.fields_by_name['test_info'].message_type = curie__test__pb2._CURIETESTINFO
_TESTSTATUSRET.fields_by_name['test_results'].message_type = curie__test__pb2._CURIETESTRESULT
_DISCOVERCLUSTERSV2ARG.fields_by_name['mgmt_server'].message_type = curie__types__pb2._MANAGEMENTSERVER
_DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY.fields_by_name['cluster_collection_vec'].message_type = curie__types__pb2._CLUSTERCOLLECTION
_DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY.containing_type = _DISCOVERCLUSTERSV2RET
_DISCOVERCLUSTERSV2RET.fields_by_name['cluster_inventory'].message_type = _DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY
_DISCOVERNODESV2ARG.fields_by_name['mgmt_server'].message_type = curie__types__pb2._MANAGEMENTSERVER
_DISCOVERNODESV2ARG.fields_by_name['cluster_collection'].message_type = curie__types__pb2._CLUSTERCOLLECTION
_DISCOVERNODESV2ARG.fields_by_name['oob_info'].message_type = curie__types__pb2._OUTOFBANDINTERFACE
_DISCOVERNODESV2RET_NODECOLLECTION.fields_by_name['node_vec'].message_type = curie__types__pb2._CLUSTERNODE
_DISCOVERNODESV2RET_NODECOLLECTION.containing_type = _DISCOVERNODESV2RET
_DISCOVERNODESV2RET.fields_by_name['node_collection_vec'].message_type = _DISCOVERNODESV2RET_NODECOLLECTION
_UPDATEANDVALIDATECLUSTERARG.fields_by_name['cluster'].message_type = curie__server__state__pb2._CURIESETTINGS_CLUSTER
_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE.fields_by_name['type'].enum_type = _UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE_TYPE
_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE.containing_type = _UPDATEANDVALIDATECLUSTERRET
_UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE_TYPE.containing_type = _UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE
_UPDATEANDVALIDATECLUSTERRET.fields_by_name['cluster'].message_type = curie__server__state__pb2._CURIESETTINGS_CLUSTER
_UPDATEANDVALIDATECLUSTERRET.fields_by_name['validation_message_vec'].message_type = _UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE
DESCRIPTOR.message_types_by_name['ScenarioValidationInfo'] = _SCENARIOVALIDATIONINFO
DESCRIPTOR.message_types_by_name['ServerStatusGetArg'] = _SERVERSTATUSGETARG
DESCRIPTOR.message_types_by_name['ServerStatusGetRet'] = _SERVERSTATUSGETRET
DESCRIPTOR.message_types_by_name['TestValidateArg'] = _TESTVALIDATEARG
DESCRIPTOR.message_types_by_name['TestValidateRet'] = _TESTVALIDATERET
DESCRIPTOR.message_types_by_name['TestCatalogArg'] = _TESTCATALOGARG
DESCRIPTOR.message_types_by_name['TestCatalogRet'] = _TESTCATALOGRET
DESCRIPTOR.message_types_by_name['TestStatusArg'] = _TESTSTATUSARG
DESCRIPTOR.message_types_by_name['TestStatusRet'] = _TESTSTATUSRET
DESCRIPTOR.message_types_by_name['DiscoverClustersV2Arg'] = _DISCOVERCLUSTERSV2ARG
DESCRIPTOR.message_types_by_name['DiscoverClustersV2Ret'] = _DISCOVERCLUSTERSV2RET
DESCRIPTOR.message_types_by_name['DiscoverNodesV2Arg'] = _DISCOVERNODESV2ARG
DESCRIPTOR.message_types_by_name['DiscoverNodesV2Ret'] = _DISCOVERNODESV2RET
DESCRIPTOR.message_types_by_name['UpdateAndValidateClusterArg'] = _UPDATEANDVALIDATECLUSTERARG
DESCRIPTOR.message_types_by_name['UpdateAndValidateClusterRet'] = _UPDATEANDVALIDATECLUSTERRET

ScenarioValidationInfo = _reflection.GeneratedProtocolMessageType('ScenarioValidationInfo', (_message.Message,), dict(
  DESCRIPTOR = _SCENARIOVALIDATIONINFO,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ScenarioValidationInfo)
  ))
_sym_db.RegisterMessage(ScenarioValidationInfo)

ServerStatusGetArg = _reflection.GeneratedProtocolMessageType('ServerStatusGetArg', (_message.Message,), dict(
  DESCRIPTOR = _SERVERSTATUSGETARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ServerStatusGetArg)
  ))
_sym_db.RegisterMessage(ServerStatusGetArg)

ServerStatusGetRet = _reflection.GeneratedProtocolMessageType('ServerStatusGetRet', (_message.Message,), dict(
  DESCRIPTOR = _SERVERSTATUSGETRET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.ServerStatusGetRet)
  ))
_sym_db.RegisterMessage(ServerStatusGetRet)

TestValidateArg = _reflection.GeneratedProtocolMessageType('TestValidateArg', (_message.Message,), dict(
  DESCRIPTOR = _TESTVALIDATEARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestValidateArg)
  ))
_sym_db.RegisterMessage(TestValidateArg)

TestValidateRet = _reflection.GeneratedProtocolMessageType('TestValidateRet', (_message.Message,), dict(
  DESCRIPTOR = _TESTVALIDATERET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestValidateRet)
  ))
_sym_db.RegisterMessage(TestValidateRet)

TestCatalogArg = _reflection.GeneratedProtocolMessageType('TestCatalogArg', (_message.Message,), dict(
  DESCRIPTOR = _TESTCATALOGARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestCatalogArg)
  ))
_sym_db.RegisterMessage(TestCatalogArg)

TestCatalogRet = _reflection.GeneratedProtocolMessageType('TestCatalogRet', (_message.Message,), dict(
  DESCRIPTOR = _TESTCATALOGRET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestCatalogRet)
  ))
_sym_db.RegisterMessage(TestCatalogRet)

TestStatusArg = _reflection.GeneratedProtocolMessageType('TestStatusArg', (_message.Message,), dict(
  DESCRIPTOR = _TESTSTATUSARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestStatusArg)
  ))
_sym_db.RegisterMessage(TestStatusArg)

TestStatusRet = _reflection.GeneratedProtocolMessageType('TestStatusRet', (_message.Message,), dict(
  DESCRIPTOR = _TESTSTATUSRET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.TestStatusRet)
  ))
_sym_db.RegisterMessage(TestStatusRet)

DiscoverClustersV2Arg = _reflection.GeneratedProtocolMessageType('DiscoverClustersV2Arg', (_message.Message,), dict(
  DESCRIPTOR = _DISCOVERCLUSTERSV2ARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverClustersV2Arg)
  ))
_sym_db.RegisterMessage(DiscoverClustersV2Arg)

DiscoverClustersV2Ret = _reflection.GeneratedProtocolMessageType('DiscoverClustersV2Ret', (_message.Message,), dict(

  ClusterInventory = _reflection.GeneratedProtocolMessageType('ClusterInventory', (_message.Message,), dict(
    DESCRIPTOR = _DISCOVERCLUSTERSV2RET_CLUSTERINVENTORY,
    __module__ = 'curie_interface_pb2'
    # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverClustersV2Ret.ClusterInventory)
    ))
  ,
  DESCRIPTOR = _DISCOVERCLUSTERSV2RET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverClustersV2Ret)
  ))
_sym_db.RegisterMessage(DiscoverClustersV2Ret)
_sym_db.RegisterMessage(DiscoverClustersV2Ret.ClusterInventory)

DiscoverNodesV2Arg = _reflection.GeneratedProtocolMessageType('DiscoverNodesV2Arg', (_message.Message,), dict(
  DESCRIPTOR = _DISCOVERNODESV2ARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverNodesV2Arg)
  ))
_sym_db.RegisterMessage(DiscoverNodesV2Arg)

DiscoverNodesV2Ret = _reflection.GeneratedProtocolMessageType('DiscoverNodesV2Ret', (_message.Message,), dict(

  NodeCollection = _reflection.GeneratedProtocolMessageType('NodeCollection', (_message.Message,), dict(
    DESCRIPTOR = _DISCOVERNODESV2RET_NODECOLLECTION,
    __module__ = 'curie_interface_pb2'
    # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverNodesV2Ret.NodeCollection)
    ))
  ,
  DESCRIPTOR = _DISCOVERNODESV2RET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.DiscoverNodesV2Ret)
  ))
_sym_db.RegisterMessage(DiscoverNodesV2Ret)
_sym_db.RegisterMessage(DiscoverNodesV2Ret.NodeCollection)

UpdateAndValidateClusterArg = _reflection.GeneratedProtocolMessageType('UpdateAndValidateClusterArg', (_message.Message,), dict(
  DESCRIPTOR = _UPDATEANDVALIDATECLUSTERARG,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.UpdateAndValidateClusterArg)
  ))
_sym_db.RegisterMessage(UpdateAndValidateClusterArg)

UpdateAndValidateClusterRet = _reflection.GeneratedProtocolMessageType('UpdateAndValidateClusterRet', (_message.Message,), dict(

  ValidationMessage = _reflection.GeneratedProtocolMessageType('ValidationMessage', (_message.Message,), dict(
    DESCRIPTOR = _UPDATEANDVALIDATECLUSTERRET_VALIDATIONMESSAGE,
    __module__ = 'curie_interface_pb2'
    # @@protoc_insertion_point(class_scope:nutanix.curie.UpdateAndValidateClusterRet.ValidationMessage)
    ))
  ,
  DESCRIPTOR = _UPDATEANDVALIDATECLUSTERRET,
  __module__ = 'curie_interface_pb2'
  # @@protoc_insertion_point(class_scope:nutanix.curie.UpdateAndValidateClusterRet)
  ))
_sym_db.RegisterMessage(UpdateAndValidateClusterRet)
_sym_db.RegisterMessage(UpdateAndValidateClusterRet.ValidationMessage)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\220\001\001'))

_CURIERPCSVC = _descriptor.ServiceDescriptor(
  name='CurieRpcSvc',
  full_name='nutanix.curie.CurieRpcSvc',
  file=DESCRIPTOR,
  index=0,
  options=_descriptor._ParseOptions(descriptor_pb2.ServiceOptions(), _b('\300\363\030\364\003\310\363\030\000')),
  serialized_start=1864,
  serialized_end=2549,
  methods=[
  _descriptor.MethodDescriptor(
    name='ServerStatusGet',
    full_name='nutanix.curie.CurieRpcSvc.ServerStatusGet',
    index=0,
    containing_service=None,
    input_type=_SERVERSTATUSGETARG,
    output_type=_SERVERSTATUSGETRET,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='TestValidate',
    full_name='nutanix.curie.CurieRpcSvc.TestValidate',
    index=1,
    containing_service=None,
    input_type=_TESTVALIDATEARG,
    output_type=_TESTVALIDATERET,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\300\344\035\220N')),
  ),
  _descriptor.MethodDescriptor(
    name='TestCatalog',
    full_name='nutanix.curie.CurieRpcSvc.TestCatalog',
    index=2,
    containing_service=None,
    input_type=_TESTCATALOGARG,
    output_type=_TESTCATALOGRET,
    options=None,
  ),
  _descriptor.MethodDescriptor(
    name='TestStatus',
    full_name='nutanix.curie.CurieRpcSvc.TestStatus',
    index=3,
    containing_service=None,
    input_type=_TESTSTATUSARG,
    output_type=_TESTSTATUSRET,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\300\344\035\350\007')),
  ),
  _descriptor.MethodDescriptor(
    name='DiscoverClustersV2',
    full_name='nutanix.curie.CurieRpcSvc.DiscoverClustersV2',
    index=4,
    containing_service=None,
    input_type=_DISCOVERCLUSTERSV2ARG,
    output_type=_DISCOVERCLUSTERSV2RET,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\300\344\035\340\247\022')),
  ),
  _descriptor.MethodDescriptor(
    name='DiscoverNodesV2',
    full_name='nutanix.curie.CurieRpcSvc.DiscoverNodesV2',
    index=5,
    containing_service=None,
    input_type=_DISCOVERNODESV2ARG,
    output_type=_DISCOVERNODESV2RET,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\300\344\035\340\247\022')),
  ),
  _descriptor.MethodDescriptor(
    name='UpdateAndValidateCluster',
    full_name='nutanix.curie.CurieRpcSvc.UpdateAndValidateCluster',
    index=6,
    containing_service=None,
    input_type=_UPDATEANDVALIDATECLUSTERARG,
    output_type=_UPDATEANDVALIDATECLUSTERRET,
    options=_descriptor._ParseOptions(descriptor_pb2.MethodOptions(), _b('\300\344\035\340\247\022')),
  ),
])

CurieRpcSvc = service_reflection.GeneratedServiceType('CurieRpcSvc', (_service.Service,), dict(
  DESCRIPTOR = _CURIERPCSVC,
  __module__ = 'curie_interface_pb2'
  ))

CurieRpcSvc_Stub = service_reflection.GeneratedServiceStubType('CurieRpcSvc_Stub', (CurieRpcSvc,), dict(
  DESCRIPTOR = _CURIERPCSVC,
  __module__ = 'curie_interface_pb2'
  ))


# @@protoc_insertion_point(module_scope)
