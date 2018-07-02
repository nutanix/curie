#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
"""
Utility for monkey-patching protoc generated message classes.

Provides support for associating encryption keys protobuf generated python
Message classes, decryption of sensitive fields, and auto-redaction of
sensitive fields when printing a human-friendly representation.

Example:

  >>> proto_patch_encryption_support(CurieSettings)
  >>> settings = CurieSettings()
  >>> settings.ParseFromString(open("input.bin").read())
  >>> cluster = settings.clusters[0]
  >>> vcenter_info = cluster.cluster_management_server_info
  >>> vcenter_info.is_encrypted_field("vcenter_password")
  True
  >>> vcenter_info.decrypt_field("vcenter_username", key=<SOME_KEY>)
  <PLAINTEXT USERNAME>
  >>> cluster.set_encryption_key(<SOME_KEY>)
  >>> vcenter_info.decrypt_field("vcenter_password")
  <PLAINTEXT PASSWORD>
  >>> print(vcenter_info)
  ...
  vcenter_user: "<REDACTED>"
  vcenter_password: "<REDACTED>"
  vcenter_datacenter_name: "CLUSTER-DC"
  vcenter_cluster_name: "CLUSTER"
  ...
"""


import logging

import google.protobuf.message
import google.protobuf.text_format

from curie.log import CHECK
from curie.util import CurieUtil, ENCRYPTED_FIELD_OPT

log = logging.getLogger(__name__)


#==============================================================================
# Private protobuf utils
#==============================================================================

def _get_top_level_type_desc(cls):
  """
  Returns the protobuf descriptor for the top-level parent of 'cls'.

  If 'cls' is itself top-level, then returns 'cls.DESCRIPTOR'.

  NB: This is essentially identical to
  'google.protobuf.descriptor._NestedDescriptorBase.GetTopLevelContainingType'.
  However, that method was removed as of 3.1.0, while the 'containing_type'
  attribute remains compatible with proto3.
  """
  curr_desc = cls.DESCRIPTOR
  while curr_desc.containing_type:
    curr_desc = curr_desc.containing_type
  return curr_desc

#==============================================================================
# Private encryption utils
#==============================================================================

def _decrypt_field(self, field, key=None):
  """
  Decrypts 'field' via CurieUtil.decrypt_aes256_cfb.

  If provided, 'key' is used, otherwise, if previously set, class-level
  encryption key returned by 'self.get_encryption_key' will be used.

  Args:
    field (str): Name of protobuf field to be decrypted.
    key (str|None): Optional. Encryption key to use, assumed to be a
      SHA-256 digest.

  Raises:
    AttributeError on invalid or non-encrypted 'field'.
    ValueError if no encryption key is provided.
  """
  key = key if key else self.get_encryption_key()
  if not self.is_encrypted_field(field):
    raise AttributeError("Field '%s' is not an encrypted field" % field)

  # TODO (jklein): Disable this transition behavior once all components are
  # in sync.
  if key is None:
    # For transition period, if 'key' is not set, assume field is still
    # plaintext.
    return getattr(self, field)
  #   raise ValueError("Encryption key cannot be None")

  return CurieUtil.decrypt_aes256_cfb(getattr(self, field), key)

def _get_encryption_key(cls):
  """
  Returns encryption key associated with this message class.
  """
  return _get_top_level_type_desc(cls)._ENCRYPTION_KEY

def _is_encrypted_field(cls, field):
  """
  Returns boolean indicating whether 'field' is marked for encryption.

  Args:
    field (str): Name of field to check.

  Raises:
    AttributeError if 'field' is not defined on message 'cls'.
  """
  field_desc = cls.DESCRIPTOR.fields_by_name.get(field)
  if field_desc is None:
    raise AttributeError("No '%s' field on message '%s'" %
                         (field, cls.DESCRIPTOR.full_name))

  field_options_desc = field_desc.GetOptions()
  if field_options_desc.HasExtension(ENCRYPTED_FIELD_OPT):
    return field_options_desc.Extensions[ENCRYPTED_FIELD_OPT]
  return False

def _set_encryption_key(cls, key):
  """
  Associates 'key' with this message class.

  To prevent data loss, the key may currently be initialized only once.

  Args:
    key (str): Encryption key to use, assumed to be a SHA-256 digest.

  Raises:
    FATAL on attempting to overwrite an existing key with a new value.
  """
  curr_key = getattr(_get_top_level_type_desc(cls), "_ENCRYPTION_KEY", None)
  if curr_key and curr_key == key:
    return

  CHECK(curr_key is None, msg="Encryption key has already been set %s %s" %
        (curr_key, key))
  setattr(_get_top_level_type_desc(cls), "_ENCRYPTION_KEY", key)

#==============================================================================
# Private monkey-patching utils
#==============================================================================

# Map of names to functions to be attached as methods on the message class.
_MIXIN_DICT = {"decrypt_field": _decrypt_field,
               "get_encryption_key": classmethod(_get_encryption_key),
               "is_encrypted_field": classmethod(_is_encrypted_field),
               "set_encryption_key": classmethod(_set_encryption_key)}

# Whether we have patched the PrintFieldValue function to support redaction.
_PATCHED = False

def _proto_patch_encryption_support(proto_desc):
  """
  Patches the protobuf message class associated with the descriptor.

  Args:
    proto_desc (google.protobuf.descriptor.Descriptor): Protobuf descriptor
      for message to be patched.
  """
  log.debug("Patching '%s'", proto_desc.name)
  for key, val in _MIXIN_DICT.items():
    setattr(proto_desc._concrete_class, key, val)
  proto_desc._concrete_class.set_encryption_key(None)
  for name, subproto_desc in proto_desc.nested_types_by_name.items():
    _proto_patch_encryption_support(subproto_desc)

def _patch_output():
  """
  Patches 'google.protobuf.text_format.PrintFieldValue' to support redacted
  fields.
  """
  global _PATCHED
  if not _PATCHED:
    _PATCHED = True

  _PrintFieldValue = google.protobuf.text_format.PrintFieldValue

  def PrintFieldValueRedacted(field, value, out, *args, **kwargs):
    field_options_desc = field.GetOptions()
    if(field_options_desc.HasExtension(ENCRYPTED_FIELD_OPT) and
       field_options_desc.Extensions[ENCRYPTED_FIELD_OPT]):
      value = "<REDACTED>"
    _PrintFieldValue(field, value, out, *args, **kwargs)

  google.protobuf.text_format.PrintFieldValue = PrintFieldValueRedacted

#==============================================================================
# Public functions
#==============================================================================

def proto_patch_encryption_support(proto):
  """
  Patches protobuf formatters if necessary as well as extending the message
  class 'proto' to support encrypted fields.

  Args:
    proto (google.protobuf.message.Message) Protobuf message class to patch.

  Returns:
    (google.protobuf.message.Message) Patched 'proto'.
  """
  # If necessary, patch protobuf output method to support redacted fields.
  _patch_output()
  if isinstance(proto, google.protobuf.message.Message):
    raise TypeError("Patching should be applied to the concrete Message "
                    "subclass, not an instance of the class")
  if not issubclass(proto, google.protobuf.message.Message):
    raise TypeError(
      "Must provide a concrete protobuf Message subclass not '%r'" % proto)

  _proto_patch_encryption_support(proto.DESCRIPTOR)
  return proto
