#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import json
import re
from abc import ABCMeta
from collections import MutableMapping


#==============================================================================
# Util methods
#==============================================================================

class JsonDataEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, JsonData):
      return obj.json()
    return super(JsonDataEncoder, self).default(obj)

def _check_arguments(
    bound_method, args=None, kwargs=None, min_args=0, max_args=None,
    allow_kwargs=False, require_kwargs=False):

  def format_name():
    return "%s.%s" % (type(bound_method.__self__).__name__,
                      bound_method.__name__)

  args = [] if args is None else args
  kwargs = {} if kwargs is None else kwargs

  if len(args) < min_args:
    raise TypeError("%s: not enough arguments" % format_name())
  if max_args is not None and len(args) > max_args:
    raise TypeError("%s: takes at most %d arguments, (%s given)" %
                    (format_name(), max_args, len(args)))
  if kwargs and not allow_kwargs:
    raise TypeError("%s: no kwargs expected" % format_name())
  if require_kwargs and not kwargs:
    raise TypeError("%s: kwargs expected, none provided" % format_name())

#==============================================================================
# Enum helpers
#==============================================================================

class EnumElement(object):
  """
  Data representing a particular value for an Enum class.
  """

  def __init__(self, name, value, parent_enum):
    self.name = name
    self.value = value
    self.parent_enum = parent_enum

  def __eq__(self, other):
    if isinstance(other, (int, long)):
      return self.value == other
    elif isinstance(other, basestring):
      return self.name == other
    elif isinstance(other, EnumElement):
      if self.value == other.value and self.parent_enum == other.parent_enum:
        return True
    return False

  def __lt__(self, other):
    if isinstance(other, (int, long)):
      return self.value < other
    elif isinstance(other, basestring):
      return self.name < other
    elif isinstance(other, EnumElement):
      if self.parent_enum == other.parent_enum:
        return self.value < other.value
    raise TypeError("%s and %s (%s)are not comparable" %
                    (self, other, type(other).__name__))

  def __ne__(self, other):
    return not self == other

  def __le__(self, other):
    return self == other or self < other

  def __gt__(self, other):
    return not (self <= other)

  def __ge__(self, other):
    return not (self < other)

  def __hash__(self):
    # Ensure aliases hash to the same value, but that numerically equivalent
    # elements from different Enums are likely distinct.
    return self.value ^ hash(self.parent_enum)

  def __nonzero__(self):
    return self.value != 0

  def __int__(self):
    return self.value

  def __str__(self):
    return self.name

  def __repr__(self):
    return "<Enum %s::%s = %s>" % (self.parent_enum, self.name, self.value)

class EnumMeta(type):
  """
  Metaclass enabling syntactic sugar for 'Enum'.
  """
  def __new__(mcs, name, bases, dct):
    # Convert numeric fields to 'EnumElement's.
    str_int_map = {}
    int_str_map = {}
    element_list = []
    for key, val in dct.items():
      if not isinstance(val, (int, long)):
        continue

      dct[key] = EnumElement(key, val, name)
      element_list.append(dct[key])
      str_int_map[key] = val
      int_str_map.setdefault(val, []).append(key)

    dct["__STR_INT_MAP__"] = str_int_map
    dct["__INT_STR_MAP__"] = int_str_map
    dct["__ENUM_ELEMENT_LIST__"] = element_list

    cls = super(EnumMeta, mcs).__new__(mcs, name, bases, dct)
    super(EnumMeta, cls).__init__(name, bases, dct)
    return cls

  def __iter__(cls):
    return iter(cls.__ENUM_ELEMENT_LIST__)

  def __contains__(cls, key):
    if isinstance(key, EnumElement):
      if cls.__name__ != key.parent_enum:
        return False
      key = key.value

    if isinstance(key, (int, long)):
      return key in cls.__INT_STR_MAP__.keys()
    elif isinstance(key, basestring):
      return (key in cls.__STR_INT_MAP__.keys())

    return False

  def __call__(cls, val):
    """
    Attempt to cast 'val' to an appropriate element of the enum.

    Args:
      val (basestring|int|long|EnumElement): Value to cast.

    Returns:
      (EnumElement) Associated element on success.

    Raises:
      TypeError: 'val' is not of an expected type
      ValueError: 'val' cannot be matched to an element of the enum.
    """
    if not isinstance(val, (basestring, int, long, EnumElement)):
      raise TypeError("Cannot cast '%s' (%s) to an Enum element" %
                      (val, val.__class__))

    # Give precedence to alias over value.
    if isinstance(val, EnumElement):
      if val.name in cls:
        val = val.name
      else:
        val = val.value

    if isinstance(val, basestring):
      if val not in cls.__STR_INT_MAP__.keys():
        raise ValueError("Enum '%s' contains no element '%s'" %
                         (cls.__name__, val))
      return getattr(cls, val)
    elif isinstance(val, (int, long)):
      if val not in cls.__INT_STR_MAP__.keys():
        raise ValueError("Enum '%s' contains no element with value '%s'" %
                         (cls.__name__, val))

      # In case of aliased values, return first declared alias.
      return getattr(cls, cls.__INT_STR_MAP__[val][0])

class Enum(object):
  """
  Class which provides "enum" functionality.

  Declared as syntactic sugar. (Subclass 'Enum' rather than set metaclass).
  """
  __metaclass__ = EnumMeta

#==============================================================================
# Schema helpers
#==============================================================================

class _TypedList(list):
  def __init__(self, *args, **kwargs):
    if "strict" in kwargs:
      self.strict = kwargs.pop("strict")
    else:
      self.strict = False

    if kwargs:
      raise TypeError("_TypedList.__init__: Unexpected kwargs: %s" % kwargs)

    _check_arguments(self.__init__, args, kwargs, min_args=1, max_args=2,
                     allow_kwargs=True)
    if not issubclass(type(args[0]), type):
      raise TypeError("_TypedList.__init__: First argument is not a type")

    # Allow this to raise type error if not iterable as is consistent with
    # the behavior of 'list'.
    self.__element_type__ = args[0]
    initial = self.__coerce_list(args[1]) if len(args) == 2 else []
    super(_TypedList, self).__init__(initial)

  def append(self, val):
    return super(_TypedList, self).append(self.__coerce_value(val))

  def extend(self, lst):
    return super(_TypedList, self).extend(self.__coerce_list(lst))

  def insert(self, index, val):
    return super(_TypedList, self).insert(index, self.__coerce_value(val))

  def __setitem__(self, index, val):
    return super(_TypedList, self).__setitem__(index, self.__coerce_value(val))

  def __setslice__(self, start, end, lst):
    return super(_TypedList, self).__setslice__(start, end,
                                                self.__coerce_list(lst))

  def __coerce_value(self, val):
    if not isinstance(val, self.__element_type__):
      if self.strict:
        raise TypeError("Coercion disallowed for field with strict=True")
      val = self.__element_type__(val)
    return val

  def __coerce_list(self, lst):
    return map(self.__coerce_value, lst)

class _TypedDict(dict):
  def __init__(self, *args, **kwargs):
    _check_arguments(self.__init__, args, kwargs, min_args=1, max_args=2,
                     allow_kwargs=True)

    self.__element_type__ = args[0]

    initial = {}
    initial.update(self.__coerce_dict(kwargs))
    # Allow this to raise type error if not iterable as is consistent with
    # the behavior of 'dict'.
    if len(args) == 2:
      # Update 'initial' assuming that '*args' consists of key/value pairs.
      initial.update(dict(zip(
          # Extract key from the key/value pairs.
          map(lambda p: p[0], args[1]),
          # Extract value from the key/value pairs, and attempt to coerce if
          # necessary.
          map(lambda p: self.__coerce_value(p[1])))))
    super(_TypedDict, self).__init__(initial)

  def setdefault(self, key, *args):
    _check_arguments(self.__init__, args, max_args=2)
    if len(args) > 1:
      raise TypeError("setdefault expected at most 2 arguments, got %d" %
                      1 + len(args))

    if args:
      args[0] = self.__coerce_value(args[0])

    return super(_TypedDict, self).setdefault(key, *args)

  def update(self, *args, **kwargs):
    _check_arguments(self.__init__, args, max_args=1)

    if args:
      arg = args[0]
      if hasattr(arg, "keys"):
        for k in arg.keys():
          arg[k] = self.__coerce_value(arg[k])
      else:
        for k, v in arg:
          arg[k] = self.__coerce_value(v)

    kwargs = self.__coerce_dict(kwargs)

    return super(_TypedDict, self).update(*args, **kwargs)

  def fromkeys(self, *args):
    _check_arguments(self.__init__, args, min_args=1, max_args=2)
    if len(args) == 2:
      args[1] = self.__coerce_value(args[1])

    return super(_TypedDict, self).fromkeys(*args)

  def copy(self):
    return _TypedDict(super(_TypedDict, self).copy())

  def __setitem__(self, key, val):
    return super(_TypedDict, self).__setitem__(key, self.__coerce_value(val))

  def __coerce_value(self, val):
    if not isinstance(val, self.__element_type__):
      val = self.__element_type__(val)
    return val

  def __coerce_list(self, lst):
    return map(self.__coerce_value, lst)

  def __coerce_dict(self, dct):
    return dict(zip(dct.keys(), map(self.__coerce_value, dct.values())))

class Field(object):
  """
  Provides schema validation, type coercion, etc. for a field in e.g. JSON.
  """
  def __init__(self, data_type, required=False, strict=False, default=None):
    self.data_type = data_type
    self.required = required
    self.default = default
    self.strict = strict

    if required and default:
      raise ValueError("Cannot set a default value for a required field")
    if not (default is None or self._check_instance(default, data_type)):
      try:
        if strict:
          raise TypeError("Disallowed for field with strict=True")
        default = self._coerce(default, data_type)
      except BaseException as exc:
        raise TypeError(
            "Invalid default for field: %s" %
            self._format_coercion_exception(default, data_type, str(exc)))

    # Declare field, setting it will handled by the metaclass.
    self.name = None

  def set_attr(self, obj, key, val):
    """
    Sets 'key' to 'val' on 'obj' where 'key' is defined by this field.

    Performes appropriate validation and/or coercion.

    Args:
      obj (? extends JsonData): Instance on which to set 'key'.
      key (str): Name of key to set to 'val'.
      val ('self.data_type'): Value to which 'key' should be set on 'obj'.

    Raises:
      TypeError propagated from '_validate_and_coerce'.
    """
    val = self._validate_and_coerce(val)
    # pylint: disable=bad-super-call
    # NB: This is correctly a super call relative to 'obj'. Pylint incorrectly
    # seems to assume that it is intended to be relative to 'self'.
    return super(JsonData, obj).__setattr__(key, val)

  def _validate_and_coerce(self, val):
    """
    Performs type checking and coercion if necessary.

    May be extended by subclasses to perform additional validation.

    Args:
      val (?): Value to validate.

    Returns:
      (?) Coerced 'val' on success.

    Raises:
      TypeError on validation/coercion failure.
    """
    try:
      if not self._check_instance(val, self.data_type):
        val = self._coerce(val, self.data_type)
    except BaseException as exc:
      raise TypeError(self._format_coercion_exception(
          val, self.data_type, str(exc), name=self.name))

    return val

  def _check_instance(self, val, data_type):
    """
    Performs type validation on 'val'.

    May be overridden by subclasses to perform custom validation beyond
    'isinstance'.

    Args:
      val (?): Value whose type to validate.
      data_type (type): Type restriction for 'val'.

    Returns:
      (bool) True if validation passes, else False.
    """
    return isinstance(val, data_type)

  def _coerce(self, val, data_type):
    """
    Coerce 'val' to type 'data_type'.

    May be overridden by subclasses to perform custom coercion beyond
    simply attempting to cast 'val' to 'data_type'.

    Args:
      val (?): Value to coerce.
      data_type (type): Type to which 'val' should be coerced.

    Returns:
      (typeof(data_type)) Coerced 'val'.

    Raises:
      TypeError if unable to perform coercion.
    """
    if self.strict:
      raise TypeError("Disallowed for field with strict=True")
    return data_type(val)

  def _format_coercion_exception(self, val, data_type, exc_msg, name=None):
    """
    Constructs formatted exception message for coercion failure.

    Args:
      val (?): Value whose coercion failed.
      data-type (type): Type to which coercion of 'val' failed.
      exc_msg (str): Exception encountered during coercion.
      name (str|None): Optional. If provided, name of field to which 'val'
        was being assigned.
    """
    ret = ["Unable to coerce value '%s' (%s)"]
    if name:
      ret.append("for field '%s'" % name)
    ret.append("to '%s': %s")

    return " ".join(ret) % (val, val.__class__, data_type, exc_msg)

class RepeatedField(Field):
  """
  Provides 'Field' functionality for fields whose data is a typed list.
  """
  def __init__(self, data_type, required=False, strict=False, default=None):
    if default is None:
      default = []
    if not isinstance(default, (list, tuple)):
      if strict:
        raise TypeError("Received non list/tuple default '%s' (%s) for "
                        "repeated field with strict=True" %
                        (default, default.__class__))
      else:
        default = [default]

    default = _TypedList(data_type, strict=strict)
    super(RepeatedField, self).__init__(data_type, required, strict, default)

  def set_attr(self, obj, key, val):
    if not self._check_instance(val, self.data_type):
      val = self._coerce(val, self.data_type)
    super(RepeatedField, self).set_attr(obj, key, val)

  def _check_instance(self, val, data_type):
    """
    See documentation for 'Field._check_instance'.
    """
    return (isinstance(val, _TypedList) and all(map(
        lambda elt:
        super(RepeatedField, self)._check_instance(elt, data_type), val)))

  def _coerce(self, val, data_type):
    """
    See documentation for 'Field.'.
    """
    if not isinstance(val, (tuple, list)):
      raise TypeError(
          "Received non list/tuple value '%s' for repeated field %s" %
          (val, self.name))

    if self.strict and not self._check_instance(val, data_type):
      raise TypeError("Disallowed for field with strict=True")

    return _TypedList(data_type,
                      map(lambda elt: Field._coerce(self, elt, self.data_type),
                          val),
                      strict=self.strict)

  def _format_coercion_exception(self, val, data_type, exc_msg, name=None):
    """
    See documentation for 'Field._format_coercion_exception'.
    """
    ret = ["Unable to coerce value '%s' (%s)"]
    if name:
      ret.append("for repeated field '%s'" % name)
    ret.append("to 'list<%s>': %s")

    return " ".join(ret) % (val, val.__class__, data_type, exc_msg)

#==============================================================================
# Fix static analysis problems
#==============================================================================

  # pylint: disable=unused-argument
  # Intentionally unused as these are dummy methods for static analysis.

  def append(self, val):
    raise AssertionError("This method should never be called, it is expected "
                         "that the 'RepeatedField' will have returned a "
                         "'_TypedList' wrapped 'list' in place of the "
                         "'RepeatedField' itself.")

  def extend(self, lst):
    raise AssertionError("This method should never be called, it is expected "
                         "that the 'RepeatedField' will have returned a "
                         "'_TypedList' wrapped 'list' in place of the "
                         "'RepeatedField' itself.")

  # pylint: enable=unused-argument

#==============================================================================
# Metaclass
#==============================================================================

class JsonDataMeta(ABCMeta):
  """
  Metaclass which allows for 'JsonData's syntactic sugar.
  """
  # NB: Must subclass 'ABCMeta' and not 'type' to be compatible with
  # subclassing 'MutableMapping'.
  def __new__(mcs, name, bases, dct):
    schema_map = {}
    required_fields = set()
    for key in dct.keys():
      if isinstance(dct[key], Field):
        dct[key].name = key
        schema_map[key] = dct[key]
        if dct[key].required:
          required_fields.add(key)
        dct[key] = None

    dct["__SCHEMA_MAP__"] = schema_map
    dct["__REQUIRED_FIELD_SET__"] = required_fields
    dct["__FIELD_NAME_SET__"] = set(schema_map.keys())

    cls = super(JsonDataMeta, mcs).__new__(mcs, name, bases, dct)
    super(JsonDataMeta, cls).__init__(name, bases, dct)
    return cls

  def __iter__(cls):
    return iter(cls.__SCHEMA_MAP__.keys())

  def __contains__(cls, val):
    return val in cls.__FIELD_NAME_SET__

#==============================================================================
# JSON data
#==============================================================================

class JsonData(MutableMapping):
  """
  Map supporting conversion to/from JSON with schema checking, automatic
  type conversion, etc.
  """
  __metaclass__ = JsonDataMeta

  @staticmethod
  def camel_case_to_underscore(text):
    """
    Transformes camel-cased 'text' to underscore delmited text.

    (e.g. camelCase to camel_case)

    Args:
      text (str): camel-cased text to transform.

    Returns:
      (str) Transformed text.
    """
    return re.sub(r"([A-Z])", r"_\1", text).lower()

  @staticmethod
  def underscore_to_camel_case(text):
    """
    Transformes underscore delimted 'text' to camel-cased text.

    (e.g. camel_case to camelCase)

    Args:
      text (str): underscore delimted text to transform.

    Returns:
      (str) Transformed text.
    """
    elts = text.split("_")
    return "".join([elts[0]] + map(str.capitalize, elts[1:]))

  @staticmethod
  def _convert_recursive(val, filter_empty, to_camel_case):
    """
    Args:
      val (?): Field value to recursively convert.
      filter_empty (bool): Whether or not to filter out unset fields when
        recursing.
      to_camel_case (bool): If True, convert underscore any delimited
        keys to camel-case.
    """
    if isinstance(val, JsonData):
      return val.json(filter_empty=filter_empty, to_camel_case=to_camel_case)
    elif isinstance(val, (list, tuple)):
      return map(lambda v: JsonData._convert_recursive(v, filter_empty,
                                                       to_camel_case), val)
    elif isinstance(val, (dict, MutableMapping)):
      keys = val.keys()
      if to_camel_case:
        keys = map(JsonData.underscore_to_camel_case, keys)
      return dict(zip(
          keys, map(lambda v: JsonData._convert_recursive(v, filter_empty,
                                                          to_camel_case),
                    val.values())))
    elif isinstance(val, EnumElement):
      return str(val)
    else:
      return val

  def __init__(self, *args, **kwargs):
    if args:
      assert (len(args) == 1 and not kwargs,
              "Must pass either kwargs or a single dict")
      kwargs = args[0]
    self.__default_convert_camel_case = True

    missing_required = self.__REQUIRED_FIELD_SET__ - set(kwargs.keys())
    if self.__default_convert_camel_case:
      missing_required -= set(map(JsonData.camel_case_to_underscore,
                                  kwargs.keys()))
    missing_required.discard(None)
    if missing_required:
      raise KeyError("Missing required fields: %s" %
                     ", ".join(map(lambda k: '%s' % k, missing_required)))
    self.__field_is_set_map__ = dict(zip(self.__FIELD_NAME_SET__,
                                         [False] * len(self.__SCHEMA_MAP__)))
    for key, val in kwargs.items():
      if self.__default_convert_camel_case:
        key = JsonData.camel_case_to_underscore(key)
      if key not in self.__FIELD_NAME_SET__:
        continue

      setattr(self, key, val)

  def __getattribute__(self, key):
    field = object.__getattribute__(self, "__SCHEMA_MAP__").get(key)
    if field is not None:
      # Error will have been raised previously if field is required, so OK
      # to assume here that we can fall back to a default.
      if not object.__getattribute__(self, "__field_is_set_map__")[key]:
        return field.default

    return object.__getattribute__(self, key)

  def __setattr__(self, key, val):
    if key in self.__FIELD_NAME_SET__:
      # NB: Call set_attr first and allow exceptions to propagate prior to
      # flagging 'key' as having been set.
      ret = self.__SCHEMA_MAP__[key].set_attr(self, key, val)
      self.__field_is_set_map__[key] = True
      return ret
    return super(JsonData, self).__setattr__(key, val)

#============================================================================
# MutableMapping interface
#============================================================================

  def __delitem__(self, key):
    if key not in self.__FIELD_NAME_SET__:
      raise AttributeError("Cannot delete non-JSON data field '%s'" % key)

    if self.__SCHEMA_MAP__[key].required:
      raise AttributeError("Cannot delete required attribute '%s'" % key)

    self.__field_is_set_map__[key] = False
    # Call 'object' setattr to bypass special handling in '__setattr__'.
    object.__setattr__(self, key, None)

  def __getitem__(self, key):
    if key not in self.__FIELD_NAME_SET__:
      if hasattr(self, key):
        raise KeyError("'%s' is not a JSON data field" % key)
      raise KeyError("Field '%s' does not exist" % key)
    return getattr(self, key)

  def __setitem__(self, key, val):
    if key not in self.__FIELD_NAME_SET__:
      if hasattr(self, key):
        raise KeyError("'%s' is not a JSON data field" % key)
      raise KeyError("Field '%s' was not defined as part of schema")

    setattr(self, key, val)

  def __iter__(self):
    return iter(self.__field_is_set_map__.keys())

  def __len__(self):
    return len(self.__SCHEMA_MAP__)

#============================================================================
# JSON util
#============================================================================

  def json(self, filter_empty=True, recurse=True, to_camel_case=True):
    """
    Converts into JSON compatible dict.

    Args:
      filter_empty (bool): Optional. Whether to filter unset fields or provide
        their default values (or null). Default True.
      recurse (bool): Optional. Whether to recursively convert fields to JSON
        compatible dicts. Default True.
      to_camel_case (bool): Optional. If True, convert underscore any delimited
        keys to camel-case. Default True.

    Returns:
      (dict) JSON serializable map.
    """
    _keys = self.keys()
    if filter_empty:
      keys = [key for (key, val) in self.__field_is_set_map__.items()
              if val is not None and getattr(self, key) is not None]
    else:
      keys = self.keys()

    vals = map(lambda k: getattr(self, k), keys)

    if to_camel_case:
      keys = map(JsonData.underscore_to_camel_case, keys)

    if recurse:
      vals = JsonData._convert_recursive(vals, filter_empty, to_camel_case)

    return dict(zip(keys, vals))

  def dumps(
      self, sort_keys=True, indent=2, filter_empty=True, to_camel_case=True):
    """
    Serializes data into JSON string.

    Args:
      sort_keys (bool): Optional. Whether to sort keys in serialized string.
        Default True.
      indent (int): Optional. Number of spaces per indent level. Default 2.
      filter_empty (bool): Optional. Whether to omit unset fields or provide
        their default values (or null). Default True.
      to_camel_case (bool): Optional. If True, convert underscore any delimited
        keys to camel-case. Default True.

    Returns:
      (str) serialized JSON representation of data.
    """
    return json.dumps(
        self.json(filter_empty=filter_empty, recurse=True,
                  to_camel_case=to_camel_case),
        indent=indent, sort_keys=sort_keys)
