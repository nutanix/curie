#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import logging
from functools import partial, wraps

import collections
import flask
import google.protobuf.message

from curie.curie_error_pb2 import CurieError, ErrorRet
from curie.error import curie_error_to_http_status
from curie.exception import CurieException
from curie.log import patch_trace

log = logging.getLogger(__name__)
patch_trace()

# Header added by 'curie_api_handler' to signal the WSGI handler that it
# should trigger a FATAL once it has finished writing the response.
ABORT_HEADER = "X-Curie-Abort"


def curie_api_handler(func):
  @wraps(func)
  def wrapper(*args, **kwargs):
    try:
      ret = func(*args, **kwargs)
      return ret.SerializeToString(), 200
    except CurieException, ex:
      log.warning("CurieException handling %s",
                  func.func_name, exc_info=True)
      ret = ErrorRet()
      ret.error_codes.append(ex.error_code)
      ret.error_msgs.append(ex.error_msg)
      http_status = curie_error_to_http_status(ex.error_code)
      return ret.SerializeToString(), http_status
    except google.protobuf.message.EncodeError, ex:
      log.warning("Protobuf deserialization error handling %s",
                  func.func_name, exc_info=True)
      ret = ErrorRet()
      ret.error_codes.append(CurieError.kInvalidParameter)
      ret.error_msgs.append(str(ex))
      return ret.SerializeToString(), 400
    except BaseException as exc:
      log.warning("Unhandled exception handling %s",
                  func.func_name, exc_info=True)
      ret = ErrorRet()
      ret.error_codes.append(CurieError.kInternalError)
      ret.error_msgs.append(repr(exc))
      resp = flask.make_response(ret.SerializeToString(), 500)
      resp.headers[ABORT_HEADER] = True
      return resp
  return wrapper


def _validate(name, val, type_or_types, values, func, err_code,
              *args, **kwargs):
  """
  Helper used by 'validate_parameter' and 'validate_return'. See either for
  more documentation.
  """
  log.trace("Validating '%s': %s", name, val)
  log.trace("valid_types=%s, valid_values=%s, valid_func=%s",
            type_or_types, values, func)
  try:
    if type_or_types and not isinstance(val, type_or_types):
      if isinstance(type_or_types, collections.Iterable):
        msg = "Expected one of '%s'" % ", ".join(
          [type.__name__ for type in type_or_types])
      else:
        msg = "Expected '%s'" % type_or_types.__name__
      raise AssertionError("Invalid '%s' type '%s': %s" %
                           (name, val.__class__.__name__, msg))
    if values and val not in values:
      if isinstance(values, list) or isinstance(values, tuple):
        msg = "Expected one of '%s'" % ", ".join(values)
      else:
        msg = "Expected '%s'" % values
      raise AssertionError("Invalid '%s' value '%s': %s" % (name, val, msg))
    if func:
      if not func(*args, **kwargs):
        raise AssertionError("Invalid '%s' value '%s': Functional validation "
                             "failed" % (name, val))
  except AssertionError as exc:
    raise CurieException(err_code, str(exc))


class validate_parameter(object):
  def __init__(self, param_name, valid_types=None, valid_values=None,
               valid_func=None):
    """
    Validates a parameter type, value, and/or using a custom function.

    Args:
      param_name (str): Name of parameter in decorated function which should be
        validated.
      valid_types (list|None): If not None, verify that the parameter is of one
        of the provided types.
      valid_values (list|None): If not None, verify that the parameter is equal
        to one of the provided values.
      valid_func (callable|None): Optional. If provided, a callable accepting
        the wrapped functions arguments and raising an AssertionError if the
        parameter is invalid.

    Raises CurieException<kInvalidParameter> If parameter fails validation.
    """
    if valid_func:
      assert (callable(valid_func),
              "Skipping uncallable object provided as validation function for "
              "'%s'" % param_name)

    self.param_name = param_name
    self.valid_types = valid_types
    self.valid_values = valid_values
    self.valid_func = valid_func

  def __call__(self, functor):
    @wraps(functor)
    def wrapped(*args, **kwargs):
      if self.param_name not in kwargs:
        log.trace("Skipping validation for absent parameter '%s'",
                  self.param_name)
        return functor(*args, **kwargs)

      val = kwargs[self.param_name]
      _validate(self.param_name, val, self.valid_types, self.valid_values,
                self.valid_func, CurieError.kInvalidParameter, *args,
                **kwargs)

      return functor(*args, **kwargs)
    return wrapped


class validate_return(object):
  def __init__(self, valid_types=None, valid_values=None, valid_func=None,
               err_code=CurieError.kInvalidParameter):
    """
    Validates return by type, value, and/or using a custom function.

    Args:
      valid_types (list|None): If not None, verify that the parameter is of one
        of the provided types.
      valid_values (list|None): If not None, verify that the parameter is equal
        to one of the provided values.
      valid_func (callable|None): If not None, a callable accepting the wrapped
        function's arguments and raising an AssertionError if the return value
        is invalid.
      err_code (CurieError.Type): Optional. CurieException error code to use
        on failure.

    Raises CurieException<kInvalidParameter> If parameter fails validation.
    """
    self.valid_types = valid_types
    self.valid_values = valid_values
    self.valid_func = valid_func
    self.err_code = err_code

    if valid_func and not callable(valid_func):
      raise CurieException(CurieError.kInternalError,
                            "Skipping uncallable object provided for return "
                            "value validation")

  def __call__(self, functor):
    @wraps(functor)
    def wrapped(*args, **kwargs):
      ret = functor(*args, **kwargs)
      _validate("return value", ret, self.valid_types, self.valid_values,
                partial(self.valid_func, ret), self.err_code, *args,
                **kwargs)
      return ret
    return wrapped
