#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import inspect
import logging
import math
import os
import socket
import time
from functools import wraps

import requests

from curie.curie_error_pb2 import CurieError, ErrorRet
from curie.curie_extensions_pb2 import descriptor_pb2
from curie.curie_server_state_pb2 import ServerStatus
from curie.exception import CurieException
from curie.log import CHECK_EQ

log = logging.getLogger(__name__)

CURIE_CLIENT_DEFAULT_RETRY_TIMEOUT_CAP_SECS = 16

# TODO: See if it's worthwhile to cache connections.

class _SvcDescriptor(object):

  def __init__(self, svc_stub):
    # Protobuf service stub class.
    self.__svc_stub = svc_stub

    # Name of protobuf package for the service.
    desc = svc_stub.GetDescriptor()
    self.__package = "".join(desc.full_name.split(desc.name)[:-1]).rstrip(".")

    # Map of option names to their values.
    self.__svc_opt_map = self.get_option_extensions(svc_stub.GetDescriptor())

    # Map of method_names to a map of their options.
    self.__rpc_name_opt_map = {}

    for rpc_desc in svc_stub.GetDescriptor().methods:
      self.__rpc_name_opt_map[
        rpc_desc.name] = self.get_option_extensions(rpc_desc)

  def get_arg_proto(self, method_name):
    method_desc = self.get_method_descriptor(method_name)
    return self.__svc_stub.GetRequestClass(method_desc)

  def get_method_descriptor(self, method_name):
    desc = self.__svc_stub.GetDescriptor().FindMethodByName(method_name)
    if desc is None:
      raise AttributeError("Invalid method: %s" % method_name)
    return desc

  def get_option_extensions(self, desc):
    desc_exts = desc.GetOptions().Extensions
    base_desc = getattr(descriptor_pb2, desc.GetOptions().DESCRIPTOR.name)
    opt_val_map = {}
    for opt_name, opt_desc in base_desc._extensions_by_name.items():
      if not desc.GetOptions().HasExtension(opt_desc):
        continue
      opt_name = opt_name.split(self.__package + ".", 1)
      opt_name = opt_name[1] if len(opt_name) > 1 else opt_name[0]
      opt_val_map[opt_name] = desc_exts[opt_desc]

    return opt_val_map

  def get_ret_proto(self, method_name):
    method_desc = self.get_method_descriptor(method_name)
    return self.__svc_stub.GetResponseClass(method_desc)

  def method_names(self):
    return self.__rpc_name_opt_map.keys()

  # TODO: Can probably auto-generate the mapping between method options and
  # service default options, certainly can with a naming convention.
  def rpc(self, method_name):
    arg_cls = self.get_arg_proto(method_name)

    curr_timeout_msecs = self.__rpc_name_opt_map[method_name].get(
      "timeout_msecs")
    if curr_timeout_msecs is None:
      curr_timeout_msecs = self.__svc_opt_map.get("default_timeout_msecs")

    curr_max_retries = self.__rpc_name_opt_map[method_name].get("max_retries")
    if curr_max_retries is None:
      curr_max_retries = self.__svc_opt_map.get("default_max_retries")

    def wrapped(this, arg, initial_timeout_secs=curr_timeout_msecs / 1000.0,
                max_retries=curr_max_retries):
      if not isinstance(arg, arg_cls):
        raise TypeError(
          "Invalid argument class '%s' for method '%s'. (Expected '%s')" % (
            arg.__class__.__name__, method_name, arg_cls.__name__))
      return this._send_rpc_sync_with_retries(
        method_name, arg, initial_timeout_secs, max_retries)
    return wrapped

def RpcClientMeta(stub):

  class Meta(type):
    def __new__(mcs, name, bases, dct):
      service = _SvcDescriptor(stub)

      for rpc_method_name in service.method_names():
        dct[rpc_method_name] = service.rpc(rpc_method_name)

      dct["_service"] = service

      return super(Meta, mcs).__new__(mcs, name, bases, dct)

  return Meta

class RpcClientUtil(object):
  """
  Base class from which Curie protobuf RPC clients should derive.

  Args:
    ip (str): RPC server IP address.
    port (int): RPC server port.
    path (str): RPC server endpoint path.
  """

  # _SvcDescriptor instance. Set by metaclass.
  _service = None

  def __init__(self, ip, port, path):
    # RPC server IP.
    self.__ip = ip

    # RPC server port.
    self.__port = port

    # RPC endpoint path.
    self.__path = path

    # RPC endpoint URL.
    self.__url = "http://%s:%d%s" % (ip, port, os.path.join("/", path))

  #----------------------------------------------------------------------------
  #
  # Public methods.
  #
  #----------------------------------------------------------------------------

  def send_rpc(self, method_name, arg, max_retries=None,
               overall_timeout_secs=None):
    """
    Calls RPC 'method_name' with argument 'arg'.

    NB: Currently all RPCs are synchronous.

    Args:
      method_name (str): Name of RPC method to be issued.
      arg (protobuf): Populated argument proto for the RPC 'method_name'.
      max_retries (int|None): Optional. If provided, a non-negative integer
        specifying the maximum number of times to retry an RPC.
        If not provided, falls back to options specified in
        curie_extension.proto if any, else 0. (Default None)

        NB: Retries are only attempted on transport error, or when recieving a
          kRetry response.
      overall_timeout_secs (numeric|None): Optional. If provided, overall
        timeout in seconds which should be enforced (cumulative across any
        retries).

      NB: If 'overall_timeout_secs' will be internally converted to a max_retry
      count. If both 'overall_timeout_secs' and 'max_retries' are provided,
      precedence will be given to the smaller of 'max_retries' and the retry
      count derived from 'overall_timeout_secs'.

    Returns:
      (ret, err) On success, 'ret' is the appropriate deserialized return
      proto and 'err' is None. On error, 'ret' is None, and err is the
      deserialized CurieError proto.

    Raises:
      TypeError if 'arg' is not a valid instance of the arg protobuf for
        'method_name'.
      (CurieError<kTimeout>) on timeout.
      (CurieError<kRetry>) on connection error.
    """
    derived_max_retries = None
    if overall_timeout_secs:
      # Compute equivalent retry cap (based on initial timeout and backoff)
      derived_max_retries = 0
      arg_spec = inspect.getargspec(getattr(self, method_name))
      kwargs = dict(zip(reversed(arg_spec.args), reversed(arg_spec.defaults)))
      derived_max_retries = math.floor(
        math.log(
          overall_timeout_secs / kwargs["initial_timeout_secs"] + 1) - 1)

      if max_retries:
        max_retries = min(max_retries, derived_max_retries)
      else:
        max_retries = derived_max_retries

    start_secs = time.time()
    try:
      if max_retries:
        result = getattr(self, method_name)(arg, max_retries=max_retries)
      else:
        result = getattr(self, method_name)(arg)
    finally:
      duration_msecs = (time.time() - start_secs) * 1000
      log.debug("RPC client: %s completed after %d ms",
                method_name, duration_msecs)
    return result

  #----------------------------------------------------------------------------
  #
  # Protected utils.
  #
  #----------------------------------------------------------------------------

  def _send_rpc_sync(self, method_name, arg, timeout_secs):
    """
    Synchronously issue RPC 'method_name' with argument 'arg'.

    Args:
      method_name (str): Name of RPC method to be issued.
      arg (protobuf): Populated argument proto for the RPC 'method_name'.
      timeout_secs (float): Desired RPC timeout in seconds. Expected a
        non-negative value coercable to float.

    Returns:
      (ret, err) On success, 'ret' is the appropriate deserialized return
      proto and 'err' is None. On error, 'ret' is None, and err is the
      deserialized CurieError proto.

    Raises:
      (CurieError<kTimeout>) on timeout.
      (CurieError<kRetry>) on connection error.
    """
    try:
      resp = requests.post(
        self.__url,
        headers={"Content-Type": "application/x-rpc",
                 "X-Rpc-Method": method_name},
        data=arg.SerializeToString(),
        timeout=timeout_secs)
    except (ValueError, TypeError) as exc:
      log.exception("Failed to issue RPC")
      raise CurieException(CurieError.kInternalError,
                            "Failed to issue RPC: %s" % exc)
    except (requests.exceptions.Timeout, socket.timeout):
      log.exception("RPC timed out")
      raise CurieException(CurieError.kTimeout,
                            "RPC '%s' timed out after %f seconds" % (
                              method_name, timeout_secs))
    except requests.exceptions.RequestException as exc:
      log.exception("Exception in RPC request")
      raise CurieException(CurieError.kRetry, str(exc))
    else:
      # Succeeded, expect appropriate serialized return proto.
      if resp.status_code == 200:
        ret_cls = self._service.get_ret_proto(method_name)
        ret = ret_cls()
        ret.ParseFromString(resp.content)
        return ret, None
      # Error, expect serialized CurieError proto.
      else:
        err = ErrorRet()
        err.ParseFromString(resp.content)
        return None, err

  def _send_rpc_sync_with_retries(self, method_name, arg, initial_timeout_secs,
                                  max_retries):
    """
    Sends RPC as in '_send_rpc_sync'. On a CurieException, retries the RPC up
    to 'max_retries' times, starting with a delay of 'initial_timeout_secs'
    seconds and using exponential backoff on subsequent retries.

    Args:
      method_name (str): Name of RPC method to be issued.
      arg (protobuf): Populated argument proto for the RPC 'method_name'.
      initial_timeout_secs (numeric): Timeout for initial RPC attempt.
        Subsequent attempts will scale this value using exponential backoff.
      max_retries (int): Maximum number of retry attempts to allow. Set to 0
        if no retry is desired.
    """
    # TODO: In the case of the CurieUnixAgent it's possible this may fail
    # with 'cmd_id already exists'. See if there are reasonable cases where
    # this will occur and handle them appropriately.
    rpc_excs = []
    curr_timeout_secs = initial_timeout_secs
    for ii in range(max_retries + 1):
      try:
        ret, err = self._send_rpc_sync(method_name, arg, curr_timeout_secs)
        # TODO: See about the convention for multiple error codes.
        if err and err.error_codes[-1] == CurieError.kRetry:
          raise CurieException(err.error_codes[-1], str(err))
        return ret, err
      except CurieException as exc:
        if exc.error_code not in [CurieError.kRetry, CurieError.kTimeout]:
          raise
        rpc_excs.append("Attempt %s: %s" % (ii, str(exc)))
        if ii < max_retries:
          if exc.error_code == CurieError.kRetry:
            log.warning(
              "RPC failed. Retrying after '%s' seconds (%s of %s attempts)",
              curr_timeout_secs, 1 + ii, max_retries)
            time.sleep(curr_timeout_secs)
          else:
            CHECK_EQ(exc.error_code, CurieError.kTimeout)
            log.warning(
              "RPC timed out. Retrying (%s of %s attempts)",
              1 + ii, max_retries)
          # Increase the timeout for the next attempt using exponential
          # backoff. We impose a cap of
          # CURIE_CLIENT_DEFAULT_RETRY_TIMEOUT_CAP_SECS secs on this unless
          # the base RPC timeout is already greater than that.
          curr_timeout_secs = min(
            2 * curr_timeout_secs,
            max(CURIE_CLIENT_DEFAULT_RETRY_TIMEOUT_CAP_SECS,
                initial_timeout_secs))
    rpc_excs.append("Exhausted retry attempts")
    raise CurieException(CurieError.kInternalError,
                          "RPC failed:\n%s" % "\n".join(rpc_excs))

class RpcServer(object):
  """
  Given a protobuf service, returns a server instance providing two
  decorators:

  @endpoint: Registers a flask request handler as an RPC endpoint.
  @handler(rpc_name): Registers decorated method as the handler for 'rpc_name'.

  Args:
    rpc_svc (GeneratedServicesType): Google protobuf service object for
    the RPC service to host.
  """
  # RPCs whose duration exceeds this value (in ms) will log a warning.
  WARNING_THRESHOLD_MSECS = 100

  __PRE_INITIALIZED_STATUS_CODES__ = set([
    ServerStatus.kUnknown, ServerStatus.kUninitialized,
    ServerStatus.kInitializing
    ])

  __PRE_AUTH_STATUS_CODES__ = __PRE_INITIALIZED_STATUS_CODES__ | set([
    ServerStatus.kInitialized, ServerStatus.kAwaitingEncryptionKey
    ])

  __PRE_RECOVERED_STATUS_CODES__ = __PRE_AUTH_STATUS_CODES__ | set([
    ServerStatus.kRecovering
    ])


  def __init__(self, rpc_svc):
    # Protobuf service class.
    self.__rpc_svc = rpc_svc

    # Map of RPC method names to their handlers.
    self.__rpc_handler_map = {}

  #----------------------------------------------------------------------------
  #
  # Decorators.
  #
  #----------------------------------------------------------------------------

  def endpoint(self, functor):
    """
    Wrapper to convert a flask HTTP handler into an RPC endpoint. Expects
    the handler to return the flask.request object for the RPC.
    """
    @wraps(functor)
    def wrapped(*args, **kwargs):
      request = functor()
      return self.__handle_request(request)
    return wrapped

  def handler(self, method_name):
    """
    Wrapper which registers the wrapped function as the handler for an
    RPC 'method_name'.
    """
    def _register_handler(method_name, functor):
      self.__rpc_handler_map[method_name] = functor
      return functor
    return lambda functor: _register_handler(method_name, functor)

  #----------------------------------------------------------------------------
  #
  # Private utils.
  #
  #----------------------------------------------------------------------------

  def __handle_request(self, req):
    """
    Handle RPC 'req' to registered endpoint. Calls and returns result from
    registered handler for a valid request, else raises a CurieException.
    """
    start_secs = time.time()
    try:
      rpc_handler, arg = self.__parse_http_rpc(req)
    except AssertionError as exc:
      raise CurieException(CurieError.kInvalidParameter, str(exc))
    try:
      result = rpc_handler(arg)
    finally:
      duration_msecs = (time.time() - start_secs) * 1000
      log.debug("RPC server: %s completed after %d ms",
                req.headers.get("X-Rpc-Method"), duration_msecs)
    return result

  def __parse_http_rpc(self, req):
    """
    Validates and deserializes inbound RPC 'req'.

    Returns:
      (rpc_handler, arg): Registered handler for the RPC and the corresponding
      deserialized argument proto.
    """
    import flask
    assert req.content_type == "application/x-rpc", (
      "Invalid content-type '%s' for RPC endpoint" % req.content_type)
    rpc_name = req.headers.get("X-Rpc-Method")
    assert rpc_name, (
      "Must provide a non-empty method name via X-Rpc-Method header")
    rpc_desc = self.__rpc_svc.DESCRIPTOR.FindMethodByName(
      rpc_name)
    assert rpc_desc, "Unknown RPC '%s'." % rpc_name
    rpc_handler = self.__rpc_handler_map.get(rpc_name, None)
    assert rpc_handler, "No registered handler for RPC '%s'" % rpc_name
    # Store the RPC name in the request context for use in debug messages.
    flask.g.rpc_name = rpc_name
    arg_cls = self.__rpc_svc.GetRequestClass(rpc_desc)
    arg = arg_cls()
    arg.ParseFromString(req.data)
    return rpc_handler, arg
