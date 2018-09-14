#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

import json
import logging
import os
import re
import sys
import tarfile
import threading
import time
from functools import wraps

import gflags
import requests
from requests.packages import urllib3

# Until certificate validation for Prism is fixed, suppress warnings related
# to skipping SSL verification.
urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(category=urllib3.exceptions.InsecurePlatformWarning)

from curie.curie_error_pb2 import CurieError
from curie.curie_server_state_pb2 import CurieSettings
from curie.curie_types_pb2 import ConnectionParams
from curie.decorator import validate_parameter, validate_return
from curie.exception import CurieException
from curie.json_util import JsonDataEncoder
from curie.log import CHECK, CHECK_EQ, CHECK_LE, CHECK_NE, patch_trace
from curie.name_util import NameUtil
from curie.ova_util import CurieOva, CurieOvf
from curie.task import TaskPoller, PrismTask
from curie.util import CurieUtil

log = logging.getLogger(__name__)
patch_trace()
FLAGS = gflags.FLAGS

# TODO (jklein): See about standardizing exceptions vs. checking return codes.
# In particular, cases where the rv will be either a UUID, None, or True are
# confusing.

# Valid projection types for various APIs. 'clusters_get' and 'hosts_get'.
_VALID_PROJ_MAP = {
  "clusters_get": ["ALL", "STATS", "PERF_STATS", "USAGE_STATS", "HEALTH",
                   "ALERTS", "ALERTS_COUNT", "BASIC_INFO"]
}
_VALID_PROJ_MAP["hosts_get"] = _VALID_PROJ_MAP["clusters_get"]
_VALID_PROJ_MAP["vms_get"] = _VALID_PROJ_MAP["clusters_get"]


# When retries for REST API calls are used either by default or internally,
# this value specifies the maximum number of retries to use.
REST_API_MAX_RETRIES = 10

gflags.DEFINE_integer("prism_api_timeout_secs",
                      300,
                      "Maximum amount of time to allow a request to the PRISM "
                      "REST API to be outstanding.")

gflags.DEFINE_integer("prism_max_parallel_tasks",
                      1024,
                      "Maximum number of parallel tasks for calls to "
                      "clone_vms, power_on_vms, power_off_vms, delete_vms, "
                      "snapshot_vms, and migrate_vms. Also used by"
                      "power_off_hosts.")


# TODO (jklein): Clean up the mess of hacks involved here. More urgent now
# that it seems to have broken in 2.7 for some reason. (See messy workaround in
# 'read' method and corresponding TODO note).
class FileHandleWithCallback(file):
  """
  Provides a file object supporting a read callback exposing bytes read.
  """
  @classmethod
  def from_file(cls, cb_progress, fh):
    object.__setattr__(fh, "cb_progress", cb_progress)
    fh.seek(0, os.SEEK_END)
    size_bytes = fh.tell()
    fh.seek(0, os.SEEK_SET)
    object.__setattr__(fh, "_size_bytes", size_bytes)
    object.__setattr__(fh, "_orig_read", fh.read)
    object.__setattr__(fh, "read", cls.read.__get__(fh))

    if not hasattr(fh, "__enter__"):
      object.__setattr__(fh, "__enter__", cls.enter_ctx.__get__(fh))
      object.__setattr__(fh, "__exit__", cls.exit_ctx.__get__(fh))

    if not hasattr(fh, "__call__"):
      object.__setattr__(fh, "__call__", cls.call_ctx.__get__(fh))

    if not hasattr(fh, "__len__"):
      object.__setattr__(fh, "len", size_bytes)

    return fh

  def __init__(self, cb_progress, *args, **kwargs):
    self.cb_progress = cb_progress
    super(FileHandleWithCallback, self).__init__(*args, **kwargs)

    self.seek(0, os.SEEK_END)
    self._size_bytes = self.tell()
    self.seek(0, os.SEEK_SET)

  def size(self):
    return self._size_bytes

  def read(self, size=None, *args, **kwargs):
    # TODO (jklein): Find a cleaner way of handling this. Right now it may
    # be an standard file-like object subclassing 'file', or it may be one of
    # the file wrappers provided by the tarfile library.
    # If it's a subclass of 'file', we can just use 'super.read'
    # If it's a tarfile wrapper, grab the hacky '_orig_read' attribute that
    # was stuck to it by '__init__'.
    if isinstance(self, file):
      ret = super(FileHandleWithCallback, self).read(size)
    else:
      ret = getattr(self, "_orig_read")(size, *args, **kwargs)
    self.cb_progress(len(ret))
    return ret

  def call_ctx(self):
    log.debug("Called with %s", self)
    return self

  def enter_ctx(self):
    log.debug("Called enter with %s", self)
    return self

  def exit_ctx(self, *args, **kwargs):
    log.debug("Called exit with %s", self)
    if hasattr(self, "close"):
      log.debug("%s has 'close' attribute %s of type %s",
                self, self.close, type(self.close))
      self.close()


class PrismAPIVersions(object):
  """
  Class to enumerate various public and private APIs exposed by Prism.

  -- 'PRISM' versions comprise versions of the documented public API.
  -- Undocumented/internal APIs are given separate entries.
  -- Public attributes are set to human-friendly names for ease of reading.
  -- Tracks Nutanix version dependencies for the various APIs.
  """

  # Public Prism REST API v1.
  PRISM_V1 = "Prism REST v1"

  # Internal REST to RPC API for Genesis. Version will be incremented in
  # response to any breaking changes in later Nutanix releases.
  GENESIS_V1 = "Genesis REST to RPC v1"

  # Minimum product versions as tuples.
  MIN_AOS_VERSION = (4, 0, 0, 0)
  MIN_CE_VERSION = (2016, 12, 22)

  # Regex to extract version number from a full or short AOS version string.
  # E.g.:
  # el7-release-euphrates-5.0.2-stable-9d20638eb2ba1d3f84f213d5976fbcd412630c6d
  # Or
  # 5.0.2
  _optional_prefixes = r"(?:el[0-9.]+(?<!\.)-)?(?:[a-zA-Z]+-){0,2}"
  _optional_suffixes = r"(?:[a-zA-Z]+-)?(?:[a-fA-F0-9]{40}\Z)?"
  _require_major_minor_opt_sub_versions = r"((?:[0-9]\.)+[0-9])(?:-|\Z)"
  AOS_VERSION_REGEX = re.compile(
    r"%s%s%s" % (_optional_prefixes, _require_major_minor_opt_sub_versions,
                 _optional_suffixes))

  # Set of HTTP status codes indicating the server has requested we
  # retry a method. These retries should happen whether or not the method is
  # idempotent, as we are deferring to the server's knowledge of what is safe.
  RETRY_STATUS_SET = frozenset([502, 503])

  @classmethod
  def get_aos_short_version(cls, version_string):
    try:
      version_capture = cls.AOS_VERSION_REGEX.findall(version_string)
      if version_capture:
        assert len(version_capture) == 1, version_capture
        return version_capture[0]
      return version_string.split("-")[3]
    except Exception:
      log.exception("Could not extract short version from string '%s'",
                    version_string)
      return "Unknown"

  @classmethod
  def is_applicable(cls, api, version):
    """
    Args:
      api (PrismAPIVersions version): API whose compatibility to check.
      version (str): Nutanix version or fullversion string.
    Returns:
      (bool) Whether 'api' is valid for Nutanix release 'version'.
    """
    if api == cls.PRISM_V1:
      return True
    elif api == cls.GENESIS_V1:
      product = "aos"
      try:
        version_capture = cls.AOS_VERSION_REGEX.findall(version)
        if version_capture:
          assert len(version_capture) == 1, version_capture
          version_string = version_capture[0]
        else:
          product = version.split("-")[2]
          version_string = version.split("-")[3]
      except Exception:
        log.exception("Could not determine API version from version string %s",
                      version)
        return False
      version_tuple = tuple(map(int, version_string.split('.')))
      if product == "ce":
        return version_tuple >= cls.MIN_CE_VERSION
      return version_tuple >= cls.MIN_AOS_VERSION
    else:
      log.error("Unknown api version to check: %s", api)
    return False


class HttpRequestException(Exception):
  """
  An internal exception class for raising exceptions for an HTTP request for
  errors that don't already cause an instance of a derived class of
  requests.exceptions.RequestException to be raised.
  """
  def __init__(self, http_status_code, http_body):
    super(HttpRequestException, self).__init__(
      "http_status_code=%s" % http_status_code, "http_body=%s" % http_body)

    # Non-200 HTTP status code.
    self.http_status_code = http_status_code

    # HTTP body.
    self.http_body = http_body


def nutanix_rest_api_method(func):
  @wraps(func, assigned=["__name__", "__doc__", "__module__"])
  def wrapper(*args, **kwargs):
    try:
      ret = func(*args, **kwargs)
      log.trace("Nutanix REST API method %s response %s",
                func.func_name, json.dumps(ret, indent=2, cls=JsonDataEncoder))
      return ret
    except HttpRequestException, ex:
      if ex.http_status_code == 401:
        log.warning("Error authenticating in Nutanix REST API method %s: %s",
                    func.func_name, ex.http_body, exc_info=True)
        raise CurieException(CurieError.kClusterApiAuthenticationError,
                              "Prism authentication unsuccessful. "
                              "Please re-enter credentials.")
      else:
        log.warning("Error in Nutanix REST API method %s: %s",
                    func.func_name, ex.http_body, exc_info=True)
        raise CurieException(CurieError.kClusterApiError,
                              "Prism REST API responded with error")
    except requests.exceptions.Timeout, ex:
      log.warning("Timeout in Nutanix REST API method %s: %s",
                  func.func_name, ex, exc_info=True)
      # Catches both ConnectTimeout and ReadTimeout exceptions, both subclasses
      # of requests.exceptions.RequestException.
      raise CurieException(CurieError.kTimeout, "Prism REST API timed out")
    except requests.exceptions.RequestException, ex:
      log.warning("Error in Nutanix REST API method %s: %s",
                  func.func_name, ex, exc_info=True)
      # Catches all other requests.exceptions.RequestException exceptions
      # besides those derived from requests.exceptions.Timeout.
      raise CurieException(CurieError.kClusterApiError,
                            "Prism REST API call failed")
  return wrapper


class TimeoutSession(requests.Session):
  """
  Allows setting a timeout to be used for all requests during this session.
  """

  def __init__(self):
    super(TimeoutSession, self).__init__()
    # '__timeout_secs' = None produces the default requests.Session behavior.
    self.__timeout_secs = None

  def get_timeout(self):
    return self.__timeout_secs

  def set_timeout(self, timeout_secs):
    """
    Updates session timeout.

    Args:
      timeout_secs (float|None): Desired timeout in seconds (value must be
        coercable to float) or None to revert to the default
        'requests.Session' behavior.

    Returns:
      (bool) True on success, else False.
    """
    # Explicitly allow None.
    if timeout_secs is not None:
      try:
        timeout_secs = float(timeout_secs)
        if timeout_secs <= 0:
          raise ValueError("timeout_secs must be positive")
      except (TypeError, ValueError):
        log.exception("Failed to set timeout to '%s'", timeout_secs)
        return False

    log.trace("Updating timeout_secs for session to '%s'", timeout_secs)
    self.__timeout_secs = timeout_secs
    return True

  def send(self, request, **kwargs):
    if kwargs.get("timeout") is None:
      kwargs["timeout"] = self.__timeout_secs
    log.trace("Sending with timeout: %.1f", kwargs["timeout"])
    return super(TimeoutSession, self).send(request, **kwargs)


class NutanixMetadata(object):
  __slots__ = ["version", "cluster_uuid", "cluster_incarnation_id"]

  def __init__(self, version=None, cluster_uuid=None,
               cluster_incarnation_id=None):
    self.version = version
    self.cluster_uuid = cluster_uuid
    self.cluster_incarnation_id = cluster_incarnation_id


class NutanixRestApiClient(object):
  """
  This class implements a Nutanix REST API client for v1 of the Nutanix REST
  API. For each REST API call that's implemented in this class, the
  corresponding method returns a deserialized JSON response on success for a
  GET or raises a CurieException if an error occurs. For non-GET requests,
  there is no return value.

  Reference: http://prismdevkit.com/nutanix-rest-api-explorer

  Note: the documentation at the above URL should be interpreted only as a hint
  as to what the actual REST API is. There are inaccuracies (e.g., invalid
  URLs, etc.).
  """

  @classmethod
  def from_proto(cls, proto, **kwargs):
    """
    Returns NutanixRestApiClient instance constructed with args from 'proto'.

    Args:
      proto (PrismInfo|NutanixInfo|ConnectionParams):
        Proto containing connection params for Prism.
      **kwargs (dict): Additional arguments to pass to the constructor.

    Raises:
      CurieException<kInvalidParameter> if the proto does not contain the
        required fields.
    """
    if isinstance(
        proto, CurieSettings.Cluster.ClusterManagementServerInfo.PrismInfo):
      args = (proto.prism_host,
              proto.decrypt_field("prism_username"),
              proto.decrypt_field("prism_password"))
    elif isinstance(proto,
                    CurieSettings.Cluster.ClusterSoftwareInfo.NutanixInfo):
      args = (proto.prism_host,
              proto.decrypt_field("prism_user"),
              proto.decrypt_field("prism_password"))
    elif isinstance(proto, ConnectionParams):
      args = (proto.address,
              proto.decrypt_field("username"),
              proto.decrypt_field("password"))
    else:
      raise CurieException(CurieError.kInvalidParameter,
                            "Expected a PrismInfo or ConnectionParams proto")

    return cls(*args, **kwargs)

  def __init__(self, host, api_user, api_password, timeout_secs=None):
    self.host = host
    # User name for API requests.
    self.__api_user = api_user

    # Password for API requests.
    self.__api_password = api_password


    # Host-parameterized template for URL request root.
    self.__host_tmpl = "https://{host}:9440"

    # Prism REST services endpoint
    self.__services_url = "/PrismGateway/services/"

    # Base Prism URL for all API V1 requests
    self.__base_url = "/PrismGateway/services/rest/v1"

    # Base Prism Management URL for all management v0.8 requests
    self.__base_mgmt_url = "/api/nutanix/v0.8"
    self.__base_mgmt_url_v2 = "/api/nutanix/v2.0"

    # HTTP session. Note that this doesn't establish a connection here.
    self.__session = TimeoutSession()
    self.__session.set_timeout(timeout_secs if timeout_secs is not None
                               else FLAGS.prism_api_timeout_secs)

    self.__session.auth = (self.__api_user, self.__api_password)
    self.__session.verify = False
    self.__session.headers["Content-Type"] = "application/json; charset=utf-8"

    # Nutanix version running on 'host', lazily set.
    self.__nutanix_version = None

    # Nutanix fullVersion running on 'host', lazily set.
    self.__nutanix_full_version = None

    # UUID for the cluster to which 'host' belongs, lazily set.
    self.__cluster_uuid = None

    # Incarnation ID for the cluster to which 'host' belongs, lazily set.
    self.__cluster_incarnation_id = None

#==============================================================================
# Utility
#==============================================================================

  def get_cluster_timestamp_usecs(self):
    # NB: We don't want or need to authenticate for this.
    request_url = "%s%s" % (self.__host_tmpl.format(host=self.host),
                            self.__services_url)
    resp = requests.head(request_url, verify=False)
    return int(time.strftime("%s", time.strptime(
      resp.headers["Date"], "%a, %d %b %Y %H:%M:%S %Z"))) * 1000

#==============================================================================
# Public APIs
#==============================================================================

  @nutanix_rest_api_method
  @validate_parameter("projection", str, _VALID_PROJ_MAP["clusters_get"])
  def clusters_get(self, projection=None,
                   cluster_id=None, cluster_name=None):
    """
    Invokes /clusters [GET] to get cluster information for all clusters, or
    /containers/{id} [GET] to get information about a specific cluster.
    Either 'cluster_id' or 'cluster_name' (but not both) must be specified
    in the latter case.

    If projection is provided as one of:
      ALL, STATS, PERF_STATS, USAGE_STATS, HEALTH, ALERTS, ALERTS_COUNT
    The query will be sent with URL params specifying the requested projection.
    """
    if not any([cluster_id, cluster_name]):
      # All clusters.
      url = "%s/clusters" % self.__base_url
    elif cluster_id is None and cluster_name is not None:
      # Specific cluster by name.
      cluster_dto = self.__get_entity_by_key(
        self.clusters_get, "name", cluster_name, raise_on_failure=True)
      cluster_id = cluster_dto["id"]
      url = "%s/clusters/%s" % (self.__base_url, cluster_id)
    elif cluster_id is not None and cluster_name is None:
      # Specific container by container ID.
      url = "%s/clusters/%s" % (self.__base_url, cluster_id)
    else:
      raise CurieException(CurieError.kInternalError,
                            "At most one of 'cluster_name' and 'cluster_id' "
                            "can be set")
    params = {}
    if projection:
      params["projection"] = projection
    return self.__get(url, url_params=params)

  @nutanix_rest_api_method
  def get_pc_uuid(self):
    """
    Attempts to determine the UUID of a connected PrismCentral.

    Returns: (str) UUID of the Prism Central if connected, otherwise None

    """
    url = "%s/multicluster/cluster_external_state" % self.__base_url
    pc_state = self.__get(url)
    try:
      pc_uuid = pc_state[0]["clusterUuid"]
      return pc_uuid
    except (IndexError, KeyError):
      log.debug("PC UUID is not provided in multicluster output.",
                exc_info=True)
      return None

  @validate_parameter("request_dict", dict, valid_func=lambda self, dct:
                      self.__has_keys(dct, ["name", ]))
  @nutanix_rest_api_method
  def containers_create(self, request_dict):
    """
    Invokes /containers [POST] to create a container. 'request_dict' is a
    Python dictionary corresponding to the ContainerDTO to use to create the
    container.
    """
    # First check if the container already exists.
    ctr_dto = self.__get_entity_by_key(self.containers_get, "name",
                                       request_dict["name"])
    if ctr_dto is not None:
      error_code = CurieError.kInvalidParameter
      error_msg = "Container %s already exists" % request_dict["name"]
      raise CurieException(error_code, error_msg)
    url = "%s/containers" % self.__base_url
    try:
      self.__post(url, request_dict, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if container %s was already created",
                  request_dict["name"])
      ctr_dto = self.__get_entity_by_key(self.containers_get, "name",
                                         request_dict["name"])
      if ctr_dto is None:
        # Container wasn't created.
        raise

  @nutanix_rest_api_method
  def containers_delete(self, container_id=None, container_name=None):
    """
    Invokes /containers/{id} [DELETE] to delete the specified container.
    Either 'container_id' or 'container_name' must be specified but not both.
    """
    CHECK((container_id is not None) != (container_name is not None))
    # First check if the container already does not exist.
    #
    # Note that while deletion is idempotent, we'll raise an exception here if
    # the container does not exist to begin with. This is to prevent mistyping
    # a container id/name from resulting in the desired container to delete not
    # being deleted.
    if container_id is not None:
      # Allow lookup to handle asserting if the container does not exist.
      self.__get_entity_by_key(self.containers_get, "id",
                               container_id, raise_on_failure=True)
      url = "%s/containers/%s" % (self.__base_url, container_id)
    else:
      # Allow lookup to handle asserting if the container does not exist.
      ctr_dto = self.__get_entity_by_key(self.containers_get, "name",
                                         container_name, raise_on_failure=True)
      container_id = ctr_dto["id"]
      url = "%s/containers/%s" % (self.__base_url, container_id)
    params = {"ignoreSmallFiles" : True}
    try:
      self.__delete(url, url_params=params, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if container %s was already deleted", container_id)
      ctr_dto = self.__get_entity_by_key(self.containers_get, "id",
                                         container_id)
      if ctr_dto is not None:
        # Container wasn't deleted.
        raise

  @nutanix_rest_api_method
  def containers_get(self,
                     container_id=None,
                     container_name=None,
                     max_retries=REST_API_MAX_RETRIES):
    """
    Invokes /containers [GET] to get information about all containers or
    /containers/{id} [GET] to get information about a specific container.
    Either 'container_id' or 'container_name' (but not both) must be specified
    in the latter case.
    """
    CHECK_NE(container_id, "")
    if container_id is None and container_name is None:
      # All containers.
      url = "%s/containers" % self.__base_url
    elif container_id is None and container_name is not None:
      # Specific container by container name.
      ctr_dto = self.__get_entity_by_key(self.containers_get, "name",
                                         container_name, raise_on_failure=True)
      container_id = ctr_dto["id"]
      url = "%s/containers/%s" % (self.__base_url, container_id)
    elif container_id is not None and container_name is None:
      # Specific container by container ID.
      url = "%s/containers/%s" % (self.__base_url, container_id)
    else:
      raise CurieException(CurieError.kInternalError,
                            "At most one of 'container_id' and "
                            "'container_name' can be set")
    return self.__get(url, max_retries=max_retries)

  @nutanix_rest_api_method
  def containers_update(self, request_dict):
    """
    Invokes /containers [PUT] to update a container. 'request_dict' is a
    Python dictionary corresponding to the ContainerDTO to use to update the
    container.
    """
    url = "%s/containers" % self.__base_url
    self.__put(url, request_dict)

  @nutanix_rest_api_method
  def datastores_get(self, max_retries=REST_API_MAX_RETRIES):
    """
    Invokes /containers/datastores [GET] to get information about all NFS
    datastores.
    """
    url = "%s/containers/datastores" % self.__base_url
    return self.__get(url, max_retries=max_retries)

  @nutanix_rest_api_method
  def datastores_create(self, container_name, datastore_name=None):
    """
    Invokes /containers/datastores/add_datastore [POST] to create a datastore
    for the container 'container_name'. If 'datastore_name' is specified, use
    that name rather than 'container_name' for the datastore name.
    """
    if datastore_name is None:
      datastore_name = container_name
    # First check if the datastore already exists.
    ds_dto = self.__get_entity_by_key(self.datastores_get, "datastoreName",
                                      datastore_name)
    if ds_dto is not None:
      error_code = CurieError.kInvalidParameter
      error_msg = "Datastore %s already exists" % datastore_name
      raise CurieException(error_code, error_msg)
    url = "%s/containers/datastores/add_datastore" % self.__base_url
    request_dict = {"containerName" : container_name,
                    "datastoreName" : datastore_name}
    try:
      self.__post(url, request_dict, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if datastore %s was already created",
                  datastore_name)
      ds_dto = self.__get_entity_by_key(self.datastores_get, "datastoreName",
                                        datastore_name)
      if ds_dto is None:
        # Datastore wasn't created.
        raise

  @nutanix_rest_api_method
  def datastores_delete(self, datastore_name, verify=True):
    """
    Invokes /containers/datastores/remove_datastore [POST] to delete the
    datastore with the name 'datastore_name'.

    Args:
      datastore_name (str): Name of datastore to delete.
      verify (bool): Optional. If True, attempt to confirm datastore no
        longer exists prior to returning.

    Raises:
      CurieError<kTimeout> on timeout
      HttpRequestException (reraised) on POST failure (unless datastore has
        already been removed)
    """
    # First check if the datastore already does not exist.
    #
    # Note that while deletion is idempotent, we'll raise an exception here if
    # the datastore does not exist to begin with. This is to prevent mistyping
    # a datastore name from resulting in the desired datastore to delete not
    # being deleted.

    # Allow lookup to handle asserting that entity exists.
    self.__get_entity_by_key(self.datastores_get, "datastoreName",
                             datastore_name, raise_on_failure=True)
    url = "%s/containers/datastores/remove_datastore" % self.__base_url
    request_dict = {"datastoreName" : datastore_name}
    try:
      self.__post(url, request_dict, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if datastore %s was already deleted",
                  datastore_name)
      ds_dto = self.__get_entity_by_key(self.datastores_get, "datastoreName",
                                        datastore_name)
      if ds_dto is not None:
        # Datastore wasn't deleted.
        raise

    if not verify:
      log.debug("As requested, skipping verification that datastore '%s' is "
                "no longer visible to any host", datastore_name)
      return

    # Verify that datastore no longer appears on any host.
    is_removed = lambda: self.__get_entity_by_key(
      self.datastores_get, "datastoreName", datastore_name) is None
    ret = CurieUtil.wait_for(
      is_removed,
      "confirmation that datastore '%s' has been removed" % datastore_name,
      timeout_secs=30, poll_secs=1)
    if ret is None:
      raise CurieException(
        CurieError.kTimeout,
        "Removal of datastore '%s' failed to complete within 30 seconds" %
        datastore_name)

    log.info("Datastore '%s' has been removed successfully", datastore_name)

  @nutanix_rest_api_method
  @validate_parameter("host_ip", basestring,
                      valid_func=lambda *args, **kwargs:
                      CurieUtil.is_ipv4_address(kwargs["host_ip"]))
  @validate_parameter("projection", str, _VALID_PROJ_MAP["hosts_get"])
  def hosts_get(
      self, host_name=None, host_id=None, host_ip=None, projection=None):
    """
    Invokes /hosts [GET] to get a listing of host information for all hosts
    on the cluster.

    NB: Only one of 'host_ip', 'host_id', or 'host_name' should be provided.

    If 'host_ip' is provided, passes params:
      {"searchString": <host_ip>,
       "searchAttributeList": "ipv4_addresses"}
    If 'host_name' is provided, passes params:
      {"searchString": <host_name>,
       "searchAttributeList": "name"}
    If 'host_id' is provided, defers to 'hosts_get_by_id'.
    """
    if (any([host_name, host_id, host_ip]) and
        not bool(host_name) ^ bool(host_id) ^ bool(host_ip)):
      raise CurieException(
        CurieError.kInvalidParameter,
        "'hosts_get' accepts at most one of 'host_ip', 'host_name', or "
        "'host_id'")

    params = {}
    # TODO (jklein): Re-enable after Prism search bug in ENG-69870 is resolved.
    # if host_ip:
    #   params["searchString"] = host_ip
    #   params["searchAttributeList"] = "ipv4_addresses"
    # elif host_name:
    #   params["searchString"] = host_name
    #   params["searchAttributeList"] = "node_name"
    if host_id:
      return self.hosts_get_by_id(host_id, projection=projection)
    if projection:
      params["projection"] = projection
    ret = self.__get("%s/hosts" % self.__base_url, url_params=params)

    if not (host_ip or host_name):
      return ret

    # TODO (jklein): Re-enable after Prism search bug in ENG-69870 is resolved.
    # if len(ret.get("entities", [])) != 1:
    if host_ip:
      attr_name = "hypervisorAddress"
      target_attr = host_ip
    else:
      attr_name = "name"
      target_attr = host_name

    entities = [entity for entity in ret.get("entities", [])
                if entity[attr_name] == target_attr]

    if len(entities) != 1:
      raise CurieException(
        CurieError.kInvalidParameter,
        "Unable to locate host '%s'" % host_ip if host_ip else host_name)

    # TODO (jklein): Re-enable after Prism search bug in ENG-69870 is resolved.
    #return ret["entities"][0]
    return entities[0]

  @nutanix_rest_api_method
  @validate_parameter("projection", (str, type(None)),
                      _VALID_PROJ_MAP["hosts_get"] + [None])
  def hosts_get_by_id(self, host_id, projection=None):
    """
    Invokes /hosts/<host_id> [GET] to lookup information for 'host_id'.
    """
    params = {}
    if projection:
      params["projection"] = projection
    return self.__get("%s/hosts/%s" % (self.__base_url, host_id),
                      url_params=params)

  @nutanix_rest_api_method
  def hosts_stats_get_by_id(self, host_id, metric_names, startTimeInUsecs=None,
                            endTimeInUsecs=None, intervalInSecs=None):
    """
    Invokes /hosts/<host_id>/stats [GET] to get stats information for a
    physical host in the cluster.
    """
    params = {"metrics": metric_names,
              "startTimeInUsecs": startTimeInUsecs,
              "endTimeInUsecs": endTimeInUsecs,
              "intervalInSecs": intervalInSecs}
    return self.__get("%s/hosts/%s/stats" % (self.__base_url, host_id),
                      url_params=params)

  @nutanix_rest_api_method
  def protection_domains_create(self, pd_name):
    """
    Invokes /protection_domains [POST] to create a protection domain with the
    name 'pd_name'.
    """
    # First check if the protection domain already exists.
    pd = self.__get_entity_by_key(self.protection_domains_get, "name", pd_name)
    if pd:
      raise CurieException(CurieError.kInvalidParameter,
                            "Protection domain %s already exists" % pd_name)
    url = "%s/protection_domains" % self.__base_url
    # Note: need to use "value" for the protection domain name as opposed to
    # "name" here due to an inconsistency in the v1 version of the Nutanix REST
    # API.
    request_dict = {"value" : pd_name}
    # Note: if a PD with the same name was recently deleted and we attempt to
    # create it again, this can fail with an error message indicating
    # this. Unfortunately, there is no precise way to determine this as
    # searching for the error text in the HTTP error response isn't robust
    # across different versions of Nutanix software. In the case of curie
    # tests, this shouldn't matter much since each test will always create PDs
    # that emded the test's IDs in the PD name so we shouldn't be
    # deleting/creating PDs with the same name.
    try:
      self.__post(url, request_dict, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if protection domain %s was already created",
                  pd_name)
      pd = self.__get_entity_by_key(self.protection_domains_get, "name",
                                    pd_name)
      if pd is None:
        # Protection domain wasn't created.
        raise

  @nutanix_rest_api_method
  def protection_domains_delete(self, pd_name):
    """
    Invokes /protection_domains/{name} [DELETE] to delete the protection domain
    with the name 'pd_name'.
    """
    # First check if the protection domain already does not exist.
    #
    # Note that while deletion is idempotent, we'll raise an exception here if
    # the protection domain does not exist to begin with. This is to prevent
    # mistyping a protection domain name from resulting in the desired
    # protection domain to delete not being deleted.

    # Allow lookup to handle asserting if PD does not exist.
    self.__get_entity_by_key(self.protection_domains_get, "name",
                             pd_name, raise_on_failure=True)
    url = "%s/protection_domains/%s" % (self.__base_url, pd_name)
    try:
      self.__delete(url, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if protection domain %s was already deleted",
                  pd_name)
      pd = self.__get_entity_by_key(self.protection_domains_get, "name",
                                    pd_name)
      if pd is not None:
        # Protection domain wasn't deleted.
        raise

  @nutanix_rest_api_method
  def protection_domains_protect_vms(self, pd_name, vm_names):
    """
    Invokes /protection_domains/{name}/protect_vms [POST] to protect the
    specified list of VMs using the protection domain 'pd_name'.
    """
    # First check that the protection domain exists and that none of the VMs
    # specified are already protected by the protection domain.

    # Allow lookup to handle asserting if PD does not exist.
    pd = self.__get_entity_by_key(self.protection_domains_get, "name",
                                  pd_name, raise_on_failure=True)
    to_protect_vm_name_set = set(vm_names)
    protected_vm_names = [vm["vmName"] for vm in pd["vms"]
                          if vm["vmName"] in to_protect_vm_name_set]
    if protected_vm_names:
      error_code = CurieError.kInvalidParameter
      error_msg = "VMs %s already protected in protection domain %s" % \
        (protected_vm_names, pd_name)
      raise CurieException(error_code, error_msg)
    url = "%s/protection_domains/%s/protect_vms" % \
      (self.__base_url, pd_name)
    request_dict = {"names" : vm_names}
    try:
      self.__post(url, request_dict, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("Checking if VMs %s already protected in protection domain "
                  "%s", to_protect_vm_name_set, pd_name)
      pd = self.__get_entity_by_key(self.protection_domains_get, "name",
                                    pd_name)
      if pd is not None:
        protected_vm_names = [vm["vmName"] for vm in pd["vms"]
                              if vm["vmName"] in to_protect_vm_name_set]
        CHECK_LE(len(protected_vm_names), len(to_protect_vm_name_set))
        if len(protected_vm_names) == len(to_protect_vm_name_set):
          log.info("VMs %s already protected in protection domain %s",
                   to_protect_vm_name_set, pd_name)
          return
        log.error("VMs %s still not protected in protection domain %s",
                  to_protect_vm_name_set - set(protected_vm_names), pd_name)
      raise

  @nutanix_rest_api_method
  def protection_domains_oob_schedules(self, pd_name):
    """
    Invokes /protection_domains/{name}/oob_schedules [POST] to protect the VMs
    using the protection domain 'pd_name'. This oob_schedule is defaulted to
    take a snapshot immediately.
    """
    pd = self.__get_entity_by_key(self.protection_domains_get, "name",
                                  pd_name, raise_on_failure=True)
    if len(pd["vms"]) <= 0:
      error_code = CurieError.kInvalidParameter
      error_msg = "No VMs exists in protection domain %s" % pd_name
      raise CurieException(error_code, error_msg)

    url = "%s/protection_domains/%s/oob_schedules" % (self.__base_url, pd_name)
    try:
      self.__post(url, {}, max_retries=REST_API_MAX_RETRIES)
    except HttpRequestException:
      log.warning("protection_domains_oob_schedules failed for one off "
                  "snapshot create %s", pd_name)
      raise

  @nutanix_rest_api_method
  def protection_domains_get(self,
                             pd_name=None,
                             max_retries=REST_API_MAX_RETRIES):
    """
    Invokes /protection_domains [GET] to get information about all
    protection_domains or /protection_domains/{name} [GET] to get information
    about a specific protection domain.
    """
    if pd_name is None:
      # All protection_domains.
      url = "%s/protection_domains" % self.__base_url
    else:
      # Specific protection domain by protection domain ID.
      url = "%s/protection_domains/%s" % (self.__base_url, pd_name)
    return self.__get(url, max_retries=max_retries)

  @nutanix_rest_api_method
  def snapshots_schedules_create(self, request_dict):
    """
    Invokes /protection_domains/{name}/schedules [POST] to create a snapshot
    schedule for a protection domain. 'request_dict' is a Python dictionary
    corresponding to the policy for the protection domain snapshot schedule.

    Example:

      request_dict = {"pdName" : "my_pd",
                      "type" : "HOURLY",
                      "everyNth" : 1,
                      "userStartTimeInUsecs" : now_usecs,
                      "retentionPolicy" : {"localMaxSnapshots": 4}}
    """
    # Check for required fields in 'request_dict'.
    for name in ["pdName", "type", "userStartTimeInUsecs"]:
      if name not in request_dict:
        error_code = CurieError.kInvalidParameter
        error_msg = "Missing required field %s" % name
        raise CurieException(error_code, error_msg)
    # First get the current set of snapshot schedules for the protection
    # domain.
    protection_domain_name = request_dict["pdName"]
    old_schedules = self.snapshots_schedules_get(protection_domain_name)
    url = "%s/protection_domains/%s/schedules" % (self.__base_url,
                                                  protection_domain_name)
    max_retries = REST_API_MAX_RETRIES
    for xx in xrange(max_retries):
      try:
        self.__post(url, request_dict)
        # Request succeeded.
        break
      except HttpRequestException:
        log.warning("Checking if snapshot schedule already created for "
                    "protection domain %s (attempt %d)",
                    protection_domain_name, xx)
        if xx == (max_retries - 1):
          error_code = CurieError.kClusterApiError
          error_msg = "Failed to create snapshot schedule on %s" % \
            protection_domain_name
          raise CurieException(error_code, error_msg)
        # Check if the request succeeded. If we succeeded, we should have one
        # more snapshot schedule, the one corresponding to 'request_dict'.
        new_schedules = self.snapshots_schedules_get(protection_domain_name)
        if len(new_schedules) == (len(old_schedules) + 1):
          break
      time.sleep(1)

  @nutanix_rest_api_method
  def snapshots_schedules_delete(self, protection_domain_name):
    """
    Invokes /protection_domains/{name}/schedules [DELETE] to delete all
    snapshot schedules for the protection domain with name
    'protection_domain_name'.
    """
    url = "%s/protection_domains/%s/schedules" % (self.__base_url,
                                                  protection_domain_name)
    self.__delete(url, max_retries=REST_API_MAX_RETRIES)

  @nutanix_rest_api_method
  def snapshots_schedules_get(self, protection_domain_name):
    """
    Invokes /protection_domains/{name}/schedules [GET] to get information about
    the periodic snapshot schedules for the protection_domain
    'protection_domain_name'.
    """
    url = "%s/protection_domains/%s/schedules" % (self.__base_url,
                                                  protection_domain_name)
    return self.__get(url)

  @nutanix_rest_api_method
  def snapshots_delete(self, protection_domain_name, snapshot_ids=()):
    """
    Invokes /protection_domains/{name}/dr_snapshots/{snapshot_id} [DELETE] to
    delete either all snapshots for the specified protection domain (empty list
    in 'snapshot_ids') or a specified list of snapshots.
    """
    # Compute the set of snapshot IDs for the snapshots to delete.
    if not snapshot_ids:
      ret = self.snapshots_get(protection_domain_name)
      snapshot_ids = [snapshot["snapshotId"] for snapshot in ret["entities"]]
      log.info("Preparing to delete all snapshots in %s",
               protection_domain_name)
    else:
      # The Nutanix REST API uses strings for snapshot IDs even though they are
      # integers, so cast the values to strings in case integers were passed
      # in.
      snapshot_ids = [str(snapshot_id) for snapshot_id in snapshot_ids]
      log.info("Preparing to delete %d snapshots in PD %s",
               len(snapshot_ids), protection_domain_name)
    snapshot_id_set = set(snapshot_ids)
    # Delete the snapshots.
    timeout_secs = min(300, len(snapshot_ids) * 30)
    t1 = time.time()
    while True:
      ret = self.snapshots_get(protection_domain_name)
      to_delete_snapshots = [snapshot for snapshot in ret["entities"]
                             if snapshot["snapshotId"] in snapshot_id_set]
      num_snapshots_deleted = 0
      for snapshot in to_delete_snapshots:
        if snapshot["state"] == "SCHEDULED":
          log.warning("Cannot delete snapshot %s yet, still scheduled",
                      snapshot["snapshotId"])
          continue
        url = "%s/protection_domains/%s/dr_snapshots/%s" % \
          (self.__base_url, protection_domain_name, snapshot["snapshotId"])
        try:
          self.__delete(url, max_retries=REST_API_MAX_RETRIES)
        except HttpRequestException:
          # Just emit a WARNING since the enclosing while loop will take of
          # checking what snapshots are remaining and attempting to delete
          # them.
          log.warning("Error deleting snapshot %s in PD %s",
                      snapshot["snapshotId"], protection_domain_name,
                      exc_info=True)
        log.info("Deleted snapshot %s in PD %s",
                 snapshot["snapshotId"], protection_domain_name)
        num_snapshots_deleted += 1
      if num_snapshots_deleted == len(to_delete_snapshots):
        break
      t2 = time.time()
      if t2 - t1 > timeout_secs:
        error_code = CurieError.kTimeout
        error_msg = "Timeout waiting to delete snapshots for PD %s" % \
          protection_domain_name
        raise CurieException(error_code, error_msg)
      time.sleep(5)

  @nutanix_rest_api_method
  def snapshots_get(self, protection_domain_name):
    """
    Invokes /protection_domains/{name}/dr_snapshots [GET] to get information
    about all snapshots for the protection_domain 'protection_domain_name'.
    """
    url = "%s/protection_domains/%s/dr_snapshots" % (self.__base_url,
                                                     protection_domain_name)
    return self.__get(url)

  @nutanix_rest_api_method
  @validate_parameter("projection", str, _VALID_PROJ_MAP["vms_get"])
  def vms_get(self, cvm_only=False, vm_ip=None, vm_name=None,
              projection="BASIC_INFO", drop_duplicates=True):
    """
    Invokes /vms/ [GET] to list VM entites for cluster.

    Note:
      If 'vm_ip' is set  projection=HEALTH: will not return the healthSummary
      as part of the entity.

    Args:
      cvm_only (bool): Optional. If True, filter entities, returning CVMs only.
      vm_ip (str): Optional. If provided, return only entities whose IP matches
        the provided address.
      vm_name (str): Optional. If provided, return only entities whose vmName
        matches the provided string.
      projection (str): Optional. If projection is provided as one of:
          ALL, STATS, PERF_STATS, USAGE_STATS, HEALTH, ALERTS, ALERTS_COUNT,
          BASIC_INFO
        The query will be sent with URL params specifying the requested
        projection.
      drop_duplicates (bool): If true, drop duplicates from the response
        "entities". This is a workaround for ENG-75906.

    NB: Provide at most one of 'vm_ip' and 'vm_name'.
    """
    if vm_ip and vm_name:
      raise CurieException(CurieError.kInvalidParameter,
                            "Provide at most one of 'vm_name' and 'vm_ip'")

    url = "%s/vms/" % self.__base_url

    params = {}
    if cvm_only:
      params["filterCriteria"] = "is_cvm==1"
    if vm_ip:
      params.update({"searchString": vm_ip,
                     "searchAttributeList": "ip_addresses"})
    elif vm_name:
      params.update({"searchString": vm_name,
                     "searchAttributeList": "vm_name"})
    if projection:
      params["projection"] = projection

    result_json = self.__get(url, url_params=params)

    # TODO(ryan.hardin): Remove `drop_duplicates` when ENG-75906 is fixed.
    if drop_duplicates:
      vm_uuids = []
      duplicate_uuids = []
      filtered_entities = []
      for vm in result_json["entities"]:
        uuid = vm["uuid"]
        if uuid in vm_uuids:
          duplicate_uuids.append(uuid)
        else:
          vm_uuids.append(uuid)
          filtered_entities.append(vm)
      num_duplicates = len(duplicate_uuids)
      if num_duplicates > 0:
        log.debug("Duplicate VM UUID(s): %r", duplicate_uuids)
        log.warning("%d redundant VM UUIDs found in response to get VMs. The "
                    "duplicate entities will be dropped. See debug logs for "
                    "more detail.", num_duplicates)
        result_json["entities"] = filtered_entities
    return result_json

  @nutanix_rest_api_method
  @validate_parameter("projection", str, _VALID_PROJ_MAP["vms_get"])
  def vms_get_by_id(self, vm_id, projection=None):
    """
    Invokes /vms/<vm_id> [GET] to lookup VM information for the VM 'vm_id'.

    Args:
      vm_id (str): UUID of VM to lookup.
      projection (str): Optional. If projection is provided as one of:
          ALL, STATS, PERF_STATS, USAGE_STATS, HEALTH, ALERTS, ALERTS_COUNT
        The query will be sent with URL params specifying the requested
        projection.
    """
    url = "%s/vms/%s/" % (self.__base_url, vm_id)

    params = {"projection": projection} if projection else {}

    return self.__get(url, url_params=params)

#==============================================================================
# Public Management APIs
#==============================================================================

  @nutanix_rest_api_method
  def ha_get(self):
    """
    Gets current HA configuration.
    """
    return self.__get("%s/ha/" % self.__base_mgmt_url)

  # TODO (jklein): Clean this up.
  def images_create(self, name, source_file, dest_ctr_id, annotation=None):
    """
    Creates new image by uploading 'source_file' as 'name' to 'dest_ctr_id'.
    """
    req = {"name": name, "annotation": annotation}
    url = "%s/images/" % self.__base_mgmt_url_v2
    # Acquire any additional headers when connected to a PC.
    pc_uuid_header = self._get_pc_uuid_header()
    resp = self.__post(url, req, headers=pc_uuid_header)
    task_id = resp.get("task_uuid")
    if not task_id:
      raise CurieException(CurieError.kManagementServerApiError,
                            "Unable to upload '%s' to container '%s' as '%s'" %
                            (source_file, dest_ctr_id, name))

    results_map = TaskPoller.execute_parallel_tasks(
      tasks=PrismTask.from_task_id(self, task_id), timeout_secs=60)
    result = results_map[task_id]
    created_entities = result.entity_list
    if len(created_entities) != 1:
      raise CurieException(CurieError.kManagementServerApiError,
                            "Unable to upload '%s' to container '%s' as '%s'" %
                            (source_file, dest_ctr_id, name))

    img_id = created_entities[0].uuid

    class progress(object):
      def __init__(self):
        self.acc = 0
        self.persistent_acc = 0
        self.total = 0
        self.timestamp = time.time()

      def __call__(self, num_bytes):
        self.acc += num_bytes
        self.persistent_acc += num_bytes

      def get(self):
        delta = time.time() - self.timestamp
        num_kb = int(self.acc / 1e3)

        self.acc = 0
        self.timestamp = time.time()

        kBps = num_kb / delta
        pct = 100 * self.persistent_acc / float(self.total)
        if pct == 100 and kBps == 0:
          return ""
        ret = ("Read %s kB in %.2fs seconds (%.2f kB/sec) %s/%s bytes (%.2f%%)"
               % (num_kb, delta, kBps, self.persistent_acc, self.total, pct))
        return ret

    prog_monitor = progress()
    if not isinstance(source_file, (file, tarfile.ExFileObject)):
      log.info("Assuming '%s' is a file path", source_file)
      fh = FileHandleWithCallback(prog_monitor, source_file, "rb")
    else:
      # TODO (jklein): Fix this
      log.info("Assuming '%s' is a file-like object", source_file)
      # fh = FileHandleWithCallback.from_file(prog_monitor, source_file)
      fh = source_file
    if callable(fh.size):
      prog_monitor.total = fh.size()
    else:
      prog_monitor.total = fh.size

    def _upload_image():
      #with fh:
      try:
        log.info("Uploading image from path '%s'", source_file)
        headers = {
            "X-Nutanix-Destination-Container": dest_ctr_id,
            "Content-Type": "application/octet-stream"
          }
        # Append any extra header to handle PC
        headers.update(pc_uuid_header)
        # The v0.8 API must be used here for the upload.
        resp = self.__issue_request(
          "%s/images/%s/upload" % (self.__base_mgmt_url, img_id),
          "PUT", data=fh, headers=headers,
          return_raw_response_obj=True, max_retries=2, timeout=60*60)
      except BaseException:
        fh.__exit__(*sys.exc_info())
        raise

      if resp.status_code != 200:
        raise CurieException(CurieError.kManagementServerApiError,
          ("Unable to upload '%s' to container '%s' as '%s'. Code: %d, "
           "Response: %s" % (source_file, dest_ctr_id, name, resp.status_code,
                             resp.text)))

    upload_thread = threading.Thread(target=_upload_image,
                                     name="ImageUpload-%s" % name)
    upload_thread.daemon = True
    upload_thread.start()

    tasks = []
    task_get_start_secs = time.time()
    while not tasks:
      tasks = self.tasks_get(entity_uuids=[img_id,]).get("entities", [])
      time.sleep(1)
      # Task creation timeout.
      if time.time() - task_get_start_secs > 120:
        raise CurieException(CurieError.kTimeout,
                              "Timed out waiting for image upload task to be "
                              "created")

    assert len(tasks) == 1, "Unable to locate unique image upload task"
    return img_id, tasks[0]["uuid"], prog_monitor

  @nutanix_rest_api_method
  def images_get(self, image_id=None):
    """
    Retrieves list of images registered with the cluster image service.
    """
    url = [self.__base_mgmt_url_v2, "images"]
    if image_id:
      url.append(image_id)
    return self.__get("/".join(url))

  @nutanix_rest_api_method
  def images_delete(self, image_uuids):
    """
    Deletes images with UUIDs in 'image_uuids'.

    Returns:
      dict<str, str> Map of UUID to deletion task UUID.
    """
    url = "/".join([self.__base_mgmt_url_v2, "images", r"%s"])
    ret_map = {}
    headers = self._get_pc_uuid_header()
    for uuid in image_uuids:
      ret_map[uuid] = self.__delete(url % uuid, headers=headers)["task_uuid"]
    return ret_map

  @nutanix_rest_api_method
  def networks_get(self, max_retries=REST_API_MAX_RETRIES):
    """
    Invokes /networks [GET] to retrieve a list of configured networks.
    """
    return self.__get("%s/networks" % self.__base_mgmt_url,
                      max_retries=max_retries)

  @nutanix_rest_api_method
  def snapshot_create(self, vm_id, snapshot_name=None, snapshot_uuid=None):
    req = {"snapshotSpecs": [{"vmUuid": vm_id, "snapshotName": snapshot_name,
                              "uuid": snapshot_uuid}]}

    return self.__post("%s/snapshots" % self.__base_mgmt_url, req)["taskUuid"]

  @nutanix_rest_api_method
  def tasks_get(self, entity_types=(), entity_uuids=(),
                operation_types=(), include_completed=False,
                start_time_usecs=0, count=None):
    """
    Fetch tasks via 'GET /tasks/'
    """
    url_params = {"entityTypes": ",".join(entity_types),
                  "entityUuids": ",".join(entity_uuids),
                  "operationTypeList": ",".join(operation_types),
                  "includeCompleted": include_completed,
                  "epochCutOffTime": start_time_usecs}
    if count is not None:
      url_params["count"] = count

    url = "%s/tasks/" % self.__base_mgmt_url
    return self.__get(url, url_params=url_params)

  @nutanix_rest_api_method
  def tasks_get_by_id(self, task_id, include_entity_names=True):
    """
    Fetch a single task via 'GET /tasks/{task_id}/.
    """
    url_params = {"includeEntityNames": include_entity_names}
    url = "%s/tasks/%s" % (self.__base_mgmt_url, task_id)
    return self.__get(url, url_params=url_params)

  @nutanix_rest_api_method
  def vms_clone(self, vm_id, clone_spec):
    return self.__post("%s/vms/%s/clone" %
                       (self.__base_mgmt_url, vm_id), clone_spec)["taskUuid"]

  @nutanix_rest_api_method
  def vdisk_clone(self, source_paths, destination_paths, overwrite=False):
    post_data = [{
      "absoluteSourceFilePath": source_path,
      "absoluteDestinationFilePath": destination_path,
      "destinationOverwriteGuarded": not overwrite
    } for source_path, destination_path in zip(source_paths, destination_paths)]
    return self.__post("%s/vdisks/snapshots/nfs" % self.__base_url,
                       post_data)

  @nutanix_rest_api_method
  def vms_create(self, vm_desc, nic_specs):
    log.info("Creating VM '%s' with %s MB RAM and %s vCPUs",
             vm_desc.name, vm_desc.memory_mb, vm_desc.num_vcpus)
    req = vm_desc.to_ahv_vm_create_spec()
    req["vmNics"] = nic_specs
    log.debug("POSTing spec:\n%s", json.dumps(req, indent=2, sort_keys=True))
    return self.__post("%s/vms/" % self.__base_mgmt_url, req)

  @nutanix_rest_api_method
  def vms_delete(self, vm_ids):
    """
    Deletes all VMs specified by 'vm_ids'.

    Returns:
      (dict) Map of VM IDs to corresponding deletion task UUIDs, or None if
        the DELETE failed for a given ID.
    """
    ret_map = {}
    for vm_id in vm_ids:
      try:
        resp = self.__delete("%s/vms/%s/" % (self.__base_mgmt_url, vm_id))
        ret_map[vm_id] = resp["taskUuid"]
      except BaseException:
        log.exception("DELETE request for VM '%s' failed", vm_id)
        ret_map[vm_id] = None

    return ret_map

  @nutanix_rest_api_method
  def vms_get_by_id_mgmt_api(self, vm_id, include_vm_disk_sizes=False,
                             include_address_assignments=False):
    """
    Invokes /vms/<vm_id> [GET] to lookup VM information for the VM 'vm_id'.

    Args:
      vm_id (str): UUID of VM to lookup.
      include_vm_disk_sizes (bool): Optional. Whether to include disk sizes
        (in bytes). Default False.
      include_address_assignments (bool): Optional. Whether to include address
        assignments: Default False.
    """
    url = "%s/vms/%s/" % (self.__base_mgmt_url, vm_id)

    return self.__get(url, url_params={
      "includeVMDiskSizes": include_vm_disk_sizes,
      "includeAddressAssignments": include_address_assignments
    })

  @nutanix_rest_api_method
  def vms_migrate(self, vm_id, host_id, live=True, bandwidth_cap=None):
    """
    Migrates 'vm_id' to 'host_id'.
    """
    # Lookup VM logical timestamp to allow for idempotent retries.
    vm_json = self.vms_get_by_id_mgmt_api(vm_id)
    req = {"live": live, "bandwidthMbps": bandwidth_cap,
           "uuid": vm_id, "hostUuid": host_id,
           "vmLogicalTimeStamp": vm_json["logicalTimestamp"]}
    return self.__post("%s/vms/%s/migrate" %
                       (self.__base_mgmt_url, vm_id), req,
                       max_retries=5)["taskUuid"]

  @nutanix_rest_api_method
  @validate_parameter("op", valid_values=["on", "off"])
  def vms_power_op(self, vm_ids, op):
    """
    Performs power operation 'op' on all VMs specified by 'vm_ids'.

    Args:
      vm_ids (list<str>): List of VM IDs to which 'op' should be applied.
      op (str): Power op to apply to 'vm_ids'. Should be one of "on", "off".

    Returns:
      (dict) Map of VM IDs to corresponding power-op task UUIDs, None if
        the operation failed for a given ID, or True if the operation is a
        no-op.

    Raises:
      CurieException<kInvalidParameter> if 'op' is not a valid operation.
    """
    # TODO (don't use True as an indicator)
    ret = {}
    vm_id_status_map = dict((vm["uuid"], vm)
                            for vm in self.vms_get()["entities"])
    for vm_id in vm_ids:
      if vm_id_status_map[vm_id].get("powerState") == op:
        ret[vm_id] = True
        continue
      try:
        resp = self.__post("%s/vms/%s/power_op/%s/" %
                           (self.__base_mgmt_url, vm_id, op), {})
        ret[vm_id] = resp["taskUuid"]
      except BaseException:
        log.exception("Power op request for VM '%s' failed", vm_id)
        ret[vm_id] = None

    return ret

  @nutanix_rest_api_method
  @validate_parameter("transition", valid_values=[
    "acpi_reboot", "acpi_shutdown", "on", "off", "powercycle"])
  def vms_set_power_state_for_vms(self, vm_ids, transition, host_ids=None):
    """
    Alter power state on 'vm_id' via 'transition'.

    Args:
      vm_ids (list<str>): VM IDs whose power state to change.
      transition (str): Power state in which to put 'vm_id'.
        One of "acpi_reboot", "acip_shutdown", "on", "off", "powercycle"
      host_ids (list<str>|None): Optional. If provided, if 'vm_id[ii]' is being
        powered on or power cycled, it should be scheduled on 'host_id[ii]'.

    Returns:
      (dict) Map of VM IDs to corresponding power-op task UUIDs, None if
        the operation failed for a given ID, or True if the operation is a
        no-op.
    """
    # TODO (jklein): Clean this up, deal with the ambiguity of having multiple
    # power APIs for VMs.
    if host_ids is None:
      host_ids = [None] * len(vm_ids)

    ret = {}
    vm_id_status_map = dict((vm["uuid"], vm)
                            for vm in self.vms_get()["entities"])
    req = {"transition": transition}

    for ii, vm_id in enumerate(vm_ids):
      if vm_id_status_map.get(vm_id, {}).get("powerState") == transition:
        ret[vm_id] = True
        continue
      try:
        if any(host_ids) and transition in ["acpi_reboot", "powercycle", "on"]:
          req["hostUuid"] = host_ids[ii]
        resp = self.__post("%s/vms/%s/set_power_state/" %
                           (self.__base_mgmt_url, vm_id), req,
                           max_retries=REST_API_MAX_RETRIES)
        ret[vm_id] = resp["taskUuid"]
      except BaseException:
        log.exception("Power op request for VM '%s' failed", vm_id)
        ret[vm_id] = None

    return ret

  @nutanix_rest_api_method
  def vms_nic_add(self, vm_id, req):
    """
    Adds a new NIC to 'vm_id' on the network 'network_uuid'.

    Args:
      vm_id (str): VM ID to which NIC should be added.
      req (dict): JSON request body.

    Returns:
      (dict/JSON) response
    """
    return self.__post("%s/vms/%s/nics/" % (self.__base_mgmt_url, vm_id), req)

#==============================================================================
# Internal APIs
#==============================================================================

  @nutanix_rest_api_method
  @validate_return(valid_func=lambda ret, self:
                   self.__has_keys(ret, ["lockdown_mode", "state", "retry",
                                         "svms"]),
                   err_code=CurieError.kClusterApiError)
  def genesis_cluster_status(self):
    """
    Invokes /genesis to issue a 'ClusterManager.status' RPC to Genesis.
    """
    # Verify that API is applicable to cluster, else abort.
    version = self.get_nutanix_metadata().version
    if PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1, version):
      return self.__issue_genesis_rpc_v1("ClusterManager", "status",
                                         max_retries=5)
    raise CurieException(CurieError.kInternalError,
                          "Unsupported Nutanix version '%s'" % version)

  @nutanix_rest_api_method
  @validate_return(valid_func=lambda ret, self:
                   self.__has_keys(ret, ["services", "state"]),
                   err_code=CurieError.kClusterApiError)
  def genesis_node_services_status(self):
    """
    Invokes /genesis to issue a 'NodeManager.services_status' RPC to Genesis.
    """
    # Verify that API is applicable to cluster, else abort.
    version = self.get_nutanix_metadata().version
    if PrismAPIVersions.is_applicable(PrismAPIVersions.GENESIS_V1, version):
      return self.__issue_genesis_rpc_v1("NodeManager", "services_status",
                                         max_retries=5)
    raise CurieException(CurieError.kInternalError,
                          "Unsupported Nutanix version '%s'" % version)

  @nutanix_rest_api_method
  def genesis_current_master(self):
    """
    Invokes /genesis to issue a 'ClusterManager.request_shutdown_token' RPC.

    Returns:
      (bool) True if token is acquired, else False.
    """
    ret = self.__issue_genesis_rpc_v1("ClusterManager", "current_master")
    log.info("Current genesis master: %s", ret)
    return ret

  @nutanix_rest_api_method
  def genesis_clear_shutdown_token(self):
    """
    Invokes /genesis to issue a 'ClusterManager.clear_shutdown_token' RPC.

    Returns:
      (bool) True if token is cleared, else False.
    """
    return self.__issue_genesis_rpc_v1("ClusterManager",
                                       "clear_shutdown_token", max_retries=10)

  @nutanix_rest_api_method
  def genesis_prepare_node_for_shutdown(self, host_id):
    """
    Invokes /genesis to issue a 'ClusterManager.prepare_node_for_shutdown' RPC.

    NB: The RPC blocks until shutdown token has been acquired.
    """
    try:
      host = self.hosts_get_by_id(host_id)
      self.__issue_genesis_rpc_v1(
        "ClusterManager", "prepare_node_for_shutdown",
        svm_ip=host["serviceVMExternalIP"],
        method_kwargs={"reason": "Curie test scenario"})
      return True
    except Exception:
      log.exception("Failed to prepare node '%s' for shutdown", host_id)
      return False

  @nutanix_rest_api_method
  def genesis_shutdown_hypervisor(self, host_id):
    """
    Invokes /genesis to issue a 'ClusterManager.shutdown_hypervisor' RPC.

    Returns:
      (bool) True on success, else False.
    """
    try:
      host = self.hosts_get_by_id(host_id)
      svm_id = int(host["serviceVMId"].split("::")[1])
      return self.__issue_genesis_rpc_v1(
        "ClusterManager", "shutdown_hypervisor",
        method_kwargs={"svm_id": svm_id})
    except Exception:
      log.exception("Failed to shutdown node '%s'", host_id)
      return False

#==============================================================================
# Public Util
#==============================================================================

  def cleanup_nutanix_state(self, test_ids):
    """
    Clean up any Nutanix-cluster specific state for specified tests. If no test
    IDs are specified (empty list), then Nutanix-cluster specific state is
    cleaned up for all tests regardless of their state.

    Assumption: all datastores for 'test_ids' are empty.
    """
    # Delete all Nutanix entities for 'test_ids'.
    try:
      if self.datastores_applicable_to_cluster():
        ds_names = set([ds_dto["datastoreName"] for ds_dto
                        in self.datastores_get()])
        to_delete_ds_names, _ = NameUtil.filter_test_entity_names(
          ds_names, test_ids=test_ids)
        for ds_name in to_delete_ds_names:
          log.info("Deleting datastore %s", ds_name)
          self.datastores_delete(ds_name)
      else:
        log.info("Skipping datastore removal, datastores not applicable to "
                 "current cluster")
      ctr_names = set([ctr_dto["name"] for ctr_dto
                       in self.containers_get()["entities"]])
      to_delete_ctr_names, _ = \
        NameUtil.filter_test_entity_names(ctr_names, test_ids=test_ids)
      for ctr_name in to_delete_ctr_names:
        log.info("Deleting container %s", ctr_name)
        self.containers_delete(container_name=ctr_name)
    except CurieException:
      # If we're here, this implies we've already deleted all VMs on the test
      # datastores successfully, so we'd expect deletion of the test datastores
      # to succeed since the datastores should be empty. However, we have seen
      # cases where ESX can have non-VM files open on the datastore (e.g.,
      # various dotfiles). Eventually, the open files seem to get closed at
      # which point we can then delete the datastores later in GC.
      log.exception("Error deleting datastores/containers for %s (will retry "
                    "in GC when curie is restarted)", test_ids)
    pd_names = [pd["name"] for pd in self.protection_domains_get()
                if pd["name"].startswith("__curie")]
    to_delete_pd_names, _ = \
      NameUtil.filter_test_entity_names(pd_names, test_ids=test_ids)
    for pd_name in to_delete_pd_names:
      log.info("Deleting protection domain %s", pd_name)
      self.snapshots_schedules_delete(pd_name)
      self.snapshots_delete(pd_name)
      self.protection_domains_delete(pd_name=pd_name)

  def datastores_applicable_to_cluster(self):
    """
    Invokes /containers/datastores [GET] checking for an HTTP 500 error.
    """
    url = "%s/containers/datastores" % self.__base_url
    resp = self.__issue_request(url, "GET", return_raw_response_obj=True)

    if resp.status_code == 200:
      return True
    elif resp.status_code == 500:
      return False
    else:
      raise HttpRequestException(resp.status_code, resp.content)

  # TODO (jklein): Clean this up.
  def get_nutanix_metadata(self, short_only=False, cached_ok=True):
    """
    Issue a Nutanix REST API call to try and fetch Nutanix-specific metadata.
    Attemps to lookup the NOS version running on the cluster, the cluster's
    UUID, and its incarnation ID.

    Args:
      short_only (bool): Optional. Whether to skip to fetching the short
        version rather than first trying for the full version. Default False.
      cached_ok (bool): Optional. If True, allow returning cached data if
        present. Default True.

    Returns:
      (NutanixMetadata) Instance where if a particular field is unavailable,
        is it set to None.
    """
    clusters = self.clusters_get()["entities"]
    CHECK_EQ(len(clusters), 1, "Failed to lookup unique cluster entity")
    cluster = clusters[0]

    ret = NutanixMetadata()
    if not short_only:
      # Prefer "fullVersion" for version information, else try "version".
      if not (cached_ok and self.__nutanix_full_version):
        self.__nutanix_full_version = cluster.get("fullVersion")
      if self.__nutanix_full_version is not None:
        log.info("Nutanix cluster %s has fullVersion %s",
                 cluster["name"], self.__nutanix_full_version)
        ret.version = self.__nutanix_full_version
      else:
        log.error("Missing fullVersion on Nutanix cluster %s", cluster["name"])

    if not (self.__nutanix_version and cached_ok):
      self.__nutanix_version = cluster.get("version")
    if self.__nutanix_version is not None:
      log.info("Nutanix cluster %s has version %s",
               cluster["name"], self.__nutanix_version)
      # If we haven't already set this to fullVersion, use the short version.
      if not ret.version:
        ret.version = self.__nutanix_version
    else:
      log.error("Missing version on Nutanix cluster %s", cluster["name"])
    if not (self.__cluster_uuid and cached_ok):
      self.__cluster_uuid = cluster.get("clusterUuid")
    if self.__cluster_uuid is not None:
      log.info("Nutanix cluster %s has UUID %s",
               cluster["name"], self.__cluster_uuid)
      ret.cluster_uuid = self.__cluster_uuid
    else:
      log.error("Missing cluster UUID on Nutanix cluster %s", cluster["name"])

    if not (self.__cluster_incarnation_id and cached_ok):
      self.__cluster_incarnation_id = cluster.get("clusterIncarnationId")
    if self.__cluster_incarnation_id is not None:
      log.info("Nutanix cluster %s has incarnation ID %s",
               cluster["name"], self.__cluster_incarnation_id)
      ret.cluster_incarnation_id = self.__cluster_incarnation_id
    else:
      log.error("Missing cluster incarnation ID on Nutanix cluster %s",
                cluster["name"])

    return ret

  def deploy_ovf(self, name, host_id, ctr_id, ova_abs_path=None,
                 ovf_abs_path=None, network_uuids=None):
    """
    Deploys VM defined by OVA (OVF) at 'ova_abs_path' ('ovf_abs_path') to
    host 'host_id' in container 'ctr_id' .

    NB: Exactly one of 'ova_abs_path' or 'ovf_abs_path' must be specified.

    Args:
      name (str): Name for the imported VM.
      host_id (str): UUID of host on which to deploy the VM.
      ctr_id (str): UUID of the container in which to deploy the VM disks.
      ova_abs_path (str|None): Absolute path for .ova to deploy.
      ovf_abs_path (str|None): Absolute path for .ovf file describing VM.
      network_uuids (list<str>|None): Optional. If provided, UUIDs of networks
        to which NICs should be attached. If None, networks will be chosen
        automatically.

    Returns: JSON from API describing the deployed VM.
    """
    assert (bool(ova_abs_path) ^ bool(ovf_abs_path)), (
      "Must specify exactly one of 'ova_abs_path' and 'ovf_abs_path'")
    if ova_abs_path:
      ova = CurieOva(ova_abs_path)
      ovf = ova.parse()
    else:
      ova = None
      ovf = CurieOvf.parse_file(ovf_abs_path)
    vm_desc, vm_networks, vm_disks = ovf.to_curie_vm_desc()
    vm_desc.host_uuid = host_id
    vm_desc.container_uuid = ctr_id
    vm_desc.num_cores = 1
    vm_desc.name = name
    log.info("Deploying VM from '%s' as '%s' to host '%s' and container '%s'",
             ova_abs_path or ovf_abs_path, vm_desc.name, host_id, ctr_id)

    disk_file_map = ovf.get_disk_file_map()
    empty_disks = []
    # Later disks are more likely to be empty, so we'll iterate in reverse.
    for disk_name, disk in reversed(vm_disks):
      disk_name = NameUtil.goldimage_vmdisk_name(name, disk_name)
      file_ref = disk_file_map[disk]
      if file_ref is None or disk.populated_size in ["0", None]:
        assert (disk.populated_size in ["0", None]), (
          "Expected populated_size for non-empty disk '%s'" % disk_name)
        log.info("Deferring creation of disk for initially empty disk: %s",
                 disk_name)
        empty_disks.append((disk_name, disk))
        continue

      if ova:
        source_file = ova.get_file_handle(file_ref.href)
      else:
        source_file = os.path.join(os.path.dirname(ovf_abs_path),
                                   file_ref.href)

      log.info("Uploading disk '%s' (%s) for VM '%s'",
               disk_name, source_file, vm_desc.name)
      img_uuid, tid, _ = self.images_create(
        NameUtil.goldimage_vmdisk_name(vm_desc.name, disk_name),
        source_file, ctr_id)
      TaskPoller.execute_parallel_tasks(
        tasks=PrismTask.from_task_id(self, tid), timeout_secs=3600)

      log.info("Uploaded disk '%s' (%s) for VM '%s'",
               disk_name, source_file, vm_desc.name)

      # NB: Required due to possible AHV bug. See XRAY-225.
      num_images_get_retries = 5
      for attempt_num in xrange(num_images_get_retries):
        images_get_data = self.images_get(image_id=img_uuid)
        image_state = images_get_data["image_state"]
        if image_state.lower() == "active":
          vm_desc.vmdisk_uuid_list.append(images_get_data["vm_disk_id"])
          break
        else:
          log.info("Waiting for created image to become active "
                   "(imageState: %s, retry %d of %d)",
                   image_state, attempt_num + 1, num_images_get_retries)
          log.debug(images_get_data)
          time.sleep(1)
      else:
        raise CurieException(CurieError.kInternalError,
                              "Created image failed to become active within "
                              "%d attempts" % num_images_get_retries)

    networks = [(e["name"], e["uuid"])
                for e in self.networks_get()["entities"]
                if network_uuids is None or e["uuid"] in network_uuids]
    # Map of networks as named in the OVF to (name, uuid) pairs for the cluster
    # network  to which they will map.
    ovf_network_cluster_network_map = {}
    nic_specs = []
    for vm_network in vm_networks:
      if vm_network not in ovf_network_cluster_network_map:
        if not networks:
          raise CurieException(
            CurieError.kInvalidParameter,
            "Not enough distinct networks exist on cluster.")
        ovf_network_cluster_network_map[vm_network] = networks.pop()
      name, uuid = ovf_network_cluster_network_map[vm_network]
      nic_specs.append(vm_desc.to_ahv_vm_nic_create_spec(uuid)["specList"][0])

    for _, disk in empty_disks:
      vm_desc.attached_disks.append(disk)

    log.info("Creating VM '%s' with %s MB RAM and %s vCPUs",
             vm_desc.name, vm_desc.memory_mb, vm_desc.num_vcpus)
    resp = self.vms_create(vm_desc, nic_specs)
    tid = resp.get("taskUuid")
    if not tid:
      raise CurieException(CurieError.kManagementServerApiError,
                            "Failed to deploy VM: %s" % resp)

    TaskPoller.execute_parallel_tasks(
      tasks=PrismTask.from_task_id(self, tid), timeout_secs=60)

    task_json = self.tasks_get_by_id(tid, True)
    vm_uuid = task_json["entityList"][0]["uuid"]

    return self.vms_get_by_id(vm_uuid)

  def get_last_login_event_usecs(self):
    resp = self.__get("%s/events" % self.__base_url,
                      url_params={"count": 1, "entityIds": "LoginInfoAudit"})
    entities = resp.get("entities")
    if not entities:
      log.error("Unable to find any cluster login events. Received response: "
                "%s", resp)
      raise CurieException(CurieError.kManagementServerApiError,
                            "Failed to get last login timestamp")

    return int(entities[0]["createdTimeStampInUsecs"])

#==============================================================================
# Internal Util
#==============================================================================

  def _get_pc_uuid_header(self):
    # 5.5 breaks images API if cluster is attached to a PC. - XRAY-837
    # Determine if the target is a 5.5 cluster and then add required header
    version = PrismAPIVersions.get_aos_short_version(
      self.get_nutanix_metadata().version)
    nos_version = tuple(map(int, version.split('.')))
    if (5, 5) <= nos_version < (5, 8):
      log.debug("Cluster has the version %s. Checking if registered to a "
                "Prism Central", version)
      pc_uuid = self.get_pc_uuid()
      if pc_uuid is not None:
        log.debug("Cluster is registered to PC %s", pc_uuid)
        if nos_version < (5, 5, 0, 2):
          message = ("Cluster is running a 5.5 version which is incompatible "
                     "with X-Ray when connected to Prism Central. Please "
                     "upgrade AOS to 5.5.0.2 or newer, or disconnect Prism "
                     "Central.")
          raise CurieException(CurieError.kManagementServerApiError, message)
        return {"X-NTNX-PC-UUID": pc_uuid}
      else:
        log.debug("Cluster is not registered to a PC. Header not required.")
        return {}
    else:
      return {}

  def _get_timeout(self):
    return self.__session.get_timeout()

  def _set_timeout(self, timeout_secs):
    return self.__session.set_timeout(timeout_secs)

  def __get(self,
            url,
            max_retries=REST_API_MAX_RETRIES,
            url_params=None,
            timeout_secs=None):
    # All GET requests in the Nutanix REST API are idempotent.
    if isinstance(url_params, dict):
      for key in url_params.keys():
        if not (isinstance(key, bool) or url_params[key]):
          del url_params[key]

    kwargs = {} if timeout_secs is None else {"timeout": timeout_secs}
    return self.__issue_request(url,
                                "GET",
                                max_retries=max_retries,
                                params=url_params,
                                **kwargs)

  def __post(self, url, request_dict, max_retries=0, **request_kwargs):
    try:
      request_json = json.dumps(request_dict)
    except TypeError, ex:
      error_code = CurieError.kInvalidParameter
      error_msg = "Error serializing JSON for %s: %s" % (request_dict, ex)
      raise CurieException(error_code, error_msg)
    # Note that 'max_retries' is 0 by default for a POST. If a caller specifies
    # 'max_retries' > 0, then the caller needs to be able to handle exceptions
    # due to non-idempotent requests (e.g., a create error due to a previous,
    # timed out create request succeeding).
    return self.__issue_request(url,
                                "POST",
                                max_retries=max_retries,
                                data=request_json,
                                **request_kwargs)

  def __put(self, url, request_dict):
    try:
      request_json = json.dumps(request_dict)
    except TypeError, ex:
      error_code = CurieError.kInvalidParameter
      error_msg = "Error serializing JSON for %s: %s" % (request_dict, ex)
      raise CurieException(error_code, error_msg)
    # All PUT requests in the Nutanix REST API are idempotent.
    return self.__issue_request(url,
                                "PUT",
                                max_retries=REST_API_MAX_RETRIES,
                                data=request_json)

  def __delete(self, url, url_params=None, max_retries=0, **request_kwargs):
    # Note that 'max_retries' is 0 by default for a DELETE. If a caller
    # specifies 'max_retries' > 0, then the caller needs to be able to handle
    # exceptions due to non-idempotent requests (e.g., a delete error due to a
    # previous, timed out delete request succeeding).
    return self.__issue_request(url,
                                "DELETE",
                                max_retries=max_retries,
                                params=url_params,
                                **request_kwargs)

  @validate_parameter("service", valid_values=["NodeManager",
                                               "ClusterManager"])
  def __issue_genesis_rpc_v1(self,
                             service,
                             method,
                             svm_ip=None,
                             method_kwargs=None,
                             max_retries=1,
                             retry_delay_secs=5):
    """
    Issues Prism REST API call to invoke a Genesis RPC.

    Args:
      service (str): Genesis service whose RPC to call. Currently this should
        be either "NodeManager" or "ClusterManager".
      method (str): Name of RPC method to call.
      svm_ip (str|None): Optiona. If provided, issue RPC via
        /genesis/proxy/<IPv6> with the IPv6 formatted 'svm_ip'.
      method_kwargs (dict|None): Optional. If provided, dict of kwargs to pass
        to the RPC.
      max_retries (int): Maximum number of retry attempts per CVM for failed
        calls.
      retry_delay_secs (int): Delay between retries in seconds.

    Returns:
      (dict/JSON) deserialized JSON response body.

    Raises:
      CurieException<kClusterApiError> on error.
    """
    method_kwargs = method_kwargs if method_kwargs else {}
    url = "%s/genesis" % self.__base_url
    if svm_ip is not None:
      url = "%s/proxy/%s" % (url, CurieUtil.ipv4_to_ipv6(svm_ip))
    rpc_body = {".oid": service,
                ".method": method,
                ".kwargs": method_kwargs}
    # NB: Genesis has hardcoded use of the gflag 'genesis_rpc_timeout_secs'
    # for determining the timeout of individual NodeManager calls generated
    # by the ClusterManager Status RPC.

    # Retrieve raw response, since we want to attempt to parse the response
    # body for error messages in the case that status is not 200 OK.
    raw_ret = self.__issue_request(url, "POST",
                                   max_retries=max_retries,
                                   retry_delay_secs=retry_delay_secs,
                                   return_raw_response_obj=True,
                                   data=json.dumps(
                                     {"value": json.dumps(rpc_body)}),
                                   timeout=120)
    # 'raw_ret' is expected to be a response with body containing a map with a
    # single key "value" mapping to a serialized JSON response with key
    # ".return".
    try:
      ret_data = raw_ret.json()
      value_section = json.loads(ret_data["value"])
      if not isinstance(value_section, dict):
        raise TypeError("JSON response did not include expected dict under "
                        "key 'value'. Raw data: %s" % ret_data)
    except (TypeError, ValueError, KeyError) as exc:
      tb = sys.exc_info()[2]
      raise CurieException(
        CurieError.kClusterApiError, "Failed to parse response to genesis "
        "request. Invalid data returned: '%s' (%s: %s)" %
        (raw_ret.text, type(exc).__name__, exc)), None, tb

    if value_section.get(".warning"):
      log.warning("Warning while issuing Genesis request: %s",
                  value_section[".warning"])
    for key in ["error", "exception"]:
      if value_section.get(".%s" % key):
        log.error("Issuing Genesis request produced %s: %s",
                  key, value_section[".%s" % key])

    # The key '.return' may not exist if the RPC was unable to be issued.
    if ".return" not in value_section:
      if max_retries > 0:
        log.info("Retrying Genesis request after %d seconds (%d retries "
                 "remaining)", retry_delay_secs, max_retries)
        time.sleep(retry_delay_secs)
        return self.__issue_genesis_rpc_v1(
          service, method, svm_ip=svm_ip, method_kwargs=method_kwargs,
          max_retries=max_retries - 1, retry_delay_secs=retry_delay_secs)
      raise CurieException(CurieError.kClusterApiError,
                            "Failed to issue Genesis RPC. Retries exhaused")

    ret = value_section[".return"]
    # Error or warning conditions may be returned in a dict.
    if isinstance(ret, dict):
      # On error some methods may provide information via the lists
      # 'errors' and/or 'warnings'.
      if ret.get("warnings"):
        log.warning("Genesis request returned with warnings: %s",
                    ret["warnings"])
      if ret.get("errors"):
        log.error("Genesis request returned with errors: %s", ret["errors"])

    return ret

  def __issue_request(self,
                      url,
                      request_type,
                      max_retries=0,
                      retry_delay_secs=1,
                      return_raw_response_obj=False,
                      retry_status_set=PrismAPIVersions.RETRY_STATUS_SET,
                      **request_kwargs):
    """
    Make an HTTP request of type 'request_type' on the URL 'url' and return the
    response. '**request_kwargs' are passed directly into the session method
    corresponding to the request type. Failures will be reattempted a maximum
    of 'max_retries' times; Other conditions, such as HTTP 503, may be
    reattempted without counting toward the maximum number of retries.
    """
    request_func = getattr(self.__session, request_type.lower(), None)
    CHECK(request_func)
    request_url = "%s%s" % (self.__host_tmpl.format(host=self.host), url)

    log.debug("%s %s %r", request_type, url, request_kwargs)
    # Number of failed calls. This excludes cases where the backend explicitly
    # instructed us to retry the request.
    num_failures = 0
    # Total number of calls made.
    num_attempts = -1
    # Cap 'num_attempts' to avoid an infnite loop.
    while num_failures <= max_retries and num_attempts < 100:
      num_attempts += 1
      curr_retry_delay_secs = retry_delay_secs
      try:
        response = request_func(request_url, **request_kwargs)
      except (CurieException,
              requests.exceptions.ConnectionError,
              requests.exceptions.Timeout) as exc:
        if isinstance(exc, requests.exceptions.ConnectionError):
          msg = "Connection error on %s %s (attempt %d): %s" % (
            request_url, request_type, num_failures, exc)
        elif isinstance(exc, requests.exceptions.Timeout):
          msg = "Timeout on %s %s (attempt %d)" % (request_url, request_type,
                                                   num_failures)
        else:
          # Ensure we don't mask unexpected CurieExceptions.
          if exc.error_code != CurieError.kRetry:
            raise
          msg = "Error pinging host, retrying: %s" % exc
        log.warning(msg)
        num_failures += 1
        if num_failures > max_retries:
          raise
      else:
        # NB: Check for retry codes prior to returning raw or parsed data.
        status_code, content = response.status_code, response.content
        if status_code in retry_status_set:
          # NB: We intentionally do not increment failure count.
          curr_retry_delay_secs = 10
          log.info("Received %s; Retrying in %s seconds",
                   status_code, curr_retry_delay_secs)
        elif return_raw_response_obj:
          return response
        elif status_code in [200, 201]:
          return response.json()
        else:
          log.warning("HTTP error on %s %s (attempt %d): %s, %s",
                      request_url, request_type, num_failures, status_code,
                      content)
          num_failures += 1
          if num_failures > max_retries:
            raise HttpRequestException(status_code, content)

      time.sleep(curr_retry_delay_secs)
    raise CurieException(CurieError.kClusterApiError,
                          ("Too many attempts (%d) (%d failures) trying to do "
                           "%s %s") % (num_attempts, num_failures,
                                       request_type, request_url))

  def __get_entity_by_key(self, functor, key, val, raise_on_failure=False):
    """
    Looks up entity returned by 'functor' where 'key' is equal to 'val'.

    Args:
      functor ((dict) callable()): Callable accepting no arguments and
        returning a standard Prism JSON response.
      key (str): Entity attribute whose value should equal 'val'.
      val (str): Value to which entity attribute 'key' should be equal.
      raise_on_failure (bool): Optional. If True, raise an exception if no
        matching entity is found rather than returning None. Default False.

    Returns:
      (dict/JSON|None) Dict comprising parsed entity DTO if found, else None.

    Raises:
      CurieException<kInvalidParameter> if no match is found and
        'raise_on_failure' is set.
    """
    ret = functor()
    if isinstance(ret, list):
      entity_list = ret
    elif isinstance(ret, dict):
      entity_list = ret.get("entities", [])
    else:
      raise CurieException(
        CurieError.kManagementServerApiError,
        "Unexpected data type returned by %s" % functor.__name__)

    for entity_dto in entity_list:
      if entity_dto[key] == val:
        return entity_dto

    if raise_on_failure:
      raise CurieException(CurieError.kInvalidParameter,
                            "'%s' returned no entities with '%s' == '%s'" %
                            (functor.__name__, key, val))

    return None

  def __has_keys(self, dct, keys):
    """
    Verifies the dictionary-like object 'dct' contains all expected 'keys'.

    Raises:
      AssertionError on failure.
    """
    missing_keys = set(dct.keys()) - set(keys)
    if missing_keys:
      raise AssertionError("Missing expected keys: %s"
                           % ", ".join(missing_keys))
    return True
