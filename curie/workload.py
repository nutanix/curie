#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
import math
import os

from curie.exception import CurieTestException
from curie.iogen.fio_config import FioConfiguration
from curie.iogen.fio_workload import FioWorkload


class Workload(object):
  """
  Manges workload information for a group of VMs.
  The Workload object is a logical grouping of configuration information and
  object references used to abstract some complexity from test steps. It also
  serves as the interpreter for the "workloads" section of a YAML test
  definition.
  """

  def __init__(self, test, name, vm_group, generator="fio",
               config_file=None, iogen_params={}):
    """
    Creates a new Workload object.

    Args:
      test: the test object.
      name: (string) a name for this workload.
      vm_group: (VMGroup) a vm group object reference the Workload should
      maintain.
      generator: (string) a string denoting the type of the workload,
        default: fio.
      config_file: (string) a path to a configuration file for the workload.
      iogen_params: (dict) a dictionary of parameters that gets passed
        directly to an iogen object as **kwargs.
    """
    self._name = name
    self._test = test
    # TODO (cwilson) Assert that the vmgroup actually exists?
    self._vm_group = vm_group
    # TODO (cwilson) Assert that the type is valid?
    self._generator = generator
    self._iogen_class = None
    self._config_class = None
    self._set_iogen_classes()
    self._iogen_params = iogen_params
    self._iogen = None
    self._prefill_iogen = None
    # TODO (cwilson) Assert that the configuration file exists?
    self._config_file = test.resource_path(config_file)

  def __str__(self):
    return "%s %s" % (self.__class__.__name__, self._name)

  @staticmethod
  def parse(test, name, definition):
    """
    Creates a new Workload object.

    Args:
      test: the test object
      name: (string) a name for this workload.
      definition: (dict) a dictionary that defines the other variables

    Returns:
      Workload: a new workload object.

    Raises:
      CurieTestException: if a workload type specified in the definition is
      invalid.

    """
    iogen_params = definition.get("iogen_params", {})
    vm_group = test.vm_groups[definition.get("vm_group", None)]
    generator_type = definition.get("generator", "fio")
    configuration_file = definition.get("config_file", None)
    workload = Workload(test, name, vm_group,
                        generator=generator_type,
                        config_file=configuration_file,
                        iogen_params=iogen_params)
    return workload

  def name(self):
    """The name of the workload."""
    return self._name

  def iogen(self):
    """An iogen object used for running the workload.

    Returns:
      iogen object: If iogen has not been accessed before, a new iogen is
      created and returned. Otherwise, it returns a reference to the original
      iogen.  The class of the iogen is determined by the type of workload.
    """
    if not self._iogen:
      self._iogen = self._iogen_class(
        self._name, self._test, self.configuration(), **self._iogen_params)
    return self._iogen

  def configuration(self):
    """The configuration for the iogen

    Returns:
      configuration object: An instance of a configuration object where the
      type is determined by the type of the workload.

    Raises:
      CurieTestException if a configuration file (_config_file) was not set.
    """
    if not self._config_file:
      raise CurieTestException("Configuration file not set for workload")
    elif not os.path.exists(self._config_file):
      raise CurieTestException("Configuration file '%s' does not exist" %
                                self._config_file)
    with open(self._config_file, "r") as config_fh:
      return self._config_class.load(config_fh)

  def prefill_iogen(self, max_concurrent=None):
    """An iogen object used for prefilling a VM.

    Args:
      max_concurrent: If specified, return an IOgen object suitable for
        prefilling a maximum of 'max_concurrent' workloads at the same time.

    Returns:
      iogen object: If this has not been accessed before, a new iogen is
      created using a prefill configuration. Otherwise, it returns a
      reference to a prefill iogen previously created by this function.
    """
    if not self._prefill_iogen:
      prefill_name = "%s_prefill" % self._name
      prefill_kwargs = dict()
      if max_concurrent is not None:
        max_concurrent_vms = min(self._vm_group.total_count(), max_concurrent)
        # Assumes VMs in a VMGroup are uniformly distributed.
        avg_concurrent_per_node = (float(max_concurrent_vms) /
                                   len(self._vm_group.nodes()))
        max_concurrent_per_node = int(math.ceil(avg_concurrent_per_node))
        iodepth = (FioConfiguration.DEFAULT_IODEPTH_PER_NODE /
                   max_concurrent_per_node)
        prefill_kwargs["iodepth"] = iodepth
      prefill_config = self.configuration().convert_prefill(**prefill_kwargs)
      self._prefill_iogen = self._iogen_class(
        prefill_name, self._test, prefill_config, **self._iogen_params)
    return self._prefill_iogen

  def vm_group(self):
    """VM Group this Workload should be performed on.

    Returns:
      VMGroup object: a reference to the vm group.

    Raises:
      CurieTestException if the vm group was not set.
    """
    if not self._vm_group:
      raise CurieTestException("VMGroup was not set.")
    return self._vm_group

  def _set_iogen_classes(self):
    if self._generator == "fio":
      self._iogen_class = FioWorkload
      self._config_class = FioConfiguration
    else:
      raise CurieTestException("Invalid workload type specified: %s" %
                                self._generator)
