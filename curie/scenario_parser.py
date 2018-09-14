#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import inspect
import logging
import os
import re
from collections import OrderedDict

import yaml
from jinja2 import Template

from curie.scenario import Phase, Scenario
from curie import steps as steps_module
from curie.result.base_result import BaseResult
from curie.vm_group import VMGroup
from curie.workload import Workload
from curie.exception import CurieTestException

log = logging.getLogger(__name__)


def load_with_variables(yaml_str, variables=None, variables_key="vars"):
  if variables is None:
    variables = {}

  # Pull the vars section out of the string and parse it as YAML.
  vars_re = re.compile(r"^%s:.+?(?=^[\S])" % variables_key,
                       flags=(re.MULTILINE | re.DOTALL))
  vars_match = vars_re.search(yaml_str)
  if vars_match:
    vars_yaml = vars_match.group(0)
    vars_data = yaml.load(vars_yaml, Loader=yaml.CSafeLoader)
    yaml_defined_variables = vars_data.get(variables_key, {})
  else:
    yaml_defined_variables = {}

  flat_variables_map = {}
  for key in yaml_defined_variables:
    variable = yaml_defined_variables[key]
    variable["value"] = variables.get(key, variable["default"])
    if variable.get("min") is not None:
      if variable["value"] < variable["min"]:
        raise ValueError("'%s' value '%s' must be greater than or equal to "
                         "minimum value '%s'" %
                         (key, variable["value"], variable["min"]))
    if variable.get("max") is not None:
      if variable["value"] > variable["max"]:
        raise ValueError("'%s' value '%s' must be less than or equal to "
                         "maximum value '%s'" %
                         (key, variable["value"], variable["max"]))
    # Flatten the variables so they are in the expected format for Jinja.
    flat_variables_map[key] = variable["value"]

  rendered_yaml_str = Template(yaml_str).render(**flat_variables_map)
  yaml_data_with_substitutions = yaml.load(rendered_yaml_str,
                                           Loader=yaml.CSafeLoader)

  # Insert the processed "vars" section back into the object.
  if yaml_defined_variables:
    yaml_data_with_substitutions[variables_key] = yaml_defined_variables
  return yaml_data_with_substitutions


def from_path(path, vars=None, *args, **kwargs):
  """Read a scenario configuration and construct a new scenario instance.

  Args:
    path (basestring): Path to a configuration file. `path` may be a directory
      containing a single configuration file.
    *args: Arguments passed to Scenario __init__.
    **kwargs: Arguments passed to Scenario __init__.

  Returns:
    Scenario: A new scenario instance.
  """
  # If path is a directory, find a configuration file inside that directory.
  if os.path.isdir(path):
    paths = Scenario.find_configuration_files(path)
    if not paths:
      raise ValueError("No configuration files found at '%s'" % path)
    elif len(paths) > 1:
      raise ValueError("Multiple configuration files found at '%s': %r" %
                       (path, paths))
    else:
      path = paths[0]
  # Parse the configuration file and construct a new scenario.
  directory, filename = os.path.split(path)
  extension = os.path.splitext(filename)[1]
  if extension.lower() in [".yml", ".yaml"]:
    with open(path) as config_file:
      try:
        scenario = from_yaml_str(config_file.read(), *args,
                                 vars=vars, source_directory=directory, **kwargs)
      except yaml.parser.ParserError as err:
        raise CurieTestException("Unable to parse YAML at path %r, check "
                                 "syntax: %r" % (path, str(err)))
  else:
    raise ValueError("Invalid file type '%s'" % path)
  return scenario


def from_yaml_str(yaml_str, vars=None, *args, **kwargs):
  """Construct a Scenario from a YAML string.

  Args:
    yaml_str (basestring): YAML-formatted scenario description.
    *args: Arguments passed to Scenario __init__.
    **kwargs: Arguments passed to Scenario __init__.

  Returns:
    Scenario: A new scenario instance.
  """
  data = load_with_variables(yaml_str, variables=vars)
  scenario = Scenario(*args, **kwargs)
  scenario.yaml_str = yaml.dump(data, Dumper=yaml.CSafeDumper)
  if data.get("id"):
    scenario.id = data.get("id")
  scenario.name = _get(data, "name").strip()
  scenario.display_name = _get(data, "display_name").strip()
  scenario.summary = _get(data, "summary").strip()
  scenario.description = _get(data, "description").strip()
  scenario.estimated_runtime = _get(data, "estimated_runtime")
  scenario.tags = _get(data, "tags", [])
  scenario.vm_groups = _parse_section(scenario, data, "vms", VMGroup)
  scenario.workloads = _parse_section(scenario, data, "workloads", Workload)
  scenario.results_map = _parse_section(scenario, data, "results", BaseResult)
  scenario.vars = _get(data, "vars", {})
  for step, phase in _parse_steps(scenario, data):
    scenario.add_step(step, phase)
  return scenario


def _parse_steps(scenario, data):
  """Iterate across phases, parsing step descriptions into step instances.

  Args:
    scenario (Scenario): Scenario to associate with each Step.
    data (dict): Parsed scenario description object.

  Returns:
    list of (BaseStep, Phase) tuples
  """
  steps = []
  for phase in Phase:
    phase_name = phase.name.lower()
    steps_data = data.get(phase_name, [])
    if not steps_data and phase_name in ["setup", "run"]:
      raise ValueError("Phase '%s' is required" % phase_name)
    for step_data in steps_data:
      name, kwargs = step_data.items()[0]
      # Find the step class based on the dotted step name.
      try:
        cls = reduce(getattr, [steps_module] + name.split("."))
      except AttributeError:
        raise ValueError("'%s': Step '%s' does not exist" % (phase_name, name))
      # Validate step arguments.
      argspec = inspect.getargspec(cls.__init__)
      required_args = argspec.args[:-len(argspec.defaults)]
      required_args.remove("self")
      required_args.remove("scenario")
      try:  # add parsing error context in except block, but only if needed
        # kwargs is assumed iterable below, check that assumption
        if required_args and not kwargs:
          raise ValueError("Missing required arguments %r" % required_args)
        for required_arg in required_args:
          if required_arg not in kwargs:
            raise ValueError("Missing required argument %r" % required_arg)
        for kwarg in kwargs:
          if kwarg not in argspec.args:
            raise ValueError("Unexpected keyword argument %r" % kwarg)
      except ValueError as err:  # prefix scenario info to error for debugging
        prefix = "%r scenario, %r phase, %r step: " % (scenario.name,
                                                       phase_name, name)
        raise ValueError(prefix + err.message)
      step_instance = cls(scenario, **kwargs)
      steps.append((step_instance, phase))
  return steps


def _parse_section(scenario, data, section_name, cls):
  """Hand off a section to be parsed by another class.

  Args:
    scenario (Scenario): Scenario to associate with this section.
    data (dict): Parsed scenario description section.
    section_name (basestring): Name of the section.
    cls (class): Class whose static `parse` method will be used.

  Returns:
    Value passed through from `cls.parse`.
  """
  section_def_list = _get(data, section_name)
  return_value = OrderedDict()
  for section_def_entry in section_def_list:
    # The section_def_entry should be a dictionary with one key,
    # the name of the vm_group, workload, etc  expected by cls.
    for name, definition in section_def_entry.iteritems():
      val = cls.parse(scenario, name, definition)
      if val is None:
        log.debug("Skipping '%s' from '%s': It has been disabled",
                  name, section_name)
      else:
        return_value[name] = val
  return return_value


def _get(data, key, default_value=None):
  """Wrapper for raising a prettier exception if data is missing.

  Args:
    data (dict): Object whose `get` method is called with `key`.
    key (object): Value passed to `data.get`.
    default_value (object or None): Optional value to return if the key is
      missing.

  Returns:
    Value passed through from `data.get`.
  """
  section_obj = data.get(key, default_value)
  if section_obj is None:
    raise ValueError("'%s' is required" % key)
  return section_obj
