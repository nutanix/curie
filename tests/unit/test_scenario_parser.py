#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import os
import unittest

import yaml
from mock import mock
from mock import sentinel as sen
from pkg_resources import resource_filename

from curie import scenario_parser
from curie.exception import CurieTestException
from curie.scenario import Scenario, Phase
from curie import steps
from curie.result.base_result import BaseResult
from curie.steps.cluster import CleanUp
from curie.vm_group import VMGroup
from curie.workload import Workload
from curie.testing import environment
from curie.testing.util import mock_cluster


class TestScenarioParser(unittest.TestCase):
  def test_load_with_variables_no_vars(self):
    yaml_str = """
vms:
  - OLTP: {}
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: OLTP
run:
  - vm_group.PowerOn:
      vm_group_name: OLTP
    """
    data = {
      'vms': [
        {'OLTP': {}}
      ],
      'setup': [
        {'vm_group.CloneFromTemplate': {
          'vm_group_name': 'OLTP'}
        }
      ],
      'run': [
        {'vm_group.PowerOn': {
          'vm_group_name': 'OLTP'}
        }
      ],
    }
    self.assertEqual(data, scenario_parser.load_with_variables(yaml_str))

  def test_load_with_variables_vars(self):
    yaml_str = """
vars:
  group_name:
    default: VDI
  count_per_node:
    default: 100
vms:
  - {{ group_name }}:
      nodes: all
      count_per_node: {{ count_per_node }}
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: "{{ group_name }}"
run:
  - vm_group.PowerOn:
      vm_group_name: "{{ group_name }}"
    """
    data = {
      'vars': {
        'group_name': {
          'default': 'VDI',
          'value': 'VDI',
        },
        'count_per_node': {
          'default': 100,
          'value': 100,
        },
      },
      'vms': [
        {'VDI': {
          'nodes': 'all',
          'count_per_node': 100,
        }}
      ],
      'setup': [
        {'vm_group.CloneFromTemplate': {
          'vm_group_name': 'VDI'}
        }
      ],
      'run': [
        {'vm_group.PowerOn': {
          'vm_group_name': 'VDI'}
        }
      ],
    }
    self.assertEqual(data, scenario_parser.load_with_variables(yaml_str))

  def test_load_with_variables_out_of_bounds_low(self):
    yaml_str = """
vars:
  count:
    min: 5
    default: 100
vms: {{count}}
    """
    self.assertEqual(
      {"vars": {"count": {"min": 5,
                          "default": 100,
                          "value": 5}},
       "vms": 5},
      scenario_parser.load_with_variables(yaml_str, variables={"count": 5}))
    with self.assertRaises(ValueError) as ar:
      scenario_parser.load_with_variables(yaml_str, variables={"count": 4})
    self.assertEqual("'count' value '4' must be greater than or equal to "
                     "minimum value '5'", str(ar.exception))

  def test_load_with_variables_out_of_bounds_high(self):
    yaml_str = """
vars:
  count:
    default: 100
    max: 200
vms: {{count}}
    """
    self.assertEqual(
      {"vars": {"count": {"max": 200,
                          "default": 100,
                          "value": 200}},
       "vms": 200},
      scenario_parser.load_with_variables(yaml_str, variables={"count": 200}))
    with self.assertRaises(ValueError) as ar:
      scenario_parser.load_with_variables(yaml_str, variables={"count": 201})
    self.assertEqual("'count' value '201' must be less than or equal to "
                     "maximum value '200'", str(ar.exception))

  def test_load_with_variables_vars_parameters(self):
    yaml_str = """
vars:
  group_name:
    default: VDI
  count_per_node:
    default: 100
vms:
  - {{ group_name }}:
      nodes: all
      count_per_node: {{ count_per_node }}
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: "{{ group_name }}"
run:
  - vm_group.PowerOn:
      vm_group_name: "{{ group_name }}"
    """
    data = {
      'vars': {
        'group_name': {
          'default': 'VDI',
          'value': 'NOT VDI',
        },
        'count_per_node': {
          'default': 100,
          'value': 100,
        },
      },
      'vms': [
        {'NOT VDI': {
          'nodes': 'all',
          'count_per_node': 100,
        }}
      ],
      'setup': [
        {'vm_group.CloneFromTemplate': {
          'vm_group_name': 'NOT VDI'}
        }
      ],
      'run': [
        {'vm_group.PowerOn': {
          'vm_group_name': 'NOT VDI'}
        }
      ],
    }
    self.assertEqual(data, scenario_parser.load_with_variables(
      yaml_str, variables={"group_name": "NOT VDI"}))

  @mock.patch("curie.scenario_parser.from_yaml_str",
              side_effect=yaml.parser.ParserError(note=str(sen.note)))
  @mock.patch("curie.scenario_parser.open")
  @mock.patch("curie.scenario_parser.os.path.splitext",
              return_value=(sen.base, ".yml"))
  @mock.patch("curie.scenario_parser.os.path.split",
              return_value=(sen.dir, sen.filename))
  @mock.patch("curie.scenario_parser.os.path.isdir", return_value=False)
  def test_from_path_invalidyaml(self, m_isdir, m_split, m_splitext, m_open,
                                 m_from_yaml_str):
    with self.assertRaises(CurieTestException) as err:
      scenario_parser.from_path(sen.path)
    self.assertEqual("Unable to parse YAML at path sentinel.path, check "
                     "syntax: %r" % str(sen.note), err.exception.message)
    m_from_yaml_str.assert_called_once_with(
      m_open.return_value.__enter__.return_value.read.return_value,
      source_directory=sen.dir, vars=None)

  def test_load_with_variables_vars_parameters_many(self):
    variables = {
      "count_per_node": 100,
      "memory": 2,
      "type": "OLTP",
    }
    yaml_str = """
description: >
  This scenario creates {{ count_per_node }} {{type}} VMs on each node, each
  with {{ memory }}GB of memory, for a total of {{ count_per_node * memory }}GB
  per node.
vars:
  type:
    default: VDI
  count_per_node:
    default: 100
  memory:
    default: 2
vms:
  - {{ type }}:
      nodes: all
      count_per_node: {{ count_per_node }}
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: "{{ type }}"
run:
  - vm_group.PowerOn:
      vm_group_name: "{{ type }}"
    """
    data = {
      'description': 'This scenario creates 100 OLTP VMs on each node, each '
                     'with 2GB of memory, for a total of 200GB per node.\n',
      'vars': {
        'type': {
          'default': 'VDI',
          'value': 'OLTP',
        },
        'count_per_node': {
          'default': 100,
          'value': 100,
        },
        'memory': {
          'default': 2,
          'value': 2,
        }
      },
      'vms': [
        {'OLTP': {
          'nodes': 'all',
          'count_per_node': 100,
        }}
      ],
      'setup': [
        {'vm_group.CloneFromTemplate': {
          'vm_group_name': 'OLTP'}
        }
      ],
      'run': [
        {'vm_group.PowerOn': {
          'vm_group_name': 'OLTP'}
        }
      ],
    }
    self.assertEqual(data, scenario_parser.load_with_variables(
      yaml_str, variables=variables))

  def test_from_path_none_found(self):
    # Create a directory that contains no YAML configurations.
    path = environment.test_output_dir(self)
    if os.path.isdir(path):
      os.rmdir(path)
    os.makedirs(path)
    with self.assertRaises(ValueError) as ar:
      scenario_parser.from_path(path)
    self.assertEqual("No configuration files found at '%s'" % path,
                     str(ar.exception))

  def test_from_path_multiple_found(self):
    path = resource_filename(
      __name__, os.path.join("..", "..", "curie", "yaml"))
    with self.assertRaises(ValueError) as ar:
      scenario_parser.from_path(path)
    self.assertIn("Multiple configuration files found at '%s': [" % path,
                  str(ar.exception))

  def test_from_path_invalid_file_type(self):
    path = os.path.realpath(__file__)
    with self.assertRaises(ValueError) as ar:
      scenario_parser.from_path(path)
    self.assertEqual("Invalid file type '%s'" % path, str(ar.exception))

  def test_from_path_packaged_scenarios(self):
    for path in Scenario.find_configuration_files():
      scenario = scenario_parser.from_path(path)
      first_setup_step = scenario.steps[Phase.SETUP][0]
      self.assertIsInstance(
        first_setup_step, CleanUp,
        "The first step in the setup phase should be cluster.CleanUp "
        "(path: %s, found: %s)" % (path, first_setup_step))

  def test_from_path_readonly(self):
    path = Scenario.find_configuration_files()[0]
    scenario = scenario_parser.from_path(path, readonly=True)
    self.assertTrue(scenario.readonly)
    scenario = scenario_parser.from_path(path, readonly=False)
    self.assertFalse(scenario.readonly)

  def test_from_path_packaged_scenarios_small_large_clusters(self):
    three_node_cluster = mock_cluster(3)
    one_hundred_twenty_eight_node_cluster = mock_cluster(128)
    for path in Scenario.find_configuration_files():
      scenario_parser.from_path(
        path, cluster=three_node_cluster)
      scenario_parser.from_path(
        path, cluster=one_hundred_twenty_eight_node_cluster)

  def test_from_yaml_missing_required_argument(self):
    yaml_str = """
      name: Malformed Scenario
      # display_name is missing!
      summary: A scenario with a missing display_name
      description: This scenario is whack.
      tags:
        - invalid
    """
    with self.assertRaises(ValueError) as ar:
      scenario_parser.from_yaml_str(yaml_str)
    self.assertIn("'display_name' is required", str(ar.exception))

  def test_from_yaml_substitution(self):
    yaml_str = """
name: good_scenario
display_name: Good Scenario
summary: A scenario which is A-OK.
description: It's {{ adjective }}!
estimated_runtime: 1234
tags:
  - valid
vars:
  adjective:
    default: great
vms:
  - OLTP:
      template: ubuntu1604
workloads: []
results: []
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: OLTP
run:
  - vm_group.PowerOn:
      vm_group_name: OLTP
    """
    scenario = scenario_parser.from_yaml_str(yaml_str)
    self.assertEqual("It's great!", scenario.description)
    loaded = yaml.load(scenario.yaml_str, Loader=yaml.CSafeLoader)
    self.assertEqual("It's great!", loaded["description"])

  def test_from_yaml_set_id(self):
    yaml_str = """
name: good_scenario
id: 12345
display_name: Good Scenario
summary: A scenario which is A-OK.
description: It's {{ adjective }}!
estimated_runtime: 1234
tags:
  - valid
vars:
  adjective:
    default: great
vms:
  - OLTP:
      template: ubuntu1604
workloads: []
results: []
setup:
  - vm_group.CloneFromTemplate:
      vm_group_name: OLTP
run:
  - vm_group.PowerOn:
      vm_group_name: OLTP
    """
    scenario = scenario_parser.from_yaml_str(yaml_str)
    self.assertEqual(12345, scenario.id)

  def test_parse_steps(self):
    scenario = Scenario()
    yaml_str = """
      vms:
        - OLTP: {}
      setup:
        - vm_group.CloneFromTemplate:
            vm_group_name: OLTP
      run:
        - vm_group.PowerOn:
            vm_group_name: OLTP
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    parsed = scenario_parser._parse_steps(scenario, data)
    self.assertIsInstance(parsed[0], tuple)
    self.assertIsInstance(parsed[0][0], steps.vm_group.CloneFromTemplate)
    self.assertEqual(parsed[0][1], Phase.SETUP)
    self.assertIsInstance(parsed[1], tuple)
    self.assertIsInstance(parsed[1][0], steps.vm_group.PowerOn)
    self.assertEqual(parsed[1][1], Phase.RUN)

  def test_parse_steps_missing_required_phase(self):
    scenario = Scenario()
    yaml_str = """
      vms:
        - OLTP: {}
      # setup phase is missing!
      run:
        - vm_group.CloneFromTemplate:
            vm_group_name: OLTP
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    with self.assertRaises(ValueError) as ar:
      scenario_parser._parse_steps(scenario, data)
    self.assertIn("Phase 'setup' is required", str(ar.exception))

  def test_parse_steps_step_not_found(self):
    scenario = Scenario()
    yaml_str = """
      setup:
        - vm_group.Invalid: {}
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    with self.assertRaises(ValueError) as ar:
      scenario_parser._parse_steps(scenario, data)
    self.assertIn("Step 'vm_group.Invalid' does not exist", str(ar.exception))

  def test_parse_steps_missing_required_argument(self):
    scenario = Scenario()
    yaml_str = """
      vms:
        - OLTP: {}
      setup:
        - vm_group.CloneFromTemplate:
            vm_group_name: OLTP
      run:
        - vm_group.CreatePeriodicSnapshots:
            vm_group_name: OLTP
            num_snapshots: 15
            # interval_minutes: missing!
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    with self.assertRaises(ValueError) as ar:
      scenario_parser._parse_steps(scenario, data)
    self.assertIn("'run' phase, 'vm_group.CreatePeriodicSnapshots' step: "
                  "Missing required argument 'interval_minutes'",
                  str(ar.exception))

  def test_parse_steps_misaligned(self):
    scenario = Scenario()
    yaml_str = """
      vms:
        - OLTP: {}
      setup:
        - vm_group.CloneFromTemplate:
            vm_group_name: OLTP
      run:
        - vm_group.PowerOn: {}
          vm_group_name: OLTP  # misaligned
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    with self.assertRaises(ValueError) as ar:
      scenario_parser._parse_steps(scenario, data)
    self.assertIn("Missing required arguments ['vm_group_name']",
                  str(ar.exception))

  def test_parse_steps_unexpected_argument(self):
    scenario = Scenario()
    yaml_str = """
      vms:
        - OLTP: {}
      setup:
        - vm_group.CloneFromTemplate:
            vm_group_name: OLTP
      run:
        - vm_group.PowerOn:
            vm_group_name: OLTP
            like: a boss
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    with self.assertRaises(ValueError) as ar:
      scenario_parser._parse_steps(scenario, data)
    self.assertIn("'run' phase, 'vm_group.PowerOn' step: Unexpected keyword "
                  "argument 'like'", str(ar.exception))

  def test_parse_results(self):
    scenario = Scenario()
    yaml_str = """
      vms:
          - OLTP: {}
      results:
        - IOPS:
            vm_group: OLTP
            result_type: iops
            result_hint: "Higher is better!"
            result_expected_value: 10
            result_value_bands:
              - name: big band 0
                upper: 1000
                lower: 10
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    parsed = scenario_parser._parse_section(scenario, data, "results",
                                            BaseResult)
    self.assertEqual(parsed["IOPS"].kwargs["result_hint"],
                     "Higher is better!")
    self.assertEqual(parsed["IOPS"].kwargs["result_expected_value"], 10)
    self.assertEqual(parsed["IOPS"].kwargs["result_value_bands"],
                     [{"name": "big band 0", "upper": 1000, "lower": 10}])

  def test_parse_section(self):
    scenario = Scenario()
    yaml_str = """
      results:
        - OLTP:
            arg_0: apple
        - VDI:
            arg_0: banana
            arg_1: cantaloupe
        - Network Bandwidth:
            # Suppose this guy is disabled.
            arg_0: durian
    """
    data = yaml.load(yaml_str)
    self.__create_stubs(scenario, data)
    mock_cls = mock.Mock()
    expected = ["return_value_0", "return_value_1", None]
    mock_cls.parse.side_effect = expected
    ret = scenario_parser._parse_section(scenario, data, "results", mock_cls)
    self.assertEqual(["OLTP", "VDI"], ret.keys())
    self.assertEqual(expected[:2], ret.values())
    mock_cls.parse.assert_has_calls([mock.call(scenario, "OLTP",
                                               {"arg_0": "apple"}),
                                     mock.call(scenario, "VDI",
                                               {"arg_0": "banana",
                                                "arg_1": "cantaloupe"}),
                                     mock.call(scenario, "Network Bandwidth",
                                               {"arg_0": "durian"}),
                                     ])

  @staticmethod
  def __create_stubs(scenario, data):
    """
    Stub out VMGroups, Workloads, and Results in a scenario.

    Allows testing _parse_steps without calling _parse_section so they can be
    tested more independently.

    Args:
      scenario (Scenario): Scenario to modify.
      data (dict): Configuration object.

    Returns:
      None
    """
    scenario.vm_groups = {}
    for vm_group in data.get("vms", []):
      vm_group_name = vm_group.keys()[0]
      scenario.vm_groups[vm_group_name] = VMGroup(scenario,
                                                  vm_group_name)
    scenario.workloads = {}
    for workload in data.get("workloads", []):
      workload_name = workload.keys()[0]
      vm_group_name = workload.values()[0]["vm_group"]
      scenario.workloads[workload_name] = Workload(scenario,
                                                   workload_name,
                                                   vm_group_name)
    scenario.results_map = {}
    for result in data.get("results", []):
      result_name = result.keys()[0]
      scenario.results_map[result_name] = BaseResult(scenario,
                                                     result_name)
