#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
#
import cStringIO
import errno
import filecmp
import functools
import logging
import os
import pickle
import unittest
from difflib import unified_diff

import mock

from curie import agent_rpc_client
from curie import charon_agent_interface_pb2 as agent_ifc_pb2
from curie import curie_error_pb2
from curie import curie_test_pb2
from curie import exception
from curie import vm
from curie.scenario import Scenario
from curie.test import vdbench_util
from curie.testing import environment

log = logging.getLogger(__name__)


class TestVdbenchCmd(unittest.TestCase):
  def test_build_vdbench_prefill_cmd(self):
    scenario = mock.Mock(Scenario)
    actual_cmd = vdbench_util.VdbenchUtil.build_vdbench_prefill_cmd(
      scenario, "vdbfile.vdb")
    expected_cmd = ("/home/nutanix/vdbench/vdbench -f vdbfile.vdb "
                    "-o /home/nutanix/output/vdbench_prefill "
                    "-i 5")
    self.assertEqual(expected_cmd, actual_cmd)


class TestVdbenchUtilParser(unittest.TestCase):
  def setUp(self):
    self.vdbench_logfile_path = os.path.join(environment.resource_dir(),
                                             "test_vdbench_util",
                                             "logfile.html")

  def test_parse_vdbench_logfile(self):
    results, errors = vdbench_util.VdbenchUtil.parse_vdbench_logfile(
      self.vdbench_logfile_path)
    self.assertTrue(results > 0)
    # Expect 32 total errors based on number in logfile.html
    expected = 32
    self.assertEquals(first=len(errors), second=expected,
                      msg="Incorrect number of IO errors identified %d, "
                          "expected %d" % (len(errors), expected))

  def test_parse_encode_vdbench_logfile(self):
    results, errors = vdbench_util.VdbenchUtil.parse_vdbench_logfile(
      self.vdbench_logfile_path)
    start_timestamp = results[0]["timestamp"]
    end_timestamp = results[-1]["timestamp"]
    test_result = curie_test_pb2.CurieTestResult()
    vdbench_util.VdbenchUtil.encode_vdbench_incremental_error_data(
      ioerrors=errors,
      data_2d=test_result.data_2d,
      start_timestamp=start_timestamp,
      end_timestamp=end_timestamp)
    x_values = list(pickle.loads(
      test_result.data_2d.pickled_2d_data.x_vals_pickled))
    y_values = list(pickle.loads(
      test_result.data_2d.pickled_2d_data.y_vals_pickled))
    self.assertEquals(max(y_values), len(errors))
    self.assertEquals(min(y_values), 0)
    self.assertTrue(len(x_values) > len(errors))
    self.assertEquals(len(x_values), len(y_values))


class TestExecuteVdbenchJobs(unittest.TestCase):
  def create_jobs(self, max_jobs_per_node, procs_per_node_map,
                  cmd_id_node_id_map):
    jobs = []
    node_ids = ["n1", "n2"]
    for node_id in node_ids:
      for ii in range(5):
        vm_side_effect = TestExecuteVdbenchJobsVMSideEffect(
          self, node_id, max_jobs_per_node, procs_per_node_map,
          cmd_id_node_id_map)
        procs_per_node_map[node_id] = 0
        vm_mock = mock.Mock(vm.Vm)
        vm_mock.vm_name.return_value = "vm%d" % ii
        vm_mock.node_id.return_value = node_id
        vm_mock.execute_async = vm_side_effect.execute_async
        jobs.append(vdbench_util.VdbenchJob(
          vm_mock, "prefill_command", "prefill", 300))
    return jobs

  @mock.patch("curie.test.vdbench_util.time.sleep")
  @mock.patch.object(agent_rpc_client.AgentRpcClient,
                     "fetch_cmd_status")
  def test_execute_vdbench_jobs(self, fetch_cmd_status_mock, sleep_mock):
    # Verify max parallel jobs per node active at any given time.
    max_jobs_per_node = 2
    procs_per_node_map = {}
    cmd_id_node_id_map = {}
    fetch_cmd_status_mock.side_effect = functools.partial(
      fetch_cmd_status_side_effect, procs_per_node_map=procs_per_node_map,
      cmd_id_node_id_map=cmd_id_node_id_map)
    jobs = self.create_jobs(max_jobs_per_node, procs_per_node_map,
                            cmd_id_node_id_map)
    completed_jobs = vdbench_util.VdbenchUtil.execute_vdbench_jobs(
      jobs, max_jobs_per_node=max_jobs_per_node)
    sleep_mock.assert_any_call(5)
    for job in completed_jobs:
      self.assertEqual(job.cmd_status.state,
                       agent_ifc_pb2.CmdStatus.kSucceeded,
                       "job failed %s" % job)
      self.assertTrue("prefill" in job.cmd_id)
      self.assertTrue(job.start_secs > -1)
      self.assertTrue(job.complete_secs > -1)
      self.assertTrue(job.cmd_status is not None)

  @mock.patch.object(agent_rpc_client.AgentRpcClient, "CmdStatus")
  def test_execute_vdbench_jobs_fail(self, cmd_status_mock):
    # Verify exception is raised if there is an error getting cmd_status.
    ret = agent_ifc_pb2.CmdStatusRet()
    ret.cmd_status.state = ret.cmd_status.kFailed
    cmd_status_mock.return_value = (ret, None)
    max_jobs_per_node = 2
    procs_per_node_map = {}
    cmd_id_node_id_map = {}
    jobs = self.create_jobs(max_jobs_per_node, procs_per_node_map,
                            cmd_id_node_id_map)
    self.assertRaises(exception.CurieTestException,
                      vdbench_util.VdbenchUtil.execute_vdbench_jobs,
                      jobs, max_jobs_per_node=max_jobs_per_node)

  @mock.patch.object(agent_rpc_client.AgentRpcClient, "CmdStatus")
  def test_execute_vdbench_jobs_err(self, cmd_status_mock):
    # Verify exception is raised if there is an error getting cmd_status.
    cmd_status_mock.side_effect = fetch_cmd_status_side_effect_err
    max_jobs_per_node = 2
    procs_per_node_map = {}
    cmd_id_node_id_map = {}
    jobs = self.create_jobs(max_jobs_per_node, procs_per_node_map,
                            cmd_id_node_id_map)
    self.assertRaises(exception.CurieTestException,
                      vdbench_util.VdbenchUtil.execute_vdbench_jobs,
                      jobs, max_jobs_per_node=max_jobs_per_node)


# Side effects for TestExecuteVdbenchJobs.
def fetch_cmd_status_side_effect(cmd_id, procs_per_node_map,
                                 cmd_id_node_id_map):
  log.debug("cmd_id %s", cmd_id)
  node_id = cmd_id_node_id_map[cmd_id]
  procs_per_node_map[node_id] -= 1
  ret = agent_ifc_pb2.CmdStatusRet()
  ret.cmd_status.state = ret.cmd_status.kSucceeded
  return ret, None


def fetch_cmd_status_side_effect_err(arg):
  ret = agent_ifc_pb2.CmdStatusRet()
  return ret, curie_error_pb2.CurieError.kInternalError


class TestExecuteVdbenchJobsVMSideEffect(object):
  """Implement side effects for mocked CurieVM."""

  def __init__(self, utest, node_id, max_jobs_per_node, procs_per_node_map,
               cmd_id_node_id_map):
    self.__utest = utest
    self.__node_id = node_id
    self.__max_jobs_per_node = max_jobs_per_node
    self.__procs_per_node_map = procs_per_node_map
    self.__cmd_id_node_id_map = cmd_id_node_id_map

  def execute_async(self, command_id, command, user="root"):
    self.__cmd_id_node_id_map[command_id] = self.__node_id
    self.__procs_per_node_map[self.__node_id] += 1
    self.__utest.assertTrue(
      self.__procs_per_node_map[self.__node_id] <= self.__max_jobs_per_node,
      "Max allowed jobs for node_id %s is %d; current jobs: %d" %
      (self.__node_id, self.__max_jobs_per_node,
       self.__procs_per_node_map[self.__node_id]))


class TestParameter(unittest.TestCase):
  def setUp(self):
    self.wd_parameter = vdbench_util.VdbenchParameter([("wd", "wd1"),
                                                       (u"sd", u"sd1"),
                                                       ("xfersize", "(1,2,3)"),
                                                       ("rdpct", 0)])
    self.general_parameter = vdbench_util.VdbenchParameter([("compratio", 2)])

  def tearDown(self):
    pass

  def test_init_wd(self):
    self.assertEquals(self.wd_parameter.keys(),
                      ["wd", "sd", "xfersize", "rdpct"])

  def test_init_general(self):
    self.assertEquals(self.wd_parameter.values(),
                      ["wd1", u"sd1", "(1,2,3)", 0])

  def test_str_wd(self):
    self.assertEquals(str(self.wd_parameter),
                      "wd=wd1,sd=sd1,xfersize=(1,2,3),rdpct=0")

  def test_str_general(self):
    self.assertEquals(str(self.general_parameter),
                      "compratio=2")

  def test_str_empty(self):
    self.assertEquals(str(vdbench_util.VdbenchParameter()),
                      "")

  def test_str_none_all(self):
    self.assertEquals(str(vdbench_util.VdbenchParameter({"size": None})),
                      "")

  def test_str_none_one(self):
    self.assertEquals(str(vdbench_util.VdbenchParameter({"range": "(0,2g)",
                                                         "size": None})),
                      "range=(0,2g)")

  def test_type_wd(self):
    self.assertEquals(self.wd_parameter.type,
                      "wd")

  def test_type_general(self):
    self.assertEquals(self.general_parameter.type,
                      "general")

  def test_name_wd(self):
    self.assertEquals(self.wd_parameter.name,
                      "wd1")

  def test_name_general(self):
    self.assertEquals(self.general_parameter.name,
                      "compratio")

  def test_loads_wd(self):
    parameter_from_str = vdbench_util.VdbenchParameter.loads(
      "wd=wd1,sd=sd1,xfersize=(1,2,3),rdpct=0")
    self.assertEquals(str(parameter_from_str),
                      str(self.wd_parameter))
    parameter_from_str = vdbench_util.VdbenchParameter.loads(
      str(self.wd_parameter))
    self.assertEquals(str(parameter_from_str),
                      str(self.wd_parameter))

  def test_loads_wd_extra_whitespace(self):
    parameter_from_str = vdbench_util.VdbenchParameter.loads(
      "wd=wd1,   sd=sd1,\txfersize=(1,2,3),\t\n\nrdpct =\t0 ")
    self.assertEquals(str(parameter_from_str),
                      str(self.wd_parameter))

  def test_loads_general(self):
    parameter_from_str = vdbench_util.VdbenchParameter.loads(
      "compratio=2")
    self.assertEquals(str(parameter_from_str),
                      str(self.general_parameter))
    parameter_from_str = vdbench_util.VdbenchParameter.loads(
      str(self.general_parameter))
    self.assertEquals(str(parameter_from_str),
                      str(self.general_parameter))

  def test_format_positional(self):
    parameter = vdbench_util.VdbenchParameter.loads("sd={0},size={1}")
    formatted = parameter.format("sd1", "2G")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G")

  def test_format_named(self):
    parameter = vdbench_util.VdbenchParameter.loads(
      "sd={sd_name},size={sd_size}")
    formatted = parameter.format(sd_name="sd1", sd_size="2G")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G")

  def test_format_extra(self):
    parameter = vdbench_util.VdbenchParameter.loads(
      "sd={sd_name},size={sd_size}")
    formatted = parameter.format(sd_name="sd1", sd_size="2G",
                                 rd_name="rd1", wd_assignment="wd*")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G")


class TestParameterFile(unittest.TestCase):
  def setUp(self):
    self.valid_vdb_path = os.path.join(environment.resource_dir(),
                                       "test_vdbench_util",
                                       "example_valid.vdb")
    self.valid_expected_contents = (
      "compratio=2\n"
      "dedupsets=4\n"
      "swat=(xx,yy)\n"
      "sd=sd1,lun=/dev/sdb,size=2G,openflags=o_direct,threads=1\n"
      "sd=sd2,lun=/dev/sdc,size=2G,openflags=o_direct,threads=1\n"
      "sd=sd3,lun=/dev/sdd,size=28G,openflags=o_direct\n"
      "sd=sd4,lun=/dev/sde,size=28G,openflags=o_direct\n"
      "sd=sd5,lun=/dev/sdf,size=28G,openflags=o_direct\n"
      "sd=sd6,lun=/dev/sdg,size=28G,openflags=o_direct\n"
      "wd=wd1,sd=sd1,xfersize=32768,rdpct=0,iorate=100,priority=1,seekpct=10\n"
      "wd=wd2,sd=sd2,xfersize=32768,rdpct=0,iorate=100,priority=2,seekpct=10\n"
      "wd=wd3,sd=sd3,xfersize=(32768,10,8192,90),rdpct=50,iorate=1000,priority=3,seekpct=80\n"
      "wd=wd4,sd=sd4,xfersize=(32768,10,8192,90),rdpct=50,iorate=1000,priority=4,seekpct=80\n"
      "wd=wd5,sd=sd5,xfersize=(32768,10,8192,90),rdpct=50,iorate=1000,priority=5,seekpct=80\n"
      "wd=wd6,sd=sd6,xfersize=(32768,10,8192,90),rdpct=50,iorate=1000,priority=6,seekpct=80\n"
      "rd=run1,wd=wd*,elapsed=3600,iorate=max\n")

  def tearDown(self):
    pass

  def test_init_empty(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    self.assertEquals(parameter_file.keys(),
                      vdbench_util.VdbenchParameterFile.line_order)

  def test_append_parameter_in_order(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append(vdbench_util.VdbenchParameter([("compratio", 2)]))
    parameter_file.append(vdbench_util.VdbenchParameter([("swat", "(xx,yy)")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("sd", "sd1"),
                                                         ("lun", "/dev/sdb")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("wd", "wd1"),
                                                         ("sd", "sd1")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("rd", "run1"),
                                                         ("wd", "wd*")]))
    expected_str = ("compratio=2\n"
                    "swat=(xx,yy)\n"
                    "sd=sd1,lun=/dev/sdb\n"
                    "wd=wd1,sd=sd1\n"
                    "rd=run1,wd=wd*\n")
    value = str(parameter_file)
    expected = expected_str
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_append_parameter_out_of_order(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append(vdbench_util.VdbenchParameter([("rd", "run1"),
                                                         ("wd", "wd*")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("wd", "wd1"),
                                                         ("sd", "sd1")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("sd", "sd1"),
                                                         ("lun", "/dev/sdb")]))
    parameter_file.append(vdbench_util.VdbenchParameter([("compratio", 2)]))
    parameter_file.append(vdbench_util.VdbenchParameter([("swat", "(xx,yy)")]))
    expected_str = ("compratio=2\n"
                    "swat=(xx,yy)\n"
                    "sd=sd1,lun=/dev/sdb\n"
                    "wd=wd1,sd=sd1\n"
                    "rd=run1,wd=wd*\n")
    value = str(parameter_file)
    expected = expected_str
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_append_list_of_tuples(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append([("rd", "run1"), ("wd", "wd*")])
    self.assertEquals(str(parameter_file),
                      "rd=run1,wd=wd*\n")

  def test_append_str(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append("rd=run1,wd=wd*")
    self.assertEquals(str(parameter_file),
                      "rd=run1,wd=wd*\n")

  def test_append_unicode(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append(u"rd=run1,wd=wd*")
    self.assertEquals(str(parameter_file),
                      "rd=run1,wd=wd*\n")

  def test_append_override_newline(self):
    parameter_file = vdbench_util.VdbenchParameterFile(newline="\r\n")
    parameter_file.append("rd=run1,wd=wd*")
    self.assertEquals(str(parameter_file),
                      "rd=run1,wd=wd*\r\n")

  def test_load_compared_to_str(self):
    with open(self.valid_vdb_path, "r") as f:
      parameter_file_from_file = vdbench_util.VdbenchParameterFile.load(f)
    value = str(parameter_file_from_file)
    expected = self.valid_expected_contents
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_load_compared_to_loads(self):
    with open(self.valid_vdb_path, "r") as f:
      parameter_file_from_file = vdbench_util.VdbenchParameterFile.load(f)
    parameter_file_from_str = vdbench_util.VdbenchParameterFile.loads(
      self.valid_expected_contents)
    self.assertEquals(parameter_file_from_file,
                      parameter_file_from_str)

  def test_format_positional(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append("sd={0},size={1}")
    parameter_file.append("rd={2},wd={3}")
    formatted = parameter_file.format("sd1", "2G", "rd1", "wd*")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G\n"
                      "rd=rd1,wd=wd*\n")

  def test_format_named(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append("sd={sd_name},size={sd_size}")
    parameter_file.append("rd={rd_name},wd={wd_assignment}")
    formatted = parameter_file.format(sd_name="sd1", sd_size="2G",
                                      rd_name="rd1", wd_assignment="wd*")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G\n"
                      "rd=rd1,wd=wd*\n")

  def test_format_extra(self):
    parameter_file = vdbench_util.VdbenchParameterFile()
    parameter_file.append("sd={sd_name},size={sd_size}")
    formatted = parameter_file.format(sd_name="sd1", sd_size="2G",
                                      rd_name="rd1", wd_assignment="wd*")
    self.assertEquals(str(formatted),
                      "sd=sd1,size=2G\n")


class TestPrefillParameters(unittest.TestCase):
  def setUp(self):
    self.valid_vdb_path = os.path.join(environment.resource_dir(),
                                       "test_vdbench_util",
                                       "example_valid.vdb")
    self.valid_vdb_prefill_path = os.path.join(environment.resource_dir(),
                                               "test_vdbench_util",
                                               "example_valid_prefill.vdb")
    self.output_dir = environment.test_output_dir(self)
    self.output_path = os.path.join(self.output_dir,
                                    "test_prefill_output_example_valid.vdb")
    # Create the output directory if it does not already exist
    try:
      os.makedirs(self.output_dir)
    except OSError as e:
      if e.errno == errno.EEXIST:
        pass

  def tearDown(self):
    pass

  def test_write_prefill_parameters(self):
    # Write the prefill parameters to a file-like object
    output_fp = cStringIO.StringIO()
    with open(self.valid_vdb_path, "r") as input_fp:
      parameter_file = vdbench_util.VdbenchUtil.write_prefill_parameters(
        input_fp,
        output_fp)
    # Verify correctness against the resource file
    output_fp.seek(0)
    value = output_fp.read()
    with open(self.valid_vdb_prefill_path, "r") as expected_fp:
      expected = expected_fp.read()
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))
    # Verify correctness against of the returned ParameterFile
    value = str(parameter_file)
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))

  def test_create_prefill_parameter_file(self):
    # Write the prefill parameters to a file
    parameter_file = vdbench_util.VdbenchUtil.create_prefill_parameter_file(
      self.valid_vdb_path,
      self.output_path)
    # Verify correctness against the resource file
    self.assertTrue(filecmp.cmp(self.valid_vdb_prefill_path,
                                self.output_path))
    # Verify correctness against of the returned ParameterFile
    value = str(parameter_file)
    with open(self.output_path, "r") as output_fp:
      expected = output_fp.read()
    self.assertEquals(value,
                      expected,
                      "\n".join(unified_diff(value.splitlines(),
                                             expected.splitlines())))
