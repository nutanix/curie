#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# Steps included in experimental are for internal experimental use only.
#
import logging
import os
import subprocess

import requests

from curie.exception import CurieTestException
from curie.steps._base_step import BaseStep

log = logging.getLogger(__name__)


class WaitOplogEmpty(BaseStep):
  """Wait for the cluster's oplog to be empty.

  Note: The firewall on the CVMs in the cluster must allow access to port
  2009 to query /h/vars.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    timeout (int): Seconds to wait for oplog to drain.
  """

  def __init__(self, scenario, timeout=1200):
    super(WaitOplogEmpty, self).__init__(scenario, annotate=False)
    self.description = "Waiting for oplog to drain"
    self.timeout = timeout

  def _run(self):
    all_cvms = set()
    verified_empty_cvms = set()
    for vm in self.scenario.cluster.vms():
      if vm.is_cvm():
        all_cvms.add(vm.vm_ip())
    if len(all_cvms) == 0:
      log.warning("No CVMs found to check for oplog empty.")
      return

    def is_cluster_oplog_empty():
      to_check = all_cvms - verified_empty_cvms
      for cvm_ip in to_check:
        url = "http://%s:2009/h/vars" % cvm_ip
        try:
          response = requests.get(
            url,
            params={
              "format": "text",
              "regex": "stargate/vdisk/total/oplog_bytes$"
            })
          response.raise_for_status()
        except requests.exceptions.ConnectionError:
          log.warning("Couldn't connect to CVM at %s to get oplog bytes.",
                      cvm_ip)
          continue
        try:
          # Expect response to look like:
          # stargate/vdisk/total/oplog_bytes 27018567680\n
          metric_name, value = response.content.strip().split()
          assert metric_name == "stargate/vdisk/total/oplog_bytes"
          oplog_bytes = int(value)
        except BaseException:
          log.exception("Received unexpected response while waiting for oplog "
                        "to be empty at URL '%s': %r", url, response.content)
          continue
        else:
          if oplog_bytes == 0:
            verified_empty_cvms.add(cvm_ip)
      return True if len(all_cvms - verified_empty_cvms) == 0 else False

    rval = self.scenario.wait_for(func=is_cluster_oplog_empty,
                                  msg="Oplog on cluster to be empty",
                                  timeout_secs=self.timeout)
    return rval


class Shell(BaseStep):
  """Run a shell command from the X-Ray VM.

  Args:
    scenario (Scenario): Scenario this step belongs to.
    cmd (str): Shell command to run.
  """

  def __init__(self, scenario, cmd, annotate=False):
    super(Shell, self).__init__(scenario, annotate=annotate)
    self.description = "Executing '%s'" % cmd
    self.cmd = cmd

  def _run(self):
    cwd = os.getcwd()
    try:
      if self.scenario.output_directory:
        os.chdir(self.scenario.output_directory)
      return_code = subprocess.check_call(self.cmd, shell=True)
    except subprocess.CalledProcessError as err:
      raise CurieTestException(
        cause=
        "Received non-zero return code %d from shell command '%s': %s" %
        (err.returncode, self.cmd, err),
        impact="The command did not complete successfully.",
        corrective_action=
        "Please check the syntax of the command requested in the %s step." %
        self.name)
    else:
      self.create_annotation("%s" % self.cmd)
      return return_code
    finally:
      os.chdir(cwd)
