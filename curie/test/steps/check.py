#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#

from _base_step import BaseStep
from curie.test.scenario_util import ScenarioUtil


class ClustersMatch(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(ClustersMatch, self).__init__(scenario, annotate=annotate)
    self.description = ("Checking that clusters defined by management "
                        "software and clustering software match")

  def _run(self):
    ScenarioUtil.prereq_runtime_storage_cluster_mgmt_cluster_match(
      self.scenario.cluster)


class ClusterReady(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(ClusterReady, self).__init__(scenario, annotate=annotate)
    self.description = "Ensuring cluster is ready"

  def _run(self):
    if not ScenarioUtil.prereq_runtime_cluster_is_ready(self.scenario.cluster):
      ScenarioUtil.prereq_runtime_cluster_is_ready_fix(self.scenario.cluster)


class VMStorageAvailable(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(VMStorageAvailable, self).__init__(scenario, annotate=annotate)
    self.description = "Ensuring that VM storage is available on all nodes"

  def _run(self):
    if not ScenarioUtil.prereq_runtime_vm_storage_is_ready(
        self.scenario.cluster):
      ScenarioUtil.prereq_runtime_vm_storage_is_ready_fix(
        self.scenario.cluster)


class OobConfigured(BaseStep):
  def __init__(self, scenario, annotate=True):
    super(OobConfigured, self).__init__(scenario, annotate=annotate)
    self.description = "Checking that out-of-band management is configured"

  def _run(self):
    ScenarioUtil.prereq_metadata_can_run_failure_scenario(
      self.scenario.cluster.metadata())
