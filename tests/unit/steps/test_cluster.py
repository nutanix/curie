#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest

from mock import mock

from curie import steps
from curie.scenario import Scenario
from curie.testing import environment
from curie.testing.util import mock_cluster
from curie.vm import Vm


class TestStepsCluster(unittest.TestCase):
  def setUp(self):
    self.cluster = mock_cluster()
    vms = [mock.Mock(spec=Vm) for _ in xrange(4)]
    for index, vm in enumerate(vms):
      vm.vm_name.return_value = "fake_vm_%d" % index
    self.cluster.vms.return_value = vms
    self.scenario = Scenario(
      cluster=self.cluster,
      output_directory=environment.test_output_dir(self))

  def test_CleanUp_default(self):
    meta_step = steps.cluster.CleanUp(self.scenario)
    for step in meta_step.itersteps():
      step()
    self.cluster.cleanup.assert_called_once_with()

  def test_CleanUp_init_cluster_None(self):
    self.scenario.cluster = None
    step = steps.cluster.CleanUp(self.scenario)
    self.assertIsInstance(step, steps.cluster.CleanUp)
