#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

import mock

from curie.acropolis_cluster import AcropolisCluster
from curie.cluster_check import ClusterCheck
from curie.scenario import Status, Phase
from curie.steps.check import PrismConnectivity, VCenterConnectivity
from curie.testing.util import mock_cluster
from curie.vsphere_cluster import VsphereCluster


class TestClusterCheck(unittest.TestCase):
  def setUp(self):
    pass

  def test_init(self):
    check = ClusterCheck(cluster_obj=mock_cluster())
    self.assertEqual(check.status(), Status.NOT_STARTED)
    self.assertEqual(check.duration_secs(), None)

  def test_init_AcropolisCluster(self):
    check = ClusterCheck(cluster_obj=mock_cluster(spec=AcropolisCluster))
    self.assertEqual(check.status(), Status.NOT_STARTED)
    self.assertEqual(check.duration_secs(), None)
    self.assertIn(PrismConnectivity,
                  [step.__class__ for step in check.steps[Phase.RUN]])
    self.assertNotIn(VCenterConnectivity,
                     [step.__class__ for step in check.steps[Phase.RUN]])

  def test_init_VsphereCluster(self):
    check = ClusterCheck(cluster_obj=mock_cluster(spec=VsphereCluster))
    self.assertEqual(check.status(), Status.NOT_STARTED)
    self.assertEqual(check.duration_secs(), None)
    self.assertIn(VCenterConnectivity,
                  [step.__class__ for step in check.steps[Phase.RUN]])
    self.assertNotIn(PrismConnectivity,
                     [step.__class__ for step in check.steps[Phase.RUN]])

  def test_start_join(self):
    check = ClusterCheck(cluster_obj=mock_cluster())

    with mock.patch.object(check, "_step_loop") as m_step_loop:
      check.start()
      check.join()

    self.assertEqual(Status.SUCCEEDED, check.status())
    m_step_loop.assert_called_once_with()

  def test_start_running(self):
    check = ClusterCheck(cluster_obj=mock_cluster())
    check._status = Status.EXECUTING

    with mock.patch.object(check, "_step_loop") as m_step_loop:
      with self.assertRaises(RuntimeError) as ar:
        check.start()

    self.assertEqual("Cluster Check (EXECUTING): Can not start because it "
                     "has already been started",
                     str(ar.exception))
    self.assertEqual(Status.EXECUTING, check.status())
    m_step_loop.assert_not_called()
