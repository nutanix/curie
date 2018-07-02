#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
#
# pylint: disable=pointless-statement
import logging
import random
import threading
import time
import unittest
import uuid
from functools import partial
from itertools import cycle

import mock
from pyVmomi import vmodl

from curie.acropolis_types import AcropolisTaskEntity, AcropolisTaskInfo
from curie.exception import CurieTestException
from curie.nutanix_rest_api_client import NutanixRestApiClient
from curie.task import HypervTask, HypervTaskPoller, PrismTask, \
  HypervTaskDescriptor
from curie.task import PrismTaskDescriptor, VsphereTask
from curie.task import TaskPoller, TaskStatus, VsphereTaskDescriptor
from curie.task import log as curie_task_log
from curie.vmm_client import VmmClient

log = logging.getLogger(__name__)


class DummyTask(object):
  def __init__(self, num_ticks=None, fail=False, not_found=False, tid=None):
    self.num_ticks = num_ticks or random.randint(3, 8)
    self.fail = fail
    self.not_found = not_found
    self.curr_tick = 0

  def id(self):
    raise NotImplementedError()


class DummyPrismTask(DummyTask):
  def __init__(self, num_ticks=None, fail=False, not_found=False, tid=None):
    super(DummyPrismTask, self).__init__(num_ticks, fail, not_found, tid)
    self._info = AcropolisTaskInfo(
      uuid=tid,
      entity_list=[AcropolisTaskEntity(uuid=uuid.uuid4())])
    self._info.progress_status = "queued"

  def get_return(self):
    self.curr_tick += 1
    self._info.percentage_complete = int(
      self.curr_tick * 100.0 / self.num_ticks)
    self._info.progress_status = "running"

    if self.curr_tick >= self.num_ticks:
      if self.fail:
        self._info.progress_status = "failed"
      else:
        self._info.progress_status = "succeeded"
        self._info.percentage_complete = 100

    return self._info.json()

  def id(self):
    return self._info.uuid


class MockTasksGetById(object):
  def __init__(self):
    self._id_ret_map = {}

  def add_task(self, task):
    self._id_ret_map[task.id()] = task.get_return

  def __call__(self, task_id):
    time.sleep(0.01)
    return self._id_ret_map[task_id]()


class CreatePrismTask(object):
  LOCK = threading.Lock()

  def __init__(self, mock_tasks_get_by_id, prism):
    self.tids = []
    self.index = -1
    self._mock_tasks_get_by_id = mock_tasks_get_by_id
    self._prism = prism

    self.used_tids = []

  def __call__(self):
    with self.LOCK:
      self.index += 1
      dummy_task = DummyPrismTask(tid=self.tids[self.index])
      self.used_tids.append(self.tids[self.index])
      self._mock_tasks_get_by_id.side_effect.add_task(dummy_task)
      return dummy_task.id()

  def create_curie_task_instance(self):
    return PrismTask(
      self._prism, PrismTaskDescriptor(create_task_func=self))


class MockVimTaskInfo(object):
  @classmethod
  def get_exc_class(cls):
    return vmodl.fault.ManagedObjectNotFound

  def __init__(self, tid, task):
    self._task = task
    self.cancelable = True
    self.cancelled = False
    self.descriptionId = "dummyVsphereTask"
    self.entityName = "dummyVsphereEntity"
    self._state = "queued"
    self.error = None
    self.description = None
    self.key = tid
    self.progress = 0

  @property
  def state(self):
    self._task._update()
    if self._state == "not_found":
      raise self.get_exc_class()()
    return self._state


class MockVimTaskInfoBaseExc(MockVimTaskInfo):
  @classmethod
  def get_exc_class(cls):
    return BaseException


class DummyVimTask(DummyTask):
  @classmethod
  def get_vim_info_class(cls):
    return MockVimTaskInfo

  def __init__(self, num_ticks=None, fail=False, not_found=False, tid=None):
    super(DummyVimTask, self).__init__(num_ticks, fail, not_found, tid)
    self.info = self.get_vim_info_class()(tid, self)

  def _update(self):
    self.curr_tick += 1
    self.info.progress = int(self.curr_tick * 100.0 / self.num_ticks)
    self.info._state = "running"

    if self.curr_tick >= self.num_ticks:
      if self.fail:
        self.info._state = "error"
        self.info.error = "Induced error"
      elif self.not_found:
        self.info._state = "not_found"
      else:
        self.info._state = "success"
        self.info.progress = 100

  def id(self):
    return self.info.key

  def CancelTask(self):
    if self.info.cancelable:
      if not self.info.cancelled:
        self.info.cancelled = True
        return

    raise vmodl.fault.NotSupported()


class DummyVimTaskBaseExc(DummyVimTask):
  @classmethod
  def get_vim_info_class(cls):
    return MockVimTaskInfoBaseExc


class CreateVsphereTask(object):
  LOCK = threading.Lock()

  def __init__(self):
    self.tids = []
    self.index = -1
    self.used_tids = []

  def __call__(self):
    with self.LOCK:
      self.index += 1
      dummy_task = DummyVimTask(tid=self.tids[self.index])
      self.used_tids.append(self.tids[self.index])
      return dummy_task

  def create_curie_task_instance(self):
    return VsphereTask(
      VsphereTaskDescriptor(create_task_func=self))


class TestCurieTask(unittest.TestCase):
  def setUp(self):
    self.prism = NutanixRestApiClient("host", "user", "pass")
    self.sample_timestamp = time.time()

  @mock.patch.object(time, "time")
  def test_get_deadline_secs(self, mock_time):
    mock_time.return_value = self.sample_timestamp
    self.assertEqual(
      TaskPoller.get_deadline_secs(None),
      TaskPoller.get_default_timeout_secs() + self.sample_timestamp)
    self.assertEqual(TaskPoller.get_deadline_secs(10),
                     self.sample_timestamp + 10)

  @mock.patch.object(time, "time")
  def test_get_timeout_secs(self, mock_time):
    mock_time.return_value = self.sample_timestamp
    self.assertEqual(TaskPoller.get_timeout_secs(None),
                     TaskPoller.get_default_timeout_secs())
    deadline = self.sample_timestamp + 10
    self.assertEqual(TaskPoller.get_timeout_secs(deadline), 10)
    deadline = self.sample_timestamp - 10
    self.assertEqual(TaskPoller.get_timeout_secs(deadline), 0)

  @mock.patch.object(NutanixRestApiClient, "tasks_get_by_id")
  def test_prism_task_from_id(self, mock_tasks_get_by_id):
    dummy_task = DummyPrismTask()
    mock_tasks_get_by_id.side_effect = MockTasksGetById()

    poller = TaskPoller(10, poll_interval_secs=0)

    prism_task = PrismTask.from_task_id(self.prism, dummy_task.id())
    mock_tasks_get_by_id.side_effect.add_task(dummy_task)
    poller.add_task(prism_task)
    poller.start()

    ret = poller.wait_for()
    self.assertNotEqual(ret, None)

  @mock.patch.object(NutanixRestApiClient, "tasks_get_by_id")
  def test_prism_task_multiple(self, mock_tasks_get_by_id):
    mock_tasks_get_by_id.side_effect = MockTasksGetById()
    self._test_task_multiple([
      CreatePrismTask(mock_tasks_get_by_id, self.prism)])

  @mock.patch.object(NutanixRestApiClient, "tasks_get_by_id")
  def test_prism_task_cancel(self, mock_tasks_get_by_id):
    mock_tasks_get_by_id.side_effect = MockTasksGetById()
    self._test_task_cancel([
      CreatePrismTask(mock_tasks_get_by_id, self.prism)])

  @mock.patch("curie.task.TaskPoller.get_default_timeout_secs")
  def test_vcenter_task_timeout(self, mock_get_default_timeout_secs):
    mock_get_default_timeout_secs.return_value = 0.1
    num_tasks = 20
    mock_tasks = []
    for index in xrange(num_tasks):
      if index < 5:
        dummy_task = DummyVimTask(num_ticks=1, tid=index)  # Completed
      else:
        dummy_task = DummyVimTask(num_ticks=1e6, tid=index)  # In progress
      vsphere_task = VsphereTask.from_vim_task(dummy_task)
      mock_tasks.append(vsphere_task)
    with self.assertRaises(CurieTestException) as ar:
      TaskPoller.execute_parallel_tasks(tasks=mock_tasks,
                                        poll_secs=0, timeout_secs=0)
    self.assertEqual("Tasks timed out after '0' seconds (5/20 succeeded)",
                     str(ar.exception))

  @mock.patch("curie.task.TaskPoller.get_default_timeout_secs")
  def test_vcenter_task_timeout_descriptors(
      self, mock_get_default_timeout_secs):
    mock_get_default_timeout_secs.return_value = 0.1
    num_tasks = 20
    mock_task_descs = []
    for index in xrange(num_tasks):
      if index < 5:
        dummy_task = DummyVimTask(num_ticks=1, tid=index)  # Completed
      else:
        dummy_task = DummyVimTask(num_ticks=1e6, tid=index)  # In progress

      def return_dummy_task(dummy_task):
        return dummy_task

      dummy_task_desc = VsphereTaskDescriptor(
        pre_task_msg="Pre-dummy %d" % index,
        post_task_msg="Post-dummy %d" % index,
        create_task_func=partial(return_dummy_task, dummy_task))
      mock_task_descs.append(dummy_task_desc)
    with self.assertRaises(CurieTestException) as ar:
      TaskPoller.execute_parallel_tasks(task_descriptors=mock_task_descs,
                                        poll_secs=0, timeout_secs=0)
    self.assertEqual("Tasks timed out after '0' seconds (5/20 succeeded)",
                     str(ar.exception))

  @mock.patch("curie.task.TaskPoller.get_default_timeout_secs")
  def test_execute_parallel_vcenter_tasks_failed(
      self, mock_get_default_timeout_secs):
    mock_get_default_timeout_secs.return_value = 0.1
    num_tasks = 20
    mock_task_descs = []
    for index in xrange(num_tasks):
      if index < 5:
        dummy_task = DummyVimTask(num_ticks=1, fail=False, tid=index)
      else:
        dummy_task = DummyVimTask(num_ticks=1, fail=True, tid=index)

      def return_dummy_task(dummy_task):
        return dummy_task

      dummy_task_desc = VsphereTaskDescriptor(
        pre_task_msg="Pre-dummy %d" % index,
        post_task_msg="Post-dummy %d" % index,
        create_task_func=partial(return_dummy_task, dummy_task))
      mock_task_descs.append(dummy_task_desc)
    with self.assertRaises(CurieTestException) as ar:
      TaskPoller.execute_parallel_tasks(task_descriptors=mock_task_descs,
                                        poll_secs=0, timeout_secs=30)
    self.assertEqual("15 of 20 tasks failed. See log for more details (most "
                     "recent error message: 'Induced error')",
                     str(ar.exception))

  @mock.patch("curie.task.TaskPoller.get_default_timeout_secs")
  def test_execute_parallel_vcenter_tasks_failed_no_raise(
      self, mock_get_default_timeout_secs):
    mock_get_default_timeout_secs.return_value = 0.1
    num_tasks = 20
    mock_task_descs = []
    for index in xrange(num_tasks):
      if index < 5:
        dummy_task = DummyVimTask(num_ticks=1, fail=False, tid=index)
      else:
        dummy_task = DummyVimTask(num_ticks=1, fail=True, tid=index)

      def return_dummy_task(dummy_task):
        return dummy_task

      dummy_task_desc = VsphereTaskDescriptor(
        pre_task_msg="Pre-dummy %d" % index,
        post_task_msg="Post-dummy %d" % index,
        create_task_func=partial(return_dummy_task, dummy_task))
      mock_task_descs.append(dummy_task_desc)
    task_map = TaskPoller.execute_parallel_tasks(
      task_descriptors=mock_task_descs, poll_secs=0, timeout_secs=30,
      raise_on_failure=False)
    tasks = task_map.values()
    for task in tasks[:5]:
      self.assertEqual("success", task._state)
    for task in tasks[5:]:
      self.assertEqual("error", task._state)

  def test_vcenter_task_not_found(self):
    dummy_task = DummyVimTask(not_found=True)
    poller = TaskPoller(10, poll_interval_secs=0)
    vsphere_task = VsphereTask.from_vim_task(dummy_task)
    poller.add_task(vsphere_task)
    poller.start()

    ret = poller.wait_for()
    self.assertNotEqual(ret, None)
    self.assertTrue(vsphere_task.is_terminal())
    self.assertEqual(vsphere_task.get_status(), TaskStatus.kNotFound)

  def test_vcenter_task_base_exception(self):
    dummy_task = DummyVimTaskBaseExc(not_found=True)
    poller = TaskPoller(10, poll_interval_secs=0)
    vsphere_task = VsphereTask.from_vim_task(dummy_task)
    poller.add_task(vsphere_task)
    poller.start()

    ret = poller.wait_for()
    self.assertEqual(vsphere_task.get_status(), TaskStatus.kInternalError)

  def test_vcenter_task_from_vim_task(self):
    dummy_task = DummyVimTask()

    poller = TaskPoller(10, poll_interval_secs=0)

    vsphere_task = VsphereTask.from_vim_task(dummy_task)
    poller.add_task(vsphere_task)
    poller.start()

    ret = poller.wait_for()
    self.assertNotEqual(ret, None)

  def test_vcenter_task_multiple(self):
    self._test_task_multiple([CreateVsphereTask()])

  def test_vcenter_task_cancel(self):
    self._test_task_cancel([CreateVsphereTask()])

  @mock.patch.object(NutanixRestApiClient, "tasks_get_by_id")
  def test_mixed_tasks(self, mock_tasks_get_by_id):
    mock_tasks_get_by_id.side_effect = MockTasksGetById()
    self._test_task_multiple([
      CreatePrismTask(mock_tasks_get_by_id, self.prism),
      CreateVsphereTask()])

  def _test_task_cancel(self, create_task_funcs):
    poller = TaskPoller(5, poll_interval_secs=0)

    for ii in range(50):
      task_func_index = ii % len(create_task_funcs)
      create_task_funcs[task_func_index].tids.append(str(uuid.uuid4()))
      task = create_task_funcs[task_func_index].create_curie_task_instance()
      poller.add_task(task)

    poller.start()

    tid = create_task_funcs[0].tids[5]

    log.info("Waiting for %s", tid)
    ret = poller.wait_for(task_id=tid)
    log.info("Done waiting for %s: %s", tid, ret)

    self.assert_(tid in ret)
    log.info("Stopping remaining tasks")
    poller.stop()

    self.assert_(poller._remaining_task_count <= 5)

    log.info("Waiting for remaining tasks...")

    poller.wait_for_all()

  def _test_task_multiple(self, create_task_funcs):
    poller = TaskPoller(5, poll_interval_secs=0)

    for ii in range(50):
      task_func_index = ii % len(create_task_funcs)
      create_task_funcs[task_func_index].tids.append(str(uuid.uuid4()))
      task = create_task_funcs[task_func_index].create_curie_task_instance()
      poller.add_task(task)

    poller.start()

    tid = create_task_funcs[0].tids[25 // len(create_task_funcs)]

    log.info("Waiting for %s", tid)
    ret = poller.wait_for(task_id=tid)
    log.info("Done waiting for %s: %s", tid, ret)

    self.assert_(tid in ret)
    self.assert_(poller._remaining_task_count <= 25 + 5)

    log.info("Waiting for remaining tasks...")
    poller.wait_for_all()


class TestHypervTaskPoller(unittest.TestCase):
  def setUp(self):
    pass

  @mock.patch("curie.task.time.sleep")
  @mock.patch("curie.task.log", wraps=curie_task_log)
  def test_execute_parallel_tasks_defaults(self, m_log, m_sleep):
    m_sleep.return_value = 0

    m_vmm_client = mock.Mock(spec=VmmClient)
    # Just using create_vm as an example.
    m_vmm_client.create_vm.side_effect = [
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]

    m_vmm_client.vm_get_job_status.side_effect = [
      [{"task_id": "1", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "10",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "20",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "50",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "60",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "completed",
        "status": "Completed", "completed": True, "progress": "100",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "90",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "completed",
        "status": "Completed", "completed": True, "progress": "100",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "completed",
        "status": "Completed", "completed": True, "progress": "100",
        "error": "",
        },
       ],
    ]

    task_desc_list = [
      HypervTaskDescriptor(
        pre_task_msg="Doing Task 1",
        post_task_msg="Finished Task 1",
        create_task_func=m_vmm_client.create_vm),
      HypervTaskDescriptor(
        pre_task_msg="Doing Task 2",
        post_task_msg="Finished Task 2",
        create_task_func=m_vmm_client.create_vm),
    ]

    HypervTaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                            vmm=m_vmm_client)

    m_vmm_client.vm_get_job_status.assert_has_calls([
      mock.call([{"task_id": "1", "task_type": "vmm"},
                 {"task_id": "2", "task_type": "vmm"},
                 ]),
      mock.call([{"task_id": "1", "task_type": "vmm"},
                 {"task_id": "2", "task_type": "vmm"},
                 ]),
      mock.call([{"task_id": "1", "task_type": "vmm"},
                 {"task_id": "2", "task_type": "vmm"},
                 ]),
      mock.call([{"task_id": "2", "task_type": "vmm"},
                 ]),
    ])

    self.assertIn(
      mock.call("Polling %d active tasks... (%d / %d tasks remaining)", 2, 2, 2),
      m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 1 running (10%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 2 running (20%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 1 running (50%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 2 running (60%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 1 completed (100%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Finished Task 1"), m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 2 running (90%)"), m_log.info.mock_calls)
    self.assertIn(
      mock.call("Polling %d active tasks... (%d / %d tasks remaining)", 1, 1, 2),
      m_log.info.mock_calls)
    self.assertIn(mock.call("Task: 2 completed (100%)"), m_log.info.mock_calls)
    self.assertIn(mock.call("Finished Task 2"), m_log.info.mock_calls)

  @mock.patch("curie.task.time.sleep")
  def test_execute_parallel_tasks_failure(self, m_sleep):
    m_sleep.return_value = 0

    m_vmm_client = mock.Mock(spec=VmmClient)
    # Just using create_vm as an example.
    m_vmm_client.create_vm.side_effect = [
      [{"task_id": "1", "task_type": "vmm"}],
      [{"task_id": "2", "task_type": "vmm"}],
    ]

    m_vmm_client.vm_get_job_status.side_effect = [
      [{"task_id": "1", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "10",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "20",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "50",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "60",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "completed",
        "status": "Completed", "completed": True, "progress": "100",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "running",
        "status": "Running", "completed": False, "progress": "90",
        "error": "",
        },
       ],
      [{"task_id": "1", "task_type": "vmm", "state": "completed",
        "status": "Completed", "completed": True, "progress": "100",
        "error": "",
        },
       {"task_id": "2", "task_type": "vmm", "state": "failed",
        "status": "Failed", "completed": True, "progress": "100",
        "error": "Hyper-V task hit by a PowerShell",
        },
       ],
    ]

    task_desc_list = [
      HypervTaskDescriptor(
        pre_task_msg="Doing Task 1",
        post_task_msg="Finished Task 1",
        create_task_func=m_vmm_client.create_vm),
      HypervTaskDescriptor(
        pre_task_msg="Doing Task 2",
        post_task_msg="Finished Task 2",
        create_task_func=m_vmm_client.create_vm),
    ]

    with self.assertRaises(CurieTestException) as ar:
      HypervTaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
                                              vmm=m_vmm_client)

    self.assertEqual(
      str(ar.exception),
      "1 of 2 tasks failed. See log for more details (most recent error "
      "message: 'Hyper-V task hit by a PowerShell')")

  # TODO(ryan.hardin): This unit test is flaky, and is currently disabled.
  # @mock.patch("curie.task.time")
  # def test_execute_parallel_tasks_timeout(self, m_time):
  #   m_time.sleep.return_value = 0
  #   m_time.time.side_effect = lambda: m_time.time.call_count * 500
  #
  #   m_vmm_client = mock.Mock(spec=VmmClient)
  #   # Just using create_vm as an example.
  #   m_vmm_client.create_vm.side_effect = [
  #     [{"task_id": "1", "task_type": "vmm"}],
  #     [{"task_id": "2", "task_type": "vmm"}],
  #   ]
  #
  #   # Cycle with in-progress tasks forever.
  #   m_vmm_client.vm_get_job_status.side_effect = cycle([
  #     [{"task_id": "1", "task_type": "vmm", "state": "completed",
  #       "status": "Completed", "completed": True, "progress": "100",
  #       "error": "",
  #       },
  #      {"task_id": "2", "task_type": "vmm", "state": "running",
  #       "status": "Running", "completed": False, "progress": "20",
  #       "error": "",
  #       },
  #      ],
  #   ])
  #
  #   def vm_stop_job_side_effect(*_, **__):
  #     m_vmm_client.vm_get_job_status.side_effect = cycle([
  #       [{"task_id": "1", "task_type": "vmm", "state": "completed",
  #         "status": "Completed", "completed": True, "progress": "100",
  #         "error": "",
  #         },
  #        {"task_id": "2", "task_type": "vmm", "state": "failed",
  #         "status": "Failed", "completed": True, "progress": "20",
  #         "error": "",
  #         },
  #        ],
  #     ])
  #     return [{"task_id": "1", "task_type": "vmm", "state": "completed",
  #              "status": "Completed", "completed": True, "progress": "100",
  #              "error": "",
  #              },
  #             {"task_id": "2", "task_type": "vmm", "state": "running",
  #              "status": "Running", "completed": False, "progress": "20",
  #              "error": "",
  #              },
  #             ]
  #
  #   m_vmm_client.vm_stop_job.side_effect = vm_stop_job_side_effect
  #
  #   task_desc_list = [
  #     HypervTaskDescriptor(
  #       pre_task_msg="Doing Task 1",
  #       post_task_msg="Finished Task 1",
  #       create_task_func=m_vmm_client.create_vm),
  #     HypervTaskDescriptor(
  #       pre_task_msg="Doing Task 2",
  #       post_task_msg="Finished Task 2",
  #       create_task_func=m_vmm_client.create_vm),
  #   ]
  #
  #   with self.assertRaises(CurieTestException) as ar:
  #     HypervTaskPoller.execute_parallel_tasks(task_descriptors=task_desc_list,
  #                                             vmm=m_vmm_client,
  #                                             timeout_secs=10)
  #
  #   self.assertEqual(str(ar.exception),
  #                    "Tasks timed out after '10' seconds (1/2 succeeded)")
  #   m_vmm_client.vm_stop_job.assert_called_once_with(
  #     [{"task_id": "2", "task_type": "vmm"}]
  #   )
