#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import json
import subprocess
import unittest

import mock

from curie.powershell import COMMAND_RUNNER_PATH, PsClient
from curie.powershell import PS_SCRIPT_PATH, PsClient
from curie.powershell import PsServerHTTPResponse, PsCommand


class TestPsServerHTTPResponse(unittest.TestCase):
  def setUp(self):
    pass

  def test_init(self):
    fake_response = {"StatusCode": 200, "Response": {"fruit": "apple"}}
    resp = PsServerHTTPResponse(fake_response)
    self.assertEqual(200, resp.status)
    self.assertEqual({"fruit": "apple"}, resp.data)


class TestPsCommand(unittest.TestCase):
  def setUp(self):
    pass

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_basic(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "{\"Response\": {\"fruit\": \"apple\"}, \"StatusCode\": 200}"
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("completed", ps.status)
    self.assertEqual({"fruit": "apple"}, ps.response)

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_empty_return_string(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = ""
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("failed", ps.status)
    self.assertEqual(None, ps.response)
    self.assertEqual("Curie failed to parse PowerShell JSON response: ''",
                     ps.error)

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_bad_status_code(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    error_list = [{"message": "Fake message from PS",
                   "details": "The fake details from PS"},
                  {"message": "Another fake message from PS",
                   "details": "More fake details from PS"},]
    stdout = json.dumps({"Response": error_list, "StatusCode": 500})
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("failed", ps.status)
    self.assertEqual(error_list, ps.response)
    self.assertEqual("PowerShell command error:\n"
                     "Fake message from PS: The fake details from PS\n"
                     "Another fake message from PS: More fake details from PS",
                     ps.error)

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_bad_proc_return_code(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "** Error in 'pwsh': corrupted double-linked list: 0xdeadbeef **"
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 1
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("failed", ps.status)
    self.assertEqual(None, ps.response)  # Non-zero code implies useless data.
    self.assertEqual("Curie received non-zero PowerShell return code 1",
                     ps.error)

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_bad_json_response(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "This string is not valid JSON!"
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("failed", ps.status)
    self.assertEqual(None, ps.response)
    self.assertEqual("Curie failed to parse PowerShell JSON response: 'This "
                     "string is not valid JSON!'", ps.error)

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_garbage_preceding_json(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "Garbage\nMore garbage!\n\n" \
             "{\"Response\": {\"fruit\": \"apple\"}, \"StatusCode\": 200}"
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps = PsCommand("Get-At-Me")

    ps.execute()

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command", "& { . %s; Get-At-Me }" % COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("completed", ps.status)
    self.assertEqual({"fruit": "apple"}, ps.response)

  def test_as_ps_task_init(self):
    ps = PsCommand("Get-At-Me")
    task = ps.as_ps_task()
    self.assertEqual("ps", task["task_type"])
    self.assertEqual(False, task["completed"])
    self.assertEqual(0, task["progress"])
    self.assertEqual(None, task["state"])
    self.assertEqual("", task["status"])
    self.assertEqual(None, task["error"])

  def test_as_ps_task_completed(self):
    ps = PsCommand("Get-At-Me")
    ps.status = "completed"
    task = ps.as_ps_task()
    self.assertEqual("ps", task["task_type"])
    self.assertEqual(True, task["completed"])
    self.assertEqual(100, task["progress"])
    self.assertEqual("completed", task["state"])
    self.assertEqual("", task["status"])
    self.assertEqual(None, task["error"])

  def test_as_ps_task_failed(self):
    ps = PsCommand("Get-At-Me")
    ps.status = "failed"
    ps.error = "Oh noes"
    task = ps.as_ps_task()
    self.assertEqual("ps", task["task_type"])
    self.assertEqual(False, task["completed"])
    self.assertEqual(0, task["progress"])
    self.assertEqual("failed", task["state"])
    self.assertEqual("", task["status"])
    self.assertEqual("Oh noes", task["error"])


class TestPsClient(unittest.TestCase):
  def setUp(self):
    pass

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_basic(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "{\"Response\": {\"fruit\": \"apple\"}, \"StatusCode\": 200}"
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps_client = PsClient("fake_address", "fake_username", "fake_password")

    data = ps_client.execute("Get-Snack", fruit="apple")

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command",
      "& { . %s; ExecuteCommand -vmm_address fake_address "
      "-username fake_username -password fake_password "
      "-function_name Get-Snack -fruit 'apple' }" %
      COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual({"fruit": "apple"}, data)
    pass

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_failed(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    error_list = [{"message": "Fake message from PS",
                   "details": "The fake details from PS"},
                  {"message": "Another fake message from PS",
                   "details": "More fake details from PS"}, ]
    stdout = "Start executing function Get-Snack\n" + json.dumps({"Response": error_list, "StatusCode": 500})
    stderr = "Logs logs logs\nBlah blah blah"

    def m_communicate_side_effect():
      m_proc.returncode = 0
      return stdout, stderr

    m_proc.communicate.side_effect = m_communicate_side_effect
    m_Popen.return_value = m_proc
    ps_client = PsClient("fake_address", "fake_username", "fake_password")

    with self.assertRaises(RuntimeError) as ar:
      ps_client.execute("Get-Snack", fruit="apple")

    m_proc.communicate.assert_called_once()
    m_Popen.assert_called_once_with([
      "pwsh", "-command",
      "& { . %s; ExecuteCommand -vmm_address fake_address "
      "-username fake_username -password fake_password "
      "-function_name Get-Snack -fruit 'apple' }" %
      COMMAND_RUNNER_PATH],
      stderr=subprocess.PIPE, stdout=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
    self.assertEqual("PowerShell command 'Get-Snack' failed:\n"
                     "PowerShell command error:\n"
                     "Fake message from PS: The fake details from PS\n"
                     "Another fake message from PS: More fake details from PS",
                     str(ar.exception))

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_async_return_code_zero(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "{\"Response\": {\"fruit\": \"apple\"}, \"StatusCode\": 200}"
    stderr = "Logs logs logs\nBlah blah blah"

    m_proc.communicate.return_value = stdout, stderr
    m_Popen.return_value = m_proc
    ps_client = PsClient("fake_address", "fake_username", "fake_password")

    cmd = ps_client.execute_async("Get-Snack", fruit="apple")

    self.assertEqual("running", cmd.status)
    m_proc.communicate.assert_not_called()

    cmd = ps_client.poll(cmd.uuid)
    self.assertEqual("running", cmd.status)
    m_proc.communicate.assert_not_called()

    m_proc.returncode = 0
    cmd = ps_client.poll(cmd.uuid)
    self.assertEqual("completed", cmd.status)
    m_proc.communicate.assert_called_once()

  @mock.patch("curie.powershell.subprocess.Popen")
  def test_execute_async_return_code_non_zero(self, m_Popen):
    m_proc = mock.MagicMock()
    m_proc.returncode = None

    stdout = "garbage"
    stderr = "Logs logs logs\nBlah blah blah"

    m_proc.communicate.return_value = stdout, stderr
    m_Popen.return_value = m_proc
    ps_client = PsClient("fake_address", "fake_username", "fake_password")

    cmd = ps_client.execute_async("Get-Snack", fruit="apple")

    self.assertEqual("running", cmd.status)
    m_proc.communicate.assert_not_called()

    cmd = ps_client.poll(cmd.uuid)
    self.assertEqual("running", cmd.status)
    m_proc.communicate.assert_not_called()

    m_proc.returncode = 1
    cmd = ps_client.poll(cmd.uuid)
    self.assertEqual("failed", cmd.status)
    m_proc.communicate.assert_called_once()
