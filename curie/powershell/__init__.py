#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import json
import logging
import os
import subprocess
import threading
import uuid
import time

import pkg_resources

log = logging.getLogger(__name__)

PS_SCRIPT_PATH = pkg_resources.resource_filename("curie", "powershell")
COMMAND_RUNNER_PATH = os.path.join(PS_SCRIPT_PATH, "CommandRunner.ps1")


class PsServerHTTPResponse(object):
  """
  Container class that maps to a HypervRestApiResponse in CommandRunner.ps1.
  """

  def __init__(self, response):
    self.status = response.get("StatusCode")
    self.data = response.get("Response")


class PsCommand(object):
  """
  Wraps execution of PowerShell commands by CommandRunner.ps1.
  """

  def __init__(self, command):
    """
    Construct a new PsCommand object.

    Args:
      command (str): Block of PowerShell code to execute in the context of
        CommandRunner.ps1 (e.g.
        'Get-VmmVM -cluster_name Cluster-A -json_params {}')
    """
    self.command = command
    self.uuid = str(uuid.uuid4())
    self.proc = None
    self.response = None
    self.status = None
    self.error = None
    self.output = None
    self.__lock = threading.RLock()

  def execute(self):
    """
    Execute the command and block until it's finished.

    Equivalent to execute_async().wait().

    Returns:
      object: The PsServerHTTPResponse data field, which maps to
        HypervRestApiResponse.Response.
    """
    with self.__lock:
      return self.execute_async().wait()

  def execute_async(self):
    """
    Execute the command asynchronously as a background process.

    The returned object can be polled to update its status.

    Returns:
      PsCommand: Reference to this object to allow chaining.
    """
    with self.__lock:
      self.proc = subprocess.Popen([
        "pwsh",
        "-command", "& { . %s; %s }" % (COMMAND_RUNNER_PATH, self.command)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=r"%s" % PS_SCRIPT_PATH)
      self.status = "running"
      return self

  def poll(self):
    """
    Update the state of a PsCommand that has been run using execute_async.

    Sets self.status. If the PsCommand has terminated, also sets self.response
    and/or self.error. If the PsCommand has been polled previously and has
    terminated, this is a no-op.

    Returns:
      PsServerHTTPResponse or None: The response from the ComamandRunner. If
        the command is still running, returns None.
    """
    with self.__lock:
      if self.proc is None:
        raise RuntimeError("Cannot poll before calling execute_async")
      elif self.response is not None:
        return self.response
      else:
        self.proc.poll()
        if self.proc.returncode is not None:
          self._finalize()
          return self.response
        else:
          return None

  def wait(self):
    """
    Block until the command finishes.

    If the PsCommand has been polled previously and has terminated, this is a
    no-op.

    Returns:
      PsServerHTTPResponse: The response from the ComamandRunner.
    """
    with self.__lock:
      if self.proc is None:
        raise RuntimeError("Cannot wait before calling execute_async")
      elif self.response is not None:
        return self.response
      else:
        self._finalize()
        return self.response

  def terminate(self):
    """
    Terminate a running command.

    This method sets self.status to "stopped".

    Returns:
      PsServerHTTPResponse or None: The response from the ComamandRunner, or
      None if no returned data is available.
    """
    with self.__lock:
      if self.proc is None:
        raise RuntimeError("Cannot terminate before calling execute_async")
      self.proc.terminate()
      self._finalize()
      self.status = "stopped"
      return self.response

  def as_ps_task(self):
    """
    State of this PsCommand resembling a response from Vmm.psm1 Get-Task.

    Used to provide a consistent interface to a task handler designed for both
    VMM tasks and generic PowerShell asynchronous commands.

    Returns:
      dict: Task data.
    """
    with self.__lock:
      return {"task_id": self.uuid,
              "task_type": "ps",
              "completed": self.status == "completed",
              "progress": 100 if self.status == "completed" else 0,
              "state": self.status,
              "status": "",
              "error": self.error}

  def _finalize(self):
    """
    Internal method for waiting for termination and setting response fields.

    Blocks until the PsCommand process exits.

    Returns:
      None
    """
    with self.__lock:
      output, logs = self.proc.communicate()
      self.output = output + "\n" + logs
      if self.proc.returncode != 0:
        msg = "Curie received non-zero PowerShell return code " \
              "%r" % self.proc.returncode
        log.error(msg)
        log.error("PowerShell process logs (%s):\n%s", self.uuid, logs)
        self.status = "failed"
        self.error = msg
        return
      else:
        log.debug("PowerShell process logs (%s):\n%s", self.uuid, logs)
      try:
        # The JSON response is the last line of stdout.
        last_line = output.splitlines()[-1] if output else ""
        response = PsServerHTTPResponse(json.loads(last_line))
      except ValueError:
        msg = "Curie failed to parse PowerShell JSON response: %r" % output
        log.error(msg)
        self.status = "failed"
        self.error = msg
      else:
        self.response = response.data
        if response.status == 200:
          self.status = "completed"
        else:
          self.status = "failed"
          msgs = []
          for error_dict in self.response:
            msgs.append("%s: %s" % (error_dict["message"],
                                    error_dict["details"]))
          self.error = "PowerShell command error:\n%s" % "\n".join(msgs)


class PsClient(object):
  """
  Create and track state of PsCommands with sessions to the same remote server.
  """

  def __init__(self, address, username, password):
    """
    Create a new PsClient instance.

    Args:
      address (str): Address to use in New-PSSession.
      username (str): Username to use in New-PSSession.
      password (str): Password to use in New-PSSession.
    """
    self.address = address
    self.username = username
    self.password = password
    self.async_commands = {}

  def execute(self, function_name, num_retries = 1, **kwargs):
    """
    Execute a command and block until its finished.

    Args:
      function_name (str): Name of the function in Vmm.psm1 to execute.
      num_retries (int): Number of retries, if function fails
      **kwargs: Keyword arguments passed through to the PowerShell function.

    Returns:
      object: The PsServerHTTPResponse data field, which maps to
        HypervRestApiResponse.Response.

    Raises:
      RuntimeError: If the executed command is unsuccessful.
    """
    num_errors = 0
    sleep_time = 30
    while(num_retries > 0):
      cmd = self._command(function_name, **kwargs)
      cmd.execute()
      if cmd.status.lower() == "completed":
        return cmd.response
      else:
        if(cmd.output.find("Start executing function") >= 0):
          num_retries -= 1
          if(num_retries == 0):
            raise RuntimeError("PowerShell command '%s' failed:\n%s" %
                         (function_name, cmd.error))
          else:
            log.debug("PowerShell command '%s' failed:\n%s, retrying...", function_name, cmd.error)
        else:
          log.debug("PowerShell command '%s' failed before executing function:\n%s, retrying...", function_name, cmd.error)
          num_errors += 1
          if (num_errors >= 3):
            raise RuntimeError("PowerShell command '%s' failed 3 times:\n%s" %
                         (function_name, cmd.error))
        log.debug("Pending restart for %d seconds.", sleep_time)
        time.sleep(sleep_time)

  def execute_async(self, function_name, **kwargs):
    """
    Execute the command asynchronously as a background process.

    The returned object can be polled to update its status.

    Returns:
      PsCommand: New PsCommand.
    """
    cmd = self._command(function_name, **kwargs)
    cmd.execute_async()
    self.async_commands[cmd.uuid] = cmd
    return cmd

  def poll(self, uuid):
    """
    Update the state of a PsCommand that has been run using execute_async.

    Args:
      uuid (str): ID of the PsCommand to poll.

    Returns:
      PsCommand: The polled PsCommand.
    """
    cmd = self.async_commands[uuid]
    cmd.poll()
    return cmd

  def _command(self, function_name, **kwargs):
    expanded_kwargs = " ".join(
      ["-%s '%s'" % key_val for key_val in kwargs.iteritems()])
    command = "ExecuteCommand " \
              "-vmm_address {vmm_address} " \
              "-username {username} " \
              "-password {password} " \
              "-function_name {function_name} " \
              "{expanded_kwargs}".format(
      vmm_address=self.address,
      username=self.username,
      password=self.password,
      function_name=function_name,
      expanded_kwargs=expanded_kwargs)
    return PsCommand(command)
