"""Nutanix SSH Client."""

import logging

import scp
from paramiko import client

# Logger for this module.
_LOGGER = logging.getLogger(__name__)


class SSHClientException(Exception):
  """Base class for SSH client exceptions."""


class SSHCommandError(SSHClientException):
  """Exception for errors running command over ssh."""

  def __init__(self, command, rv, sout, serr):
    super(SSHCommandError, self).__init__()
    self.command = command
    self.rv = rv
    self.sout = sout
    self.serr = serr

  def __str__(self):
    return (
      "command: {0}\n"
      "status_code: {1}\n"
      "stdout:\n{2}\n"
      "stderr:\n{3}\n".format(
        self.command, self.rv, self.sout, self.serr))


class NutanixSSHClient(client.SSHClient):
  """Specialization of paramiko SSHClient for Nutanix.

  This class provides an ssh client for use with a Nutanix CVM.

  * General enhancements:
    * Set missing host key policy to auto add by default.
    * Add 'sync_command' method which runs command, and blocks until command
      completes.
    * Must be created with connection information. The Paramiko SSHClient
      instance does not maintain connection information (host, username,
      password, etc.) for each connection. While this keeps the SSHClient
      object general, it limits the value of creating separate clients.
    * Return 'self' during connect which can be used as a context manager.
    * Sets logging channel. By default it is set to 'paramiko.transport' which
      means no logging happens.
    * 'sync_command', 'transfer_to', and 'transfer_from' methods open and
      close connection as part of their operation.

  * Nutanix enhancements
    * Prefix 'exec_command' command argument  with 'env /etc/environment',
      by default.
  """

  def __init__(self, host, *args, **kwargs):
    """Initialize NutanixSSHClient object.

    Args:
      See paramiko.client.SSHClient.connect for acceptable connection
      arguments.
    """
    super(NutanixSSHClient, self).__init__()
    self._host = host
    self._connection_args = args
    self._connection_kwargs = kwargs
    self.set_missing_host_key_policy(client.AutoAddPolicy())
    self.set_log_channel(__name__)

  def connect(self): #pylint: disable=arguments-differ
    """Connect to configured connection and authenticate to it.

    Note:
      Unlike the paramiko.client.SSHClient.connect method which takes
      connection arguments, this method takes no arguments and requires all
      connections arguments be set when this object is created. The
      justification is that the common case is to make many
      identical connections (same host, same user, etc.) since one
      connection is required for each command. It is expected that it is
      less common to make multiple connections to multiple hosts with one
      command each. However if that is the desired behavior, the paramiko
      SSHClient may be used instead.

    Returns:
      None
    """
    super(NutanixSSHClient, self).connect(
      self._host, *self._connection_args, **self._connection_kwargs)

  def sync_command(self, command, default_env=True, *args, **kwargs):
    """Run a command on 'default_connect' and block until it completes.

    This assumes that the stdout and stderr from 'command' can comfortably fit
    into memory.

    Args:
      command (str): command to execute
      default_env (bool): By default execute command in default environment.
        Set to False to disable.
      *args, **kwargs: See 'paramiko.client.SSHClient.exec_command'.

    Returns:
      Output (stdout only) as a str from running command.

    Raises:
      SSHCommandError if the return code from the command is non-zero.
    """
    with self as ssh_client:
      ssh_client.connect()
      _, sout, serr = ssh_client.exec_command(
        command, default_env=default_env, *args, **kwargs)
      command_rv = sout.channel.recv_exit_status()
      if command_rv:
        raise SSHCommandError(command, command_rv, sout.read(), serr.read())
      return sout.read()

  def exec_command(self, command,
                   default_env=True,
                   *args, **kwargs):  #pylint: disable=arguments-differ
    """Execute a command on the SSH server with the default environment.

    See: paramiko.client.SSHClient.exec_command for full documentation

    Args:
       default_env (bool): By default prefix 'command' with 'source /etc/env'
       before executing it on the server. Many commands will not be found or
       execute properly unless the environment is set in this way first.

    Note:
      This method differs from the base class's method in that it takes an
      additional 'default_env' argument.

    Returns:
      The stdin, stdout, and stderr of the executing command as a 3-tuple.

      """
    if default_env:
      __command = "source /etc/profile; {0}".format(command)
    else:
      __command = command
    return super(NutanixSSHClient, self).exec_command(
      __command, *args, **kwargs)

  def transfer_from(self, remote_path, local_path, recursive=False):
    """Copy file or directory from default connection.

    Args:
      remote_path: Location of remote directory or file.
      local_path: Location of local directory or file.
      recursive (bool): Must be set to True when 'remote_path' is a directory.
    """
    _LOGGER.debug("copying %s:%s -> %s", self._host, remote_path, local_path)
    _remote_path = str(remote_path)
    _local_path = str(local_path)
    with self as ssh_client:
      ssh_client.connect()
      with scp.SCPClient(ssh_client.get_transport()) as scp_client:
        scp_client.get(_remote_path, _local_path, recursive=recursive)

  def transfer_to(self, local_path, remote_path, recursive=False):
    """Copy file or directory to default connection.
    Args:
      local_path: Location of local file or directory.
      remote_path: Location of remote file or directory.
      recursive (bool): Must be set to True if 'local_path' is a directory.
    """
    _LOGGER.debug("copying %s -> %s:%s", self._host, local_path, remote_path)
    _local_path = str(local_path)
    _remote_path = str(remote_path)
    with self as ssh_client:
      ssh_client.connect()
      with scp.SCPClient(ssh_client.get_transport()) as scp_client:
        scp_client.put(_local_path, _remote_path, recursive=recursive)
