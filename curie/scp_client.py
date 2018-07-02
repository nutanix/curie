#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# DESCRIPTION: class that can be used to programmatically transfer files
# to/from a host using scp.
#

import logging
import os
import pkg_resources
import stat

import gflags

from curie.util import CurieUtil

log = logging.getLogger(__name__)
FLAGS = gflags.FLAGS


class ScpClient(object):
  def __init__(self, host, user, private_key_path=None):
    # DNS name or IP address of the host to transfer files to/from.
    self.__host = host

    # User name for scp command.
    self.__user = user

    # Path to the private key to use to scp files.
    if private_key_path is None:
      private_key_path = pkg_resources.resource_filename(__name__,
                                                         "etc/curie_ssh")
    self.__private_key_path = private_key_path
    try:
      if not os.path.exists(self.__private_key_path):
        raise ValueError("%s doesn't exist." % self.__private_key_path)
    except Exception:
      log.exception("Exception while finding private key")
      raise

    os.chmod(self.__private_key_path, stat.S_IRUSR | stat.S_IWUSR)

    # Base scp options.
    self.__base_scp_opts = \
      " ".join(["-o CheckHostIp=no",
                "-o ConnectTimeout=30",
                "-o IdentityFile=%s" % self.__private_key_path,
                "-o StrictHostKeyChecking=no",
                "-o TCPKeepAlive=yes",
                "-o UserKnownHostsFile=/dev/null",
                "-q"])

  def transfer_from(self,
                    remote_path,
                    local_path,
                    timeout_secs,
                    recursive=False):
    """
    Transfer 'remote_path' on _host to 'local_path'. 'timeout_secs' specifies a
    timeout on the transfer. 'recursive' specifies whether the transfer is
    recursive or not. Returns True on success, else False on failure or a
    timeout.
    """
    if recursive:
      opts = self.__base_scp_opts + " -r"
    else:
      opts = self.__base_scp_opts
    cmd = "scp %s '%s@%s:%s' %s" % \
      (opts, self.__user, self.__host, remote_path, local_path)
    log.debug("Transferring %s:%s to %s using %s",
              self.__host, remote_path, local_path, cmd)
    status, _, _ = CurieUtil.timed_command(cmd, timeout_secs)
    return status == 0

  def transfer_to(self,
                  local_path,
                  remote_path,
                  timeout_secs,
                  recursive=False):
    """
    Transfer 'local_path' to 'remote_path' on _host. 'timeout_secs' specifies a
    timeout on the transfer. 'recursive' specifies whether the transfer is
    recursive or not. Returns True on success, else False on failure or a
    timeout.
    """
    if recursive:
      opts = self.__base_scp_opts + " -r"
    else:
      opts = self.__base_scp_opts
    cmd = "scp %s %s '%s@%s:%s'" % \
      (opts, local_path, self.__user, self.__host, remote_path)
    log.debug("Transferring %s to %s:%s to using %s",
              local_path, self.__host, remote_path, cmd)
    status, _, _ = CurieUtil.timed_command(cmd, timeout_secs)
    return status == 0
