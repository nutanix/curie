#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: all the web and API HTTP handlers are thread-safe. All other
# methods (e.g., run) are meant to be called one time and from a single caller,
# the main curie_unix_agent program.
#

import errno
import logging
import os
import pwd
import re
import shutil
import signal
import subprocess
import threading
import time
import traceback
from functools import wraps

import flask
import gflags
import google.protobuf.message

from curie import charon_agent_interface_pb2
from curie.charon_agent_interface_pb2 import CmdStatus
from curie.curie_error_pb2 import CurieError, ErrorRet
from curie.error import curie_error_to_http_status
from curie.exception import CurieException
from curie.log import CHECK, CHECK_EQ
from curie.name_util import NameUtil
from curie.os_util import OsUtil
from curie.rpc_util import RpcServer
from curie.util import CurieUtil

log = logging.getLogger(__name__)
FLAGS = gflags.FLAGS

gflags.DEFINE_integer("curie_agent_port",
                      5001,
                      "TCP port that curie agent listens on.")

gflags.DEFINE_string("curie_agent_dir",
                     "/home/nutanix/curie/agent",
                     "Directory where the curie agent keeps all of its "
                     "state.")

gflags.DEFINE_bool("curie_agent_flask_debug_enabled",
                   False,
                   "If True, run Flask in debug mode.")

gflags.DEFINE_bool("curie_agent_flask_suppress_werkzeug_logs",
                   True,
                   "If True, disable werkzeug logspam.")

gflags.DEFINE_integer("curie_agent_cmd_poll_period_secs",
                      1,
                      "Period for periodic updating of command status.")

gflags.DEFINE_integer("curie_agent_cmd_gc_period_secs",
                      5,
                      "Period for garbage collecting commands.")

def curie_unix_agent_api_handler(func):
  @wraps(func)
  def wrapper(*args, **kwargs):
    try:
      return func(*args, **kwargs), 200
    except CurieException, ex:
      log.warning("CurieException handling %s: %s, %s",
                  func.func_name, ex, traceback.format_exc())
      ret = ErrorRet()
      ret.error_codes.append(ex.error_code)
      ret.error_msgs.append(ex.error_msg)
      http_status = curie_error_to_http_status(ex.error_code)
      return ret.SerializeToString(), http_status
    except google.protobuf.message.EncodeError as ex:
      log.warning("Protobuf deserialization error handling %s",
                  func.func_name, exc_info=True)
      ret = ErrorRet()
      ret.error_codes.append(CurieError.kInvalidParameter)
      ret.error_msgs.append(str(ex))
      return ret.SerializeToString(), 400
    except Exception as ex:
      log.exception("Unknown exception handling %s", func.func_name)
      ret = ErrorRet()
      ret.error_codes.append(CurieError.kInternalError)
      ret.error_msgs.append(str(ex))
      http_status = curie_error_to_http_status(CurieError.kInternalError)
      return ret.SerializeToString(), http_status
  return wrapper

class CurieCmdState(object):
  def __init__(self, cmd_id, proc=None, pid=None):
    # Command ID.
    self.cmd_id = cmd_id
    arg_path = os.path.join(CurieUnixAgent.cmd_dir(cmd_id), "arg.bin")
    arg = charon_agent_interface_pb2.CmdExecuteArg()
    arg.ParseFromString(open(arg_path).read())

    # Command.
    self.cmd = arg.cmd

    # If not None, a subprocess.Popen object for the curie_cmd_wrapper child
    # process wrapping the command.
    self.proc = proc

    # If not None, the PID for the curie_cmd_wrapper process wrapping the
    # command. If None, the command is not running.
    self.pid = pid

class CurieUnixAgent(object):
  def __init__(self, name=__name__):
    CHECK(FLAGS.curie_agent_dir, "--curie_agent_dir must be set")

    # If requested, monkey-patch werkzeug to suppress Flask logspam.
    if FLAGS.curie_agent_flask_suppress_werkzeug_logs:
      log.debug(
        "--curie_agent_flask_suppress_werkzeug_logs is set. Monkey-patching "
        "'werkzeug._internal._log' to suppress unwanted output")
      CurieUtil.monkey_patch_werkzeug_logger()

    # Flask application.
    self.__app = flask.Flask(name)

    # RPC server.
    self.__rpc_server = RpcServer(
      charon_agent_interface_pb2.CurieAgentRpcSvc)

    # UID of the user running the agent.
    self.__agent_uid = os.getuid()

    # Garbage directory.
    self.__garbage_dir = os.path.join(FLAGS.curie_agent_dir, "garbage")

    # Whether we're done with initialization or not.
    self.__initialized = False

    # Thread that periodically updates the status of commands in
    # self.__cmd_map.
    self.__cmd_poller_thread = None

    # Thread that periodically garbage collects state for commands that can be
    # removed.
    self.__cmd_gc_thread = None

    # Lock that protects the fields below and all non stdout/stderr state for
    # all commands on disk.
    self.__lock = threading.Lock()

    # Mapping from command ID to command state.
    self.__cmd_map = {}

    #--------------------------------------------------------------------------
    #
    # Handlers for web pages.
    #
    #--------------------------------------------------------------------------

    @self.__app.route("/")
    def web_index():
      return ""

    #--------------------------------------------------------------------------
    #
    # RPC endpoint.
    #
    #--------------------------------------------------------------------------

    @self.__app.route("/rpc", methods=["POST"])
    @self.__rpc_server.endpoint
    def rpc_endpoint():
      return flask.request

    #--------------------------------------------------------------------------
    #
    # RPC handlers.
    #
    #--------------------------------------------------------------------------

    @self.__rpc_server.handler("CmdExecute")
    @curie_unix_agent_api_handler
    def api_cmd_execute(arg):
      return self.__api_cmd_execute(arg)

    @self.__rpc_server.handler("CmdStatus")
    @curie_unix_agent_api_handler
    def api_cmd_status(arg):
      return self.__api_cmd_status(arg)

    @self.__rpc_server.handler("CmdStop")
    @curie_unix_agent_api_handler
    def api_cmd_stop(arg):
      return self.__api_cmd_stop(arg)

    @self.__rpc_server.handler("CmdRemove")
    @curie_unix_agent_api_handler
    def api_cmd_remove(arg):
      return self.__api_cmd_remove(arg)

    @self.__rpc_server.handler("CmdList")
    @curie_unix_agent_api_handler
    def api_cmd_list(arg):
      return self.__api_cmd_list(arg)

    @self.__rpc_server.handler("FileGet")
    @curie_unix_agent_api_handler
    def api_file_get(arg):
      return self.__api_file_get(arg)

  def initialize(self):
    "Initialize agent state."
    CHECK(not self.__initialized)
    # Create initial directory structure and empty settings file if needed.
    if not os.path.exists(FLAGS.curie_agent_dir):
      self.__setup()
    # Delete incomplete command directories.
    cmds_dir = self.cmds_dir()
    for name in os.listdir(cmds_dir):
      cmd_dir = os.path.join(cmds_dir, name)
      arg_path = os.path.join(cmd_dir, "arg.bin")
      status_path = os.path.join(cmd_dir, "status.bin")
      if not os.path.exists(arg_path) or not os.path.exists(status_path):
        log.warning("Deleting incomplete command directory %s", cmd_dir)
        shutil.rmtree(cmd_dir)
    # Delete old garbage and create an empty garbage directory.
    if os.path.exists(self.__garbage_dir):
      shutil.rmtree(self.__garbage_dir)
    os.mkdir(self.__garbage_dir)
    # Recover self.__cmd_map based on state on disk and running commands.
    self.__recover_cmd_map()
    # Mark agent as being initialized.
    self.__initialized = True
    # Start thread to perform periodic updates of command state and GC.
    self.__cmd_poller_thread = \
      threading.Thread(target=self.__cmd_poller_thread_func)
    self.__cmd_poller_thread.start()

  def run(self):
    "Run the Flask server forever."
    CHECK(self.__initialized)
    if self.__agent_uid != 0:
      log.warning("curie_unix_agent not running as root")
    log.info("Running agent on TCP port %d", FLAGS.curie_agent_port)
    self.__app.run(debug=FLAGS.curie_agent_flask_debug_enabled,
                   host="0.0.0.0",
                   port=FLAGS.curie_agent_port,
                   threaded=True)

  @staticmethod
  def cmds_dir(root=None):
    if root is None:
      root = FLAGS.curie_agent_dir
    return os.path.join(root, "cmds")

  @staticmethod
  def cmd_dir(cmd_id, root=None):
    return os.path.join(CurieUnixAgent.cmds_dir(root=root),
                        NameUtil.sanitize_filename(cmd_id))

  def __setup(self):
    "Create the directory structure and an empty settings file for curie."
    log.info("Creating initial directory structure at %s",
             FLAGS.curie_agent_dir)
    # Remove the temporary directory that we might have been populating during
    # a previous crash so we start with a clean slate.
    tmp_dir = FLAGS.curie_agent_dir + ".tmp"
    if os.path.exists(tmp_dir):
      shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir)
    # Fill in subdirectories.
    os.mkdir(self.cmds_dir(root=tmp_dir))
    # Rename the temporary directory to its final path so the curie agent can
    # use it.
    os.rename(tmp_dir, FLAGS.curie_agent_dir)

  def __cmd_poller_thread_func(self):
    while True:
      with self.__lock:
        cmd_ids = self.__cmd_map.keys()
        for cmd_id in cmd_ids:
          cmd_state = self.__cmd_map[cmd_id]
          # Poll the command if it's still running.
          if cmd_state.pid is not None:
            self.__poll_cmd(cmd_state)
      time.sleep(FLAGS.curie_agent_cmd_poll_period_secs)

  def __cmd_gc_thread_func(self):
    while True:
      for name in os.listdir(self.__garbage_dir):
        path = os.path.join(self.__garbage_dir, name)
        log.info("Deleting garbage directory %s", path)
        shutil.rmtree(path)
      time.sleep(FLAGS.curie_agent_cmd_gc_period_secs)

  def __poll_cmd(self, cmd_state):
    """
    Check if the command with ID 'cmd_id' has exited. If it has, finalize its
    state in memory/disk.

    Assumes self.__lock is held.
    """
    if cmd_state.proc is not None:
      wait_pid, wait_status = os.waitpid(cmd_state.pid, os.WNOHANG)
      if wait_pid == cmd_state.pid:
        # Command is finished (child process).
        if os.WIFEXITED(wait_status):
          exit_status = os.WEXITSTATUS(wait_status)
        else:
          # We use -2 to indicate a non-normal exit.
          exit_status = -2
        self.__maybe_finalize_cmd_state(cmd_state, exit_status=exit_status)
      else:
        # Command is still running.
        CHECK_EQ(wait_pid, 0)
    else:
      try:
        os.kill(cmd_state.pid, 0)
        # Command is still running.
      except OSError, ex:
        CHECK_EQ(ex.errno, errno.ESRCH, msg=str(ex))
        # Command is finished (non-child process).
        self.__maybe_finalize_cmd_state(cmd_state, exit_status=None)

  def __api_cmd_execute(self, arg):
    if flask.request.data == "":
      raise CurieException(CurieError.kInvalidParameter, "Empty request")
    try:
      cmd_uid = pwd.getpwnam(arg.user).pw_uid
    except KeyError:
      raise CurieException(CurieError.kInvalidParameter,
                            "Invalid user %s" % arg.user)
    ret = charon_agent_interface_pb2.CmdExecuteRet()
    with self.__lock:
      if arg.cmd_id not in self.__cmd_map:
        self.__execute_cmd(arg.cmd_id, arg.cmd, cmd_uid)
      else:
        # It's possible this is a retry of a request that previously succeeded.
        log.warning("Command %s already exists", arg.cmd_id)
    return ret.SerializeToString()

  def __api_cmd_status(self, arg):
    if flask.request.data == "":
      raise CurieException(CurieError.kInvalidParameter, "Empty request")
    cmd_id = arg.cmd_id
    cmd_dir = self.cmd_dir(cmd_id)
    ret = charon_agent_interface_pb2.CmdStatusRet()
    with self.__lock:
      cmd_state = self.__cmd_map.get(cmd_id)
      if cmd_state is None:
        raise CurieException(CurieError.kInvalidParameter,
                              "Command %s not found" % cmd_id)
      status_path = os.path.join(cmd_dir, "status.bin")
      ret.cmd_status.ParseFromString(open(status_path).read())
    if arg.include_output and ret.cmd_status.state != CmdStatus.kRunning:
      stdout_path = os.path.join(cmd_dir, "stdout.txt")
      stderr_path = os.path.join(cmd_dir, "stderr.txt")
      if ret.cmd_status.HasField("exit_status"):
        exit_status = ret.cmd_status.exit_status
      else:
        log.warning("No exit status for command %s", cmd_id)
        exit_status = None
      if os.path.exists(stdout_path):
        ret.stdout = open(stdout_path).read()
      else:
        log.warning("No stdout for command %s that exited with status %s",
                    cmd_id, exit_status)
      if os.path.exists(stderr_path):
        ret.stderr = open(stderr_path).read()
      else:
        log.warning("No stderr for command %s that exited with status %s",
                    cmd_id, exit_status)
    return ret.SerializeToString()

  def __api_cmd_stop(self, arg):
    if flask.request.data == "":
      raise CurieException(CurieError.kInvalidParameter, "Empty request")
    cmd_id = arg.cmd_id
    with self.__lock:
      cmd_state = self.__cmd_map.get(cmd_id)
      if cmd_state is None:
        raise CurieException(CurieError.kInvalidParameter,
                              "Command %s not found" % cmd_id)
      ret = charon_agent_interface_pb2.CmdStopRet()
      if cmd_state.pid is not None:
        # Kill the command wrapper process group which will kill the
        # curie_cmd_wrapper process and all of its descendants.
        log.info("Killing command %s, PID %d", cmd_id, cmd_state.pid)
        try:
          os.killpg(cmd_state.pid, signal.SIGKILL)
        except OSError, ex:
          CHECK_EQ(ex.errno, errno.ESRCH, msg=str(ex))
        # We use -1 to indicate the command was stopped.
        self.__maybe_finalize_cmd_state(cmd_state, exit_status=-1)
      return ret.SerializeToString()

  def __api_cmd_remove(self, arg):
    if flask.request.data == "":
      raise CurieException(CurieError.kInvalidParameter, "Empty request")
    cmd_id = arg.cmd_id
    cmd_dir = self.cmd_dir(cmd_id)
    with self.__lock:
      ret = charon_agent_interface_pb2.CmdRemoveRet()
      cmd_state = self.__cmd_map.get(cmd_id)
      if cmd_state is None:
        # It's possible this is a retry of a request that previously succeeded.
        log.warning("Command %s not found", cmd_id)
        return ret.SerializeToString()
      if cmd_state.pid is not None:
        # Kill the command wrapper process group which will kill the
        # curie_cmd_wrapper process and all of its descendants.
        log.info("Killing command %s as part of a remove, PID %d",
                 cmd_id, cmd_state.pid)
        try:
          os.killpg(cmd_state.pid, signal.SIGKILL)
        except OSError, ex:
          CHECK_EQ(ex.errno, errno.ESRCH, msg=str(ex))
      # Move the command's directory to the garbage directory to be garbage
      # collected later.
      garbage_cmd_dir = os.path.join(self.__garbage_dir,
                                     os.path.basename(cmd_dir))
      os.rename(cmd_dir, garbage_cmd_dir)
      del self.__cmd_map[cmd_id]
      return ret.SerializeToString()

  def __api_cmd_list(self, arg):
    with self.__lock:
      ret = charon_agent_interface_pb2.CmdListRet()
      for cmd_id, cmd_state in self.__cmd_map.iteritems():
        status_path = os.path.join(self.cmd_dir(cmd_id), "status.bin")
        cmd_status = CmdStatus()
        cmd_status.ParseFromString(open(status_path).read())
        cmd_summary = ret.cmd_summary_list.add()
        cmd_summary.cmd_id = cmd_id
        cmd_summary.cmd = cmd_state.cmd
        cmd_summary.state = cmd_status.state
      return ret.SerializeToString()

  def __api_file_get(self, arg):
    ret = charon_agent_interface_pb2.FileGetRet()
    if not os.path.exists(arg.path):
      raise CurieException(CurieError.kInvalidParameter,
                            "File %s does not exist" % arg.path)
    with open(arg.path) as fobj:
      fobj.seek(arg.offset)
      if arg.HasField("length"):
        ret.data = fobj.read(arg.length)
      else:
        ret.data = fobj.read()
    return ret.SerializeToString()

  def __recover_cmd_map(self):
    """
    Populate self.__cmd_map based on the commands directory and running command
    processes. Also kill any unknown command processes.
    """
    # Find all curie_cmd_wrapper processes and construct a mapping from
    # command ID to curie_cmd_wrapper PID.
    cmd_id_pid_map = {}
    for name in os.listdir("/proc"):
      if not name.isdigit():
        continue
      pid = int(name)
      try:
        cmdline = open("/proc/%d/cmdline" % pid).read().replace("\0", " ")
      except (IOError, OSError), ex:
        continue
      match = re.search("python .*curie_cmd_wrapper (\d+)", cmdline)
      if not match:
        continue
      cmd_id = match.group(1)
      cmd_id_pid_map[cmd_id] = pid
    # Kill any unknown commands (ones with no entry in the commands directory).
    cmd_ids = set(os.listdir(self.cmds_dir()))
    for cmd_id, pid in cmd_id_pid_map.iteritems():
      if NameUtil.sanitize_filename(cmd_id) not in cmd_ids:
        log.warning("Killing process group %d for unknown command %s",
                    pid, cmd_id)
        try:
          os.killpg(pid, signal.SIGKILL)
        except OSError, ex:
          CHECK_EQ(ex.errno, errno.ESRCH, msg=str(ex))
    # Reconstruct self.__cmd_map.
    for cmd_id in cmd_ids:
      status_path = os.path.join(self.cmd_dir(cmd_id), "status.bin")
      cmd_status = CmdStatus()
      cmd_status.ParseFromString(open(status_path).read())
      if cmd_status.HasField("pid"):
        pid = cmd_status.pid
      else:
        pid = None
      cmd_state = CurieCmdState(cmd_id, proc=None, pid=pid)
      if pid is not None and cmd_id not in cmd_id_pid_map:
        log.warning("Command %s exited while the agent was down", cmd_id)
        self.__maybe_finalize_cmd_state(cmd_state, exit_status=None)
      self.__cmd_map[cmd_id] = cmd_state
      log.info("Recovered state for command %s (state %s)",
               cmd_id, CmdStatus.Type.Name(cmd_status.state))
    return cmd_id_pid_map

  def __execute_cmd(self, cmd_id, cmd, cmd_uid):
    """
    Start asynchronously executing the command 'cmd' with command ID 'cmd_id'
    as user 'cmd_uid'.

    Assumes self.__lock is held.
    """
    cmd_dir = self.cmd_dir(cmd_id)
    os.mkdir(cmd_dir)
    if cmd_uid != self.__agent_uid:
      CHECK_EQ(self.__agent_uid, 0)
      # Change the ownership of 'cmd_dir' so the command can write to its
      # stdout and stderr files.
      pwd_entry = pwd.getpwuid(cmd_uid)
      os.chown(cmd_dir, pwd_entry.pw_uid, pwd_entry.pw_gid)
    # Run the command using the command wrapper. We run the command using the
    # wrapper so we can identify commands run by the agent by matching against
    # process command lines should the agent crash and restart.
    wrapper_path = "/usr/local/bin/curie_cmd_wrapper"
    wrapped_cmd = map(str, [wrapper_path, cmd_id, cmd_dir, cmd_uid, cmd])
    wrapper_stdout_path = os.path.join(cmd_dir, "wrapper_stdout.txt")
    wrapper_stderr_path = os.path.join(cmd_dir, "wrapper_stderr.txt")
    log.info("Executing command %s (%s)", cmd_id, cmd)
    log.info("Wrapped command %s (%s)", cmd_id, wrapped_cmd)
    proc = subprocess.Popen(wrapped_cmd,
                            stdout=open(wrapper_stdout_path, "w", 0),
                            stderr=open(wrapper_stderr_path, "w", 0),
                            close_fds=True)
    # Fill in 'cmd_dir' with the RPC request and the command's initial status.
    arg_path = os.path.join(cmd_dir, "arg.bin")
    OsUtil.write_and_rename(arg_path, flask.request.data)
    cmd_status = CmdStatus()
    cmd_status.state = CmdStatus.kRunning
    cmd_status.pid = proc.pid
    cmd_status.stdout_path = os.path.join(cmd_dir, "stdout.txt")
    cmd_status.stderr_path = os.path.join(cmd_dir, "stderr.txt")
    status_path = os.path.join(cmd_dir, "status.bin")
    status_data = cmd_status.SerializeToString()
    OsUtil.write_and_rename(status_path, status_data)
    # Add command's state to the agent's state.
    cmd_state = CurieCmdState(cmd_id, proc=proc, pid=proc.pid)
    self.__cmd_map[cmd_id] = cmd_state
    return cmd_state

  def __maybe_finalize_cmd_state(self, cmd_state, exit_status=None):
    """
    Finalize the command state in memory/disk if the command isn't already in a
    terminal state. An 'exit_status' value of -1 indicates the command was
    stopped. An 'exit_status' value of -2 indicates that the command had a
    non-normal exit (e.g. was terminated by a signal), which we just classify
    as failed.

    Assumes self.__lock is held.
    """
    status_path = os.path.join(self.cmd_dir(cmd_state.cmd_id), "status.bin")
    cmd_status = CmdStatus()
    cmd_status.ParseFromString(open(status_path).read())
    if cmd_status.state != CmdStatus.kRunning:
      # Command may have already been stopped.
      return
    log.info("Finalizing status for command %s: exit_status %s",
             cmd_state.cmd_id, exit_status)
    CHECK(cmd_status.HasField("pid"), msg=cmd_state.cmd_id)
    CHECK(not cmd_status.HasField("exit_status"), msg=cmd_state.cmd_id)
    if exit_status is not None:
      if exit_status == -1:
        cmd_status.state = CmdStatus.kStopped
      elif exit_status == 0:
        cmd_status.state = CmdStatus.kSucceeded
      else:
        cmd_status.state = CmdStatus.kFailed
      cmd_status.exit_status = exit_status
    else:
      cmd_status.state = CmdStatus.kUnknown
    cmd_status.ClearField("pid")
    status_data = cmd_status.SerializeToString()
    OsUtil.write_and_rename(status_path, status_data)
    cmd_state.proc = None
    cmd_state.pid = None
