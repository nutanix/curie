#
# Copyright (c) 2011 Nutanix Inc. All rights reserved.
#

import logging
import operator

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException

TRACE_LEVEL = logging.DEBUG - 5


# TODO(jklein): Might be preferable to subclass logging.Logger to add the
# trace method and then use logging.setLoggerClass to ensure we use our
# subclass.
def patch_trace():
  # Add a trace method by patching logging.Logger.
  def __trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE_LEVEL):
      self._log(TRACE_LEVEL, message, args, **kwargs)

  logging.addLevelName(TRACE_LEVEL, "TRACE")
  logging.Logger.trace = __trace


def initialize(log_file=None, debug_log_file=None, logtostderr=False,
               debug=False):
  """
  Initialize logging subsystem to log to the file 'logfile'.

  If gflags.FLAGS.logtostderr is True, a standard error stream handler is
  created.

  Args:
    log_directory (basestring): Optionally specify directory in which to create
      log files. If no directory is specified, no log files will be created.
  """
  patch_trace()
  logger = logging.getLogger("curie")

  logger.propagate = False

  # Update the log level.
  if debug or debug_log_file:
    min_log_level = logging.DEBUG
  else:
    min_log_level = logging.INFO
  logger.setLevel(min_log_level)

  # Exit early if logger is already initialized.
  if logger.handlers:
    logger.debug("Logger %r already initialized; No new handlers will be "
                 "added", logger)
    return

  formatter = logging.Formatter("%(asctime)s %(levelname)s "
                                "%(filename)s:%(lineno)d %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
  debug_formatter = logging.Formatter("%(asctime)s %(process)d %(thread)d "
                                      "%(levelname)s %(filename)s:%(lineno)d "
                                      "%(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S")

  if logtostderr:
    log_stream_handler = logging.StreamHandler()  # Defaults to stderr.
    if min_log_level <= logging.DEBUG:
      log_stream_handler.setFormatter(debug_formatter)
    else:
      log_stream_handler.setFormatter(formatter)
    logger.addHandler(log_stream_handler)
  if log_file:
    log_fh = logging.FileHandler(filename=log_file)
    if debug and not debug_log_file:
      log_fh.setFormatter(debug_formatter)
      log_fh.setLevel(logging.DEBUG)
    else:
      log_fh.setFormatter(formatter)
      log_fh.setLevel(logging.INFO)
    logger.addHandler(log_fh)
  if debug_log_file:
    log_fh_debug = logging.FileHandler(filename=debug_log_file)
    log_fh_debug.setFormatter(debug_formatter)
    log_fh_debug.setLevel(logging.DEBUG)
    logger.addHandler(log_fh_debug)


def _CHECK_BINARY_OP(x, y, op, symbol, msg="", **kwargs):
  if op(x, y):
    return

  output = ["%s %s %s failed," % (x, symbol, y)]
  if msg:
    output.append("%s," % msg)

  raise CurieException(CurieError.kInternalError, output)


def CHECK(expr, msg=""):
  _CHECK_BINARY_OP(
    expr, True, lambda x, y: bool(x) == y, "to bool is", msg=msg)


def CHECK_EQ(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.eq, "==", msg=msg)


def CHECK_NE(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.ne, "!=", msg=msg)


def CHECK_LT(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.lt, "<", msg=msg)


def CHECK_LE(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.le, "<=", msg=msg)


def CHECK_GT(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.gt, ">", msg=msg)


def CHECK_GE(expr1, expr2, msg=""):
  _CHECK_BINARY_OP(expr1, expr2, operator.ge, ">=", msg=msg)
