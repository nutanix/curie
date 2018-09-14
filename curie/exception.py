#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
import traceback

from curie.curie_error_pb2 import CurieError


# Exception class for all other uses in curie except for failing a running
# test (CurieTestException should be used in that case).
class CurieException(Exception):
  def __init__(self, error_code, error_msg):
    Exception.__init__(self)
    # Error code (see curie_interface.proto).
    self.error_code = error_code

    # Error message (see curie_interface.proto).
    self.error_msg = error_msg

  def __str__(self):
    return "%s (%s)" % (self.error_msg, CurieError.Type.Name(self.error_code))


class CurieTestException(RuntimeError):
  # TODO(ryan.hardin): Make `impact` and `corrective_action` mandatory?
  # Optional for now to allow this to be backward-compatible.
  def __init__(self, cause=None, impact=None, corrective_action=None, tb=None):
    super(CurieTestException, self).__init__(cause)
    self.cause = cause
    self.impact = impact
    self.corrective_action = corrective_action
    if tb is not None:
      self.traceback = tb
    else:
      self.traceback = traceback.format_exc().rstrip()

  def __repr__(self):
    return ("%s(cause=%r, impact=%r, corrective_action=%r, tb=%r)" %
            (self.__class__.__name__,
             self.cause, self.impact, self.corrective_action, self.traceback))

  def __str__(self):
    if self.traceback.lower() == "none":
      traceback_str = "Traceback: None"
    else:
      traceback_str = self.traceback
    # TODO(ryan.hardin): Remove backward-compatible formatter after `impact`
    # and `corrective_action` are mandatory.
    if self.cause and not (self.impact or self.corrective_action):
      return self.cause
    else:
      return ("Cause: %s\n\nImpact: %s\n\nCorrective Action: %s\n\n%s" %
              (self.cause, self.impact, self.corrective_action, traceback_str))

  def __eq__(self, other):
    return (self.cause == other.cause and
            self.impact == other.impact and
            self.corrective_action == other.corrective_action and
            self.traceback == other.traceback)


class ScenarioStoppedError(CurieTestException):
  pass


class ScenarioTimeoutError(CurieTestException):
  pass


class NoVMGroupDefinedError(CurieTestException):
  def __init__(self, vm_group_name, step):
    super(NoVMGroupDefinedError, self).__init__(
      cause="No VM Group named '%s'" % vm_group_name,
      impact="The scenario '%s' is not valid, and can not be run." %
             step.scenario.display_name,
      corrective_action=
      "If you are the author of the scenario, please check the syntax of the "
      "%s step; Ensure that the 'vm_group_name' parameter refers to a VM "
      "Group defined in the 'vms' section." % step.name)


class NoWorkloadDefinedError(CurieTestException):
  def __init__(self, workload_name, step):
    super(NoWorkloadDefinedError, self).__init__(
      cause="No Workload named '%s'" % workload_name,
      impact="The scenario '%s' is not valid, and can not be run." %
             step.scenario.display_name,
      corrective_action=
      "If you are the author of the scenario, please check the syntax of the "
      "%s step; Ensure that the 'workload_name' parameter refers to a "
      "Workload defined in the 'workloads' section." % step.name)


class NoVMsFoundError(CurieTestException):
  def __init__(self, failure_message, step):
    super(NoVMsFoundError, self).__init__(
      cause="%s: No VM(s) found" % failure_message,
      impact="There was an unexpected response from the cluster being tested, "
             "or the sequence of steps in the scenario '%s' is invalid." %
             step.scenario.display_name,
      corrective_action=
      "If you are the author of the scenario, please check that the VM Group "
      "is cloned from template using the vm_group.CloneFromTemplate step "
      "before the %s step is called. If the scenario is correct and this "
      "error occurs, VMs that were cloned previously are no longer appearing "
      "in the API response from the cluster. Please verify that the VMs have "
      "not been deleted by some other process." % step.name)
