#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

import logging

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException
from curie.vm import Vm

log = logging.getLogger(__name__)


# NB: AcropolisVm is intentionally used for non-Curie VMs for which many
# of the Vm operations do not apply.
# pylint: disable=abstract-method
class AcropolisVm(Vm):
  def __init__(self, vm_params):
    super(AcropolisVm, self).__init__(vm_params)

  def is_powered_on(self):
    """See 'CurieVM.is_powered_on' for documentation."""
    power_state = self._cluster.get_power_state_for_vms([self])[self._vm_id]
    if power_state is None:
      raise CurieException(
        CurieError.kInternalError,
        "Unable to check power state for VM '%s'. VM not found in Prism" %
        self._vm_name)

    log.debug("Prism reports power state '%s' for VM '%s'",
              power_state, self._vm_name)
    return power_state == "on"
