#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

from curie.acropolis_vm import AcropolisVm
from curie.scp_client import ScpClient
from curie.unix_vm_mixin import CurieUnixVmMixin


class AcropolisUnixVm(CurieUnixVmMixin, AcropolisVm):
  def __init__(self, vm_params):
    super(AcropolisUnixVm, self).__init__(vm_params)
    # Client for transferring files to/from the VM.
    self._scp_client = ScpClient(self._vm_ip, "nutanix")
