#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

from curie.hyperv_vm import HyperVVm
from curie.scp_client import ScpClient

from curie.unix_vm_mixin import CurieUnixVmMixin

class HyperVUnixVM(CurieUnixVmMixin,HyperVVm):

  def __init__(self, vm_params, json_vm):
    super(HyperVUnixVM, self).__init__(vm_params, json_vm)
    # Client for transferring files to/from the VM.
    self._scp_client = ScpClient(self._vm_ip, "nutanix")
