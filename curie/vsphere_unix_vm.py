#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

from curie.scp_client import ScpClient
from curie.unix_vm_mixin import CurieUnixVmMixin
from curie.vsphere_vm import VsphereVm


class VsphereUnixVm(CurieUnixVmMixin, VsphereVm):
  def __init__(self, vm_params):
    super(VsphereUnixVm, self).__init__(vm_params)
    # Client for transferring files to/from the VM.
    self._scp_client = ScpClient(self._vm_ip, "nutanix")
