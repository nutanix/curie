#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

from curie.vm import Vm
from curie.vmm_client import VmmClient


class HyperVVm(Vm):

  def __init__(self, vm_params, json__vm):
    self._json_vm = json__vm
    super(HyperVVm, self).__init__(vm_params)

  def is_powered_on(self):
    return VmmClient.is_powered_on(self._json_vm)
