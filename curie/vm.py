#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
# Thread-safety: this class is not thread-safe.
#

from curie import charon_agent_interface_pb2
from curie.agent_rpc_client import AgentRpcClient
from curie.charon_agent_interface_pb2 import CmdStatus
from curie.exception import CurieTestException
from curie.log import CHECK, CHECK_NE
from curie.util import CurieUtil


class VmDescriptor(object):
  # NB: VM Entity returns 'numVCpus' (note caps), while clone/create POST
  # operations take 'numVcpus' (not different caps).
  __PRISM_VM_CURIE_VM_KEY_MAP__ = {
    "vmName": "name",
    "memoryCapacityInBytes": "memory_mb",
    "numVCpus": "num_vcpus",
    "uuid": "vm_uuid",
    "hostUuid": "host_uuid",
    "containerUuids": "container_uuid",
    "nutanixVirtualDiskUuids": "vmdisk_uuid_list"
    }
  @classmethod
  def from_prism_entity_json(cls, vm_json):
    kwargs = {}
    for prism_key, curie_key in cls.__PRISM_VM_CURIE_VM_KEY_MAP__.items():
      kwargs[curie_key] = vm_json[prism_key]

    kwargs["memory_mb"] /= 2**20
    kwargs["container_uuid"] = kwargs["container_uuid"][0]

    return cls(**kwargs)

  def __init__(
      self, name=None, memory_mb=None, num_cores=None, num_vcpus=None,
      vm_uuid=None, host_uuid=None, container_uuid=None,
      vmdisk_uuid_list=None, attached_disks=None, annotation=None):
    self.name = name
    self.memory_mb = memory_mb
    self.num_cores = num_cores
    self.num_vcpus = num_vcpus
    self.vm_uuid = vm_uuid
    self.host_uuid = host_uuid
    self.container_uuid = container_uuid
    self.vmdisk_uuid_list = vmdisk_uuid_list if vmdisk_uuid_list else []
    self.annotation = annotation
    self.attached_disks = attached_disks if attached_disks else []

  def to_ahv_vm_create_spec(self):
    req = {
      "name": self.name,
      "memoryMb": self.memory_mb,
      "numCoresPerVcpu": self.num_cores,
      "numVcpus": self.num_vcpus,
      "hostId": self.host_uuid,
      "vmDisks": [],
      "vmNics": [],
      "vmCustomizationConfig": {},
    }
    for vm_disk_uuid in self.vmdisk_uuid_list:
      req["vmDisks"].append({
        "diskAddress": {"deviceBus": "scsi"},
        "vmDiskClone": {
          "containerUuid": self.container_uuid,
          "vmDiskUuid": vm_disk_uuid,
          }
        })

    for disk in self.attached_disks:
      size_bytes = int(disk.capacity) * int(disk.units.multiplier)
      req["vmDisks"].append({
        "diskAddress": {"deviceBus": "scsi"},
        "vmDiskCreate": {
          "containerUuid": self.container_uuid,
          "size": int(size_bytes)
          }
        })

    return req

  def to_ahv_vm_clone_spec(self, name_list, ctr_uuid=None):
    ctr_uuid = self.container_uuid if ctr_uuid is None else ctr_uuid
    common_spec = {"uuid": None,
                   "memoryMb": self.memory_mb,
                   "numCoresPerVcpu": self.num_cores,
                   "numVcpus": self.num_vcpus}
    req = {"specList": [],
           "vmCustomizationConfig": {},
           "containerUuid": ctr_uuid}

    for name in name_list:
      curr_spec = {"name": name}
      curr_spec.update(common_spec)
      req["specList"].append(curr_spec)

    return req

  def to_ahv_vmdisk_create_spec(self, size_bytes):
    return {
        "diskAddress": {"deviceBus": "scsi"},
        "isEmpty": True,
        "vmDiskCreate": {
          "containerUuid": self.container_uuid,
          "size": size_bytes
          }
        }

  def to_ahv_vm_nic_create_spec(self, network_uuid):
    req = {
      "specList": [{
        "networkUuid": network_uuid,
        "macAddress": None,
        "model": None,
        "requestIp": False,
        "requestedIpAddress": None
        }],
      #"uuid": For idempotence
      }

    return req


class VmParams(object):
  # Use '__slots__' to prevent typos from silently creating a new field rather
  # than raising an exception.
  __slots__ = ["cluster", "vm_id", "vm_name", "vm_ip", "node_id", "is_cvm"]

  def __init__(self, cluster, vm_id):
    # Cluster this node is part of.
    self.cluster = cluster

    # VM ID. This vm ID need not be unique across all vms managed by a given
    # management server. However, it must be unique within the set of VMs for
    # the cluster.
    self.vm_id = vm_id

    # If set, the name for the VM. On some types of clusters, the VM name may
    # not be unique.
    self.vm_name = None

    # If set, the IP address for the VM.
    self.vm_ip = None

    # If set, the node ID of the node that runs the VM.
    self.node_id = None

    # If True, VM is a Nutanix CVM.
    self.is_cvm = False


class Vm(object):
  def __init__(self, vm_params):
    CHECK(vm_params.cluster)
    CHECK(vm_params.vm_id)
    # See the comments in VmParams for these fields.
    self._cluster = vm_params.cluster
    self._vm_id = vm_params.vm_id
    self._vm_name = vm_params.vm_name
    self._vm_ip = vm_params.vm_ip
    self._node_id = vm_params.node_id
    self._is_cvm = vm_params.is_cvm

  def __eq__(self, other):
    if not isinstance(other, Vm):
      return False

    return (self.cluster() == other.cluster() and
            self.node_id() == other.node_id() and
            self.vm_id() == other.vm_id())

  def __hash__(self):
    return hash(self.cluster()) ^ hash(self.node_id()) ^ hash(self.vm_id())

  def __str__(self):
    return self.vm_name()

  def cluster(self):
    return self._cluster

  def vm_id(self):
    return self._vm_id

  def vm_name(self):
    return self._vm_name

  def vm_ip(self):
    return self._vm_ip

  def node_id(self):
    return self._node_id

  def is_cvm(self):
    return self._is_cvm

  def is_powered_on(self):
    """
    Returns True if the VM is powered on, else False.

    NB: This method only checks the VM's power state. It does not guarantee
    that the VM is accessible (cf. 'Vm.is_accessible')

    Returns:
      (bool) True if VM is powered on, else False.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def is_accessible(self):
    """
    Returns True if the virtual machine is accessible for guest OS operations
    for the test. This is a stronger test than if the virtual machine is
    powered on or not, since it's possible the virtual machine is powered on
    but is still booting and hasn't started up key services.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def transfer_to(self,
                  local_path,
                  remote_path,
                  timeout_secs,
                  recursive=False):
    "Transfer 'local_path' to 'remote_path' on VM. Returns True on success."
    raise NotImplementedError("Subclasses need to implement this")

  def transfer_from(self,
                    remote_path,
                    local_path,
                    timeout_secs,
                    recursive=False):
    "Transfer 'remote_path' on VM to 'local_path'. Returns True on success."
    raise NotImplementedError("Subclasses need to implement this")

  def execute_async(self, cmd_id, cmd, user="nutanix"):
    """
    Asynchronously execute 'cmd' as the given user. 'cmd_id' must be unique
    amongst all commands executed on this VM.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def execute_sync(self,
                   cmd_id,
                   cmd,
                   timeout_secs,
                   user="nutanix",
                   include_output=False):
    """
    Synchronously execute 'cmd' as the given user with the given timeout.
    'cmd_id' must be unique amongst all commands executed on this VM.
    'include_output' indicates whether stdout and stderr should be included in
    the return value (both are None if this is False). Returns rv, stdout,
    stderr for 'cmd'.

    Note: this method should only be used for commands that don't generate
    large amounts of stdout/stderr.
    """
    raise NotImplementedError("Subclasses need to implement this")

  def check_cmd(self, cmd_id, desired_state=CmdStatus.kSucceeded):
    """Check if a command has reached a desired state.

    Args:
      cmd_id (str): ID of the command to check.
      desired_state (CmdStatus): Desired command state.

    Returns:
      CmdStatus protobuf on success, and None if the command is in progress.

    Raises:
      CurieTestException if the command has reached a terminal state that is
        different from the desired state.
    """
    rpc_client = AgentRpcClient(self._vm_ip)
    arg = charon_agent_interface_pb2.CmdStatusArg()
    arg.cmd_id = cmd_id
    ret, err = rpc_client.CmdStatus(arg)
    if ret.cmd_status.state == desired_state:
      # Command has reached the desired state. The desired state could be a
      # non-terminal state or a terminal state.
      cmd_status = ret.cmd_status
      CHECK(cmd_status)
      return cmd_status
    elif ret.cmd_status.state != CmdStatus.kRunning:
      # Command has reached a terminal state. If we're here, this implies
      # that the command's terminal state is not the desired state because
      # if it was, then we would have already returned above.
      CHECK_NE(ret.cmd_status.state, desired_state)
      error_msg = ("Command %s terminal state %s != desired state %s" %
                   (cmd_id,
                    CmdStatus.Type.Name(ret.cmd_status.state),
                    CmdStatus.Type.Name(desired_state)))
      raise CurieTestException(error_msg)
    return None

  def wait_for_cmd(self,
                   cmd_id,
                   timeout_secs,
                   poll_secs=30,
                   desired_state=CmdStatus.kSucceeded):
    """
    Wait up to 'timeout_secs' for the command with ID 'cmd_id' to reach the
    state 'desired_state'. Returns a CmdStatus protobuf for the command on
    success, else raises CurieTestException if either a timeout occurs or the
    command has reached a terminal state that is different from the desired
    state.
    """
    cmd_status = CurieUtil.wait_for(lambda: self.check_cmd(cmd_id,
                                                            desired_state),
                                     "command %s" % cmd_id,
                                     timeout_secs,
                                     poll_secs=poll_secs)
    if cmd_status is not None:
      return cmd_status
    else:
      raise CurieTestException("Timeout waiting for command '%s'" % cmd_id)
