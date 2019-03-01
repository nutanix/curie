#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#

import re

from curie.log import CHECK_EQ

# VM name prefix to use for all VMs created by curie.
CURIE_VM_NAME_PREFIX = "__curie"

# VM name prefixes for goldimage VMs and test VMs created by curie.
CURIE_GOLDIMAGE_VM_NAME_PREFIX = "%s_goldimage" % CURIE_VM_NAME_PREFIX
CURIE_GOLDIMAGE_VM_DISK_PREFIX = "%s_goldimage_disk" % CURIE_VM_NAME_PREFIX
CURIE_TEST_VM_NAME_PREFIX = "%s_test" % CURIE_VM_NAME_PREFIX

# Entity name prefix to use for all entities created by curie.
CURIE_ENTITY_NAME_PREFIX = "__curie"


class NameUtil(object):
  @staticmethod
  def goldimage_vmdisk_name(goldimage_name, disk_name):
    """
    Returns a unique name for a disk belonging to a goldimage VM.
    """
    return "%s_%s_%s" % (CURIE_GOLDIMAGE_VM_DISK_PREFIX,
                         goldimage_name, disk_name)

  @staticmethod
  def goldimage_vm_name(test, goldimage_name):
    """
    Returns a unique VM name for the goldimage VM for goldimage
    'goldimage_name' to be used by 'test'.

    The output is limited to 80 characters for compliance with vCenter.
    """
    return ("%s_%d_%s" % (CURIE_GOLDIMAGE_VM_NAME_PREFIX,
                          test.test_id(),
                          goldimage_name))[:61]

  @staticmethod
  def library_server_goldimage_path(cluster_name, goldimage_name):
    """
    Returns a unique library server share path of the goldimage location.
    """
    return "%s_%s\%s" % (CURIE_VM_NAME_PREFIX,
                         cluster_name, goldimage_name)

  @staticmethod
  def library_server_target_path(cluster_name):
    """
    Returns a unique library server share path for all curie files.
    """
    return "%s_%s" % (CURIE_VM_NAME_PREFIX, cluster_name)

  @staticmethod
  def test_vm_name(test, test_local_vm_name):
    """
    Returns a unique VM name for the test VM with test-local name
    'test_local_vm_name' to be used by 'test'.

    The output is limited to 80 characters for compliance with vCenter.
    """
    return ("%s_%d_%s" % (CURIE_TEST_VM_NAME_PREFIX,
                          test.test_id(),
                          test_local_vm_name))[:61]

  @staticmethod
  def entity_name(test, test_local_entity_name):
    """
    Returns a unique entity name for the entity with test-local name
    'test_local_entity_name' to be used by 'test'.
    """
    return "%s_%d_%s" % (CURIE_ENTITY_NAME_PREFIX,
                         test.test_id(),
                         test_local_entity_name)

  @staticmethod
  def filter_test_vm_names(vm_names, test_ids, include_goldimages=True):
    if include_goldimages:
      prefix = CURIE_VM_NAME_PREFIX
    else:
      prefix = CURIE_TEST_VM_NAME_PREFIX

    return NameUtil._filter_test_entity_names(vm_names, test_ids, prefix)

  @staticmethod
  def filter_test_entity_names(entity_names, test_ids):
    return NameUtil._filter_test_entity_names(
      entity_names, test_ids, CURIE_ENTITY_NAME_PREFIX)

  @staticmethod
  def filter_test_vms(vms, test_ids, include_goldimages=True):
    """
    Given the list of CurieVMs 'vms', return a tuple where the first element
    is the subset of that list corresponding to the curie VMs for the
    specified tests and the second element is a list of the corresponding test
    IDs. If no test IDs are specified (empty list), then return information for
    all curie VMs in 'vm_names'.
    """
    vm_names = [vm.vm_name() for vm in vms]
    vm_name_map = dict(zip(vm_names, vms))
    test_vm_names, test_vm_test_ids = NameUtil.filter_test_vm_names(
      vm_names, test_ids, include_goldimages=include_goldimages)
    test_vms = [vm_name_map[vm_name] for vm_name in test_vm_names]
    CHECK_EQ(len(test_vms), len(test_vm_test_ids))
    return test_vms, test_vm_test_ids

  @staticmethod
  def sanitize_filename(filename):
    """Convert a string to a safe filename.

    Any sequence of characters that are not a letter, a digit, a hyphen, or a
    period will be converted to an underscore.

    Args:
      filename (str): Filename to sanitize.

    Returns: Sanitized filename.
    """
    return re.sub("[^0-9a-zA-Z\.\-]+", "_", filename)

  @staticmethod
  def get_vm_name_prefix():
    return CURIE_VM_NAME_PREFIX

  @staticmethod
  def template_name_from_vm_name(template_name):
    return template_name.replace("_test", "_temp", 1)

  @staticmethod
  def is_goldimage_vm(vm_name):
    if vm_name.startswith(CURIE_GOLDIMAGE_VM_NAME_PREFIX):
      return True
    return False

  @staticmethod
  def is_hyperv_cvm_vm(vm_name):
    test_entity_name_pat = re.compile("^NTNX-[0-9]*-[A-Z]-CVM")
    if(test_entity_name_pat.search(vm_name)):
      return True
    return False

  @staticmethod
  def _filter_test_entity_names(entity_names, test_ids, prefix):
    """
    Given the list of entity names 'entity_names', return a tuple where the
    first element is the subset of that list corresponding to the curie
    entities for the specified tests and the second element is a list of the
    corresponding test IDs. If no test IDs are specified (empty list), then
    return information for all curie entities in 'entity_names'.
    """
    test_entity_names = []
    test_entity_test_ids = []
    test_id_set = set(test_ids)
    test_entity_name_pat = re.compile("^%s.*?_(\d+)_.*" % prefix)
    for entity_name in entity_names:
      match = test_entity_name_pat.search(entity_name)
      if not match:
        # Skip entities not created by curie.
        continue
      test_id = int(match.group(1))
      if len(test_ids) > 0:
        # Add this curie entity name if it matches one of the specified tests.
        if test_id in test_id_set:
          test_entity_names.append(entity_name)
          test_entity_test_ids.append(test_id)
      else:
        # We're matching all curie entities, so just add the curie entity
        # name.
        test_entity_names.append(entity_name)
        test_entity_test_ids.append(test_id)
    CHECK_EQ(len(test_entity_names), len(test_entity_test_ids))
    return (test_entity_names, test_entity_test_ids)
