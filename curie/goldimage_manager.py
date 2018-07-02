#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import os
import subprocess

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException


class GoldImageManager(object):
  """
  Manage base OS disk images for use by Curie VMs.

  The GoldImageManager relies on a directory that contains disk images of
  various image formats and may target more than one architecture. The manager
  expects that the filenames of the images follow this convention:
     <image_name>_<architecture>.<format>
     e.g. debian9-x86_64.qcow2

  The manager can also issue qemu-img convert commands to convert disk images
  to alternative formats.
  """

  ARCH_X86_64 = "x86_64"
  ARCH_PPC64LE = "ppc64le"
  FORMAT_RAW = "raw"
  FORMAT_QCOW2 = "qcow2"
  FORMAT_VMDK = "vmdk"
  FORMAT_VHDX = "vhdx"
  FORMAT_VHDX_ZIP = "vhdx.zip"
  VALID_ARCH = [ARCH_X86_64, ARCH_PPC64LE]
  VALID_FORMAT = [FORMAT_RAW, FORMAT_QCOW2, FORMAT_VMDK, FORMAT_VHDX,
                  FORMAT_VHDX_ZIP]

  def __init__(self, images_root):
    """
    Args:
      images_root (str): Full path to a directory that contains the images.
    """
    self.images_root = images_root

  @staticmethod
  def get_goldimage_filename(image_name, format_str=FORMAT_VMDK,
                             arch=ARCH_X86_64):
    """
    Provides the expected filename of a goldimage based on
    Args:
      image_name (str): The name of the image to get.
      format_str (str): A format to get the image in. Valid options include
        raw, qcow2, vmdk or vhdx.
      arch (str): The target architecture of the image. Valid options include
        x86_64 or ppc64le.
    Returns:
      str: Filename of goldimage
    """
    filename = "%s-%s.%s" % (image_name, arch, format_str)
    return filename

  def get_goldimage_path(self, image_name, format_str=FORMAT_VMDK,
                         arch=ARCH_X86_64, auto_convert=True):
    """
    Find an image file based on the name, format, and architecture.

    If the image doesn't exist in the current format, if auto_convert is set
    and a QCOW2 formatted version of the image exists, attempt to convert to
    the desired format.

    Args:
      image_name (str): The name of the image to get.
      format_str (str): A format to get the image in. Valid options include
        raw, qcow2, vmdk or vhdx.
      arch (str): The target architecture of the image. Valid options include
        x86_64 or ppc64le.
      auto_convert (bool): Whether or not an image should be automatically
        converted from an existing format to the desired format.

    Returns:
      str: Full path to image.

    Raises:
      CurieException:
        If the image doesn't exist or could not be created.
    """

    filename = self.get_goldimage_filename(image_name, format_str, arch)
    qcow2_filename = self.get_goldimage_filename(image_name, self.FORMAT_QCOW2,
                                                 arch)

    qcow2_filepath = os.path.join(self.images_root, qcow2_filename)
    full_path = os.path.join(self.images_root, filename)
    if os.path.exists(full_path):
      return full_path
    elif os.path.exists(qcow2_filepath):
      if auto_convert:
        dest_image_path = qcow2_filepath.split(".")[0] + "." + format_str
        rv = self.convert_image_format(qcow2_filepath, dest_image_path,
                                       format_str)
        if rv:
          raise CurieException(
            CurieError.kInternalError,
            "Error attempting to convert image %s from QCOW2 to %s" %
            (qcow2_filepath, format_str))
        return full_path
      else:
        raise CurieException(
          CurieError.kInternalError,
          "Goldimage %s does not exist in %s. The QCOW2 image does exist, "
          "however auto-conversion was not attempted." %
          (filename, self.images_root))
    else:
      raise CurieException(
        CurieError.kInternalError,
        "Goldimage %s does not exist. The QCOW2 image also does not "
        "exist in %s." % (filename, self.images_root))

  @staticmethod
  def convert_image_format(source_image_path, dest_image_path, dest_format):
    """
    Converts a disk image from source_image_path to the desired format.

    Args:
      source_image_path (str): Full path to source image.
      dest_image_path (str): Full path to destination image.
      dest_format (str): The desired destination format.

    Returns:
      int: Return code from 'qemu-img convert' call.
    """
    convert_cmd = "qemu-img convert -O %s %s %s" % (
      dest_format, source_image_path, dest_image_path)
    return subprocess.call(convert_cmd)
