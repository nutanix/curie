#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#

import logging
import mock
from mock import call
import unittest

from curie.exception import CurieException
from curie.goldimage_manager import GoldImageManager

log = logging.getLogger(__name__)


class TestGoldImageManager(unittest.TestCase):
  def setUp(self):
    self.base_path = "/some/path/"
    self.manager = GoldImageManager(self.base_path)

  @mock.patch("curie.goldimage_manager.os.path.exists")
  def test_get_path_valid(self, path_exists):
    # Tests that the desired goldimage already exists and is returned.
    path_exists.return_value = True
    rv = self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
                                         format_str="raw")
    self.assertEqual(rv, "/some/path/ubuntu1604-x86_64.raw")

  @mock.patch("curie.goldimage_manager.os.path.exists")
  def test_get_path_valid_complex_ext(self, path_exists):
    # Tests that the desired goldimage already exists and is returned.
    path_exists.return_value = True
    rv = self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
      format_str=GoldImageManager.FORMAT_VHDX_ZIP)
    self.assertEqual(rv, "/some/path/ubuntu1604-x86_64.vhdx.zip")

  @mock.patch("curie.goldimage_manager.os.path.exists")
  @mock.patch("curie.goldimage_manager.GoldImageManager.convert_image_format")
  def test_get_path_convert_valid(self, patch_convert, path_exists):
    # Test that the desired image doesn't exist, but the raw does, so a
    # conversion is attempted.
    patch_convert.return_value = 0
    path_exists.side_effect = [False, True]
    rv = self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
                                         format_str="vmdk")
    path_exists.assert_has_calls([call("/some/path/ubuntu1604-x86_64.vmdk"),
                                  call("/some/path/ubuntu1604-x86_64.raw")])
    self.assertEqual(rv, "/some/path/ubuntu1604-x86_64.vmdk")

  @mock.patch("curie.goldimage_manager.os.path.exists")
  @mock.patch("curie.goldimage_manager.GoldImageManager.convert_image_format")
  def test_get_path_convert_valid(self, patch_convert, path_exists):
    # Test that the desired image doesn't exist, but the raw does, so a
    # conversion is attempted but this conversion fails.
    patch_convert.return_value = 1

    path_exists.side_effect = [False, True]
    with self.assertRaises(CurieException):
      self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
                                      format_str="vmdk")

  @mock.patch("curie.goldimage_manager.os.path.exists")
  @mock.patch("curie.goldimage_manager.GoldImageManager.convert_image_format")
  def test_get_path_convert_invalid(self, patch_convert, path_exists):
    # Test that we attempt to convert but that the raw goldimage doesn't exist.
    patch_convert.return_value = 0
    path_exists.side_effect = [False, False]
    with self.assertRaises(CurieException):
      self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
                                      format_str="vmdk")

  @mock.patch("curie.goldimage_manager.os.path.exists")
  @mock.patch("curie.goldimage_manager.GoldImageManager.convert_image_format")
  def test_gat_path_no_convert_invalid(self, patch_convert, path_exists):
    # Test that we don't have the goldimage we want and that conversion isn't
    # attempted.
    patch_convert.return_value = 0
    path_exists.side_effect = [False, False]
    with self.assertRaises(CurieException):
      self.manager.get_goldimage_path("ubuntu1604", arch="x86_64",
                                      format_str="vmdk", auto_convert=False)
