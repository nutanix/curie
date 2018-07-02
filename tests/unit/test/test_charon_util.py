#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
import unittest

from curie.util import CurieUtil


class TestCurieUtil(unittest.TestCase):
  def setUp(self):
    self.sequence = ["item_%d" % x for x in range(6)]

  def test_slice_with_string_all(self):
    for text in ["all", ":"]:
      sliced = CurieUtil.slice_with_string(self.sequence, text)
      self.assertEqual(sliced.values(), self.sequence)
      self.assertEqual(sliced.keys(), range(6))

  def test_slice_with_string_single(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "2")
    self.assertEqual(sliced.values(), ["item_2"])
    self.assertEqual(sliced.keys(), [2])

  def test_slice_with_string_two_indices(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "2, 4")
    self.assertEqual(sliced.values(), ["item_2", "item_4"])
    self.assertEqual(sliced.keys(), [2, 4])

  def test_slice_with_string_two_indices_reverse(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "4, 2")
    self.assertEqual(sliced.values(), ["item_4", "item_2"])
    self.assertEqual(sliced.keys(), [4, 2])

  def test_slice_with_string_range(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "1:3")
    self.assertEqual(sliced.values(), ["item_1", "item_2"])
    self.assertEqual(sliced.keys(), [1, 2])

  def test_slice_with_string_two_ranges(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "1:3, 4:")
    self.assertEqual(sliced.values(), ["item_1", "item_2", "item_4", "item_5"])
    self.assertEqual(sliced.keys(), [1, 2, 4, 5])

  def test_slice_with_string_mixed_range(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "1:3, 4")
    self.assertEqual(sliced.values(), ["item_1", "item_2", "item_4"])
    self.assertEqual(sliced.keys(), [1, 2, 4])

  def test_slice_with_string_mixed_range_reverse(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "4, 1:3")
    self.assertEqual(sliced.values(), ["item_4", "item_1", "item_2"])
    self.assertEqual(sliced.keys(), [4, 1, 2])

  def test_slice_with_string_three_indices_mixed_order(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "4, 1, 3")
    self.assertEqual(sliced.values(), ["item_4", "item_1", "item_3"])
    self.assertEqual(sliced.keys(), [4, 1, 3])

  def test_slice_with_string_negative_indices_converted_to_positive(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "-1, -2")
    self.assertEqual(sliced.values(), ["item_5", "item_4"])
    self.assertEqual(sliced.keys(), [5, 4])

  def test_slice_with_string_negative_range_converted_to_positive(self):
    sliced = CurieUtil.slice_with_string(self.sequence, ":-3")
    self.assertEqual(sliced.values(), ["item_0", "item_1", "item_2"])
    self.assertEqual(sliced.keys(), [0, 1, 2])

  def test_slice_with_string_single_index_min(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "-6")
    self.assertEqual(sliced.values(), ["item_0"])
    self.assertEqual(sliced.keys(), [0])

  def test_slice_with_string_single_index_max(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "5")
    self.assertEqual(sliced.values(), ["item_5"])
    self.assertEqual(sliced.keys(), [5])

  def test_slice_with_string_single_range_min(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "-6:")
    self.assertEqual(sliced.values(), self.sequence)
    self.assertEqual(sliced.keys(), range(6))

  def test_slice_with_string_single_range_max(self):
    sliced = CurieUtil.slice_with_string(self.sequence, ":6")
    self.assertEqual(sliced.values(), self.sequence)
    self.assertEqual(sliced.keys(), range(6))

  def test_slice_with_string_single_index_out_of_bounds_min(self):
    with self.assertRaises(IndexError):
      CurieUtil.slice_with_string(self.sequence, "0, -7")

  def test_slice_with_string_single_index_out_of_bounds_max(self):
    with self.assertRaises(IndexError):
      CurieUtil.slice_with_string(self.sequence, "0, 6")

  def test_slice_with_n_end(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "1:(n/2)+1")
    self.assertEqual(sliced.values(), ["item_1", "item_2", "item_3"])
    self.assertEqual(sliced.keys(), [1, 2, 3])

  def test_slice_with_n_start(self):
    sliced = CurieUtil.slice_with_string(self.sequence, "(n/2)-1:")
    self.assertEqual(sliced.values(), ["item_2", "item_3", "item_4", "item_5"])
    self.assertEqual(sliced.keys(), [2, 3, 4, 5])

  def test_slice_with_string_empty_slice(self):
    for ii in range(len(self.sequence)):
      sliced = CurieUtil.slice_with_string(self.sequence, "%d:%d" % (ii, ii))
      self.assertEqual(sliced.values(), [])
      self.assertEqual(sliced.keys(), [])

    # Consistency with python built-in slice behavior allowing empty
    # out-of-bounds slices.
    out_of_bounds = len(self.sequence) + 10
    sliced = CurieUtil.slice_with_string(self.sequence, "%d:%d" %
                                          (out_of_bounds, out_of_bounds))
    self.assertEqual(sliced.values(), [])
    self.assertEqual(sliced.keys(), [])

  def test_slice_with_invalid_input(self):
    with self.assertRaises(ValueError):
      CurieUtil.slice_with_string(self.sequence, "(m/2)-1:")
    with self.assertRaises(ValueError):
      CurieUtil.slice_with_string(
          self.sequence, "import os; os.execv(\"/bin/ls\", [\".\"])")
