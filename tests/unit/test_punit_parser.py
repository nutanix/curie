#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# pylint: disable=pointless-statement
import unittest

from curie.punit_parser import PUnit


class TestCuriePUnitParser(unittest.TestCase):
  UNIT_NAMES = ["byte", "decibel", "decibel ( simple-name_ )"]

  def setUp(self):
    pass

  def test_base_name(self):
    for text in self.UNIT_NAMES:
      self.assertEqual(str(PUnit.from_string(text)), "%s*1" % text)

  def test_multiplied_unit_only(self):
    for text in self.UNIT_NAMES:
      self.assertEqual(str(PUnit.from_string("* %s" % text)), "%s*1" % text)

  def test_divided_unit_only(self):
    for text in self.UNIT_NAMES:
      self.assertEqual(str(PUnit.from_string("/ %s" % text)), "%s^-1*1" % text)

  def test_base_and_multiplied_unit(self):
    for text in self.UNIT_NAMES:
      for text2 in self.UNIT_NAMES:
        result = str(PUnit.from_string("%s * %s" % (text, text2)))
        if text == text2:
          self.assertEqual(result, "%s^2*1" % text)
        else:
          self.assertEqual(result, "%s*1" % "*".join(sorted([text, text2])))

  def test_base_and_divided_unit(self):
    for text in self.UNIT_NAMES:
      for text2 in self.UNIT_NAMES:
        result = str(PUnit.from_string("%s / %s" % (text, text2)))
        if text == text2:
          self.assertEqual(result, "1")
        else:
          self.assertEqual(result, "%s*%s^-1*1" % (text, text2))

  def test_multiplied_and_divided_unit(self):
    for text in self.UNIT_NAMES:
      for text2 in self.UNIT_NAMES:
        result = str(PUnit.from_string("* %s / %s" % (text, text2)))
        if text == text2:
          self.assertEqual(result, "1")
        else:
          self.assertEqual(result, "%s*%s^-1*1" % (text, text2))

  def test_modifier1(self):
    self.assertEqual(str(PUnit.from_string("* 10")), "10")
    self.assertEqual(str(PUnit.from_string("/ 10")), "0.1")

  def test_modifier2(self):
    self.assertEqual(str(PUnit.from_string("* 10 ^ 2")), "100")
    self.assertEqual(str(PUnit.from_string("/ 10 ^ -2")), "100")
    self.assertEqual(str(PUnit.from_string("/ 10 ^ 2")), "0.01")
    self.assertEqual(str(PUnit.from_string("* 10 ^ -2")), "0.01")

  def test_modifier1_and_modifier2(self):
    self.assertEqual(str(PUnit.from_string("* 2 * 10 ^ 2")), "200")
    self.assertEqual(str(PUnit.from_string("* 2 / 10 ^ -2")), "200")
    self.assertEqual(str(PUnit.from_string("* 2 / 10 ^ 2")), "0.02")
    self.assertEqual(str(PUnit.from_string("* 2 * 10 ^ -2")), "0.02")

    self.assertEqual(str(PUnit.from_string("/ 2 * 10 ^ 2")), "50.0")
    self.assertEqual(str(PUnit.from_string("/ 2 / 10 ^ -2")), "50.0")
    self.assertEqual(str(PUnit.from_string("/ 2 / 10 ^ 2")), "0.005")
    self.assertEqual(str(PUnit.from_string("/ 2 * 10 ^ -2")), "0.005")
