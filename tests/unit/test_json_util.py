#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
#
# pylint: disable=pointless-statement
import unittest

from curie.json_util import Enum, Field, RepeatedField, JsonData


class TestEnum(unittest.TestCase):

  def setUp(self):
    class A(Enum):
      a = 1
      b = 2
      c = 3

      d = 1

    class B(Enum):
      a = 1
      b = 2
      c = 3

      d = 1

    self.A = A
    self.B = B

  def test_equality(self):
    A = self.A
    B = self.B

    self.assertEqual(1, A.a)
    self.assertEqual(1, A.d)
    self.assertEqual("a", A.a)
    self.assertEqual("d", A.d)

    self.assertEqual(A.a, A.d)

    self.assertNotEqual(A.a, B.a)

  def test_ordering(self):
    A = self.A
    B = self.B

    with self.assertRaises(TypeError):
      A.a < B.b
    with self.assertRaises(TypeError):
      A.a <= B.b
    with self.assertRaises(TypeError):
      A.a > B.b
    with self.assertRaises(TypeError):
      A.a >= B.b

    self.assertLess(1, A.b)
    self.assertLess("a", A.b)

    self.assertLess(A(1), A.b)
    self.assertLess(A("a"), A.b)
    self.assertLess(A("d"), A.b)

  def test_casting(self):
    A = self.A
    B = self.B

    self.assertEqual(A(1), A.a)
    self.assertEqual(A(1), A.d)
    self.assertEqual(A("a"), A.d)
    self.assertEqual(A("d"), A.d)
    self.assertEqual(A(1), A("a"))
    self.assertEqual(A("a"), A("d"))

    self.assertEqual(int(A.a), int(B.a))
    self.assertEqual(int(A.a), int(B.d))
    self.assertEqual(str(A.a), str(B.a))
    self.assertNotEqual(str(A.a), str(B.d))
    self.assertNotEqual(str(A.a), str(A.d))


    with self.assertRaises(ValueError):
      A(5)
    with self.assertRaises(ValueError):
      A("A")

  def test_hashing(self):
    A = self.A
    B = self.B

    self.assertEqual(hash(A.a), hash(A.d))
    self.assertNotEqual(hash(A.a), hash(B.a))

  def test_iteration(self):
    A = self.A
    B = self.B

    self.assertEqual(len(list(A)), 4)
    self.assertEqual(len(set(A)), 3)

    self.assertEqual(map(int, A), map(int, B))
    self.assertEqual(map(str, A), map(str, B))

    for x in A:
      pass

  def test_contains(self):
    A = self.A

    for x in A:
      self.assertTrue(x in A)
      self.assertTrue(str(x) in A)
      self.assertFalse(str(x).upper() in A)
    self.assertFalse(max(map(int, A)) + 10 in A)

class TestJsonData(unittest.TestCase):

  def setUp(self):
    class E(Enum):
      a = 1
      b = 2
      c = 3
      d = 1

    class JsonObj(JsonData):
      int_field = Field(int)
      int_required_field = Field(int, required=True)
      int_field_with_default = Field(int, default=100)
      strict_int_field = Field(int, strict=True)

      repeated_int_field = RepeatedField(int)
      strict_repeated_int_field = RepeatedField(int, strict=True)
      required_repeated_int_field = RepeatedField(int, required=True)

      enum_field = Field(E)

      other_attribute = "TEST"

    class NestedJsonObj(JsonData):
      J_field = Field(JsonObj)


    self.E = E
    self.JsonObj = JsonObj
    self.NestedJsonObj = NestedJsonObj

  def test_basic_fields(self):
    E = self.E
    JsonObj = self.JsonObj
    NestedJsonObj = self.NestedJsonObj

    j = JsonObj(int_required_field=10, required_repeated_int_field=[20])
    j = JsonObj(int_required_field="10", required_repeated_int_field=["20"])
    self.assertEqual(j.int_required_field, 10)
    self.assertEqual(j.required_repeated_int_field, [20])
    self.assertEqual(j.int_field_with_default, 100)

    with self.assertRaises(TypeError):
      j.strict_int_field = "10"
    with self.assertRaises(TypeError):
      j.strict_repeated_int_field = 10
    with self.assertRaises(TypeError):
      j.strict_repeated_int_field = ["10"]

  def test_contains(self):
    JsonObj = self.JsonObj

    self.assertTrue("repeated_int_field" in JsonObj)
    self.assertFalse("other_attribute" in JsonObj)

  def test_iterate(self):
    JsonObj = self.JsonObj

    self.assertEqual(len(list(JsonObj)), 8)
    for x in JsonObj:
      pass

  def test_mutate_repeated_field(self):
    JsonObj = self.JsonObj

    j = JsonObj(required_repeated_int_field=[20], int_required_field=10)
    self.assertEqual(j.required_repeated_int_field, [20])

    j.required_repeated_int_field.extend([1, 1, "10"])

    with self.assertRaises(ValueError):
      j.required_repeated_int_field.append("TEST")
    with self.assertRaises(ValueError):
      j.required_repeated_int_field.extend([1, 1, "10", "TEST"])
    with self.assertRaises(TypeError):
      j.strict_repeated_int_field.append("10")
    with self.assertRaises(TypeError):
      j.strict_repeated_int_field.extend([1, 2, 3, 4, "TEST"])
    with self.assertRaises(TypeError):
      j.strict_repeated_int_field.extend([1, 2, 3, 4, "10"])
