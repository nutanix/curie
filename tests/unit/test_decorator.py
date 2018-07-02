#
# Copyright (c) 2017 Nutanix Inc. All rights reserved.
#
import unittest

import mock
from flask import Flask, Response
from google.protobuf.message import EncodeError

from curie.curie_error_pb2 import CurieError
from curie.decorator import curie_api_handler, validate_parameter
from curie.exception import CurieException
from curie.util import CurieUtil


class TestDecorator(unittest.TestCase):
  def setUp(self):
    pass

  def test_curie_api_handler(self):
    mock_func = mock.Mock()
    mock_func.__name__ = "mock_func"

    mock_ret = mock_func.return_value
    mock_ret.SerializeToString.return_value = "abc"

    wrapped = curie_api_handler(mock_func)
    ret = wrapped(1, "two", 3.0)

    mock_func.assert_called_once_with(1, "two", 3.0)
    self.assertEqual("abc", ret[0])
    self.assertEqual(200, ret[1])

  def test_curie_api_handler_CurieException(self):
    mock_func = mock.Mock()
    mock_func.__name__ = "mock_func"

    mock_func.side_effect = CurieException(CurieError.kInternalError,
                                            "Something terrible")

    wrapped = curie_api_handler(mock_func)
    ret = wrapped(1, "two", 3.0)

    mock_func.assert_called_once_with(1, "two", 3.0)
    self.assertEqual("\x08\x08\x12\x12Something terrible", ret[0])
    self.assertEqual(500, ret[1])

  def test_curie_api_handler_protobuf_EncodeError(self):
    mock_func = mock.Mock()
    mock_func.__name__ = "mock_func"

    mock_func.side_effect = EncodeError("Something terrible")

    wrapped = curie_api_handler(mock_func)
    ret = wrapped(1, "two", 3.0)

    mock_func.assert_called_once_with(1, "two", 3.0)
    self.assertEqual("\x08\x03\x12\x12Something terrible", ret[0])
    self.assertEqual(400, ret[1])

  def test_curie_api_handler_ValueError(self):
    app = Flask(__name__)
    mock_func = mock.Mock()
    mock_func.__name__ = "mock_func"

    mock_func.side_effect = ValueError("This is an unhandled exception")

    wrapped = curie_api_handler(mock_func)
    with app.app_context():
      ret = wrapped(1, "two", 3.0)

    mock_func.assert_called_once_with(1, "two", 3.0)
    self.assertIsInstance(ret, Response)
    self.assertEqual("500 INTERNAL SERVER ERROR", ret.status)
    self.assertEqual(500, ret.status_code)
    self.assertIn("X-Curie-Abort", ret.headers)


class TestValidateParameter(unittest.TestCase):
  def setUp(self):
    pass

  def test_validate_parameter_single_type(self):

    @validate_parameter("a", valid_types=str)
    def test_func(a=None, b=None):
      pass

    with self.assertRaises(CurieException) as ar:
      test_func(a=3, b="b")

    self.assertEqual(str(ar.exception), "Invalid 'a' type 'int': Expected "
                                        "'str' (kInvalidParameter)")

    test_func(a="apple", b="b")  # This should validate correctly.

  def test_validate_parameter_multiple_types(self):

    @validate_parameter("a", valid_types=(str, unicode))
    def test_func(a=None, b=None):
      pass

    with self.assertRaises(CurieException) as ar:
      test_func(a=3, b="b")

    self.assertEqual(str(ar.exception),
                     "Invalid 'a' type 'int': Expected one of 'str, unicode' "
                     "(kInvalidParameter)")

    test_func(a=b"apple", b="b")
    test_func(a=u"apple", b="b")

  def test_validate_parameter_by_single_value(self):

    @validate_parameter("a", valid_values="z")
    def test_func(a=None, b=None):
      pass

    with self.assertRaises(CurieException) as ar:
      test_func(a="not_the_right_value", b="b")

    self.assertEqual(str(ar.exception),
                     "Invalid 'a' value 'not_the_right_value': Expected 'z' "
                     "(kInvalidParameter)")

    test_func(a="z", b="b")

  def test_validate_parameter_by_multiple_values(self):

    @validate_parameter("a", valid_values=["z", "Z"])
    def test_func(a=None, b=None):
      pass

    with self.assertRaises(CurieException) as ar:
      test_func(a="not_the_right_value", b="b")

    self.assertEqual(str(ar.exception),
                     "Invalid 'a' value 'not_the_right_value': Expected one "
                     "of 'z, Z' (kInvalidParameter)")

    test_func(a="z", b="b")
    test_func(a="Z", b="b")

  def test_validate_parameter_by_function_that_returns_bool(self):

    @validate_parameter("host_ip", basestring,
                        valid_func=lambda *args, **kwargs:
                        CurieUtil.is_ipv4_address(kwargs["host_ip"]))
    def test_func(host_ip=None):
      pass

    with self.assertRaises(CurieException) as ar:
      test_func(host_ip="not_an_ipv4_address")

    self.assertEqual(str(ar.exception),
                     "Invalid 'host_ip' value 'not_an_ipv4_address': "
                     "Functional validation failed (kInvalidParameter)")

    test_func(host_ip="12.34.56.78")
