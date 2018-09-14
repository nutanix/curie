#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

from curie.exception import CurieTestException, CurieTestException


class TestException(unittest.TestCase):
  def setUp(self):
    pass

  def test_CurieValidationError_no_traceback(self):
    exc = CurieTestException("Something exploded.",
                               "Critical parts of the system are now broken.",
                               "Shut. Down. Everything.")

    self.assertEqual("Something exploded.", exc.cause)
    self.assertEqual("Critical parts of the system are now broken.",
                     exc.impact)
    self.assertEqual("Shut. Down. Everything.", exc.corrective_action)
    self.assertEqual("None", exc.traceback)
    self.assertEqual("Cause: Something exploded.\n\n"
                     "Impact: Critical parts of the system are now broken.\n\n"
                     "Corrective Action: Shut. Down. Everything.\n\n"
                     "Traceback: None", str(exc))
    self.assertEqual("CurieTestException(cause='Something exploded.', "
                     "impact='Critical parts of the system are now broken.', "
                     "corrective_action='Shut. Down. Everything.', "
                     "tb='None')", repr(exc))
    reconstructed = eval(repr(exc))
    self.assertEqual(exc, reconstructed)

  def test_CurieValidationError_with_traceback(self):
    with self.assertRaises(CurieTestException) as ar:
      try:
        raise ValueError("This is a dummy error!")
      except:
        raise CurieTestException("Cause.", "Impact.", "Corrective action.")

    self.assertTrue(ar.exception.traceback.startswith(
      "Traceback (most recent call last):\n"))
    self.assertIn("test_exception.py", ar.exception.traceback)
    self.assertIn("raise ValueError(\"This is a dummy error!\")",
                  ar.exception.traceback)
    self.assertIn("Traceback (most recent call last):\n",
                  str(ar.exception))
    self.assertIn("test_exception.py", str(ar.exception))
    self.assertIn("raise ValueError(\"This is a dummy error!\")",
                  str(ar.exception))
    self.assertIn("tb=\'Traceback (most recent call last):\\n",
                  repr(ar.exception))
    reconstructed = eval(repr(ar.exception))
    self.assertEqual(ar.exception, reconstructed)
