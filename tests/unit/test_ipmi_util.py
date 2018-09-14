#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
import unittest

import mock

from curie.ipmi_util import IpmiUtil


class TestIpmiUtil(unittest.TestCase):
  def setUp(self):
    pass

  @mock.patch("curie.ipmi_util.CurieUtil.timed_command")
  def test_get_chassis_status(self, m_timed_command):
    stdout = ("System Power         : off\n"
              "Power Overload       : false\n"
              "Power Interlock      : inactive\n"
              "Main Power Fault     : false\n"
              "Power Control Fault  : false\n"
              "Power Restore Policy : previous\n"
              "Last Power Event     : \n"
              "Chassis Intrusion    : inactive\n"
              "Front-Panel Lockout  : inactive\n"
              "Drive Fault          : false\n"
              "Cooling/Fan Fault    : false\n")
    m_timed_command.return_value = (0, stdout, "")
    ipmi_util = IpmiUtil("fake_ip", "fake_username", "fake_password")
    chassis_status = ipmi_util.get_chassis_status()
    m_timed_command.assert_called_once_with(
      "/usr/bin/ipmitool -I lanplus -H fake_ip -P fake_password "
      "-U fake_username chassis status",
      formatted_cmd="/usr/bin/ipmitool -I lanplus -H fake_ip -P <REDACTED> "
                    "-U <REDACTED> chassis status", timeout_secs=60)
    self.assertEqual("off", chassis_status["System Power"])
    self.assertEqual("false", chassis_status["Power Overload"])
    self.assertEqual("", chassis_status["Last Power Event"])
    self.assertEqual("false", chassis_status["Cooling/Fan Fault"])
