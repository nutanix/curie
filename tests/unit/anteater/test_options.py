#
# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#
#

import unittest
from curie.anteater import Options


class TestOptions(unittest.TestCase):

  def setUp(self):
    pass

  def test_init_config_check(self):
    expected = not Options.DEFAULT_CHECK
    opts = Options({"check": expected})
    self.assertEqual(expected, opts.check)

  def test_init_config_connection(self):
    expected = Options.DEFAULT_CONNECTION + "NONDEFAULT"
    opts = Options({"connection": expected})
    self.assertEqual(expected, opts.connection)

  def test_init_config_forks(self):
    expected = Options.DEFAULT_FORKS + 10
    opts = Options({"forks": expected})
    self.assertEqual(expected, opts.forks)

  def test_init_config_listhosts(self):
    expected = not Options.DEFAULT_LISTHOSTS
    opts = Options({"listhosts": expected})
    self.assertEqual(expected, opts.listhosts)

  def test_init_config_listtags(self):
    expected = not Options.DEFAULT_LISTTAGS
    opts = Options({"listtags": expected})
    self.assertEqual(expected, opts.listtags)

  def test_init_config_listtasks(self):
    expected = not Options.DEFAULT_LISTTASKS
    opts = Options({"listtasks": expected})
    self.assertEqual(expected, opts.listtasks)

  def test_init_config_module_path(self):
    expected = "not_default"
    opts = Options({"module_path": expected})
    self.assertEqual(expected, opts.module_path)

  def test_init_config_syntax(self):
    expected = not Options.DEFAULT_SYNTAX
    opts = Options({"syntax": expected})
    self.assertEqual(expected, opts.syntax)

  def test_init_noconfig(self):
    opts = Options()
    self.assertEqual(Options.DEFAULT_CHECK, opts.check)
    self.assertEqual(Options.DEFAULT_CONNECTION, opts.connection)
    self.assertEqual(Options.DEFAULT_FORKS, opts.forks)
    self.assertEqual(Options.DEFAULT_LISTHOSTS, opts.listhosts)
    self.assertEqual(Options.DEFAULT_LISTTAGS, opts.listtags)
    self.assertEqual(Options.DEFAULT_LISTTASKS, opts.listtasks)
    self.assertEqual(Options.DEFAULT_MODULE_PATH, opts.module_path)
    self.assertEqual(Options.DEFAULT_SYNTAX, opts.syntax)

  def test_init_extra_var(self):
    expected = "IMABONUS"
    opts = Options({"extra_var": expected})
    self.assertEqual(expected, opts.extra_var)
