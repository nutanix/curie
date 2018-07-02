# Copyright (c) 2018 Nutanix Inc. All rights reserved.
#


class Options(object):
  """
  Options class to replace Ansible OptParser
  """

  DEFAULT_CHECK = False
  DEFAULT_CONNECTION = "ssh"
  DEFAULT_FORKS = 32
  DEFAULT_LISTHOSTS = False
  DEFAULT_LISTTAGS = False
  DEFAULT_LISTTASKS = False
  DEFAULT_MODULE_PATH = None
  DEFAULT_SYNTAX = False

  def __init__(self, config=None):
    """ Initialize new Ansible Options object.

    All Ansible configuration options should set in the ./ansible.cfg
    file to avoid putting them in the codebase.  Some Ansible options must go
    in the codebase because they are not read from this configuration file,
    however this should be the second option.

    Args:
      config (dict): dictionary containing configuration settings
    """
    if not config:
      ans_cfg = {}
    else:
      ans_cfg = config

    # These values are not set in Ansible constants.py and are not picked up in
    # ansible.cfg
    self.check = ans_cfg.get("check", self.DEFAULT_CHECK)
    self.connection = ans_cfg.get("connection", self.DEFAULT_CONNECTION)
    self.forks = ans_cfg.get("forks", self.DEFAULT_FORKS)
    self.listhosts = ans_cfg.get("listhosts", self.DEFAULT_LISTHOSTS)
    self.listtags = ans_cfg.get("listtags", self.DEFAULT_LISTTAGS)
    self.listtasks = ans_cfg.get("listtasks", self.DEFAULT_LISTTASKS)
    self.module_path = ans_cfg.get("module_path", self.DEFAULT_MODULE_PATH)
    self.syntax = ans_cfg.get("syntax", self.DEFAULT_SYNTAX)

    for key, value in ans_cfg.items():
      setattr(self, key, value)
