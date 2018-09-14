#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#

import collections
import os
import re

import cStringIO

from curie.os_util import OsUtil


class FioConfiguration(collections.OrderedDict):
  """
  Manage an FIO configuration file.
  """

  DEFAULT_IODEPTH_PER_NODE = 128

  section_re = re.compile(r"\[(.+)\]")  # Text surrounded by square brackets.
  prefill_defaults = "\n".join(["[global]",
                                "ioengine=libaio",
                                "direct=1",
                                "bs=1m",
                                "iodepth=%d" % DEFAULT_IODEPTH_PER_NODE,
                                "rw=write"])

  def __init__(self, *args, **kwargs):
    super(FioConfiguration, self).__init__(*args, **kwargs)

  def convert_prefill(self, **kwargs):
    """
    Converts an existing configuration to a prefill configuration file.

    Args:
      **kwargs: Keyword arguments can be used to override global defaults.

    Returns:
      new_config: (FioConfiguration object) The converted config.
    """
    new_config = FioConfiguration.loads(FioConfiguration.prefill_defaults)
    for job, params in self.iteritems():
      if job == "global":
        continue
      new_config.set(job, "filename", params["filename"])
      new_config.set(job, "size", params["size"])
      if params.get("offset") is not None:
        new_config.set(job, "offset", params.get("offset"))
    # Override global defaults using keyword arguments.
    for key, val in kwargs.iteritems():
      new_config.set("global", key, val)
    return new_config

  def format(self, *args, **kwargs):
    """
    Fills in a configuration template with the provided argument data.

    This function is basically a pass-through to Python's string format
    function.

    Returns: FioConfiguration object
    """
    formatted = FioConfiguration.dumps(self).format(*args, **kwargs)
    return FioConfiguration.loads(formatted)

  def save(self, file_location):
    """
    Saves the configuration to the location specified.

    Args:
      file_location: (str) The filepath to save the file to.
    """
    directory_path = os.path.dirname(file_location)
    if not os.path.exists(directory_path):
      os.makedirs(directory_path)
    OsUtil.write_and_rename(file_location, FioConfiguration.dumps(self))

  def set(self, section_name, parameter, value=None):
    """
    Sets a parameter in a configuration.

    Args:
      section_name: (str) The name of the job/section to place the parameter.
      parameter: (str) The name of the parameter.
      value: (str) The value to set.  May be None if the parameter does not
        require a value.
    """
    self.setdefault(section_name, collections.OrderedDict())
    self[section_name][parameter] = value

  @staticmethod
  def load(fh):
    """
    Loads a configuration file.

    Args:
      fh: (file handle) reads the configuration from a file handle.

    Returns: FioConfiguration object
    """
    parameter_file = FioConfiguration()
    section_name = None
    fh.seek(0, 0)
    # ';' and '#' denote comments for fio
    whitespace_or_comment_re = re.compile("^\s*(;.*|#.*)?$")
    for line in fh:
      line = line.strip()
      if whitespace_or_comment_re.match(line):
        continue
      # Check if this line is the start of a new section.
      m = FioConfiguration.section_re.search(line)
      if m is not None:
        section_name = m.group(1)
        if parameter_file.get(section_name):
          raise ValueError("Duplicate section name found in FIO config.")
      # Store the key/value pair in the dict - or None, if it's a flag.
      else:
        unpacked = line.split("=")
        if len(unpacked) == 1:
          unpacked.append(None)
        parameter_file.set(section_name, unpacked[0], unpacked[1])
    return parameter_file

  @staticmethod
  def loads(string):
    """
    Loads a configuration from a string.

    Args:
      string: (str) The configuration string to load.

    Returns: FioConfiguration object
    """
    # Wrap the string with StringIO so we can use the existing load() function.
    fp = cStringIO.StringIO(string)
    return FioConfiguration.load(fp)

  @staticmethod
  def dump(fio_config, fh):
    """
    Saves the configuration to the file handle.

    Args:
      fio_config: (curie.iogen.FioConfiguration) FIO Configuration.
      fh: (filehandle) The file handle to save the config to.
    """
    fh.write(FioConfiguration.dumps(fio_config))

  @staticmethod
  def dumps(fio_config):
    """
    Provides the string representation of the configuration.

    Args:
      fio_config: (curie.iogen.FioConfiguration) FIO Configuration.

    Returns: (str) configuration
    """
    fp = cStringIO.StringIO()
    for section_name in fio_config.iterkeys():
      fp.write("[%s]\n" % section_name)
      for key, val in fio_config[section_name].iteritems():
        if val is not None:
          fp.write("%s=%s\n" % (str(key), str(val)))
        else:
          fp.write("%s\n" % str(key))
      fp.write("\n")
    return fp.getvalue()
