#
# Copyright (c) 2015 Nutanix Inc. All rights reserved.
#
"""
Util for building simple classes based on XML data.

To create a node, define a class, setting __metaclass__ to XmlMeta, and
setting the class-level attribute _DESCRIPTOR to an instance of
XmlElementDescriptor.

Child nodes may be created by subclassing.

Example, to represent the following:
  <TAG>
    <NESTED_TAG>
      Some text
    </NESTED_TAG>
  </TAG>

One might define:

class Tag(object):
  _DESCRIPTOR = XmlElementDescriptor("TAG")
  __metaclass__ = XmlMeta

class NestedTag(Tag):
  _DESCRIPTOR = XmlElementDescriptor("NESTED_TAG")

  def __init__(self, text):
    self.__node = self._DESCRIPTOR.clone()
    self.__node.text = text
    super(NestedTag, self).__init__(self.__node)

data = NestedTag("Some text")
print(data.xml(True))

<?xml version='1.0' encoding='ASCII'?>
<TAG>
  <NESTED_TAG>Some text</NESTED_TAG>
<TAG>
"""

from copy import deepcopy

from lxml import etree

from curie.log import CHECK


class XmlElementDescriptor(object):
  # Map of XML types to the python types which should be used in parsing.
  # If a specific type is not found, will default to returning the string
  # representation directly.
  _XML_PY_TYPE_MAP = {
    "uint8": int,
    "uint16": int,
    "string": str
    }

  @classmethod
  def xml_py_type_cast(cls, xml_type, value):
    """
    Cast the string 'value' of XML type 'xml_type' to appropriate python type.

    If an appropriate type is not found, defaults to returning 'value' as
    a string.

    Args:
      xml_type (str): XML type attribute associated with 'value'.
      value (str): XML serialized value, originally of type 'xml_type'.

    Returns:
      'value' cast to appropriate type if found, else 'value' as a string.
    """
    py_type = cls._XML_PY_TYPE_MAP.get(xml_type, str)
    return py_type(value)

  def __init__(self, tag, attrs=None):
    # XML tag for this element
    self.__tag = tag

    # Dict of attributes for this element.
    self.__attrs = attrs if attrs else {}

    # xpath for this element. Initially just "//<tag>", updated as necessary.
    self.__xpath = "//%s" % tag

    # Prototype element. Copied per-instance of an XML-based class.
    self.__node_prototype = etree.Element(tag, attrib=attrs)

    # Root of element tree for this node.
    self.__root_tree = self.__node_prototype.getroottree()

    # Whether this prototype has been initialized.
    self.__is_initialized = False

  def attrs(self):
    return self.__attrs

  def tag(self):
    return self.__tag

  def xpath(self):
    return self.__xpath

  def clone(self):
    """
    Returns a clone of the prototype which may be used/modified as desired.
    """
    root = deepcopy(self.__root_tree.getroot())
    return root.xpath(self.__xpath)[0]

  def _initialize(self, root_tree, xpath):
    """
    Updates root tree and element xpath.

    Handles updates as appropriate after resolving changes to the tree due to
    class inheritance.
    """
    CHECK(not self.__is_initialized, "Cannot reinitialize this descriptor")
    self.__is_initialized = True
    self.__root_tree = root_tree
    self.__xpath = xpath

class XmlMeta(type):
  def __init__(cls, name, bases, dct):
    super(XmlMeta, cls).__init__(name, bases, dct)

    node_desc = getattr(cls, "_DESCRIPTOR")
    s_classes = [s_cls for s_cls in cls.mro() if
                 issubclass(getattr(s_cls, "__metaclass__", object), XmlMeta)]

    ii = 1
    curr_node = getattr(s_classes[-1], "_DESCRIPTOR").clone()
    while ii < len(s_classes):
      prev_node = curr_node
      curr_node = getattr(s_classes[-1 - ii], "_DESCRIPTOR").clone()
      prev_node.append(curr_node)
      ii += 1

    root_tree = curr_node.getroottree()
    node_desc._initialize(root_tree, root_tree.getpath(curr_node))

    def xpath():
      return node_desc.xpath()
    setattr(cls, "xpath", staticmethod(xpath))

class XmlData(object):
  # Set by XmlMeta for concrete subclasses.
  @staticmethod
  def xpath():
    NotImplementedError("XmlMeta failed to set xpath")

  def __init__(self, node):
    # Per-instance copy of etree.Element for this node.
    self.__node = node

  def __str__(self):
    try:
      return self.xml()
    except BaseException as exc:
      return "%s (invalid data): %s" % (repr(self), str(exc))

  def headers(self):
    return {}

  def lookup_xpath(self, xpath):
    """Returns first node at 'xpath' relative to self.node()'s root tree."""
    return self.root().xpath(xpath)[0]

  def node(self):
    return self.__node

  def root(self):
    return self.node().getroottree().getroot()

  def xml(self, pretty=False, xml_declaration=True):
    return etree.tostring(
      self.root(), xml_declaration=xml_declaration, pretty_print=pretty)
