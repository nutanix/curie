#
# Copyright (c) 2016 Nutanix Inc. All rights reserved.
#
# Adapted from ABNF definition given DMTF DSP0004:2.5.0, Annex C.
#
# NB: PLY lex, yacc have specific requirements on docstring formatting.
# pylint: disable=line-too-long
#

import ply.lex
import ply.yacc

from curie.curie_error_pb2 import CurieError
from curie.exception import CurieException

#==============================================================================
# Lexer
#==============================================================================

tokens = ("DIV",
          "EXP",
          "MINUS",
          "PLUS",
          "TIMES",
          "SIMPLE_NAME",
          "DECIBEL_BASE_UNIT",
          "POSITIVE_DECIMAL_NUMBER",
          "POSITIVE_WHOLE_NUMBER")

t_ignore_SPACE = r"\ "

t_DIV = r"/"
t_EXP = r"\^"
t_MINUS = r"\-"
t_PLUS = r"\+"
t_TIMES = r"\*"

_SIMPLE_NAME = r"[a-zA-Z_]([ \n\t]?[a-zA-Z_0-9-]+)*"
_DECIBEL_BASE_UNIT = r"decibel(\ ?\(\ ?%s\ ?\))?" % _SIMPLE_NAME

# NB: Must declare 't_POSITIVE_DECIMAL_NUMBER' prior to
# 't_POSITIVE_WHOLE_NUMBER' so that 't_POSITIVE_WHOLE_NUMBER' doesn't
# incorrectly match the integral part of a decimal.

def t_POSITIVE_DECIMAL_NUMBER(t):
  r"[1-9][0-9]*\.[0-9]*"
  return t

def t_POSITIVE_WHOLE_NUMBER(t):
  r"[1-9][0-9]*"
  return t

# NB: Similarly, must declare 't_DECIBEL_BASE_UNIT' prior to 't_SIMPLE_NAME' to
# give priority.

@ply.lex.TOKEN(_DECIBEL_BASE_UNIT)
def t_DECIBEL_BASE_UNIT(t):
  return t

@ply.lex.TOKEN(_SIMPLE_NAME)
def t_SIMPLE_NAME(t):
  return t

def t_error(args):
  raise CurieException(CurieError.kInternalError, "TOKEN ERROR: %s" % args)

#==============================================================================
# PUnit class
#==============================================================================

class PUnit(object):
  @classmethod
  def from_string(cls, string):
    lexer = ply.lex.lex()
    parser = ply.yacc.yacc()
    return parser.parse(lexer=lexer, input=string)

  def __init__(self, unit_map=None, multiplier=1):
    self.unit_map = {} if unit_map is None else unit_map
    self.multiplier = multiplier

  def add_unit(self, unit):
    self.unit_map.setdefault(unit, 0)
    self.unit_map[unit] += 1

  def add_div_unit(self, unit):
    self.unit_map.setdefault(unit, 0)
    self.unit_map[unit] += 1

  def set_multiplier(self, multiplier):
    self.multiplier = multiplier

  def __str__(self):
    output = []
    output_negative = []
    for key, val in sorted(self.unit_map.iteritems()):
      if val == 1:
        output.append(key)
      elif val == 0:
        continue
      elif val > 0:
        output.append("%s^%d" % (key, val))
      else:
        output_negative.append("%s^%d" % (key, val))

    output.extend(output_negative)
    output.append("%s" % self.multiplier)
    return "*".join(output)

  def __imul__(self, other):
    if isinstance(other, PUnit):
      for key, val in other.unit_map.iteritems():
        self.unit_map.setdefault(key, 0)
        self.unit_map[key] += val
      self.multiplier *= other.multiplier
    else:
      self.multiplier *= other

    return self

  def __mul__(self, other):
    if isinstance(other, PUnit):
      unit_map = self.unit_map.copy()
      for key, val in other.unit_map.iteritems():
        unit_map.setdefault(key, 0)
        unit_map[key] += val
      return PUnit(unit_map=unit_map,
                   multiplier=self.multiplier * other.multiplier)
    else:
      return PUnit(unit_map=self.unit_map.copy(),
                   multiplier=self.multiplier * other)

  def __rmul__(self, other):
    return self.__mul__(other)

  def __div__(self, other):
    if isinstance(other, PUnit):
      unit_map = self.unit_map.copy()
      for key, val in other.unit_map.iteritems():
        unit_map.setdefault(key, 0)
        unit_map[key] -= val

      return PUnit(unit_map=unit_map,
                   multiplier=self.multiplier / other.multiplier)
    else:
      return PUnit(
          unit_map=dict((k, -v) for k, v in self.unit_map.iteritems()),
          multiplier=self.multiplier / other)

  def __rdiv__(self, other):
    return self.__div__(other)

#==============================================================================
# Parser
#==============================================================================

def _product(lst):
  prod = PUnit()
  for elt in lst:
    prod *= elt

  return prod

def p_programmatic_unit(p):
  """
  programmatic-unit : punit-head punit-body punit-tail
                    | punit-head punit-tail
  """
  p[0] = _product(p[1:])

def p_punit_body(p):
  """
  punit-body : punit-mult
             | punit-div
             | punit-mult punit-div
  """
  p[0] = _product(p[1:])

def p_punit_tail(p):
  """
  punit-tail :
             | modifier1
             | modifier2
             | modifier1 modifier2
  """
  p[0] = _product(p[1:])

def p_punit_mult(p):
  """
  punit-mult : multiplied-base-unit
             | punit-mult multiplied-base-unit
  """
  p[0] = _product(p[1:])

def p_punit_div(p):
  """
  punit-div : divided-base-unit
            | punit-div divided-base-unit
  """
  p[0] = _product(p[1:])

def p_multiplied_base_unit(p):
  """
  multiplied-base-unit : TIMES base-unit
  """
  p[0] = p[2]

def p_divided_base_unit(p):
  """
  divided-base-unit : DIV base-unit
  """
  p[0] = 1 / p[2]

def p_modifier1(p):
  """
  modifier1 : operator number
  """
  if p[1] == "*":
    p[0] = int(p[2])
  else:
    p[0] = 1.0 / int(p[2])

def p_modifier2(p):
  """
  modifier2 : operator base EXP exponent
  """
  if p[1] == "*":
    p[0] = int(p[2]) ** p[4]
  else:
    p[0] = int(p[2]) ** -p[4]

def p_operator(p):
  """
  operator : TIMES
           | DIV
  """
  p[0] = p[1]

def p_number(p):
  """
  number : positive-number
         | PLUS positive-number
         | MINUS positive-number
  """
  p[0] = int(p[len(p) - 1])
  if p[1] == "-":
    p[0] *= -1

def p_base(p):
  """
  base : POSITIVE_WHOLE_NUMBER
  """
  p[0] = int(p[1])

def p_exponent(p):
  """
  exponent : POSITIVE_WHOLE_NUMBER
           | PLUS POSITIVE_WHOLE_NUMBER
           | MINUS POSITIVE_WHOLE_NUMBER

  """
  p[0] = int(p[len(p) - 1])
  if p[1] == "-":
    p[0] *= -1

def p_positive_number(p):
  """
  positive-number : POSITIVE_WHOLE_NUMBER
                  | POSITIVE_DECIMAL_NUMBER
  """
  if "." in p[1]:
    p[0] = float(p[1])
  else:
    p[0] = int(p[1])

def p_punit_head(p):
  """
  punit-head :
             | base-unit
  """
  p[0] = _product(p[1:])

def p_base_unit(p):
  """
  base-unit : SIMPLE_NAME
            | DECIBEL_BASE_UNIT
  """
  p[0] = PUnit({p[1]: 1})

def p_error(p):
  raise CurieException(CurieError.kInternalError,
                        "Parse Error: Unexpected token '%s'" % p)
