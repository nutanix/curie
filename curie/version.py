#! /usr/bin/env python
# Copyright (c) Nutanix Inc. All rights reserved.
#
# NB: '__build_type__' and '__commit__' placeholders are filled in during
# packaging.

from pkg_resources import get_distribution, DistributionNotFound
try:
    __version__ = get_distribution("curie").version
except DistributionNotFound:
    __version__ = "UnknownVersion"
