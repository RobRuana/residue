# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""Tests for :mod:`residue.types` module."""

from __future__ import absolute_import
from residue.types import CoerceUTF8, JSON, UTCDateTime, UUID


def test_CoerceUTF8():
    assert CoerceUTF8()


def test_JSON():
    assert JSON()


def test_UTCDateTime():
    assert UTCDateTime()


def test_UUID():
    assert UUID()
