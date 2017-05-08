# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""Tests for :mod:`residue.types` module."""

from __future__ import absolute_import
from residue.utils import check_constraint_naming_convention, fingerprint_sql
from sqlalchemy import CheckConstraint, MetaData, Table


def test_fingerprint_sql():
    assert fingerprint_sql("select * from user where name = 'Foo    Bar'") == \
        'b23bfc4cc7ad535ba6463473669f6597'

    assert fingerprint_sql('''
        select *
        from user
        where name = 'Foo    Bar'
        ''') == \
        'b23bfc4cc7ad535ba6463473669f6597'


def test_check_constraint_naming_convention():
    table = Table('account', MetaData())
    constraint = CheckConstraint('failed_logins > 3')
    assert check_constraint_naming_convention(constraint, table) == \
        '82ae7c7955635a85bb8ae76bf656a878'
