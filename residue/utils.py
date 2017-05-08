# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQL and SQLAlchemy related utilities."""

from __future__ import absolute_import
import re
import uuid


__all__ = [
    'check_constraint_naming_convention', 'default_naming_convention',
    'fingerprint_sql', 'NAMESPACE_SQL']


NAMESPACE_SQL = uuid.UUID('75c7e3be-a5c7-414d-bc66-d64ae5d03f3d')
_single_quote_whitespace = re.compile(r"\s+(?=([^']*'[^']*')*[^']*$)")


def check_constraint_naming_convention(constraint, table):
    """
    Creates a unique name for an unnamed CheckConstraint.

    The name is generated using `fingerprint_sql`.

    >>> from sqlalchemy import CheckConstraint, MetaData, Table
    >>> table = Table('account', MetaData())
    >>> constraint = CheckConstraint('failed_logins > 3')
    >>> check_constraint_naming_convention(constraint, table)
    '82ae7c7955635a85bb8ae76bf656a878'

    Args:
        constraint (CheckConstraint): The check constraint for which the name
            should be generated.
        table (Table): The table to which the check contraint belongs.

    Returns:
        str: A 32 character hex uuid5 of a normalized version of
            `constraint.sqltext`.

    See Also:
        http://alembic.zzzcomputing.com/en/latest/naming.html

    """
    return fingerprint_sql(str(constraint.sqltext))


def fingerprint_sql(sqltext):
    """
    Returns the uuid5 hexdigest of a normalized version of `sqltext`.

    Normalization involves replacing all substrings of non-single quoted
    whitespace with a single space. The namespace used to create the uuid5
    is `NAMESPACE_SQL`::

        uuid.UUID('75c7e3be-a5c7-414d-bc66-d64ae5d03f3d')

    >>> fingerprint_sql("select * from user where name = 'Foo    Bar'")
    'b23bfc4cc7ad535ba6463473669f6597'

    >>> fingerprint_sql('''
    ... select *
    ... from user
    ... where name = 'Foo    Bar'
    ... ''')
    'b23bfc4cc7ad535ba6463473669f6597'

    Args:
        sqltext (str): Some raw SQL string.

    Returns:
        str: A 32 character hex uuid5 of a normalized version of `sqltext`.
    """
    sqltext = _single_quote_whitespace.sub(' ', sqltext).strip()
    return uuid.uuid5(NAMESPACE_SQL, sqltext).hex


# Consistent naming conventions are necessary for alembic to be able to
# reliably upgrade and downgrade versions. For more details, see:
# http://alembic.zzzcomputing.com/en/latest/naming.html
default_naming_convention = {
    'unnamed_ck': check_constraint_naming_convention,
    'ix': 'ix_%(column_0_label)s',
    'uq': 'uq_%(table_name)s_%(column_0_name)s',
    'ck': 'ck_%(table_name)s_%(unnamed_ck)s',
    'fk': 'fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s',
    'pk': 'pk_%(table_name)s'}
