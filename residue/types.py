# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQLAlchemy user defined types."""

from __future__ import absolute_import
import json
import uuid

import six
from pytz import UTC
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import CHAR, DateTime, String, TypeDecorator, Unicode


__all__ = ['CoerceUTF8', 'JSON', 'UTCDateTime', 'UUID']


class CoerceUTF8(TypeDecorator):
    """
    Coerce python bytestrings to unicode before storing in the database.
    """
    impl = Unicode

    def process_bind_param(self, value, dialect):
        if isinstance(value, type(b'')):
            value = value.decode('utf-8')
        return value


class JSON(TypeDecorator):
    """
    Converts to and from JSON formatted string when storing in the database.
    """
    impl = String

    def __init__(self, comparator=None):
        self.comparator = comparator
        super(JSON, self).__init__()

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        elif isinstance(value, six.string_types):
            return value
        else:
            return json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return json.loads(str(value))

    def copy_value(self, value):
        if self.mutable:
            return json.loads(json.dumps(value))
        else:
            return value

    def compare_values(self, x, y):
        if self.comparator:
            return self.comparator(x, y)
        else:
            return x == y


class UTCDateTime(TypeDecorator):
    """
    Converts datetimes to UTC before storing in the database.

    Uses timezone aware DateTime instance to generate the column in Postgresql,
    otherwise uses a timezone naive DataTime instance.
    """
    impl = DateTime

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(DateTime(timezone=True))
        else:
            return dialect.type_descriptor(DateTime(timezone=False))

    def process_bind_param(self, value, dialect):
        if value is not None:
            if dialect.name == 'postgresql':
                return value.astimezone(UTC)
            else:
                return value.astimezone(UTC).replace(tzinfo=None)

    def process_result_value(self, value, dialect):
        if value is not None:
            return value.replace(tzinfo=UTC)


class UUID(TypeDecorator):
    """
    Platform-independent UUID type.

    Uses Postgresql's native UUID type when connecting to Postgres, otherwise
    stores a string hex value using CHAR(32).
    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(postgresql.UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return uuid.UUID(value).hex
            else:
                return value.hex

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return str(uuid.UUID(value))
