# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""Tests for :mod:`residue.types` module."""

from __future__ import absolute_import
import datetime

import pytest
from pockets import uncamel
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm.query import Query
from sqlalchemy.schema import Column
from sqlalchemy.sql import func
from sqlalchemy.types import UnicodeText, Date

import residue.query
from residue.query import constrain_query_by_date, generate_date_series, normalize_query_by_date, RE_STARTSWITH_DIGIT


UTCNOW = datetime.datetime.utcnow()
UTC20DAYSAGO = UTCNOW - datetime.timedelta(days=20)
UTC40DAYSAGO = UTCNOW - datetime.timedelta(days=40)
UTC60DAYSAGO = UTCNOW - datetime.timedelta(days=60)


@as_declarative()
class Base(object):
    @declared_attr
    def __tablename__(cls):
        return uncamel(cls.__name__)

    id = Column(UnicodeText, primary_key=True)


class Attendee(Base):
    email = Column(UnicodeText, nullable=False)
    birthdate = Column(Date, nullable=True)


@pytest.fixture(autouse=True)
def datetime_utcnow(monkeypatch):

    class _datetime(object):
        @classmethod
        def utcnow(cls):
            return UTCNOW

    monkeypatch.setattr(datetime, 'datetime', _datetime)
    monkeypatch.setattr(residue.query, 'datetime', _datetime)
    return UTCNOW


@pytest.fixture(params=[UTC60DAYSAGO, UTC40DAYSAGO, None])
def start_date(request):
    start_date = request.param if request.param else UTCNOW
    start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    return (request.param, start_date, start_date_str)


@pytest.fixture(params=[UTC20DAYSAGO, UTCNOW, None])
def end_date(request):
    end_date = request.param if request.param else UTCNOW
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    return (request.param, end_date, end_date_str)


@pytest.fixture(params=['1 month', '10 day', None])
def interval(request):
    interval = request.param
    interval_str = interval if interval else '1 month'
    return (request.param, interval, interval_str)


@pytest.fixture(params=['2 day', 'day', None])
def granularity(request):
    granularity = request.param
    if not granularity:
        granularity_str = '1 day'
    elif not RE_STARTSWITH_DIGIT.match(granularity):
        granularity_str = '1 {}'.format(granularity)
    else:
        granularity_str = granularity
    return (request.param, granularity, granularity_str)


def test_generate_date_series_start_date_none(end_date, interval, granularity):
    (interval_param, interval, interval_str) = interval
    (end_date_param, end_date, end_date_str) = end_date
    (granularity_param, granularity, granularity_str) = granularity
    start_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S.%f')

    query = generate_date_series(None, end_date_param, interval_param, granularity_param)
    expression = query.compile(dialect=postgresql.dialect())
    expected = 'generate_series(DATE {} - INTERVAL {}, {}, {})'.format(
        start_date_str, interval_str, end_date_str, granularity_str)
    actual = str(expression) % expression.params
    assert expected == actual


def test_generate_date_series_end_date_none(start_date, interval, granularity):
    (interval_param, interval, interval_str) = interval
    (start_date_param, start_date, start_date_str) = start_date
    (granularity_param, granularity, granularity_str) = granularity
    if start_date_param and interval_param:
        end_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        end_date_str = UTCNOW.strftime('%Y-%m-%d %H:%M:%S.%f')

    if not start_date_param:
        expected = 'generate_series(DATE {} - INTERVAL {}, {}, {})'.format(
            end_date_str, interval_str, end_date_str, granularity_str)
    elif not interval_param:
        expected = 'generate_series({}, {}, {})'.format(start_date_str, end_date_str, granularity_str)
    else:
        expected = 'generate_series({}, DATE {} + INTERVAL {}, {})'.format(
            start_date_str, end_date_str, interval_str, granularity_str)

    query = generate_date_series(start_date_param, None, interval_param, granularity_param)
    expression = query.compile(dialect=postgresql.dialect())
    actual = str(expression) % expression.params
    assert expected == actual


def test_generate_date_series_interval_none(start_date, end_date, granularity):
    interval_str = '1 month'
    (end_date_param, end_date, end_date_str) = end_date
    (start_date_param, start_date, start_date_str) = start_date
    (granularity_param, granularity, granularity_str) = granularity

    if not start_date_param:
        expected = 'generate_series(DATE {} - INTERVAL {}, {}, {})'.format(
            end_date_str, interval_str, end_date_str, granularity_str)
    else:
        expected = 'generate_series({}, {}, {})'.format(start_date_str, end_date_str, granularity_str)

    query = generate_date_series(
        start_date_param, end_date_param, None, granularity_param)
    expression = query.compile(dialect=postgresql.dialect())
    actual = str(expression) % expression.params
    assert expected == actual


def test_constrain_query_by_date(start_date, end_date, interval):
    (interval_param, interval, interval_str) = interval
    (start_date_param, start_date, start_date_str) = start_date
    (end_date_param, end_date, end_date_str) = end_date

    query = constrain_query_by_date(
        Query(Attendee.birthdate).selectable, Attendee.birthdate,
        start_date_param, end_date_param, interval_param)

    where_clause = ''
    if start_date_param:
        if end_date_param:
            where_clause = """ \n\
WHERE \
attendee.birthdate >= :birthdate_1 AND \
attendee.birthdate <= :birthdate_2\
"""
        elif interval_param:
            where_clause = """ \n\
WHERE \
attendee.birthdate >= :birthdate_1 AND \
attendee.birthdate <= DATE :start_date_param_1 + INTERVAL :interval_param_1\
"""
        else:
            where_clause = """ \n\
WHERE attendee.birthdate >= :birthdate_1"""
    elif end_date_param:
        if interval_param:
            where_clause = """ \n\
WHERE \
attendee.birthdate <= :birthdate_1 AND \
attendee.birthdate >= DATE :end_date_param_1 - INTERVAL :interval_param_1\
"""
        else:
            where_clause = """ \n\
WHERE attendee.birthdate <= :birthdate_1"""
    elif interval_param:
        where_clause = """ \n\
WHERE \
attendee.birthdate >= DATE :current_date_param_1 - INTERVAL :interval_param_1\
"""

    expected = """\
SELECT attendee.birthdate AS attendee_birthdate \n\
FROM attendee{}""".format(where_clause)

    assert expected == str(query)


def test_normalize_query_by_date():
    date_label = 'date_label'
    report_label = 'count_label'
    query = Query([
        Attendee.birthdate.label(date_label),
        func.count(Attendee.id).label(report_label)
    ]).group_by(date_label)
    query = normalize_query_by_date(
        query,
        date_label,
        report_label,
        start_date=UTC20DAYSAGO,
        end_date=UTCNOW)
    expected = """\
SELECT date_label, coalesce(count_label, :param_1) AS count_label \n\
FROM (\
SELECT attendee.birthdate AS date_label, count(attendee.id) AS count_label \n\
FROM attendee GROUP BY date_label \
UNION \
SELECT generate_series(\
:generate_series_1, \
:generate_series_2, \
:generate_series_3) AS date_label, :param_2 AS count_label) AS anon_1 \
ORDER BY anon_1.date_label"""
    assert expected == str(query)
