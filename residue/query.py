# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQLAlchemy query functions."""

from __future__ import absolute_import
import re
from datetime import datetime

from sqlalchemy.sql import and_, bindparam, literal, func, select, text, union


__all__ = ['constrain_query_by_date', 'generate_date_series', 'normalize_query_by_date']


RE_STARTSWITH_DIGIT = re.compile(r'^\s*\d+')


def constrain_query_by_date(query, column, start_date=None, end_date=None, interval='1 month'):
    """
    Applies a WHERE clause which constrains a query within start and end dates.

    For example, if column is "user.birthdate" and query looks like::

        select name from user;

    The result would look something like this::

        select name from user
        where birthdate > '2017-01-01' and birthdate < '2017-01-02';

    Args:
        query (sqlalchemy.orm.query.Query): The query to which the date
            constraining WHERE clause should be applied.
        column (sqlalchemy.sql.schema.Column): The date column which should be
            used in the constraining WHERE clause.
        start_date (datetime): The earliest date in the constraint.
        end_date (datetime): The most recent date in the constraint.
        interval (str): Alternately, the length of time from either
            `start_date` or `end_date` expressed as a Postgres interval.
            Defaults to '1 month'.

    Returns:
        sqlalchemy.orm.query.Query: The constrained query.
    """
    if start_date:
        if end_date:
            # If the start_date and the end_date are defined then we use those
            return query.where(and_(column >= start_date, column <= end_date))
        elif interval:
            # If the start_date and the interval are defined then we use the
            # start_date plus the interval as the end_date
            return query.where(and_(
                column >= start_date,
                column <= text('DATE :start_date_param_1 + INTERVAL :interval_param_1', bindparams=[
                    bindparam('start_date_param_1', start_date),
                    bindparam('interval_param_1', interval)])))
        else:
            # If ONLY the start_date is defined then we just use that
            return query.where(column >= start_date)
    elif end_date:
        if interval:
            # If the end_date and the interval are defined then we use the
            # end_date minus the interval as the start_date
            return query.where(and_(
                column <= end_date,
                column >= text('DATE :end_date_param_1 - INTERVAL :interval_param_1', bindparams=[
                    bindparam('end_date_param_1', end_date),
                    bindparam('interval_param_1', interval)])))
        else:
            # If ONLY the end_date is defined then we just use that
            return query.where(column <= end_date)
    elif interval:
        # If ONLY the interval is defined then we use the current date minus
        # the interval as the start_date
        return query.where(column >= text('DATE :current_date_param_1 - INTERVAL :interval_param_1', bindparams=[
            bindparam('current_date_param_1', datetime.utcnow()),
            bindparam('interval_param_1', interval)]))

    # If NOTHING was defined then the query is returned unmodified
    return query


def generate_date_series(start_date=None, end_date=None, interval='1 month', granularity='1 day'):
    """
    Generates a date series; useful for grouping queries into date "buckets".

    The resulting query looks something like this::

        generate_series(DATE '2017-04-17', DATE '2017-05-07', '1 day')

    Args:
        start_date (datetime): Start date of the series.
        end_date (datetime): End date of the series.
        interval (str): Alternately, the length of time from either
            `start_date` or `end_date` expressed as a Postgres interval.
            Defaults to '1 month'.
        granularity (str): The granularity of each date "bucket" expressed
            as a Postgres interval. Defaults to '1 day'.

    Returns:
        sqlalchemy.orm.query.Query: A date series query.
    """
    if not granularity:
        granularity = '1 day'
    elif not RE_STARTSWITH_DIGIT.match(granularity):
        granularity = '1 {}'.format(granularity)

    if start_date:
        if end_date:
            # If the start_date and the end_date are defined then we use those
            return func.generate_series(start_date, end_date, granularity)
        elif interval:
            # If the start_date and the interval are defined then we use the
            # start_date plus the interval as the end_date
            return func.generate_series(
                start_date,
                text('DATE :start_date_param_1 + INTERVAL :interval_param_1', bindparams=[
                     bindparam('start_date_param_1', start_date),
                     bindparam('interval_param_1', interval)]),
                granularity)
        else:
            # If ONLY the start_date is defined then we use the current date
            # as the end_date
            return func.generate_series(start_date, datetime.utcnow(), granularity)

    if not end_date:
        # If the start_date and end_date are both undefined, we set the
        # end_date to the current date
        end_date = datetime.utcnow()

    if not interval:
        # If the interval is undefined, we set the interval to "1 month"
        interval = '1 month'

    return func.generate_series(
        text('DATE :end_date_param_1 - INTERVAL :interval_param_1', bindparams=[
            bindparam('end_date_param_1', end_date),
            bindparam('interval_param_1', interval)]),
        end_date,
        granularity)


def normalize_query_by_date(query, date_label, report_label, start_date=None, end_date=None, interval='1 month',
                            granularity='1 day'):
    """
    Fills in missing date "buckets" for an aggregate query grouped by date.

    Aggregate queries grouped by date are often used for generating reports,
    like "How many widgets did I sell on each day of last month?" These queries
    often look similar to this::

        SELECT date_of_sale AS day, count(id) AS sales_count
        FROM sales
        WHERE date_of_sale > now() - interval '1 month'
        GROUP BY day ORDER BY day;

    This kind of query will ONLY return days that contain at least one sale.
    Days with zero sales will not be returned at all, leaving gaps in the
    report. To combat this, we can normalize the query by generating a UNION
    with a Postgres date series::

        SELECT date_of_sale AS day, coalesce(sales_count, 0) AS sales_count
        FROM (
          SELECT date_of_sale AS day, count(id) AS sales_count
          FROM sales
          WHERE date_of_sale > now() - interval '1 month'
          GROUP BY day
        UNION
          SELECT
            generate_series(now() - interval '1 month', now(), '1 day') AS day,
            0 AS sales_count
        ) AS union_query ORDER BY union_query.day;

    Args:
        query (sqlalchemy.orm.query.Query): The original query grouped by date,
            in which the "group by date" column is labelled using
            column.label(date_label), and the "aggregate" column is labelled
            using column.label(report_label).
        date_label (str): The label used to label the "group by date" column.
        report_label (str): The label used to label the "aggregate" column.
        start_date (datetime): Start date of the query.
        end_date (datetime): End date of the query.
        interval (str): Alternately, the length of time from either
            `start_date` or `end_date` expressed as a Postgres interval.
            Defaults to '1 month'.
        granularity (str): The granularity of each date "bucket" expressed
            as a Postgres interval. Defaults to '1 day'.

    Returns:
        sqlalchemy.orm.query.Query: A normalized aggregate date query.
    """
    series = generate_date_series(start_date, end_date, interval, granularity)
    series_query = select([series.label(date_label), literal(0).label(report_label)])
    query = union(query, series_query).alias()
    query = select([
        text(date_label),
        func.coalesce(text(report_label), literal(0)).label(report_label)
    ], from_obj=query).order_by(date_label)
    return query
