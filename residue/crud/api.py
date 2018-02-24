# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""CRUD service for SQLAlchemy."""

from __future__ import absolute_import
import sys
import json
import collections
from copy import deepcopy
from functools import wraps

import six
from pockets import is_listy, listify, mappify
from pockets.autolog import log
from sqlalchemy import select, func
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.mapper import Mapper
from sqlalchemy.orm.properties import ColumnProperty
from sqlalchemy.orm.util import class_mapper
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.expression import cast, and_, or_, asc, desc, literal
from sqlalchemy.types import Text, Integer, String

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache  # noqa: F401


__all__ = ['normalize_sort', 'normalize_data', 'normalize_query', 'crud_exceptions', 'make_crud_api', 'CrudException']


class CrudException(Exception):
    pass


def _collect_fields(d):
    if 'field' in d:
        return {d['field']}
    elif 'and' in d or 'or' in d:
        attrs = set()
        for comp in ['and', 'or']:
            for subquery in d.get(comp, []):
                attrs.update(_collect_fields(subquery))
        return attrs
    elif 'comparison' in d or 'value' in d:
        return {'id'}
    else:
        return d.keys()


def _extract_sort_field(model, value, index=0):
    field = None
    fields = listify(value)
    for f in fields:
        if isinstance(f, six.string_types):
            parts = f.split('.')
            if len(parts) == 1 and field is None:
                if not model or (model and hasattr(model, parts[0])):
                    field = parts[0]
            elif len(parts) > 1 and model and parts[0] == model.__name__:
                field = parts[1]
        else:
            field = f

    if field and isinstance(field, six.string_types) and model:
        attr = getattr(model, field)
        if not (isinstance(attr, InstrumentedAttribute) and isinstance(attr.property, ColumnProperty)) \
                and not isinstance(attr, ClauseElement):
            raise ValueError(
                'SQLAlchemy model classes may only be sorted by columns that exist in the database. '
                'Provided: {}.{}'.format(model.__name__, field))
    return field or 'id'


def _get_queries(x):
    queries = []
    if isinstance(x, (list, tuple)):
        for e in x:
            queries.extend(_get_queries(e))
    elif isinstance(x, dict):
        queries.append(x)
        for e in x.values():
            queries.extend(_get_queries(e))
    return [d for d in queries if isinstance(d.get("_model"), six.string_types)]


def normalize_sort(model, sort):
    if sort and isinstance(sort, six.string_types):
        first_char = sort.lstrip()[0]
        if first_char == '[' or first_char == '{':
            sort = json.loads(sort)

    if isinstance(sort, six.string_types):
        return [{'field': _extract_sort_field(model, sort), 'dir': 'asc'}]
    elif is_listy(sort):
        sorters = []
        for s in sort:
            sorters.extend(normalize_sort(model, s))
        return sorters
    elif isinstance(sort, dict):
        field = sort.get('property', sort.get('fields', sort.get('field', [])))
        direction = sort.get('direction', sort.get('dir', 'asc')).lower()
        return [{'field': _extract_sort_field(model, field), 'dir': direction}]
    return [{'field': 'id', 'dir': 'asc'}]


def normalize_data(data, count=1):
    """
    Normalizes a CrudApi data parameter.

    A singular data can be a string, a list of strings, or a dict::

        'attr'
        ['attr1', 'attr2']
        {'attr1':True, 'attr2':True}


    A plural data must be specified as a list of lists or a list of dicts::

        [['attr1', 'attr2'], ['attr1', 'attr2']]
        [{'attr1':True, 'attr2':True}, {'attr1':True, 'attr2':True}]


    Note that if data is specified as a list of strings, it is
    considered to be singular. Only a list of lists or a list of
    dicts is considered plural.

    Returns the plural form of data as the comprehensive form of a list of
    dictionaries mapping <keyname> to True, extended to count length. If a
    singular data is given, the result will be padded by repeating
    that value. If a plural data is given, it will be padded with
    None, for example::

        >>> normalize_data('attr', 1)
        [{'attr': True}]
        >>> normalize_data('attr', 3)
        [{'attr': True}, {'attr': True}, {'attr': True}]

    """
    if not data:
        return listify(None, minlen=count)

    if isinstance(data, six.string_types):
        data = [{data: True}]
    elif isinstance(data, collections.Mapping):
        data = [data]
    elif isinstance(data, collections.Iterable):
        if any(isinstance(element, six.string_types) for element in data):
            data = [data]
        data = [mappify(v) for v in data]
    else:
        raise TypeError('Unknown datatype: {}: {!r}', type(data), data)

    if len(data) < count:
        if len(data) == 1:
            data.extend([deepcopy(data[0]) for i in range(count - len(data))])
        else:
            data.extend([None for i in range(count - len(data))])
    return data


def normalize_query(query, top_level=True, supermodel=None):
    """
    Normalizes a variety of query formats to a known standard query format.

    The comprehensive form of the query parameter is as follows::

        query = [{
            '_model': 'ModelClassName',
            '_label': 'Optional identifier',

            # Either provide <logical_operator> OR <comparison>
            <logical_operator>|<comparison>
        }]+

        logical_operator =
            'OR'|'AND': [<query>[, <query>]*]

        comparison =
            'comparison': <comparison_function>,
            'field': 'model_field_name',
            'value': 'model_field_value'

        comparison_function =
            'eq'|'ne'|'lt'|'le'|'gt'|'ge'|'in'|'notin'|'isnull'|'isnotnull'|
            'contains'|'icontains'|'like'|'ilike'|'startswith'|'endswith'|
            'istartswith'|'iendswith'

    """
    if query is None:
        raise ValueError('None passed for query parameter')

    query = listify(deepcopy(query))

    queries = []
    for q in query:
        if isinstance(q, six.string_types):
            queries.append({'_model': q, '_label': q})
        elif isinstance(q, dict):
            if 'distinct' in q:
                if isinstance(q['distinct'], six.string_types):
                    q['distinct'] = [q['distinct']]

            if 'groupby' in q:
                if isinstance(q['groupby'], six.string_types):
                    q['groupby'] = [q['groupby']]

            if 'and' in q or 'or' in q:
                op = 'and' if 'and' in q else 'or'

                if not isinstance(q[op], (list, set, tuple)):
                    raise ValueError('Clause must be of type list, set, or tuple not {}, given {}'.format(
                        type(q[op]), q[op]))

                q[op] = normalize_query(q[op], False, q.get('_model', supermodel))
                if len(q[op]) == 1:
                    q = q[op][0]
                elif '_model' not in q:
                    # Pull the _model up from the sub clauses. Technically the
                    # query format requires the _model be declared in the
                    # clause, but we are going to be liberal in what we accept.
                    model = supermodel
                    for clause in q[op]:
                        if '_model' in clause:
                            model = clause['_model']
                            break

                    if model is None:
                        raise ValueError('Clause objects must have a "_model" attribute, given:\n{}'.format(q))
                    q['_model'] = model

            if '_model' in q:
                queries.append(q)
            elif supermodel is not None:
                q['_model'] = supermodel
                queries.append(q)
            else:
                raise ValueError('Query objects must have a "_model" attribute, given:\n{}'.format(q))
        else:
            raise ValueError('Query objects must be either a dict or string, given {}:\n{}'.format(type(q), q))
    return queries


def crud_exceptions(fn):
    """A decorator designed to catch exceptions from the crud api methods."""
    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            a = [x for x in (args or [])]
            kw = {k: v for k, v in (kwargs or {}).items()}
            log.error('Error calling {}.{} {!r} {!r}'.format(fn.__module__, fn.__name__, a, kw), exc_info=True)
            exc_class, exc, tb = sys.exc_info()
            raise six.reraise(CrudException, CrudException(str(exc)), tb)
    return wrapped


def make_crud_api(Session):

    class CrudApi(object):

        @classmethod
        def _collect_models(cls, query):
            models = set()
            for d in listify(query):
                try:
                    model = Session.resolve_model(d['_model'])
                except Exception:
                    log.debug('unable to resolve model {} in query {}', d.get('_model'), d)
                else:
                    models.add(model)
                    for attr_name in _collect_fields(d):
                        curr_model = model
                        for prop_name in attr_name.split('.'):
                            if hasattr(curr_model, prop_name):
                                prop = getattr(curr_model, prop_name)
                                if isinstance(prop, InstrumentedAttribute) and hasattr(prop.property, 'mapper'):
                                    curr_model = prop.property.mapper.class_
                                    models.update([curr_model])
                                    if prop_name in d:
                                        subquery = deepcopy(d[prop_name])
                                        if isinstance(subquery, (list, set, tuple)) \
                                                and not filter(lambda x: isinstance(x, dict), subquery):
                                            subquery = {i: True for i in subquery}
                                        elif isinstance(subquery, six.string_types):
                                            subquery = {subquery: True}
                                        if isinstance(subquery, dict):
                                            subquery['_model'] = curr_model.__name__
                                        models.update(cls._collect_models(subquery))
                            else:
                                break
            return models

        @classmethod
        def _get_models(cls, *args, **kwargs):
            return {model.__name__ for model in cls._collect_models(_get_queries([args, kwargs]))}

        @classmethod
        def _sort_query(cls, query, model, sort):
            sort = normalize_sort(model, sort)
            for sorter in sort:
                dir = {'asc': asc, 'desc': desc}[sorter['dir']]
                field = sorter['field']
                if model:
                    field = getattr(model, field)
                    if issubclass(type(field.__clause_element__().type), String):
                        field = func.lower(field)
                query = query.order_by(dir(field))
            return query

        @classmethod
        def _limit_query(cls, query, limit, offset):
            if offset is not None:
                query = query.offset(offset)
            if limit is not None and limit != 0:
                query = query.limit(limit)
            return query

        # this only works in postgresql
        @classmethod
        def _distinct_query(cls, query, filters):
            distinct_clause = filters.get('distinct', None)
            if distinct_clause:
                if isinstance(distinct_clause, bool):
                    query = query.distinct()
                else:
                    model = Session.resolve_model(filters.get('_model'))
                    columns = [getattr(model, field) for field in distinct_clause]
                    query = query.distinct(*columns)
            return query

        @classmethod
        def _groupby_query(cls, query, filters):
            groupby_clause = filters.get('groupby', None)
            if groupby_clause:
                model = Session.resolve_model(filters.get('_model'))
                columns = [getattr(model, field) for field in groupby_clause]
                query = query.group_by(*columns)
            return query

        @classmethod
        def _filter_query(cls, query, model, filters=None, limit=None, offset=None, sort=None):
            if filters:
                query = cls._distinct_query(query, filters)
                query = cls._groupby_query(query, filters)
                filters = cls._resolve_filters(filters, model)
                if filters is not None:
                    query = query.filter(filters)
            if sort:
                query = cls._sort_query(query, model, sort)
            query = cls._limit_query(query, limit, offset)
            return query

        @classmethod
        def _resolve_comparison(cls, comparison, column, value):
            if isinstance(value, dict):
                model_class = Session.resolve_model(value.get('_model'))
                field = value.get('select', 'id')
                value = select([getattr(model_class, field)], cls._resolve_filters(value))

            return {
                'eq': lambda field, val: field == val,
                'ne': lambda field, val: field != val,
                'lt': lambda field, val: field < val,
                'le': lambda field, val: field <= val,
                'gt': lambda field, val: field > val,
                'ge': lambda field, val: field >= val,
                'in': lambda field, val: field.in_(val),
                'notin': lambda field, val: ~field.in_(val),
                'isnull': lambda field, val: field == None,
                'isnotnull': lambda field, val: field != None,
                'contains': lambda field, val: field.like('%'+val+'%'),
                'icontains': lambda field, val: field.ilike('%'+val+'%'),
                'like': lambda field, val: field.like('%'+val+'%'),
                'ilike': lambda field, val: field.ilike('%'+val+'%'),
                'startswith': lambda field, val: field.startswith(val),
                'endswith': lambda field, val: field.endswith(val),
                'istartswith': lambda field, val: field.ilike(val+'%'),
                'iendswith': lambda field, val: field.ilike('%'+val)
            }[comparison](column, value)  # noqa: E711

        @classmethod
        def _resolve_filters(cls, filters, model=None):
            model = Session.resolve_model(filters.get('_model', model))
            and_clauses = filters.get('and', None)
            or_clauses = filters.get('or', None)
            if and_clauses:
                return and_(*[cls._resolve_filters(c, model) for c in and_clauses])
            elif or_clauses:
                return or_(*[cls._resolve_filters(c, model) for c in or_clauses])
            elif 'field' in filters or 'value' in filters or 'comparison' in filters:
                field = filters.get('field', 'id').split('.')
                value = filters.get('value')
                comparison = filters.get('comparison', 'eq')

                if len(field) == 1:
                    column = getattr(model, field[0])
                    return cls._resolve_comparison(comparison, column, value)
                elif len(field) == 2:
                    property = field[0]
                    field = field[1]
                    related_table = class_mapper(model).get_property(property)
                    related_model = related_table.argument
                    if isinstance(related_model, Mapper):
                        related_model = related_model.class_
                    elif callable(related_model):
                        related_model = related_model()
                    related_field = getattr(related_model, field)

                    clause = cls._resolve_comparison(comparison, related_field, value)
                    if getattr(related_table, 'primaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            related_table.primaryjoin)
                    if getattr(related_table, 'secondaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            related_table.secondaryjoin)
                    return clause
                else:
                    property = field[0]
                    join_property = field[1]
                    field = field[2]

                    join_table = class_mapper(model).get_property(property)
                    join_model = join_table.argument

                    if isinstance(join_model, Mapper):
                        join_model = join_model.class_
                    elif callable(join_model):
                        join_model = join_model()

                    related_table = class_mapper(join_model).get_property(join_property)
                    related_model = related_table.argument
                    if isinstance(related_model, Mapper):
                        related_model = related_model.class_
                    elif callable(related_model):
                        related_model = related_model()
                    related_field = getattr(related_model, field)

                    clause = cls._resolve_comparison(comparison, related_field, value)
                    if getattr(join_table, 'primaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            join_table.primaryjoin)
                    if getattr(join_table, 'secondaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            join_table.secondaryjoin)

                    if getattr(related_table, 'primaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            related_table.primaryjoin)
                    if getattr(related_table, 'secondaryjoin', None) is not None:
                        clause = and_(
                            clause,
                            related_table.secondaryjoin)

                    return clause
            else:
                return None

        @crud_exceptions
        def count(self, query):
            """
            Count the model objects matching the supplied query parameters

            @param query: Specifies the model types to count. May be a string, a list
            of strings, or a list of dicts with a "_model" key specified.

            Returns:
                int: The count of each of the supplied model types, in a list of
                    dicts, like so::

                        [{
                            '_model' : 'Player',
                            '_label' : 'Player on a Team',
                            'count' : 12
                        }]

            """
            filters = normalize_query(query)
            results = []
            with Session() as session:
                for filter in filters:
                    model = Session.resolve_model(filter['_model'])
                    result = {'_model': filter['_model'],
                              '_label': filter.get('_label', filter['_model'])}
                    if getattr(model, '_crud_perms', {}).get('read', True):
                        if filter.get('groupby', False):
                            columns = []
                            for attr in filter['groupby']:
                                columns.append(getattr(model, attr))

                            rows = CrudApi._filter_query(
                                session.query(func.count(columns[0]), *columns), model, filter).all()
                            result['count'] = []
                            for row in rows:
                                count = {'count': row[0]}
                                index = 1
                                for attr in filter['groupby']:
                                    count[attr] = row[index]
                                    index += 1
                                result['count'].append(count)
                        else:
                            result['count'] = CrudApi._filter_query(session.query(model), model, filter).count()
                    results.append(result)
            return results

        @crud_exceptions
        def read(self, query, data=None, order=None, limit=None, offset=0):
            """
            Get the model objects matching the supplied query parameters,
            optionally setting which part of the objects are in the returned dictionary
            using the supplied data parameter

            Arguments:
                session (sqlalchemy.Session): SQLAlchemy session to use.
                query: one or more queries (as c{dict} or [c{dict}]), corresponding
                    to the format of the query parameter described in the module-level
                    docstrings. This query parameter will be normalized
                data: one or more data specification (as c{dict} or [c{dict}]),
                    corresponding to the format of the data specification parameter
                    described in the module-level docstrings. The length of the data
                    parameter should either be 1 which will be the spec for each query
                    specified, OR of length N, where N is the number of queries after
                    normalization. If not provided the _data parameter will be expected
                    in each query
                limit: The limit parameter, when provided with positive integer "L"
                    at most "L" results will be returned. Defaults to no limit
                offset: The offset parameter, when provided with positive integer
                    "F", at most "L" results will be returned after skipping the first "F"
                    results (first based on ordering)

            Returns:
                list: one or more data specification dictionaries with models that
                    match the provided queries including all readable fields without
                    following foreign keys (the default if no data parameter is included),
                    OR the key/values specified by the data specification parameter. The
                    number of items returned and the order in which they appear are
                    controlled by the limit, offset and order parameters. Represented as::

                        return {
                            total: <int> # count of ALL matching objects, separate from <limit>
                            results: [c{dict}, c{dict}, ... , c{dict}] # subject to <limit>
                        }

            """
            with Session() as session:
                filters = normalize_query(query)
                data = normalize_data(data, len(filters))
                if len(filters) == 1:
                    filter = filters[0]
                    model = Session.resolve_model(filter['_model'])
                    total = 0
                    results = []
                    if getattr(model, '_crud_perms', {}).get('read', True):
                        total = CrudApi._filter_query(session.query(model), model, filter).count()
                        results = CrudApi._filter_query(session.query(model), model, filter, limit, offset, order).all()

                    return {'total': total, 'results': [r.crud_read(data[0]) for r in results]}

                elif len(filters) > 1:
                    queries = []
                    count_queries = []
                    queried_models = []
                    sort_field_types = {}
                    for filter_index, filter in enumerate(filters):
                        model = Session.resolve_model(filter['_model'])
                        if getattr(model, '_crud_perms', {}).get('read', True):
                            queried_models.append(model)
                            query_fields = [
                                model.id,
                                cast(literal(model.__name__), Text).label("_table_name"),
                                cast(literal(filter_index), Integer)]

                            for sort_index, sort in enumerate(normalize_sort(model, order)):
                                sort_field = getattr(model, sort['field'])
                                sort_field_types[sort_index] = type(sort_field.__clause_element__().type)
                                query_fields.append(sort_field.label('anon_sort_{}'.format(sort_index)))
                            queries.append(CrudApi._filter_query(session.query(*query_fields), model, filter))
                            count_queries.append(CrudApi._filter_query(session.query(model.id), model, filter))

                    total = count_queries[0].union(*(count_queries[1:])).count()
                    query = queries[0].union(*(queries[1:]))
                    normalized_sort_fields = normalize_sort(None, order)
                    for sort_index, sort in enumerate(normalized_sort_fields):
                        dir = {'asc': asc, 'desc': desc}[sort['dir']]
                        sort_field = 'anon_sort_{}'.format(sort_index)
                        if issubclass(sort_field_types[sort_index], String):
                            sort_field = 'lower({})'.format(sort_field)
                        query = query.order_by(dir(sort_field))
                    if normalized_sort_fields:
                        query = query.order_by("_table_name")
                    rows = CrudApi._limit_query(query, limit, offset).all()

                    result_table = {}
                    result_order = {}
                    query_index_table = {}
                    for i, row in enumerate(rows):
                        id = str(row[0])
                        model = Session.resolve_model(row[1])
                        query_index = row[2]
                        result_table.setdefault(model, []).append(id)
                        result_order[id] = i
                        query_index_table[id] = query_index

                    for model, ids in result_table.items():
                        result_table[model] = session.query(model).filter(model.id.in_(ids)).all()

                    ordered_results = len(result_order) * [None]
                    for model, instances in result_table.items():
                        for instance in instances:
                            ordered_results[result_order[instance.id]] = instance
                    results = [r for r in ordered_results if r is not None]

                    return {'total': total, 'results': [r.crud_read(data[query_index_table[r.id]]) for r in results]}
                else:
                    return {'total': 0, 'results': []}

        @crud_exceptions
        def create(self, data):
            """
            Create a model object using the provided data specifications.

            Arguments:
                session (sqlalchemy.Session): SQLAlchemy session to use.
                data (dict or [dict]): One or more residue data dictionaries. A
                    new object will be created for each data specification
                    dictionary provided.

            Returns:
                list: A list containing the newly created objects.

            """
            data = normalize_data(data)
            if any('_model' not in attrs for attrs in data):
                raise CrudException('_model is required to create a new item')

            created = []
            with Session() as session:
                for attrs in data:
                    model = Session.resolve_model(attrs['_model'])
                    instance = model()
                    session.add(instance)
                    instance.crud_create(**attrs)
                    session.flush()  # any items that were created should now be queryable
                    created.append(instance.crud_read())
            return created

        @crud_exceptions
        def update(self, query, data):
            """
            Updates the model objects matching the supplied query parameter.

            The matching objects will be updated according to the fields and values
            specified in the data parameter.

            Arguments:
                session (sqlalchemy.Session): SQLAlchemy session to use.
                query (dict or [dict]): One or more residue query dictionaries.
                    This query parameter will be normalized.
                data (dict or [dict]): One or more residue data dictionaries. The
                    length of the data parameter should be N, where N is the number
                    of queries after normalization.

            Returns:
                bool: True if the objects were successfully updated.

            """
            filters = normalize_query(query)
            data = normalize_data(data, len(filters))
            with Session() as session:
                for filter, attrs in zip(filters, data):
                    model = Session.resolve_model(filter['_model'])
                    for instance in CrudApi._filter_query(session.query(model), model, filter):
                        instance.crud_update(**attrs)
                        # any items that were created should now be queryable
                        session.flush()
            return True

        @crud_exceptions
        def delete(self, query):
            """
            Delete the model objects matching the supplied query parameters

            Arguments:
                session (sqlalchemy.Session): SQLAlchemy session to use.
                query (dict or [dict]): One or more residue query dictionaries.
                    This query parameter will be normalized.

            Returns:
                int: The number of objects successfully deleted.

            """
            deleted = 0
            filters = normalize_query(query)
            with Session() as session:
                for filter in filters:
                    model = Session.resolve_model(filter['_model'])
                    if getattr(model, '_crud_perms', {}).get('can_delete', False):
                        to_delete = CrudApi._filter_query(session.query(model), model, filter)
                        count = to_delete.count()
                        assert count in [0, 1], "each query passed to crud.delete must return at most 1 item"
                        if count == 1:
                            # don't log if there wasn't actually a deletion
                            item_to_delete = to_delete.one()
                            session.delete(item_to_delete)
                            deleted += count
            return deleted

    return CrudApi()
