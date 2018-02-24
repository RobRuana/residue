# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""CRUD support for SQLAlchemy orm objects."""

from __future__ import absolute_import
import inspect
import re
import uuid
from collections import defaultdict, Callable, Mapping
from copy import deepcopy
from itertools import chain
from datetime import date, datetime, time

import six
from pockets import cached_classproperty, classproperty, collect_superclass_attr_names, is_data, is_listy, mappify
from pockets.autolog import log
from sqlalchemy import orm
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.orm.properties import ColumnProperty, RelationshipProperty
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql import ClauseElement
from sqlalchemy.types import Boolean, DateTime, Integer, String, Text, UnicodeText

try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache  # noqa: F401


__all__ = [
    'get_model_by_table', 'get_primary_key_column_names', 'get_unique_constraint_column_names',
    'get_one_to_many_foreign_key_column_name', 'CrudModelMixin', 'crudable', 'crud_validation',
    'text_length_validation', 'regex_validation']


@lru_cache()
def get_model_by_table(base, table):
    """
    Returns declarative class associated with given table.

    Arguments:
        base (sqlalchemy.ext.declarative.api.Base): Declarative model base or
            a subclass of the declarative model base.
        table (sqlalchemy.sql.schema.Table): SQLAlchemy Table object.

    Returns:
        class: Declarative class or None if not found.
    """
    for cls in base._decl_class_registry.values():
        if hasattr(cls, '__table__') and cls.__table__ is table:
            return cls
    return None


@lru_cache()
def get_one_to_many_foreign_key_column_name(model, name):
    """
    Returns the constituent column names for the foreign key on the remote
    table of the one-to-many relationship specified by name.

    Args:
        model (class or object): The given model class or model instance.
        name (string): The name of the attribute on `model` which is a
            one-to-many relationship.

    Return:
        list: One-to-many foreign key column names as a list of strings.
    """
    if not inspect.isclass(model):
        return get_one_to_many_foreign_key_column_name(model.__class__, name)

    attr = getattr(model, name, None)
    if not attr:
        # Unknown attribute.
        return []

    remote_columns = getattr(attr.property, 'remote_side', None)
    if not remote_columns:
        # This is not a one-to-many relationship.
        return []

    remote_tables = set(c.table.name for c in remote_columns)
    if len(remote_tables) > 1:
        # This is a many-to-many relationship with a cross reference table.
        return []

    foreign_key_column_names = []
    for remote_column in remote_columns:
        if getattr(remote_column, 'foreign_keys', False):
            foreign_key_column_names.append(remote_column.name)
        else:
            remote_model = get_model_by_table(model, remote_column.table)
            if remote_model:
                # Quasi foreign keys don't actually have foreign_keys set,
                # but they need to be treated as though they did.
                foreign_keys = getattr(remote_model, 'quasi_foreign_keys', [])
                if remote_column.name in foreign_keys:
                    foreign_key_column_names.append(remote_column.name)

    return foreign_key_column_names


@lru_cache()
def get_primary_key_column_names(model):
    """
    Returns the constituent column names for the primary key of the given
    model.

    Args:
        model (class or object): The given model class or model instance.

    Return:
        list: Primary key column names as a list of strings.
    """
    return [column.name for column in model.__table__.primary_key.columns]


@lru_cache()
def get_unique_constraint_column_names(model):
    """
    Returns the constituent column names for each unique constraint on the
    given model.

    Args:
        model (class or object): The given model class or model instance.

    Return:
        list: Unique constraint column names as a list of lists of strings.
    """
    return [[column.name for column in constraint.columns]
            for constraint in model.__table__.constraints
            if isinstance(constraint, UniqueConstraint)]


class CrudModelMixin(object):
    extra_defaults = []
    type_casts = {uuid.UUID: str}
    type_map = {}
    type_map_defaults = {
        int: 'int',
        six.binary_type: 'string',
        six.text_type: 'string',
        float: 'float',
        datetime: 'date',
        date: 'date',
        time: 'date',
        bool: 'boolean',
        uuid.UUID: 'string',
        String: 'string',
        UnicodeText: 'string',
        Text: 'string',
        DateTime: 'date',
        Integer: 'int',
        Boolean: 'boolean',
    }

    # Override what attributes will show in the repr. Defaults to primary keys
    # and unique constraints.
    repr_attr_names = ()

    # In addition to any default attributes, also show these in the repr.
    extra_repr_attr_names = ()

    # columns that are used as foreign keys, but don't have an explicit
    # foreign key constraint
    quasi_foreign_keys = ()

    @classmethod
    def _create_or_fetch(cls, session, value, **backref_mapping):
        """
        Fetch an existing or create a new instance of this class. Fetching uses
        the values from the value positional argument (the id if available, or
        if any keys that correspond to unique constraints are present). In both
        cases the instance will still need to be updated using whatever new
        values you want.

        Args:
            cls (class): The class object we're going to fetch or create
            session (Session): the session object
            value (any): the dictionary value to fetch with
            **backref_mapping: the backref key name and value of the "parent"
                object of the object you're fetching or about to create. If
                the backref value of a fetched instance is not the same as the
                value of what's passed in, we will instead create a new
                instance. This is because we want to prevent "stealing" an
                existing object in a one-to-one relationship unless an id is
                explicitly passed.

        Returns:
            A previously existing or newly created (and added to the session)
            model instance.
        """
        assert len(backref_mapping) <= 1, 'only one backref key is allowed at this time: {}'.format(backref_mapping)

        if backref_mapping:
            backref_name = list(backref_mapping.keys())[0]
            parent_id = backref_mapping[backref_name]
        else:
            backref_name, parent_id = None, None

        id = None
        if isinstance(value, Mapping):
            id = value.get('id', None)
        elif isinstance(value, six.string_types):
            id = value

        instance = None
        if id is not None:
            try:
                instance = session.query(cls).filter(cls.id == id).first()
            except Exception:
                log.error('Unable to fetch instance based on id value {!r}', value, exc_info=True)
                raise TypeError('Invalid instance ID type for relation: {} (value: {})'.format(cls.__name__, value))
        elif isinstance(value, Mapping):
            # If there's no id, check to see if we have a dictionary that
            # includes all of the columns associated with a UniqueConstraint.
            for column_names in cls.unique_constraint_column_names:
                if all((s in value and value[s]) for s in column_names):
                    # all those column names are provided,
                    # use that to query by chaining together all the necessary
                    # filters to construct that query
                    q = session.query(cls)
                    filter_kwargs = dict((s, value[s]) for s in column_names)
                    try:
                        instance = q.filter_by(**filter_kwargs).one()
                    except NoResultFound:
                        continue
                    except MultipleResultsFound:
                        log.error('Multiple results found for {} unique constraint: {}', cls.__name__, column_names)
                        raise
                    else:
                        break
                else:
                    log.debug('Unable to search using unique constraints: {} with {}', column_names, value)

        if instance and id is None and backref_mapping and getattr(instance, backref_name, None) != parent_id:
            log.warning(
                'Attempting to change the owner of {} without an explicitly '
                'passed id; a new {} instance will be used instead',
                instance, cls.__name__)
            instance = None

        if not instance:
            log.debug('creating new: {} with id {}', cls.__name__, id)
            if id is None:
                instance = cls()
            else:
                instance = cls(id=id)
            session.add(instance)
        return instance

    @property
    @lru_cache()
    def _type_casts_for_to_dict(self):
        type_casts = CrudModelMixin.type_casts.copy()
        type_casts.update(self.type_casts)
        return defaultdict(lambda: lambda x: x, type_casts)

    @classproperty
    def to_dict_default_attrs(cls):
        attr_names = []
        super_attr_names = collect_superclass_attr_names(cls, terminal_class=cls.BaseClass)
        for name in super_attr_names:
            if not name.startswith('_') or name in cls.extra_defaults:
                attr = getattr(cls, name)
                descriptor = getattr(attr, 'descriptor', None)

                is_attr = not (
                    callable(attr)
                    or isinstance(descriptor, hybrid_property)
                    or isinstance(attr, (property, InstrumentedAttribute, ClauseElement)))
                is_instrumented = isinstance(attr, InstrumentedAttribute) and isinstance(attr.property, ColumnProperty)

                if is_attr or is_instrumented:
                    attr_names.append(name)
        return attr_names

    def to_dict(self, attrs=None, validator=lambda self, name: True):
        obj = {}
        if attrs is not None:
            attrs = mappify(attrs)

        # It's still possible for the client to blacklist this, but by default
        # we're going to include them.
        if attrs is None or attrs.get('_model', True):
            obj['_model'] = self.__class__.__name__
        if attrs is None or attrs.get('id', True):
            obj['id'] = self.id

        def cast_type(value):
            # Ensure that certain types are cast appropriately for daily usage
            # e.g. we want the result of HashedPasswords to be the string
            # representation instead of the object.
            return self._type_casts_for_to_dict[value.__class__](value)

        if attrs is None:
            for name in self.to_dict_default_attrs:
                if validator(self, name):
                    obj[name] = cast_type(getattr(self, name))
        else:
            for name in self.extra_defaults + list(attrs.keys()):
                # If we're not supposed to get the attribute according to the
                # validator, OR the client intentionally blacklisted it, then
                # skip this value.
                if not validator(self, name) or not attrs.get(name, True):
                    continue
                attr = getattr(self, name, None)
                if isinstance(attr, self.BaseClass):
                    obj[name] = attr.to_dict(attrs[name], validator)
                elif isinstance(attr, (list, set, tuple, frozenset)):
                    obj[name] = []
                    for item in attr:
                        if isinstance(item, self.BaseClass):
                            obj[name].append(
                                item.to_dict(attrs[name], validator))
                        else:
                            obj[name].append(item)
                elif callable(attr):
                    obj[name] = cast_type(attr())
                else:
                    obj[name] = cast_type(attr)

        return obj

    def from_dict(self, attrs, validator=lambda self, name, val: True):
        relations = []
        # Merge_relations modifies the dictionaries that are passed to it in
        # order to support updates in deeply-nested object graphs. To ensure
        # that we don't have dirty state between applying updates to different
        # model objects, we need a fresh copy.
        attrs = deepcopy(attrs)
        for name, value in attrs.items():
            if not name.startswith('_') and validator(self, name, value):
                attr = getattr(self.__class__, name)
                if isinstance(attr, InstrumentedAttribute) and isinstance(attr.property, RelationshipProperty):
                    relations.append((name, value))
                else:
                    setattr(self, name, value)

        def required(kv):
            cols = list(getattr(self.__class__, kv[0]).property.local_columns)
            return len(cols) != 1 or cols[0].primary_key or cols[0].nullable
        relations.sort(key=required)

        for name, value in relations:
            self._merge_relations(name, value, validator)

        return self

    @classproperty
    def primary_key_column_names(cls):
        return get_primary_key_column_names(cls)

    @classproperty
    def unique_constraint_column_names(cls):
        return get_unique_constraint_column_names(cls)

    @classmethod
    def one_to_many_foreign_key_column_name(cls, name):
        column_names = get_one_to_many_foreign_key_column_name(cls, name)
        return column_names[0] if column_names else None

    def _merge_relations(self, name, value, validator=lambda self, name, val: True):
        attr = getattr(self.__class__, name)
        if not isinstance(attr, InstrumentedAttribute) or not isinstance(attr.property, RelationshipProperty):
            return

        session = orm.Session.object_session(self)
        assert session, "Can't call _merge_relations on objects not attached to a session"

        attr_property = attr.property
        relation_cls = attr_property.mapper.class_

        # E.g., if this a Team with many Players, and we're handling the
        # attribute name "players," we want to set the team_id on all
        # dictionary representations of those players.
        backref_id_name = self.one_to_many_foreign_key_column_name(name)
        original_value = getattr(self, name)

        if is_listy(original_value):
            new_instances = []
            if value is None:
                value = []

            if isinstance(value, six.string_types):
                value = [value]

            for item in value:
                if backref_id_name is not None and isinstance(item, dict) and not item.get(backref_id_name):
                    item[backref_id_name] = self.id

                relation_inst = relation_cls._create_or_fetch(
                    session, item, **{backref_id_name: self.id} if backref_id_name else {})

                if isinstance(item, dict):
                    if relation_inst._sa_instance_state.identity:
                        validator = _crud_write_validator
                    else:
                        validator = _crud_create_validator
                    relation_inst.from_dict(item, validator)

                new_instances.append(relation_inst)

            relation = original_value
            remove_instances = [stale for stale in relation if stale not in new_instances]
            for stale_instance in remove_instances:
                relation.remove(stale_instance)
                if attr_property.cascade.delete_orphan:
                    session.delete(stale_instance)

            for new_instance in new_instances:
                if new_instance.id is None or new_instance not in relation:
                    relation.append(new_instance)

        elif isinstance(value, (Mapping, six.string_types)):
            if backref_id_name is not None and not value.get(backref_id_name):
                # If this is a dictionary, it's possible we're going to be
                # creating a new thing, if so, we'll add a backref to the
                # "parent" if one isn't already set.
                value[backref_id_name] = self.id

            relation_instance = relation_cls._create_or_fetch(session, value)
            stale_instance = original_value
            if stale_instance and stale_instance.id != relation_instance.id and attr_property.cascade.delete_orphan:
                session.delete(stale_instance)

            if isinstance(value, Mapping):
                relation_instance.from_dict(value, validator)
                # We want this this to be immediatelt queryable.
                session.flush([relation_instance])

            setattr(self, name, relation_instance)

        elif value is None:
            # The first branch handles the case of setting a many-to-one value
            # to None. So this is for the one-to-one-mapping case.
            # Setting a relation to None is nullifying the relationship, which
            # has potential side effects in the case of cascades, etc...
            setattr(self, name, value)
            stale_instance = original_value
            if stale_instance and attr_property.cascade.delete_orphan:
                session.delete(stale_instance)

        else:
            raise TypeError(
                'Merging relations on {} not supported for values of type: {} '
                '(value: {})'.format(name, value.__class__.__name__, value))

    def __setattr__(self, name, value):
        if name in getattr(self, '_validators', {}):
            for validator_dict in self._validators[name]:
                if not validator_dict['model_validator'](self, value):
                    message = validator_dict.get('validator_message')
                    raise ValueError(
                        'Validation failed for {}.{} with value {!r}: {}'
                        .format(self.__class__.__name__, name, value, message))
        object.__setattr__(self, name, value)

    def crud_read(self, attrs=None):
        return self.to_dict(attrs, validator=_crud_read_validator)

    def crud_create(self, **kwargs):
        return self.from_dict(kwargs, validator=_crud_create_validator)

    def crud_update(self, **kwargs):
        return self.from_dict(kwargs, validator=_crud_write_validator)

    def __repr__(self):
        """
        Useful string representation for logging.

        Note:
            __repr__ does NOT return unicode on Python 2, since python decodes
            it using the default encoding: http://bugs.python.org/issue5876.

        """
        # If no repr attr names have been set, default to the set of all
        # unique constraints. This is unordered normally, so we'll order and
        # use it here.
        if not self.repr_attr_names:
            # Flatten the unique constraint list
            _unique_attrs = chain(*self.unique_constraint_column_names)
            _primary_keys = self.primary_key_column_names

            attr_names = tuple(sorted(set(chain(_unique_attrs, _primary_keys, self.extra_repr_attr_names))))
        else:
            attr_names = self.repr_attr_names

        if not attr_names and hasattr(self, 'id'):
            # There should be SOMETHING, so use id as a fallback.
            attr_names = ('id',)

        if attr_names:
            _kwarg_list = ' '.join('{}={!r}'.format(s, getattr(self, s, 'undefined')) for s in attr_names)
            kwargs_output = ' {}'.format(_kwarg_list)
        else:
            kwargs_output = ''

        # Specifically using the string interpolation operator and the repr of
        # getattr so as to avoid any "hilarious" encode errors for non-ascii
        # characters.
        u_str = '<%s%s>' % (self.__class__.__name__, kwargs_output)
        return u_str if six.PY3 else u_str.encode('utf-8')


def _crud_read_validator(self, name):
    _crud_perms = getattr(self, '_crud_perms', None)
    if _crud_perms is not None and not _crud_perms.get('read', True):
        raise ValueError('Attempt to read non-readable model {}'.format(self.__class__.__name__))
    elif name in self.extra_defaults:
        return True
    elif _crud_perms is None:
        return not name.startswith('_')
    else:
        return name in _crud_perms.get('read', {})


def _crud_write_validator(self, name, value=None):
    _crud_perms = getattr(self, '_crud_perms', None)
    if getattr(self, name, None) == value:
        return True
    elif not _crud_perms or not _crud_perms.get('update', False):
        raise ValueError('Attempt to update non-updateable model {}'.format(self.__class__.__name__))
    elif name not in _crud_perms.get('update', {}):
        raise ValueError('Attempt to update non-updateable attribute {}.{}'.format(self.__class__.__name__, name))
    else:
        return name in _crud_perms.get("update", {})


def _crud_create_validator(self, name, value=None):
    _crud_perms = getattr(self, '_crud_perms', {})
    if not _crud_perms or not _crud_perms.get('can_create', False):
        raise ValueError('Attempt to create non-createable model {}'.format(self.__class__.__name__))
    else:
        return name in _crud_perms.get("create", {})


class crudable(object):
    """
    Decorator that specifies which model attributes are part of the CRUD API.

    Intended to be used on SQLAlchemy model classes, for example::

        @crudable(
            create=True,
            read=['__something'],
            no_read=['password'],
            update=[],
            no_update=[],
            delete=True,
            data_spec={
                attr={
                    read=True,
                    update=True,
                    desc='description'
                    defaultValue=<some default>
                    validators={<validator_name>, <validator value>}
                }
            }
        )
        class MyModelObject(Base):
            # ...

    The resulting object will have a class attribute named "crud_spec" which
    is a dictionary like::

        {
            create: True/False,
            read: {<attribute name>, <attribute name>},
            update: {<attribute name>, <attribute name>},
            delete: True/False,
            data_spec: {
                manually_specified_attr: {
                    desc: 'description',
                    type: '<type>'
                    read: True/False # only needed if attribute is unspecified
                    update": True/False
                }
                attr_with_manual_description: {
                    desc: 'description',
                    type: '<type>'
                }
            }
        }

    Attributes:
        never_read (tuple): Names of attributes that default to being not
            readable.
        never_update (tuple): Names of attribute that default to being not
            updatable.
        always_create (tuple): Names of attributes that default to being always
            creatable.
        default_labels (dict): Attribute name and label pairs, to simplify
            setting the same label for each and every instance of an attribute
            name.
    """

    never_read = ('metadata',)
    never_update = ('id',)
    always_create = ('id',)
    default_labels = {}

    def __init__(
            self,
            can_create=True,
            create=None,
            no_create=None,
            read=None,
            no_read=None,
            update=None,
            no_update=None,
            can_delete=True,
            data_spec=None):
        """
        Args:
            can_create (bool): If True (default), the decorated class can be
                created.
            create (collections.Iterable): If provided, interpreted as the
                attribute names that can be specified when the object is
                created in addition to the items are updateable. If not
                provided (default) all attributes that can be updated plus the
                primary key are allowed to be passed to the create method.
            no_create (collections.Iterable): If provided, interpreted as the
                attribute names that will not be allowed to be passed to
                create, taking precedence over anything specified in the create
                parameter. If not provided (default) everything allowed by the
                create parameter will be acceptable.
            read (collections.Iterable): If provided, interpreted as the
                attribute names that can be read, and ONLY these names can be
                read. If not provided (default) all attributes not starting
                with an underscore (e.g. __str__, or _hidden) will be readable,
            no_read (collections.Iterable): if provided, interpreted as the
                attribute names that can't be read, taking precedence over
                anything specified in the read parameter. If not provided
                (default) everything allowed by the read parameter will be
                readable.
            update (collections.Iterable): If provided, interpreted as the
                attribute names that can be updated, in addition to the list of
                items are readable. If None (default) default to the list of
                readable attributes. Pass an empty iterable to use the default
                behavior listed under the read docstring if there were
                attributes passed to read that you don't want update to default
                to.
            no_update (collections.Iterable): if provided, interpreted as the
                attribute names that can't be updated, taking precedence over
                anything specified in the update parameter. If None (default)
                default to the list of non-readable attributes. Pass an empty
                iterable to use the default behavior listed under the no_read
                docstring if there were attributes passed to no_read that you
                don't want no_update to default to.
            can_delete (bool): If True (default), the decorated class can be
                deleted.
            data_spec (dict): Any additional information that should be added
                to the `model.get_crud_definition`. See that function for
                complete documentation, but the key items are:
                "desc" - Human-readable description, will default to docstrings
                    if available, else not be present in the final spec.
                "label" - a Human-readable short label to help remember the
                    purpose of a particular field, without going into detail.
                    If not specifically provided, it will not be present in the
                    spec.
                "type" - the human-readable "type" for an attribute meaning
                    that a conversion to this type will be performed on the
                    server. If possible this will be determined automatically
                    using isinstance(), otherwise "auto" will be set:
                        auto (default) - no type conversion
                        string - `str`
                        boolean - `bool`
                        int - `int`
                        float - `float`
                "defaultValue" - the value that is considered the default,
                    either because a model instance will use this default value
                    if unspecified, or a client should present this option as
                    the default for a user
                "validators" - a `dict` mapping a validator name (e.g. "max")
                    and the value to be used in validation (e.g. 1000, for a
                    max value of 1000). This is intended to support client side
                    validation.
        """

        self.can_create = can_create
        self.can_delete = can_delete
        if no_update is not None and create is None:
            create = deepcopy(no_update)
        self.read = read or []
        self.no_read = no_read or []
        self.update = update or []
        self.no_update = no_update or [x for x in self.no_read if x not in self.update]
        self.create = create or []
        self.no_create = no_create or [x for x in self.no_update if x not in self.create]

        self.no_read.extend(self.never_read)
        self.no_update.extend(self.never_update)

        self.data_spec = data_spec or {}

    def __call__(self, cls):

        def _crud_perms(cls):
            crud_perms = {
                'can_create': self.can_create,
                'can_delete': self.can_delete,
                'read': [],
                'update': [],
                'create': []
            }

            read = self.read
            for name in collect_superclass_attr_names(cls):
                if not name.startswith('_'):
                    attr = getattr(cls, name)
                    properties = (InstrumentedAttribute, property, ClauseElement)
                    primitives = (int, float, bool, datetime, date, time, six.binary_type, six.text_type, uuid.UUID)
                    if isinstance(attr, properties) or isinstance(attr, primitives):
                        read.append(name)

            read = list(set(read))
            for name in read:
                if not self.no_read or name not in self.no_read:
                    crud_perms['read'].append(name)

            update = self.update + deepcopy(crud_perms['read'])
            update = list(set(update))
            for name in filter(lambda s: s not in self.no_update, update):
                if name in cls.__table__.columns:
                    crud_perms['update'].append(name)
                else:
                    attr = getattr(cls, name)
                    if (isinstance(attr, property) and getattr(attr, 'fset', False)) or (
                            isinstance(attr, InstrumentedAttribute)
                            and isinstance(attr.property, RelationshipProperty)
                            and attr.property.viewonly is not True):
                        crud_perms['update'].append(name)

            create = self.create + deepcopy(crud_perms['update'])
            for name in self.always_create:
                create.append(name)
                if name in self.no_create:
                    self.no_create.remove(name)

            create = list(set(create))
            for name in create:
                if not self.no_create or name not in self.no_create:
                    crud_perms['create'].append(name)

            cls._cached_crud_perms = crud_perms
            return cls._cached_crud_perms

        def _crud_spec(cls):
            crud_perms = cls._crud_perms

            field_names = list(
                set(crud_perms['read']) |
                set(crud_perms['update']) |
                set(crud_perms['create']) |
                set(self.data_spec.keys()))

            fields = {}
            for name in field_names:
                validators = getattr(cls, '_validators', {}).get(name, [])
                # If using different validation decorators or in the data spec
                # causes multiple spec kwargs to be specified, we're going to
                # error here for duplicate keys in dictionaries. Since we don't
                # want to allow two different expected values for maxLength
                # being sent in a crud spec for example.
                field_validator_kwargs = {
                    key: value
                    # Collect each spec_kwarg for all validators of an attr.
                    for validator in validators
                    for key, value in validator.get('spec_kwargs', {}).items()
                }

                if field_validator_kwargs:
                    self.data_spec.setdefault(name, {})
                    # Manually specified crud validator keyword arguments
                    # overwrite the decorator-supplied keyword arguments.
                    field_validator_kwargs.update(self.data_spec[name].get('validators', {}))
                    self.data_spec[name]['validators'] = field_validator_kwargs

                field = deepcopy(self.data_spec.get(name, {}))
                field['name'] = name
                try:
                    attr = getattr(cls, name)
                except AttributeError:
                    # If the object doesn't have the attribute, AND it's in the
                    # field list, that means we're assuming it was manually
                    # specified in the data_spec argument.
                    fields[name] = field
                    continue

                field['read'] = name in crud_perms['read']
                field['update'] = name in crud_perms['update']
                field['create'] = name in crud_perms['create']

                if field['read'] or field['update'] or field['create']:
                    fields[name] = field
                elif name in fields:
                    del fields[name]
                    continue

                if 'desc' not in field and not is_data(attr):
                    # No desc specified, but there's a relevant docstring,
                    # so we use that instead.

                    # If there's 2 consecutive newlines, assume that there's a
                    # separator in the docstring and that the top part only
                    # is the description, if there's not, use the whole thing.
                    # Either way, replace newlines with spaces since docstrings
                    # often break the same sentence accross lines due to space.
                    doc = inspect.getdoc(attr)
                    if doc:
                        doc = doc.partition('\n\n')[0].replace('\n', ' ').strip()
                        field['desc'] = doc

                if 'type' not in field:
                    if isinstance(attr, InstrumentedAttribute) and isinstance(attr.property, ColumnProperty):
                        col = attr.property.columns[0]
                        col_type = type(col.type)
                        field['type'] = cls._type_map.get(col_type, 'auto')
                        field_default = getattr(col, 'default', None)
                        # Only put the default here if it exists, and it's not
                        # an automatic thing like "time.utcnow()".
                        if field_default is not None \
                                and field['type'] != 'auto' \
                                and not isinstance(field_default.arg, (Callable, property)):
                            field['defaultValue'] = field_default.arg
                    elif hasattr(attr, "default"):
                        field['defaultValue'] = attr.default
                    else:
                        field['type'] = cls._type_map.get(type(attr), 'auto')
                        # Only set a default if this isn't a property or some
                        # other kind of "constructed attribute".
                        if field['type'] != 'auto' and not isinstance(attr, (Callable, property)):
                            field['defaultValue'] = attr
                if isinstance(attr, InstrumentedAttribute) and isinstance(attr.property, RelationshipProperty):
                    field['_model'] = attr.property.mapper.class_.__name__

            crud_spec = {'fields': fields}
            cls._cached_crud_spec = crud_spec
            return cls._cached_crud_spec

        def _type_map(cls):
            return dict(cls.type_map_defaults, **cls.type_map)

        cls._type_map = cached_classproperty(_type_map)
        cls._crud_spec = cached_classproperty(_crud_spec)
        cls._crud_perms = cached_classproperty(_crud_perms)
        return cls


class crud_validation(object):
    """
    Base class for adding validators to a model.

    Supports adding to the crud spec, or to the save action.
    """

    def __init__(self, attr_name, model_validator, validator_message, **spec_kwargs):
        """

        Args:
            attr_name (str): The attribute to which this validator applies.
            model_validator (callable): A callable that accepts the attribute
                value and returns False or None if invalid, or True if the
                value is valid.
            validator_message (str): Failure message if the validation fails.
            **spec_kwargs: The key/value pairs that should be added to the
                the crud spec for this attribute name. This generally supports
                making the same sorts of validations in a client (e.g.
                javascript).
        """
        self.attr_name = attr_name
        self.model_validator = model_validator
        self.validator_message = validator_message
        self.spec_kwargs = spec_kwargs

    def __call__(self, cls):
        if not hasattr(cls, '_validators'):
            cls._validators = {}
        else:
            # In case we subclass something with a _validators attribute.
            cls._validators = deepcopy(cls._validators)

        cls._validators.setdefault(self.attr_name, []).append({
            'model_validator': self.model_validator,
            'validator_message': self.validator_message,
            'spec_kwargs': self.spec_kwargs
        })
        return cls


class text_length_validation(crud_validation):
    """
    Validates the length of a text attribute.
    """
    def __init__(
            self, attr_name, min_length=None, max_length=None,
            min_text='The minimum length of this field is {0}.',
            max_text='The maximum length of this field is {0}.',
            allow_none=True):

        def model_validator(instance, text):
            if text is None:
                return allow_none
            text_length = len(six.text_type(text))
            return all([min_length is None or text_length >= min_length,
                        max_length is None or text_length <= max_length])

        kwargs = {}
        if min_length is not None:
            kwargs['minLength'] = min_length
            if max_text is not None:
                kwargs['minLengthText'] = min_text
        if max_length is not None:
            kwargs['maxLength'] = max_length
            if max_text is not None:
                kwargs['maxLengthText'] = max_text

        message = 'Length of value should be between {} and {} (inclusive; ' \
            'None means no min/max).'.format(min_length, max_length)

        crud_validation.__init__(self, attr_name, model_validator, message, **kwargs)


class regex_validation(crud_validation):
    """
    Validates a text field against the given regular expression.
    """
    def __init__(self, attr_name, regex, message):

        def regex_validator(instance, text):
            # If the field isn't nullable, that will trigger an error later
            # at the SQLAlchemy level, but since None can't be passed to
            # re.search() we want to pass this validation check.
            if text is None:
                return True

            return re.search(regex, text) is not None

        crud_validation.__init__(
            self,
            attr_name,
            regex_validator,
            message,
            regexText=message,
            regexString=regex)
