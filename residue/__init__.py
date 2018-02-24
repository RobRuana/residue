# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQLAlchemy CRUD Utilities"""

from __future__ import absolute_import
import inspect
import re
import uuid
from types import MethodType

import six
import sqlalchemy
from pockets import camel, uncamel, collect_subclasses
from pockets.autolog import log
from sqlalchemy.ext import declarative
from sqlalchemy.orm import configure_mappers, sessionmaker, Query

from residue._version import __version__  # noqa: F401
from residue.crud.api import make_crud_api
from residue.crud.orm import CrudModelMixin

from residue.crud import *  # noqa: F401,F403
from residue.query import *  # noqa: F401,F403
from residue.types import *  # noqa: F401,F403


NAMESPACE_SQL = uuid.UUID('75c7e3be-a5c7-414d-bc66-d64ae5d03f3d')
RE_NONCHAR_WHITESPACE = re.compile('[\W\s]+')


def check_constraint_naming_convention(constraint, table):
    """
    Creates a unique name for an unnamed CheckConstraint.

    The generated name is the SQL text of the CheckConstraint with
    non-alphanumeric, non-underscore operators converted to text, and all
    other non-alphanumeric, non-underscore substrings replaced by underscores.

    If the generated name is longer than 32 characters, a uuid5 based on the
    generated name will be returned instead.

    >>> from sqlalchemy import CheckConstraint, MetaData, Table
    >>> table = Table('account', MetaData())
    >>> constraint = CheckConstraint('failed_logins > 3')

    See: http://docs.sqlalchemy.org/en/latest/core/constraints.html#configuring-constraint-naming-conventions

    """
    # The text of the replacements doesn't matter, so long as it's unique
    replacements = [
        ('||/', 'cr'), ('<=', 'le'), ('>=', 'ge'), ('<>', 'nq'), ('!=', 'ne'),
        ('||', 'ct'), ('<<', 'ls'), ('>>', 'rs'), ('!!', 'fa'), ('|/', 'sr'),
        ('@>', 'cn'), ('<@', 'cb'), ('&&', 'an'), ('<', 'lt'), ('=', 'eq'),
        ('>', 'gt'), ('!', 'ex'), ('"', 'qt'), ('#', 'hs'), ('$', 'dl'),
        ('%', 'pc'), ('&', 'am'), ('\'', 'ap'), ('(', 'lpr'), (')', 'rpr'),
        ('*', 'as'), ('+', 'pl'), (',', 'cm'), ('-', 'da'), ('.', 'pd'),
        ('/', 'sl'), (':', 'co'), (';', 'sc'), ('?', 'qn'), ('@', 'at'),
        ('[', 'lbk'), ('\\', 'bs'), (']', 'rbk'), ('^', 'ca'), ('`', 'tk'),
        ('{', 'lbc'), ('|', 'pi'), ('}', 'rbc'), ('~', 'td')]

    constraint_name = str(constraint.sqltext).strip()
    for operator, text in replacements:
        constraint_name = constraint_name.replace(operator, text)

    constraint_name = RE_NONCHAR_WHITESPACE.sub('_', constraint_name)
    if len(constraint_name) > 32:
        constraint_name = uuid.uuid5(NAMESPACE_SQL, str(constraint_name)).hex
    return constraint_name


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


# SQLAlchemy doesn't expose its default constructor as a nicely importable
# function, so we grab it from the function defaults.
_spec_args, _spec_varargs, _spec_kwargs, _spec_defaults = inspect.getargspec(declarative.declarative_base)
declarative_base_constructor = dict(zip(reversed(_spec_args), reversed(_spec_defaults)))['constructor']


def declarative_base(*orig_args, **orig_kwargs):
    """
    Replacement for SQLAlchemy's declarative_base, which adds these features:
    1) This is a decorator.
    2) This allows your base class to set a constructor.
    3) This provides a default constructor which automatically sets defaults
       instead of waiting to do that until the object is committed.
    4) Automatically setting __tablename__ to snake-case.
    5) Automatic integration with the SessionManager class.
    """
    orig_args = list(orig_args)

    def _decorate_base_class(klass):

        class Mixed(klass, CrudModelMixin):
            def __init__(self, *args, **kwargs):
                """
                Variant on SQLAlchemy model __init__ which sets default values on
                initialization instead of immediately before the model is saved.
                """
                if '_model' in kwargs:
                    assert kwargs.pop('_model') == self.__class__.__name__
                declarative_base_constructor(self, *args, **kwargs)
                for attr, col in self.__table__.columns.items():
                    if kwargs.get(attr) is None and col.default:
                        self.__dict__.setdefault(attr, col.default.execute())

        orig_kwargs['cls'] = Mixed
        if 'name' not in orig_kwargs:
            orig_kwargs['name'] = klass.__name__
        if 'constructor' not in orig_kwargs:
            orig_kwargs['constructor'] = klass.__init__ if '__init__' in klass.__dict__ else Mixed.__init__

        Mixed = declarative.declarative_base(*orig_args, **orig_kwargs)
        Mixed.BaseClass = _SessionInitializer._base_classes[klass.__module__] = Mixed
        Mixed.__tablename__ = declarative.declared_attr(lambda cls: uncamel(cls.__name__))
        return Mixed

    is_class_decorator = not orig_kwargs \
        and len(orig_args) == 1 \
        and inspect.isclass(orig_args[0]) \
        and not isinstance(orig_args[0], sqlalchemy.engine.Connectable)

    if is_class_decorator:
        return _decorate_base_class(orig_args.pop())
    else:
        return _decorate_base_class


class _SessionInitializer(type):
    _base_classes = {}

    def __new__(cls, name, bases, attrs):
        SessionClass = type.__new__(cls, name, bases, attrs)
        if hasattr(SessionClass, 'engine'):
            if not hasattr(SessionClass, 'BaseClass'):
                for module, bc in _SessionInitializer._base_classes.items():
                    if module == SessionClass.__module__:
                        SessionClass.BaseClass = bc
                        break
                else:
                    raise AssertionError('No BaseClass specified and @declarative_base was never invoked in {}'.format(
                        SessionClass.__module__))
            if not hasattr(SessionClass, 'session_factory'):
                SessionClass.session_factory = sessionmaker(
                    bind=SessionClass.engine, autoflush=False, autocommit=False, query_cls=SessionClass.QuerySubclass)

            SessionClass.initialize_db()
            SessionClass.crud = make_crud_api(SessionClass)
        return SessionClass


@six.add_metaclass(_SessionInitializer)
class SessionManager(object):
    class SessionMixin(object):
        pass

    class QuerySubclass(Query):
        pass

    def __init__(self):
        self.session = self.session_factory()
        for name, val in self.SessionMixin.__dict__.items():
            if not name.startswith('__'):
                assert not hasattr(self.session, name) and hasattr(val, '__call__')
                setattr(self.session, name, MethodType(val, self.session))

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type is None:
                self.session.commit()
        finally:
            self.session.close()

    def __del__(self):
        if self.session.transaction._connections:
            log.error('SessionManager went out of scope without underlying connection being closed; '
                      'did you forget to use it as a context manager?')
            self.session.close()

    @classmethod
    def initialize_db(cls, drop=False, create=True):
        configure_mappers()
        cls.BaseClass.metadata.bind = cls.engine
        if drop:
            cls.BaseClass.metadata.drop_all(cls.engine, checkfirst=True)
        if create:
            cls.BaseClass.metadata.create_all(cls.engine, checkfirst=True)

    @classmethod
    def all_models(cls):
        return collect_subclasses(cls.BaseClass)

    @classmethod
    def resolve_model(cls, name):
        if inspect.isclass(name) and issubclass(name, cls.BaseClass):
            return name

        subclasses = {ModelClass.__name__: ModelClass for ModelClass in cls.all_models()}
        permutations = [name, camel(name), camel(name, upper_segments=0)]
        for name in permutations:
            if name in subclasses:
                return subclasses[name]

            if name.lower().endswith('s'):
                singular = name.rstrip('sS')
                if singular in subclasses:
                    return subclasses[singular]

            if name.lower().endswith('ies'):
                singular = name[:-3] + 'y'
                if singular in subclasses:
                    return subclasses[singular]

        for name in permutations:
            if name in cls.BaseClass.metadata.tables:
                return cls.BaseClass.metadata.tables[name]

        raise ValueError('Unrecognized model: {}'.format(name))
