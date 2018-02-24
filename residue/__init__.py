# -*- coding: utf-8 -*-
# Copyright (c) 2017 the Residue team, see AUTHORS.
# Licensed under the BSD License, see LICENSE for details.

"""SQLAlchemy CRUD Utilities"""

# flake8: noqa

from __future__ import absolute_import
from residue._version import __version__
from residue.query import *
from residue.utils import *




# import inspect
# import json
# import re
# import types
# import uuid

# import six
# import sqlalchemy
# from pockets import camel, uncamel
# from sqlalchemy import event
# from sqlalchemy.dialects import postgresql
# from sqlalchemy.ext import declarative
# from sqlalchemy.orm import configure_mappers, sessionmaker, Query
# from sqlalchemy.types import CHAR, DateTime, String, TypeDecorator, Unicode





# # SQLAlchemy doesn't expose its default constructor as a nicely importable
# # function, so we grab it from the function defaults.
# if six.PY2:
#     _dec_spec = inspect.getargspec(declarative.declarative_base)
#     _spec_args, _spec_varargs, _spec_kwargs, _spec_defaults = _dec_spec
# else:
#     _dec_spec = inspect.getfullargspec(declarative.declarative_base)
#     _spec_args, _spec_defaults = _dec_spec.args, _dec_spec.defaults
# declarative_base_constructor = dict(
#     zip(reversed(_spec_args), reversed(_spec_defaults)))['constructor']


# def declarative_base(*orig_args, **orig_kwargs):
#     """
#     Replacement for SQLAlchemy's declarative_base, which adds these features:
#     1) This is a decorator.
#     2) This allows your base class to set a constructor.
#     3) This provides a default constructor which automatically sets defaults
#        instead of waiting to do that until the object is committed.
#     4) Automatically setting __tablename__ to snake-case.
#     5) Automatic integration with the SessionManager class.
#     """
#     orig_args = list(orig_args)

#     def _decorate_base_class(klass):

#         class Mixed(klass, CrudMixin):
#             def __init__(self, *args, **kwargs):
#                 """
#                 Variant on SQLAlchemy model __init__ which sets default values on
#                 initialization instead of immediately before the model is saved.
#                 """
#                 if '_model' in kwargs:
#                     assert kwargs.pop('_model') == self.__class__.__name__
#                 declarative_base_constructor(self, *args, **kwargs)
#                 for attr, col in self.__table__.columns.items():
#                     if col.default:
#                         self.__dict__.setdefault(attr, col.default.execute())

#         orig_kwargs['cls'] = Mixed
#         if 'name' not in orig_kwargs:
#             orig_kwargs['name'] = klass.__name__
#         if 'constructor' not in orig_kwargs:
#             orig_kwargs['constructor'] = klass.__init__ if '__init__' in klass.__dict__ else Mixed.__init__

#         Mixed = declarative.declarative_base(*orig_args, **orig_kwargs)
#         Mixed.BaseClass = _SessionInitializer._base_classes[klass.__module__] = Mixed
#         Mixed.__tablename__ = declarative.declared_attr(lambda cls: uncamel(cls.__name__))
#         return Mixed

#     is_class_decorator = not orig_kwargs and \
#             len(orig_args) == 1 and \
#             inspect.isclass(orig_args[0]) and \
#             not isinstance(orig_args[0], sqlalchemy.engine.Connectable)

#     if is_class_decorator:
#         return _decorate_base_class(orig_args.pop())
#     else:
#         return _decorate_base_class


# class _SessionInitializer(type):
#     _base_classes = {}

#     def __new__(cls, name, bases, attrs):
#         SessionClass = type.__new__(cls, name, bases, attrs)
#         if hasattr(SessionClass, 'engine'):
#             if not hasattr(SessionClass, 'BaseClass'):
#                 for module, bc in _SessionInitializer._base_classes.items():
#                     if module == SessionClass.__module__:
#                         SessionClass.BaseClass = bc
#                         break
#                 else:
#                     raise AssertionError('no BaseClass specified and @declarative_base was never invoked in {}'.format(SessionClass.__module__))
#             if not hasattr(SessionClass, 'session_factory'):
#                 SessionClass.session_factory = sessionmaker(bind=SessionClass.engine, autoflush=False, autocommit=False,
#                                                             query_cls=SessionClass.QuerySubclass)
#             SessionClass.initialize_db()
#             SessionClass.crud = make_crud_service(SessionClass)
#         return SessionClass


# @six.add_metaclass(_SessionInitializer)
# class SessionManager(object):
#     class SessionMixin(object):
#         pass

#     class QuerySubclass(Query):
#         pass

#     def __init__(self):
#         self.session = self.session_factory()
#         for name, val in self.SessionMixin.__dict__.items():
#             if not name.startswith('__'):
#                 assert not hasattr(self.session, name) and hasattr(val, '__call__')
#                 setattr(self.session, name, types.MethodType(val, self.session))

#     def __enter__(self):
#         return self.session

#     def __exit__(self, exc_type, exc_value, traceback):
#         try:
#             if exc_type is None:
#                 self.session.commit()
#         finally:
#             self.session.close()

#     def __del__(self):
#         if self.session.transaction._connections:
#             log.error('SessionManager went out of scope without underlying connection being closed; did you forget to use it as a context manager?')
#             self.session.close()

#     @classmethod
#     def initialize_db(cls, drop=False, create=True):
#         configure_mappers()
#         cls.BaseClass.metadata.bind = cls.engine
#         if drop:
#             cls.BaseClass.metadata.drop_all(cls.engine, checkfirst=True)
#         if create:
#             cls.BaseClass.metadata.create_all(cls.engine, checkfirst=True)

#     @classmethod
#     def resolve_model(cls, name):
#         if inspect.isclass(name) and issubclass(name, cls.BaseClass):
#             return name

#         subclasses = {ModelClass.__name__: ModelClass for ModelClass in collect_subclasses(cls)}
#         permutations = [name, camel(name), camel(name, cap_segment=0)]
#         for name in permutations:
#             if name in subclasses:
#                 return subclasses[name]

#             if name.lower().endswith('s'):
#                 singular = name.rstrip('sS')
#                 if singular in subclasses:
#                     return subclasses[singular]

#             if name.lower().endswith('ies'):
#                 singular = name[:-3] + 'y'
#                 if singular in subclasses:
#                     return subclasses[singular]

#         for name in permutations:
#             if name in cls.BaseClass.metadata.tables:
#                 return cls.BaseClass.metadata.tables[name]

#         raise ValueError('Unrecognized model: {}'.format(name))
