from __future__ import absolute_import
import shutil
import uuid
from datetime import datetime

import pytest
import sqlalchemy
from sqlalchemy.ext.declarative import as_declarative
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.types import Boolean, Integer, UnicodeText
from sqlalchemy.sql import case
from sqlalchemy.sql.schema import Column, ForeignKey, ForeignKeyConstraint, \
    Table, UniqueConstraint
from sqlalchemy_utils.types import JSONType, UUIDType

from residue.crud.orm import crudable, regex_validation, \
    text_length_validation, get_model_by_table, get_primary_key_column_names, \
    get_unique_constraint_column_names, \
    get_one_to_many_foreign_key_column_names, CrudModelMixin


@as_declarative()
class Base(object):
    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)


@crudable(update=['tags', 'employees'])
@text_length_validation('name', 1, 100)
class User(Base):
    __tablename__ = 'user'
    name = Column(UnicodeText(), nullable=False, unique=True)
    tags = relationship('Tag', cascade='all,delete,delete-orphan',
                        backref='user', passive_deletes=True)
    employees = relationship('Account', cascade='all,delete,delete-orphan',
                             passive_deletes=True)


@crudable()
class Boss(Base):
    __tablename__ = 'boss'
    name = Column(UnicodeText(), nullable=False, unique=True)


@crudable(no_update=['username'])
@regex_validation(
    'username',
    r'[0-9a-zA-z]+',
    'Usernames may only contain alphanumeric characters')
class Account(Base):
    __tablename__ = 'account'
    user_id = Column(UUIDType(), ForeignKey('user.id', ondelete='RESTRICT'),
                     nullable=False)
    user = relationship(User)
    username = Column(UnicodeText(), nullable=False, unique=True)
    password = Column(UnicodeText(), nullable=False)

    boss_id = Column(UUIDType(), ForeignKey('boss.id', ondelete='SET NULL'),
                     nullable=True)
    boss = relationship(Boss, backref='employees')


@crudable(no_update=['name', 'user_id'])
class Tag(Base):
    __tablename__ = 'tag'
    __table_args__ = (UniqueConstraint('user_id', 'name'),)

    name = Column(UnicodeText(), nullable=False)
    user_id = Column(UUIDType(), ForeignKey('user.id', ondelete='CASCADE'),
                     nullable=False)


@text_length_validation('mixed_in_attr', 1, 10)
class CrudableTestMixin(object):
    """Test that validation decorators on Mixins work as expected"""
    mixed_in_attr = Column(UnicodeText(), default='default string')
    extra_data = Column(JSONType(), default={}, server_default='{}')


@crudable(
    data_spec={
        'date_attr': {
            'date_format': 'Y-M-d',
            'desc': 'this is a manual desc'
        },
        'overridden_desc': {
            'desc': 'this is an overridden desc',
            'validators': {
                'maxLength': 2
            },
        },
        'manual_attr': {
            'desc': 'this is a manually-specified attribute',
            'name': 'manual_attr',
            'create': True,
            'read': True,
            'type': 'auto',
            'update': True,
            'validators': {
                'maxLength': 2
            }
        }
    }
)
@text_length_validation('string_model_attr', 2, 100)
@regex_validation('string_model_attr', '^[A-Za-z0-9\.\_\-]+$', 'test thing')
@text_length_validation('overridden_desc', 1, 100)
@text_length_validation('nonexistant_field', 1, 100)
class CrudableClass(CrudableTestMixin, Base):
    """
    Testbed class for getting the crud definition for a class that be crudable.
    """

    __tablename__ = 'crudable_class'

    string_attr = 'str'
    int_attr = 1
    bool_attr = True
    float_attr = 1.0
    date_attr = datetime(2011, 1, 1, 0, 0, 0)
    string_model_attr = Column(UnicodeText(), default='default string')
    int_model_attr = Column(Integer())
    bool_model_attr = Column(Boolean())

    @property
    def settable_property(self):
        """this is the docstring"""
        return None

    @settable_property.setter
    def settable_property(self, thing):
        pass

    @hybrid_property
    def string_and_int_hybrid_property(self):
        """this is the docstring"""
        return '{} {}'.format(self.string_model_attr, self.int_model_attr)

    @string_and_int_hybrid_property.expression
    def string_and_int_hybrid_property(cls):
        return case([
            (cls.string_model_attr is None, ''),
            (cls.int_model_attr is None, '')
        ], else_=(cls.string_model_attr + ' ' + cls.int_model_attr))

    @property
    def unsettable_property(self):
        """
        this is an epydoc-decorated docstring

        @return: None
        """
        return None

    def method(self):
        pass

    @property
    def overridden_desc(self):
        """docstring but not desc"""
        return None


@crudable()
class BasicClassMixedIn(CrudableTestMixin, Base):
    __tablename__ = 'basic_class_mixed_in'
    pass


class TestGetModelByTable(object):

    @as_declarative()
    class BaseTestModel(object):
        pass

    class ModelA(BaseTestModel):
        __tablename__ = 'model_a'
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)

    class ModelB(ModelA):
        __tablename__ = 'model_b'
        pk = Column(UUIDType(), ForeignKey('model_a.pk'), primary_key=True,
                    default=uuid.uuid4)

    @pytest.mark.parametrize('cls', [ModelA, ModelB])
    def test_get_model_by_table(self, cls):
        assert cls is get_model_by_table(self.BaseTestModel, cls.__table__)
        assert cls is get_model_by_table(self.ModelA, cls.__table__)
        assert cls is get_model_by_table(self.ModelB, cls.__table__)

    def test_get_model_by_table_unknown_table(self):
        assert None is get_model_by_table(self.BaseTestModel, Table())


class TestGetOneToManyForeignKeyColumnName(object):

    @as_declarative()
    class BaseTestModel(object):
        pass

    class Stadium(BaseTestModel):
        __tablename__ = 'stadium'
        city = Column(UnicodeText(), primary_key=True)
        sport = Column(UnicodeText(), primary_key=True)

    class Team(BaseTestModel):
        __tablename__ = 'team'
        __table_args__ = (
            ForeignKeyConstraint(
                ['stadium_city', 'stadium_sport'],
                ['stadium.city', 'stadium.sport']),)

        id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        players = relationship('Player', backref='team')
        stadium_city = Column(UnicodeText())
        stadium_sport = Column(UnicodeText())
        stadiums = relationship('Stadium', backref='teams')
        labels = relationship('Label', backref='teams',
                              primaryjoin='Label.fk == Team.id',
                              foreign_keys='Label.fk')

    class Player(BaseTestModel):
        __tablename__ = 'player'
        id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        team_id = Column(UUIDType(), ForeignKey('team.id'))
        labels = relationship('Label', backref='players',
                              primaryjoin='Label.fk == Player.id',
                              foreign_keys='Label.fk')

    class Label(BaseTestModel):
        __tablename__ = 'label'
        quasi_foreign_keys = ['fk']
        id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        text = Column(UnicodeText())
        fk = Column(UUIDType(), default=uuid.uuid4)

    @pytest.mark.parametrize('cls,attr,expected', [
        (Team, 'players', ['team_id']),
        (Player, 'team', []),
        (Stadium, 'teams', ['stadium_city', 'stadium_sport']),
        (Team, 'stadiums', []),
        (Player, 'labels', ['fk']),
        (Label, 'players', []),
        (Team, 'labels', ['fk']),
        (Label, 'teams', []),
    ])
    def test_get_one_to_many_foreign_key_column_names(
            self, cls, attr, expected):
        assert set(expected) == \
            set(get_one_to_many_foreign_key_column_names(cls, attr))
        assert set(expected) == \
            set(get_one_to_many_foreign_key_column_names(cls(), attr))


class TestGetPrimaryKeyColumnNames(object):

    @as_declarative()
    class BaseTestModel(object):
        pass

    class OnePK(BaseTestModel):
        __tablename__ = 'one_pk'
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)

    class TwoPKs(BaseTestModel):
        __tablename__ = 'two_pks'
        pk1 = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        pk2 = Column(UUIDType(), primary_key=True, default=uuid.uuid4)

    class ThreePKs(BaseTestModel):
        __tablename__ = 'three_pks'
        pk1 = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        pk2 = Column(Integer(), primary_key=True)
        pk3 = Column(Integer(), primary_key=True)

    @pytest.mark.parametrize('cls,expected', [
        (OnePK, ['pk']),
        (TwoPKs, ['pk1', 'pk2']),
        (ThreePKs, ['pk1', 'pk2', 'pk3']),
    ])
    def test_get_primary_key_column_names(self, cls, expected):
        assert set(expected) == set(get_primary_key_column_names(cls))
        assert set(expected) == set(get_primary_key_column_names(cls()))


class TestGetUniqueConstraintColumnNames(object):

    @as_declarative()
    class BaseTestModel(object):
        pass

    class OnePK(BaseTestModel):
        __tablename__ = 'one_pk'
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)

    class OneUC(BaseTestModel):
        __tablename__ = 'one_uc'
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        uc = Column(UUIDType(), unique=True, default=uuid.uuid4)

    class TwoUCs(BaseTestModel):
        __tablename__ = 'two_ucs'
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        uc1 = Column(UUIDType(), unique=True, default=uuid.uuid4)
        uc2 = Column(UUIDType(), unique=True, default=uuid.uuid4)

    class CompositeUC(BaseTestModel):
        __tablename__ = 'composite_uc'
        __table_args__ = (UniqueConstraint('uc2a', 'uc2b'),)
        pk = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
        uc1 = Column(UUIDType(), unique=True, default=uuid.uuid4)
        uc2a = Column(UUIDType(), default=uuid.uuid4)
        uc2b = Column(UUIDType(), default=uuid.uuid4)

    @pytest.mark.parametrize('cls,expected', [
        (OnePK, []),
        (OneUC, [['uc']]),
        (TwoUCs, [['uc1'], ['uc2']]),
        (CompositeUC, [['uc1'], ['uc2a', 'uc2b']]),
    ])
    def test_get_unique_constraint_column_names(self, cls, expected):
        def sortify(x):
            return sorted([sorted(s) for s in x])

        assert sortify(expected) == \
            sortify(get_unique_constraint_column_names(cls))
        assert sortify(expected) == \
            sortify(get_unique_constraint_column_names(cls()))


class Session(SessionManager):
    engine = sqlalchemy.create_engine('sqlite:////tmp/test_sa.db')

    class SessionMixin(object):
        def user(self, name):
            return self.query(User).filter_by(name=name).one()

        def account(self, username):
            return self.query(Account).filter_by(username=username).one()


def create(model, **params):
    with Session() as session:
        model = Session.resolve_model(model)
        item = model(**params)
        session.add(item)
        session.commit()
        return item.to_dict()


def query_from(obj, attr='id'):
    return {
        '_model': obj['_model'],
        'field': attr,
        'value': obj[attr]
    }


@pytest.fixture(scope='module')
def init_db(request):
    class db:
        pass
    patch_session(Session, request)
    db.turner = create('User', name='Turner')
    db.hooch = create('User', name='Hooch')
    create('Tag', user_id=db.turner['id'], name='Male')
    create('Tag', user_id=db.hooch['id'], name='Male')
    db.ninja = create('Tag', user_id=db.turner['id'], name='Ninja')
    db.pirate = create('Tag', user_id=db.hooch['id'], name='Pirate')
    db.boss = create('Boss', name='Howard Hyde')
    db.turner_account = create('Account', username='turner_account',
                               password='password', user_id=db.turner['id'],
                               boss_id=db.boss['id'])
    db.hooch_account = create('Account', username='hooch_account',
                              password='password', user_id=db.hooch['id'])
    return db


@pytest.fixture(autouse=True)
def db(request, init_db):
    shutil.copy('/tmp/sideboard.db', '/tmp/sideboard.db.backup')
    request.addfinalizer(
        lambda: shutil.move('/tmp/sideboard.db.backup', '/tmp/sideboard.db'))
    return init_db


class TestDeclarativeBaseConstructor(object):
    def test_default_init(self):
        # default is applied at initialization instead of on save
        assert User().id

    def test_overriden_init(self):
        @declarative_base
        class WithOverriddenInit(object):
            id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)

            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class Foo(WithOverriddenInit):
            bar = Column(Boolean())

        assert Foo().id is None

    def test_declarative_base_without_parameters(self):

        @declarative_base
        class BaseTest:
            pass

        assert BaseTest.__tablename__ == 'base_test'

    def test_declarative_base_with_parameters(self):

        @declarative_base(name=str('NameOverride'))
        class BaseTest:
            pass

        assert BaseTest.__tablename__ == 'name_override'


class TestCrudCount(object):
    def assert_counts(self, query, **expected):
        actual = {
            count['_label']: count['count']
            for count in Session.crud.count(query)}
        assert len(expected) == len(actual)
        for label, count in expected.items():
            assert count == actual[label]

    def test_subquery(self):
        results = Session.crud.count({
            '_model': 'Tag',
            'groupby': ['name'],
            'field': 'user_id',
            'comparison': 'in',
            'value': {
                '_model': 'User',
                'select': 'id',
                'field': 'name',
                'value': 'Turner'}})
        expected = {
            'Male': 1,
            'Ninja': 1
        }
        for result in results[0]['count']:
            assert result['count'] == expected[result['name']]

    def test_compound_subquery(self):
        query = {
            '_model': 'Tag',
            'groupby': ['name'],
            'field': 'user_id',
            'comparison': 'in',
            'value': {
                '_model': 'User',
                'select': 'id',
                'or': [{
                    'field': 'name',
                    'value': 'Turner'
                }, {
                    'field': 'name',
                    'value': 'Hooch'
                }]
            }
        }
        results = Session.crud.count(query)
        expected = {
            'Ninja': 1,
            'Pirate': 1,
            'Male': 2
        }
        for result in results[0]['count']:
            assert result['count'] == expected[result['name']]

    def test_distinct(self):
        pytest.skip('Query.distinct(*columns) is postgresql-only')
        results = Session.crud.count({'_model': 'Tag'})
        assert results[0]['count'] == 4

        results = Session.crud.count({
            '_model': 'Tag',
            'distinct': ['name']
        })
        results[0]['count'] == 3

    def test_groupby(self):
        results = Session.crud.count({
            '_model': 'Tag',
            'groupby': ['name']
        })
        expected = {
            'Male': 2,
            'Ninja': 1,
            'Pirate': 1
        }
        for result in results[0]['count']:
            result['count'] == expected.get(result['name'], 0)

    def test_single_basic_query_string(self):
        self.assert_counts('User', User=2)

    def test_single_basic_query_dict(self):
        self.assert_counts({'_model': 'User'}, User=2)

    def test_multi_basic_query_string(self):
        self.assert_counts(['User', 'Tag'], User=2, Tag=4)

    def test_multi_basic_query_dict(self):
        self.assert_counts([{'_model': 'User'}, {'_model': 'Tag'}],
                           User=2, Tag=4)

    def test_single_complex_query(self):
        self.assert_counts({
            '_label': 'HoochCount',
            '_model': 'User',
            'field': 'name',
            'value': 'Hooch'}, HoochCount=1)

    def test_multi_complex_query(self):
        self.assert_counts([{
            '_label': 'HoochCount',
            '_model': 'User',
            'field': 'name',
            'value': 'Hooch'
        }, {
            '_label': 'MaleCount',
            '_model': 'Tag',
            'field': 'name',
            'value': 'Male'
        }], HoochCount=1, MaleCount=2)

    def test_multi_complex_query_with_same_models(self):
        hooch_query = {
            '_model': 'User',
            '_label': 'HoochCount',
            'or': [{
                '_model': 'User',
                'field': 'name',
                'value': 'Hooch'
            }, {
                '_model': 'User',
                'field': 'name',
                'value': 'Hoochert'
            }]
        }
        turner_query = {
            '_model': 'User',
            '_label': 'TurnerCount',
            'field': 'name',
            'value': 'Turner'
        }
        all_query = {'_model': 'User'}

        self.assert_counts(
            [hooch_query, turner_query, all_query],
            User=2, HoochCount=1, TurnerCount=1)


class TestCrudValidations(object):
    def test_length(self):
        pytest.raises(CrudException, Session.crud.update,
                      {'_model': 'User'}, {'name': ''})
        pytest.raises(CrudException, Session.crud.update,
                      {'_model': 'User'}, {'name': 'x' * 101})

    def test_regex(self):
        pytest.raises(CrudException, Session.crud.update,
                      {'_model': 'Account'}, {'username': '!@#'})


class TestCollectModels(object):
    def assert_models(self, *args):
        expected_models = set(args[:-1])
        actual_models = Session.crud._collect_models(args[-1])
        assert expected_models == actual_models

    def test_single(self):
        self.assert_models(User, {'_model': 'User'})

    def test_multiple(self):
        self.assert_models(
            User, Account, [{'_model': 'User'}, {'_model': 'Account'}])

    def test_foreign_key(self):
        self.assert_models(
            Account, User, {'_model': 'Account', 'field': 'user.name'})

    def test_nested_keys(self):
        self.assert_models(
            Account, User, Tag,
            {'_model': 'Account', 'field': 'user.name.tags'})


class TestCrudableClass(object):
    expected_crud_spec = {
        'fields': {
            'id': {
                'name': 'id',
                'type': 'auto',
                'create': True,
                'read': True,
                'update': False,
            },
            'string_attr': {
                'name': 'string_attr',
                'type': 'string',
                'create': False,
                'read': True,
                'update': False,
                'defaultValue': 'str',
            },
            'int_attr': {
                'name': 'int_attr',
                'type': 'int',
                'create': False,
                'read': True,
                'update': False,
                'defaultValue': 1,
            },
            'extra_data': {
                'create': True,
                'name': 'extra_data',
                'read': True,
                'type': 'auto',
                'update': True
            },
            'bool_attr': {
                'name': 'bool_attr',
                'type': 'boolean',
                'create': False,
                'read': True,
                'update': False,
                'defaultValue': True,
            },
            'float_attr': {
                'name': 'float_attr',
                'type': 'float',
                'create': False,
                'read': True,
                'update': False,
                'defaultValue': 1.0,
            },
            'date_attr': {
                'name': 'date_attr',
                'type': 'date',
                'create': False,
                'read': True,
                'update': False,
                'desc': 'this is a manual desc',
                'defaultValue': datetime(2011, 1, 1, 0, 0),
                'date_format': 'Y-M-d',
            },
            'string_model_attr': {
                'name': 'string_model_attr',
                'type': 'string',
                'create': True,
                'read': True,
                'update': True,
                'defaultValue': 'default string',
                'validators': {
                    u'maxLength': 100,
                    u'maxLengthText':
                        u'The maximum length of this field is {0}.',
                    u'minLength': 2,
                    u'minLengthText':
                        u'The minimum length of this field is {0}.',
                    u'regexString': u'^[A-Za-z0-9\\.\\_\\-]+$',
                    u'regexText': u'test thing'}
            },
            'mixed_in_attr': {
                'create': True,
                'defaultValue': 'default string',
                'name': 'mixed_in_attr',
                'read': True,
                'type': 'string',
                'update': True,
                'validators': {
                    'maxLength': 10,
                    'maxLengthText':
                        'The maximum length of this field is {0}.',
                    'minLength': 1,
                    'minLengthText':
                        'The minimum length of this field is {0}.',
                }
            },
            'bool_model_attr': {
                'name': 'bool_model_attr',
                'type': 'boolean',
                'create': True,
                'read': True,
                'update': True,
            },
            'int_model_attr': {
                'name': 'int_model_attr',
                'type': 'int',
                'create': True,
                'read': True,
                'update': True,
            },
            'settable_property': {
                'desc': 'this is the docstring',
                'name': 'settable_property',
                'type': 'auto',
                'create': True,
                'read': True,
                'update': True,
            },
            'unsettable_property': {
                'desc': 'this is an epydoc-decorated docstring',
                'name': 'unsettable_property',
                'type': 'auto',
                'create': False,
                'read': True,
                'update': False,
            },
            'manual_attr': {
                'name': 'manual_attr',
                'type': 'auto',
                'create': True,
                'read': True,
                'update': True,
                'desc': 'this is a manually-specified attribute',
                'validators': {
                    'maxLength': 2
                }
            },
            'overridden_desc': {
                'create': False,
                'desc': 'this is an overridden desc',
                'name': 'overridden_desc',
                'read': True,
                'type': 'auto',
                'update': False,
                'validators': {
                    'maxLength': 2,
                    'maxLengthText':
                        'The maximum length of this field is {0}.',
                    'minLength': 1,
                    'minLengthText':
                        'The minimum length of this field is {0}.',
                }
            }
        }
    }

    def test_crud_spec(self):
        assert self.expected_crud_spec == CrudableClass._crud_spec

    def test_basic_crud_spec(self):
        field_names = ('id', 'mixed_in_attr', 'extra_data')
        fields = {f: self.expected_crud_spec['fields'][f] for f in field_names}
        expected_basic = {'fields': fields}
        assert expected_basic == BasicClassMixedIn._crud_spec

    def test_handle_no_crud_spec_attribute(self):
        with pytest.raises(AttributeError):
            object._crud_spec


def test_get_models():
    def assert_models(xs, models):
        assert set(xs) == Session.crud._get_models(models)

    assert_models([], 0)
    assert_models([], {})
    assert_models([], [])
    assert_models([], '')
    assert_models([], None)
    assert_models([], {'_model': 0})
    assert_models([], {'_model': {}})
    assert_models([], {'_model': []})
    assert_models([], {'_model': None})
    assert_models(['User'], {'_model': 'User'})
    assert_models(['User'], [{'_model': 'User'}])
    assert_models(['User'], ({'_model': 'User'},))
    assert_models(['User'], {'foo': {'_model': 'User'}})
