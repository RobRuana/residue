import sqlalchemy
from sqlalchemy import event
from sqlalchemy.orm import sessionmaker


def patch_session(Session, request):
    orig_engine, orig_factory = Session.engine, Session.session_factory
    request.addfinalizer(lambda: setattr(Session, 'engine', orig_engine))
    request.addfinalizer(lambda: setattr(Session, 'session_factory', orig_factory))

    db_path = '/tmp/residue.db'
    Session.engine = sqlalchemy.create_engine('sqlite+pysqlite:///' + db_path)
    event.listen(Session.engine, 'connect', lambda conn, record: conn.execute('pragma foreign_keys=ON'))
    Session.session_factory = sessionmaker(bind=Session.engine, autoflush=False, autocommit=False,
                                           query_cls=Session.QuerySubclass)
    Session.initialize_db(drop=True)
