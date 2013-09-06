from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine('mysql://root:cchd0@315@localhost/datadb', echo=True)

Session = scoped_session(sessionmaker(bind=engine))
Base = declarative_base()

import quality

def db_init():
    Base.metadata.create_all(engine)
