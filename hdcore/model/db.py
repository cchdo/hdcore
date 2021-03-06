from sqlalchemy import create_engine
from sqlalchemy.sql import select
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.dialects.postgresql import array as pg_array
from sqlalchemy import (Table, Column, MetaData, Integer, String, Boolean,
DateTime, ForeignKey, Date, Time, Numeric)
from ujson import dumps as json_serializer

metadata = MetaData()
hydro_data = Table('hydro_data', metadata,
        Column('id', Integer, primary_key = True),
        Column('data', JSONB),
        Column('key_param', Integer),
        Column('key_value', String),
        Column('current', Boolean),
        Column('cruise_id', Integer),
        )
parameters = Table('parameters', metadata,
        Column('id', Integer, primary_key=True),
        Column('name', String),
        Column('type', String),
        Column('units', String),
        Column('units_repr', String),
        Column('quality', Integer, ForeignKey("parameters.id")),
        Column('canonical_id', Integer),
        Column('format_string', String),
        Column('quality_class', String),
        )
quality = Table('quality', metadata,
        Column('id', Integer, primary_key = True),
        Column('quality_class', String),
        Column('value', String),
        Column('description', String),
        Column('has_data', Boolean),
        Column('default_data_present', Boolean),
        Column('default_data_missing', Boolean),
        )
cruises = Table('cruises', metadata,
        Column('id', Integer, primary_key=True),
        Column('expocode', String),
        )
profiles = Table('profiles', metadata,
        Column('id', Integer, primary_key=True),
        Column('cruise_id', Integer),
        Column('samples', ARRAY(Integer)),
        Column('current', Boolean),
        Column('created_at', DateTime),
        Column('previous_id', Integer),
        Column('parameters', ARRAY(Integer)),
        Column('station', String),
        Column('cast', String),
        Column('type', String),
        Column('date_z', Date),
        Column('time_z', Time),
        Column('latitude', Numeric),
        Column('longitude', Numeric),
        Column('comments', String),
        Column('post_data', String),
        )
pending_profiles = Table("pending_profiles", metadata,
        Column('id', Integer, primary_key=True),
        Column('group', Integer),
        Column('data', JSONB),
        )

engine = create_engine('postgresql://abarna@localhost:5432/postgres',
        json_serializer=json_serializer)
