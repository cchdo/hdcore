from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy import (Column, DateTime,
        Integer, String, Enum, ForeignKey, Float)

#for now and testing, this will change to postgresql when more final
engine = create_engine('sqlite:///:memory:', echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class XYT(Base):
    __tablename__ = 'xyt'

    id = Column(Integer, primary_key=True)
    lon = Column(Float, nullable=False)
    lat = Column(Float, nullable=False)
    datetime = Column(DateTime, nullable=False)

    def __init__(self, lon, lat, datetime):
        self.lon = lon
        self.lat = lat
        self.datetime = datetime

    def __repr__(self):
        return "<XYT (%s, %s, %2)>" % (self.lon, self.lat, self.datetime)

class Parameter(Base):
    __tablename__ = "parameters"

    #TODO make this conform to CF standards and have some sort of translation
    #between CCHDO and CF standards
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    short_name = Column(String, nullable=False)
    precision = Column(String, nullable=False)
    range_max = Column(Float)
    range_min = Column(Float)
    unit_id = Column(Float, ForeignKey("units.id"))
    
    def __init__(self, name, short_name, precision, range_max=None,
            range_min=None):
        self.name = name
        self.short_name = short_name
        self.precision = precision
        self.range_max = range_max
        self.range_min = range_min

    def __repr__(self):
        return "<Parameter (%s (%s), %s, (%s - %s))>" % (self.name,
                self.short_name, self.precision, self.range_min,
                self.range_max)

class Unit(Base):
    __tablename__ = "units"
    # ideally this class will be able to convert/keep track of unit conversions

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Unit ('%s')>" % (self.name)


class Expocode(Base):
    __tablename__ = "expocodes"

    id = Column(Integer, primary_key=True)
    expocode = Column(String, nullable=False) # this should be indexed?

    def __init__(self, expocode):
        self.expocode = expocode

    def __repr__(self):
        return "<Expocode: %s>" % (self.expocode)


class Section(Base):
    __tablename__ = "sections"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    trackline = Column(String, nullable=False) #TODO make this use a trackline
    # are all sections part of a program?
    program_id = Column(Integer, ForeignKey('programs.id'))
    description = Column(String)

    #TODO add the relationship

    def __init__(self, name, trackline, description):
        self.name = name
        self.trackline = trackline
        self.description = description

    def __repr__(self):
        return "<Section ('%s', '%s', '%8s')" % (self.name, self.description,
                self.trackline)


class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    cruise_id = Column(Integer, ForeignKey('cruises.id'), nullable=False)
    XYT_id = Column(Integer, ForeignKey('xyt.id'), nullable=False)
    station_number = Column(Integer, nullable=False)
    cast_number = Column(Integer, nullable=False)
    bottom_depth = Column(Integer)

    def __init__(self, station_number, cast_number, bottom_depth=None):
        self.station_number = station_number
        self.cast_number = cast_number
        self.bottom_depth = bottom_depth

    def __repr__(self):
        return "<Station (%s, %s, %s)>" % (self.station_number,
                self.cast_number, self.bottom_depth)


class Port(Base):
    __tablename__ = "ports"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)

    def __init__(self, name, country, lat, lon):
        self.name = name
        self.country = country
        self.lat = lat
        self.lon = lon

    def __repr__(self):
        return "<Port ('%s', '%s', (%s, %s))>" % (self.name, self.country,
                self.lon, self.lat)


class Cruise(Base):
    __tablename__ = "cruises"

    id = Column(Integer, primary_key=True)
    expocode_id = Column(Integer, ForeignKey('expocodes.id'), nullable=False)
    start_port_id = Column(Integer, ForeignKey('ports.id'), nullable=False)
    end_port_id = Column(Integer, ForeignKey('ports.id'), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    section_id = Column(Integer, ForeignKey('sections.id'))
    platform_id = Column(Integer, ForeignKey('platforms.id'))

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def __repr__(self):
        #TODO make this better!
        return "<Cruise ('%s', '%s')>" % (self.start_date, self.end_date)


class Platform(Base):
    __tablename__ = "platforms"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    #TODO Make this an enum
    kind = Column(String, nullable=False)
    NODC_code = Column(String, nullable=False) 
    institution_id = Column(Integer, ForeignKey('institutions.id'), nullable=False)

    def __init__(self, name, kind, NODC_code):
        self.name = name
        self.kind = kind
        self.NODC_code = NODC_code

    def __repr__(self):
        return "<Platform ('%s', '%s', '%s')>" % (self.name, self.kind,
                self.NODC_code)


class Program(Base):
    __tablename__ = "programs"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    long_name = Column(String, nullable=False)
    description = Column(String, nullable=False)

    def __init__(self, name, long_name, description):
        self.name = name
        self.long_name = long_name
        self.description = description

    def __repr__(self):
        return "<Program ('%s', '%s', '%s',)>" % (self.name, self.long_name,
                self.description)


class Measurement(Base):
    __tablename__ = "measurements"

    id = Column(Integer, primary_key=True)
    XYT_id = Column(Integer, ForeignKey('xyt.id'), nullable=False)
    parameter_id = Column(Integer, ForeignKey('parameters.id'), nullable=False)
    #TODO add blame (like the PI responsible)
    quality_id = Column(Integer, ForeignKey('quality.id'), nullable=False)

    #only if known...
    instrument_id = Column(Integer, ForeignKey('instruments.id'))

    def __init__(self):
        # I don't know if this is needed
        pass

    def __repr__(self):
        return "<Measurement (%s)>" % (self.id)


class Quality(Base):
    __tablename__ = "quality"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    description = Column(String, nullable=False)

    def __init__(self, name, description):
        self.name = name
        self.description = description

    def __repr__(self):
        return "<Quality (%s, '%s', '%s')>" % (self.id, self.name,
                self.description)

class Institution(Base):
    __tablename__ = "institutions"

    #this will eventually take over for the current CCHDO Database, the idea is
    #to be able to generate the citations at the top of datafiles automatically

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    abr_name = Column(String, nullable=False)

    def __init__(self, name, abr_name):
        self.name = name
        self.abr_name = abr_name

    def __repr__(self):
        return "<Institution ('%s', '%s')>" % (self.name, self.abr_name)

class Instrument(Base):
    __tablename__ = "instruments"
    
    id = Column(Integer, primary_key=True)
    short_name = Column(String, nullable=False)
    make = Column(String, nullable=False)
    model = Column(String, nullable=False)
    serial = Column(String, nullable=False)

    def __init__(self, short_name, make, model, serial):
        self.short_name = short_name
        self.make = make
        self.model = model
        self.serial = serial

    def __repr__(self):
        return "<Instrument ('%s', '%s', '%s', '%s')>" % (self.short_name,
                self.make, self.model, self.serial)


Base.metadata.create_all(engine)
