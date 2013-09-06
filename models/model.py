from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship, backref
from sqlalchemy import (Column, DateTime,
        Integer, String, Enum, ForeignKey, Float, Text)
from sqlalchemy.orm.exc import NoResultFound

#for now and testing, this will change to postgresql when more final

from models import Base

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
        return "<XYT (%s, %s, %s)>" % (self.lon, self.lat, self.datetime)

    @classmethod
    def find_or_new(cls, session, new_lon, new_lat, new_datetime):
        try:
            q = session.query(cls).filter_by(lon = new_lon, lat = new_lat,
                    datetime = new_datetime).one()
            return q
        except NoResultFound:
            return cls(new_lon, new_lat, new_datetime)

class Parameter(Base):
    __tablename__ = "parameters"

    #TODO make this conform to CF standards and have some sort of translation
    #between CCHDO and CF standards
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    short_name = Column(String(255), nullable=False,)# unique=True)
    precision = Column(String(255), nullable=False)
    range_max = Column(Float)
    range_min = Column(Float)
    unit_id = Column(Integer, ForeignKey("units.id"))

    # it shall be convention that the relationship should be established by the
    # class with the foreign key in it

    unit = relationship("Unit", backref="parameters")
    
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

    @classmethod
    def find_or_new(cls, session, new_name, new_shortname, new_precision):
        try:
            q = session.query(cls).filter_by(name = new_name).one()
            return q
        except NoResultFound:
            return cls(new_name, new_shortname, new_precision)

class Unit(Base):
    __tablename__ = "units"
    # ideally this class will be able to convert/keep track of unit conversions

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return "<Unit ('%s')>" % (self.name)

    @classmethod
    def find_or_new(cls, session, new_name):
        try:
            q = session.query(cls).filter_by(name = new_name).one()
            return q
        except NoResultFound:
            return cls(new_name)


class Expocode(Base):
    __tablename__ = "expocodes"

    id = Column(Integer, primary_key=True)
    expocode = Column(String(255), nullable=False) # this should be indexed?

    def __init__(self, expocode):
        self.expocode = expocode

    def __repr__(self):
        return "<Expocode: %s>" % (self.expocode)

    @classmethod
    def find_or_new(cls, session, new_expocode):
        try:
            q = session.query(cls).filter_by(expocode = new_expocode).one()
            return q
        except NoResultFound:
            return cls(new_expocode)


class Section(Base):
    __tablename__ = "sections"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    trackline = Column(String(255), nullable=False) #TODO make this use a trackline
    # are all sections part of a program?
    program_id = Column(Integer, ForeignKey('programs.id'))
    description = Column(String(255))

    program = relationship("Program", backref="sections")

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

    xyt = relationship("XYT", backref="station")
    cruise = relationship("Cruise", backref="stations")
    measurements = association_proxy("xyt", "measurements")

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
    name = Column(String(255), nullable=False)
    country = Column(String(255), nullable=False)
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
    expocode_id = Column(Integer, ForeignKey(Expocode.id), nullable=False,)
    start_port_id = Column(Integer, ForeignKey('ports.id'), )#nullable=False)
    end_port_id = Column(Integer, ForeignKey('ports.id'), )#nullable=False)
    start_date = Column(DateTime,)# nullable=False)
    end_date = Column(DateTime,)# nullable=False)
    section_id = Column(Integer, ForeignKey('sections.id'))
    platform_id = Column(Integer, ForeignKey('platforms.id'))

    expocode = relationship("Expocode", backref=backref("cruise",
        uselist=False), post_update=True)
    start_port = relationship("Port", backref="cruise_origins",
            foreign_keys=start_port_id)
    end_port = relationship("Port", backref="cruise_destinations",
            foreign_keys=end_port_id)
    section = relationship("Section", backref="section")
    platform = relationship("Platform", backref="cruises")

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date

    def __repr__(self):
        #TODO make this better!
        return "<Cruise ('%s', '%s')>" % (self.start_date, self.end_date)

    @classmethod
    def find_or_new(cls, session, expocode, start_date, end_date):
        try:
            q = session.query(cls).filter_by(expocode = expocode).one()
            return q
        except NoResultFound:
            return cls(start_date, end_date)

class Platform(Base):
    __tablename__ = "platforms"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    #TODO Make this an enum
    kind = Column(String(255), nullable=True)
    code = Column(String(255), nullable=True, unique=True) 
    institution_id = Column(Integer, ForeignKey('institutions.id'),
            nullable=True)
    country = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    notes = Column(Text, nullable=True)

    institution = relationship("Institution", backref="platforms")

    def __init__(self, name, kind=None, code=None, country=None, title=None,
            notes=None):
        self.name = name
        self.kind = kind
        self.code = code
        self.country = country
        self.title = title
        self.notes = notes

    def __repr__(self):
        return "<Platform ('%s', '%s', '%s')>" % (self.name, self.country,
                self.code)


class Program(Base):
    __tablename__ = "programs"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    long_name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)

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
    quality_id = Column(Integer, ForeignKey('quality.id'), nullable=True)
    primary_id = Column(Integer, ForeignKey('measurements.id'), nullable=True)

    #temporary data place to live!
    value = Column(Float, nullable=False)

    #only if known...
    instrument_id = Column(Integer, ForeignKey('instruments.id'))

    xyt = relationship("XYT", backref="measurements")
    parameter = relationship("Parameter", backref="xyt")
    quality = relationship("Quality", backref="measurements")
    instruemnt = relationship("Instrument", backref="measuremnts")
    primary = relationship("Measurement", remote_side=[id],
    backref="dependants", post_update=True)

    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "<Measurement (%s, '%s')>" % (self.value, self.parameter.name)

    def __cmp__(self, other):
        return float(self.value) - float(other.value)

    @property
    def is_primary(self):
        if self is self.primary:
            return True
        else:
            return False

    def ordered_deps(self, order, not_found=None):
        """Returns a list of the dependants as specified in order, omitting
        dependants not speicfied in order
        
        not_found determines behavior if some element of order cannot be found
        in the dependents
        
        Maybe the order should be a list of parameter objects? for now just
        some names...
        """

        measurements = self.dependants
        measurements_k = {}
        for measurement in measurements:
            measurements_k[measurement.parameter.name] = measurement

        ordered = []
        
        for o in order:
            if o in measurements_k:
                ordered.append(measurements_k[o])
            else:
                if not_found == "except":
                    raise #something error TODO
                ordered.append(not_found)

        return ordered

    def value_list(self, order, flags=None, fill=False):
        """We need some way of representing some z axis and the various
        parameters that were measured at the z axis. These parameters will also
        need to be sorted.

        flags is a list of bools  equal in length to order that specifies which
        params should have flags

        right now only works if called on a primary measuremnet instance, maybe
        that will change in the future
        """
        value_list = []
        if self.is_primary is False:
            raise #Somethign error, TODO, impliment actual exceptions

        if flags is not None:
            if len(order) is not len(flags):
                raise BaseException# Something TODO
        if flags is None:
            flags = [None for x in order]

        for p in zip(self.ordered_deps(order), flags):
            if p[0] is not None:
                value_list.append(str(p[0].value))
                if p[1] is True and p[0].quality is not None:
                    value_list.append(p[0].quality.name)
            elif p[1] is True:
                value_list.append(None)
                value_list.append(None)
            else:
                value_lsit.append(None)

        return value_list



class Institution(Base):
    __tablename__ = "institutions"

    #this will eventually take over for the current CCHDO Database, the idea is
    #to be able to generate the citations at the top of datafiles automatically

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    abr_name = Column(String(255), nullable=False)

    def __init__(self, name, abr_name):
        self.name = name
        self.abr_name = abr_name

    def __repr__(self):
        return "<Institution ('%s', '%s')>" % (self.name, self.abr_name)

class Instrument(Base):
    __tablename__ = "instruments"
    
    id = Column(Integer, primary_key=True)
    short_name = Column(String(255), nullable=False)
    make = Column(String(255), nullable=False)
    model = Column(String(255), nullable=False)
    serial = Column(String(255), nullable=False)

    def __init__(self, short_name, make, model, serial):
        self.short_name = short_name
        self.make = make
        self.model = model
        self.serial = serial

    def __repr__(self):
        return "<Instrument ('%s', '%s', '%s', '%s')>" % (self.short_name,
                self.make, self.model, self.serial)

