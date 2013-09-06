from models import Base
from sqlalchemy import (
        Column,
        Integer,
        String,
        Text,
        Table,
        ForeignKey,
        )
from sqlalchemy.orm import relationship

quality_map = Table("quality_map", Base.metadata,
        Column("from_id", Integer, ForeignKey("quality.id"), primary_key=True),
        Column("to_id", Integer, ForeignKey("quality.id"), primary_key=True),
        )

class NoMappedQualityError(Exception):
    """Exception raised when no quality of the given scheme is mapped to the
    current quality
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class Quality(Base):
    __tablename__ = "quality"

    id = Column(Integer, primary_key=True)
    type = Column(String(50), nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    help = Column(Text, nullable=True)
    mapped = relationship("Quality",
            secondary=quality_map,
            primaryjoin=id==quality_map.c.from_id,
            secondaryjoin=id==quality_map.c.to_id,
            )

    def __init__(self, name, description):
        self.name = name
        self.description = description

    def __repr__(self):
        return "<Quality (%s, '%s', '%s')>" % (self.id, self.name,
                self.description)

    def as_quality(self, scheme):
        quality = [x for x in self.mapped if x.type == scheme ]
        if len(quality) is 0:
            raise NoMappedQualityError("No quality mapped from {0} to {1}".format(self.type, scheme))
        if len(quality) is 1:
            return quality[0]

    @classmethod
    def find_or_new(cls, session, new_name, description):
        try:
            q = session.query(cls).filter_by(name = new_name).one()
            return q
        except NoResultFound:
            return cls(new_name, description)

