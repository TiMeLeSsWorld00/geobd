from server import Base

from sqlalchemy import Column, Integer, String, ForeignKey, Float
from geoalchemy2 import Geometry


class Cities(Base):
    __tablename__ = 'cities'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    geopos = Column(Geometry('POLYGON'))
    description = Column(String)


class Apartments(Base):
    __tablename__ = 'apartments'

    id = Column(Integer, primary_key=True)
    address = Column(String)
    geopos = Column(Geometry('POINT'), nullable=False)
    description = Column(String)
    city_id = Column(Integer, ForeignKey('cities.id'))
    url = Column(String)
    price_total = Column(Float)
    floor = Column(Float)
    separated_wc_count = Column(Float)
    total_area = Column(Float)
    kitchen_area = Column(Float)
    rooms_count = Column(Float)
    repair_type = Column(String)
    floors_count = Column(Float)
    build_year = Column(Float)
    passenger_lifts_count = Column(Float)
    price_per_unit = Column(Float)


class Organizations(Base):
    __tablename__ = 'organizations'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    email = Column(String)
    description = Column(String)
    categories = Column(String)
