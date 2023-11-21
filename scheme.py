from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from sqlalchemy import Table, Column, Integer, String
from geoalchemy2 import Geometry
from shapely.geometry import Point
import pandas as pd

Base = declarative_base()


class Cities(Base):
    __tablename__ = 'cities'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    geopos = Column(Geometry('POLYGON'), nullable=False)
    description = Column(String)


class Apartments(Base):
    __tablename__ = 'apartments'

    id = Column(Integer, primary_key=True)
    address = Column(String)
    geopos = Column(Geometry('POINT'), nullable=False)
    description = Column(String)
    city_id = Column(Integer, ForeignKey('cities.id'))


class Organizations(Base):
    __tablename__ = 'organizations'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    table_name = Column(String)


class Cafe(Base):
    __tablename__ = 'cafe'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    email = Column(String)
    geopos = Column(Geometry('POINT'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))


class Market(Base):
    __tablename__ = 'market'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    email = Column(String)
    geopos = Column(Geometry('POINT'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))


class Gym(Base):
    __tablename__ = 'gym'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(String)
    email = Column(String)
    geopos = Column(Geometry('POINT'))
    organization_id = Column(Integer, ForeignKey('organizations.id'))


def add_organisation(organisation_name: str):
    class Org(Base):
        __tablename__ = organisation_name

        id = Column(Integer, primary_key=True)
        name = Column(String)
        address = Column(String)
        email = Column(String)
        geopos = Column(Geometry('POINT'))
        organization_id = Column(Integer, ForeignKey('organizations.id'))


def read_csv(filename: str):
    df = pd.read_csv('data/cafe.csv')
    print(df.columns)
    df = df[['Наименование', 'Широта', 'Долгота']]

    # for index, row in df.iterrows():
    #     print(row['Наименование'], row['Широта'], row['Долгота'])
    # df = df.groupby('Наименование').agg(['Широта', 'Долгота'])
    # print(df)


def main():
    read_csv('lol')


if __name__ == '__main__':
    main()
