from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, ForeignKey, select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String
from geoalchemy2 import Geometry
from shapely.geometry import Point
import pandas as pd
from tqdm import tqdm

PG_NAME = 'geobd'
PG_PORT = 5432
PG_HOST = 'localhost'
PG_PASS = '123456'
PG_USER = 'postgres'

# Установка соединения с базой данных
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_NAME}')
Base = declarative_base()

# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()


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
    name = Column(String, unique=True)
    email = Column(String)
    table_name = Column(String)


# class Cafe(Base):
#     __tablename__ = 'cafe'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     address = Column(String)
#     email = Column(String)
#     geopos = Column(Geometry('POINT'))
#     organization_id = Column(Integer, ForeignKey('organizations.id'))
#
#
# class Market(Base):
#     __tablename__ = 'market'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     address = Column(String)
#     email = Column(String)
#     geopos = Column(Geometry('POINT'))
#     organization_id = Column(Integer, ForeignKey('organizations.id'))
#
#
# class Gym(Base):
#     __tablename__ = 'gym'
#
#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     address = Column(String)
#     email = Column(String)
#     geopos = Column(Geometry('POINT'))
#     organization_id = Column(Integer, ForeignKey('organizations.id'))

connection = engine.connect()

def add_organisation(organisation_name: str, lon: float, lat: float):
    class Org(Base):
        __tablename__ = organisation_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        # name = Column(String, unique=True)
        address = Column(String)
        email = Column(String)
        geopos = Column(Geometry('POINT'), unique=True)
        organization_id = Column(Integer, ForeignKey('organizations.id'))


    stmt = select(Organizations).where(Organizations.name == organisation_name)
    organisations_ref = list(connection.execute(stmt))
    if not len(organisations_ref):
        new_organisation = Organizations(name=organisation_name)
        session.add(new_organisation)
        session.commit()
        stmt = select(Organizations).where(Organizations.name == organisation_name)
        organisations_ref = list(connection.execute(stmt))

    organisation_id = organisations_ref[0][0]

    if not inspect(engine).has_table(organisation_name):
        Org.__table__.create(engine)
    c1 = Org(
        # name='Ravi Kumar',
        # address='Station Road Nanded',
        # email='ravi@gmail.com',
        geopos=Point(lon, lat).wkt,
        organization_id=organisation_id,
    )

    stmt = select(Org).where(Org.geopos == Point(lon, lat).wkt)
    if not len(list(connection.execute(stmt))):
        session.add(c1)
        session.commit()



def read_csv(filename: str):
    df = pd.read_csv('data/cafe.csv')
    print(df.columns)
    df = df[['Наименование', 'Широта', 'Долгота']]

    for index, row in tqdm(df.iterrows(), total=len(df)):
        add_organisation(row['Наименование'], row['Широта'], row['Долгота'])
    # df = df.groupby('Наименование').agg(['Широта', 'Долгота'])
    # print(df)


def main():
    if not inspect(engine).has_table("organizations"):
        Organizations.__table__.create(engine)
    read_csv('lol')


if __name__ == '__main__':
    main()
