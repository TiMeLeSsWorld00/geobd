import ast

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData, ForeignKey, select, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, Float, BigInteger
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
    # geopos = Column(Geometry('POLYGON'))
    # description = Column(String)


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


connection = engine.connect()


def read_csv(filename: str):
    df = pd.read_csv('data/cian_houses_sale.csv')
    print(df.columns)
    df = df.fillna(0)
    # df = df[['Наименование', 'Широта', 'geopos']]
    #
    for index, row in tqdm(df.iterrows(), total=len(df)):
        lon, lat = ast.literal_eval(row['geopos'])['coordinates']
        c1 = Apartments(
            address=row['address'],
            geopos = Point(lon, lat).wkt,
            description = row['description'],
            city_id = 1,
            url = row['url'],
            price_total =row['price_total'],
            floor = row['floor'],
            # separated_wc_count = row['separated_wc_count'],
            total_area = row['total_area'],
            kitchen_area = row['kitchen_area'],
            rooms_count = row['rooms_count'],
            repair_type = row['repair_type'],
            floors_count = row['floors_count'],
            build_year = row['build_year'],
            # passenger_lifts_count = row['passenger_lifts_count'],
            price_per_unit = row['price_per_unit'],
        )
        stmt = select(Apartments).where(Apartments.geopos == Point(lon, lat).wkt)
        if not len(list(connection.execute(stmt))):
            session.add(c1)
            session.commit()
    # df = df.groupby('Наименование').agg(['Широта', 'Долгота'])
    # print(df)


def main():
    if not inspect(connection).has_table("apartments"):
        Cities.__table__.create(engine)
        c1 = Cities(
            name='Санкт-Петербург',
        )
        session.add(c1)
        session.commit()

        Apartments.__table__.create(engine)
    read_csv('lol')


if __name__ == '__main__':
    main()
