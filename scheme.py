from sqlalchemy import create_engine, MetaData, ForeignKey, select, inspect, Float
from sqlalchemy import Table, Column, Integer, String
from geoalchemy2 import Geometry
from shapely.geometry import Point
import pandas as pd
from tqdm import tqdm
from server import Base, session, engine, connection
from models import Organizations


def add_organisation(df_row):
    organisation_name = df_row['Наименование']
    lon = df_row['Широта']
    lat = df_row['Долгота']
    city_id = 1
    class Org(Base):
        __tablename__ = organisation_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        address = Column(String)
        geopos = Column(Geometry('POINT'), unique=True)
        organization_id = Column(Integer, ForeignKey('organizations.id'))
        address_comment = Column(String)
        post_index = Column(String)
        microdistrict = Column(String)
        district = Column(String)
        city_id = Column(Integer, ForeignKey('cities.id'))
        work_time = Column(String)
        timezone = Column(String)
        rating = Column(Float)
        reviews_number = Column(Float)
        phone_number = Column(String)
        website = Column(String)
        gis_url = Column(String)

    stmt = select(Organizations).where(Organizations.name == organisation_name)
    organisations_ref = list(connection.execute(stmt))
    if not len(organisations_ref):
        new_organisation = (
            Organizations(
                name=organisation_name,
                email=df_row['E-mail 1'],
                description=df_row['Описание'],
                categories=df_row['Рубрики'],
            )
        )
        session.add(new_organisation)
        session.commit()
        stmt = select(Organizations).where(Organizations.name == organisation_name)
        organisations_ref = list(connection.execute(stmt))

    organisation_id = organisations_ref[0][0]

    if not inspect(engine).has_table(organisation_name):
        Org.__table__.create(engine)
    new_table_row = Org(
        geopos=Point(lon, lat).wkt,
        organization_id=organisation_id,
        address=df_row['Адрес'],
        address_comment=df_row['Комментарий к адресу'],
        post_index=df_row['Почтовый индекс'],
        microdistrict=df_row['Микрорайон'],
        district=df_row['Район'],
        city_id=city_id,
        work_time=df_row['Часы работы'],
        timezone=df_row['Часовой пояс'],
        rating=df_row['Рейтинг'],
        reviews_number=df_row['Количество отзывов'],
        phone_number=df_row['Телефон 1'],
        website=df_row['Веб-сайт 1'],
        gis_url=df_row['2GIS URL'],
    )

    stmt = select(Org).where(Org.geopos == Point(lon, lat).wkt)
    if not len(list(connection.execute(stmt))):
        session.add(new_table_row)
        session.commit()



def read_csv(filename: str):
    df = pd.read_csv(filename)
    print(df.columns)
    for index, row in tqdm(df.iterrows(), total=len(df)):
        add_organisation(row)


def main():
    read_csv('data/cafe.csv')


if __name__ == '__main__':
    main()
