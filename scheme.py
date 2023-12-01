from sqlalchemy import create_engine, MetaData, ForeignKey, select, inspect
from sqlalchemy import Table, Column, Integer, String
from geoalchemy2 import Geometry
from shapely.geometry import Point
import pandas as pd
from tqdm import tqdm
from server import Base, session, engine, connection
from models import Organizations


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
