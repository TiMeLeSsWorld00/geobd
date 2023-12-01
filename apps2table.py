import ast

from sqlalchemy import select, inspect
from shapely.geometry import Point
import pandas as pd
from tqdm import tqdm
from server import session, engine, connection

from models import Cities, Apartments

def read_csv(filename: str):
    df = pd.read_csv('data/cian_houses_sale.csv')
    print(df.columns)
    df = df.fillna(0)
    # df = df[['Наименование', 'Широта', 'geopos']]
    #
    for index, row in tqdm(df.iterrows(), total=len(df)):
        lat, lon = ast.literal_eval(row['geopos'])['coordinates']
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
