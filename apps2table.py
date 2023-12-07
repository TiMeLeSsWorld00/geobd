import ast

from sqlalchemy import select, inspect
from shapely.geometry import Point
import pandas as pd
from tqdm import tqdm
from server import session, engine, connection

from models import Cities, Apartments


def read_csv(filename: str):
    df = pd.read_csv(filename)
    print(df.columns)
    df = df.fillna(0)

    spb_id = 1

    for index, row in tqdm(df.iterrows(), total=len(df)):
        lat, lon = ast.literal_eval(row['geopos'])['coordinates']
        c1 = Apartments(
            address=row['address'],
            geopos=Point(lon, lat).wkt,
            description=row['description'],
            city_id=spb_id,
            url=row['url'],
            price_total=row['price_total'],
            floor=row['floor'],
            total_area=row['total_area'],
            kitchen_area=row['kitchen_area'],
            rooms_count=row['rooms_count'],
            repair_type=row['repair_type'],
            floors_count=row['floors_count'],
            build_year=row['build_year'],
            price_per_unit=row['price_per_unit'],
        )
        stmt = select(Apartments).where(Apartments.geopos == Point(lon, lat).wkt)
        if not len(list(connection.execute(stmt))):
            session.add(c1)
            session.commit()


def main():
    read_csv('data/cian_houses_sale.csv')


if __name__ == '__main__':
    main()
