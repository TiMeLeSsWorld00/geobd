from typing import List

from geoalchemy2 import Geography
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import functions, Geometry
from sqlalchemy import Column, Integer, String, ForeignKey, Float
from tqdm import tqdm

from models import Apartments
from server import Base, session, engine, connection
from sqlalchemy import text


def obtain_appartments():
    result = session.execute(select(Apartments.id, Apartments.address, Apartments.geopos))
    return list(result)


def make_request(orgs_names: list[str], point):
    segment = """
        (SELECT 
            id, 
            ST_Distance(
                ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography, 
                geopos::geography
            ) AS distance
        FROM 
            "{org_name}"
        ORDER BY distance ASC
        LIMIT 1)
    """
    request = """
    SELECT SUM(KEK.distance) FROM (
    {body}
    ) AS KEK;
    """
    body = []
    for org_name in orgs_names:
        body.append(segment.format(org_name=org_name, lon=point[0], lat=point[1]))
        body.append("UNION ALL")
    query = request.format(body="".join(body[:-1]))
    return query


def calc_min_dist(organisation_name: str, target_point):
    class YourTable(Base):
        __tablename__  = organisation_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        address = Column(String)
        email = Column(String)
        geopos = Column(Geometry('POINT'), unique=True)
        organization_id = Column(Integer, ForeignKey('organizations.id'))


    # Заданная точка
    # target_point = 'SRID=4326;POINT(-90.2 30.3)'

    # Запрос для получения расстояний от заданной точки до всех точек в таблице "Вкусно — и точка"
    stmt = select(
        YourTable.id,
        func.ST_Distance(func.ST_GeogFromText(target_point), YourTable.geopos).label('distance')
    ).order_by('distance', )

    # Выполнение запроса
    result = connection.execute(stmt)

    return list(result)[0].distance


def convert_geopos(geopos: str):
    from shapely.wkb import loads
    from shapely.wkt import dumps

    # Бинарное представление геометрии
    binary_geometry = bytes.fromhex(geopos)

    # Преобразование бинарного представления в объект Shapely
    geometry = loads(binary_geometry)

    # Преобразование объекта Shapely в формат WKT
    wkt_geometry = dumps(geometry)

    return wkt_geometry


def kek(names: List[str]):
    min_dist = 10e7
    apps = obtain_appartments()
    min_app = apps[0]
    for app in tqdm(apps):
        wkt_geometry = convert_geopos(str(app.geopos))
        # dist = calc_min_dist(name[0], wkt_geometry)
        point = wkt_geometry[:-1].split('(')[1].split(" ")
        request = make_request(names, point)
        result = connection.execute(text(request))
        dist = list(result)[0].sum
        if (min_dist > dist):
            min_dist = dist
            min_app = app
            print(min_app)
            print(f"ID: {app.id}, геопоз: {app.geopos}, dist: {dist}")

    return {'id': min_app.id, 'sum_dist': min_dist}


def main():
    print(kek(['Вкусно — и точка', 'Цех85']))

if __name__ == '__main__':
    main()


# пайплайн
# 1. идём по выбранным квартирам
# 2. считаем для каждой квартиры для всех организаций
# 3. считаем модуль суммы
# 4. находим минимальный
