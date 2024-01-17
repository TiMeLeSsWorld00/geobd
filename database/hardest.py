from typing import List

from sqlalchemy import select, func, text, Column, Integer, String, ForeignKey
from geoalchemy2 import Geometry
from tqdm import tqdm

from database.models import Apartments
from database.server import Base, session, connection


def obtain_apartments():
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


def orgs_geoposes(orgs_names: list[str], target_point):
    ans = []
    for organisation_name in orgs_names:
        class YourTable(Base):
            __tablename__ = organisation_name
            __table_args__ = {'extend_existing': True}

            id = Column(Integer, primary_key=True)
            address = Column(String)
            email = Column(String)
            geopos = Column(Geometry('POINT'), unique=True)
            organization_id = Column(Integer, ForeignKey('organizations.id'))

        # Запрос для получения расстояний от заданной точки до всех точек в таблице "Вкусно — и точка"
        stmt = select(
            YourTable.id,
            YourTable.geopos,
            func.ST_Distance(func.ST_GeogFromText(target_point), YourTable.geopos).label('distance')
        ).order_by('distance', )

        # Выполнение запроса
        result = list(connection.execute(stmt))[0]
        lon, lat = convert_geopos(str(result.geopos))[:-1].split('(')[1].split(" ")
        distance = result.distance
        ans.append({'name': organisation_name, 'geopos': [lat, lon], 'dist': distance})
    print(ans)
    return ans


def calc_min_dist(organisation_name: str, target_point):
    class YourTable(Base):
        __tablename__  = organisation_name
        __table_args__ = {'extend_existing': True}

        id = Column(Integer, primary_key=True)
        address = Column(String)
        email = Column(String)
        geopos = Column(Geometry('POINT'), unique=True)
        organization_id = Column(Integer, ForeignKey('organizations.id'))

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
    apps = obtain_apartments()
    min_app = apps[0]
    for app in tqdm(apps):
        wkt_geometry = convert_geopos(str(app.geopos))
        point = wkt_geometry[:-1].split('(')[1].split(" ")
        request = make_request(names, point)
        result = connection.execute(text(request))
        dist = list(result)[0].sum
        if (min_dist > dist):
            min_dist = dist
            min_app = app
            print(min_app)
            print(f"ID: {app.id}, геопоз: {app.geopos}, dist: {dist}")
    best_geopos = convert_geopos(str(min_app.geopos))
    lon, lat = best_geopos[:-1].split('(')[1].split(" ")
    orgs_data = orgs_geoposes(names, best_geopos)
    return {'id': min_app.id, 'sum_dist': min_dist, 'geopos': [lat, lon], 'orgs_data': orgs_data}


# пайплайн
# 1. идём по выбранным квартирам
# 2. считаем для каждой квартиры для всех организаций
# 3. считаем модуль суммы
# 4. находим минимальный
