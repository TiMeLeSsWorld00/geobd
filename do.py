from geoalchemy2 import Geography
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import functions, Geometry
from sqlalchemy import Column, Integer, String, ForeignKey, Float
from tqdm import tqdm

PG_NAME = 'geobd'
PG_PORT = 5432
PG_HOST = 'localhost'
PG_PASS = '123456'
PG_USER = 'postgres'

# Установка соединения с базой данных
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_NAME}')
Session = sessionmaker(bind=engine)
session = Session()

# Определение сущности
Base = declarative_base()

# class YourTable(Base):
#     __tablename__  = 'Вкусно — и точка'
#     id = Column(Integer, primary_key=True)
#     # name = Column(String, unique=True)
#     address = Column(String)
#     email = Column(String)
#     geopos = Column(Geometry('POINT'), unique=True)
#     organization_id = Column(Integer, ForeignKey('organizations.id'))
#
# # Заданная точка
# target_point = 'SRID=4326;POINT(-90.2 30.3)'
#
# # Запрос для получения расстояний от заданной точки до всех точек в таблице "Вкусно — и точка"
# stmt = select(
#     YourTable.id,
#     func.ST_Distance(func.ST_GeogFromText(target_point), YourTable.geopos).label('distance')
# ).order_by('distance')
#
# # Выполнение запроса
# result = session.execute(stmt)
#
# # Вывод результатов
# for row in result:
#     print(f"ID: {row.id}, Расстояние: {row.distance}")

def obtain_appartments():
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

    result = session.execute(select(Apartments.id, Apartments.address, Apartments.geopos))
    # print(list(result))
    # for row in result:
    #     print(f"ID: {row.id}, адресс: {row.address}, геопоз: {row.geopos}")
    return list(result)


connection = engine.connect()

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
    ).order_by('distance')

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

def main():
    from sqlalchemy import text
    apps = obtain_appartments()
    app_res = dict()
    name = ['Вкусно — и точка', 'Цех85']

    min_dist = 10e7
    for app in tqdm(apps):
        wkt_geometry = convert_geopos(str(app.geopos))
        # dist = calc_min_dist(name[0], wkt_geometry)
        point = wkt_geometry[:-1].split('(')[1].split(" ")
        request = make_request(name, point)
        result = connection.execute(text(request))
        dist = list(result)[0].sum
        if (min_dist > dist):
            min_dist = dist
            print(f"ID: {app.id}, геопоз: {app.geopos}, dist: {dist}")


    point = [55, 33]
    request = make_request(name, point)
    result = connection.execute(text(request))
    print(list(result))

if __name__ == '__main__':
    main()


# пайплайн
# 1. идём по выбранным квартирам
# 2. считаем для каждой квартиры для всех организаций
# 3. считаем модуль суммы
# 4. находим минимальный