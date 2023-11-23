from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry
from sqlalchemy import Table, Column, Integer, String
from geoalchemy2 import Geometry
from shapely.geometry import Point


PG_NAME = 'geobd'
PG_PORT = 5432
PG_HOST = 'localhost'
PG_PASS = '123456'
PG_USER = 'postgres'

# Установка соединения с базой данных
engine = create_engine(f'postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_NAME}')
metadata = MetaData()

# # Определение таблицы
# spatial_table = Table('spatial_table', metadata,
#                       Column('id', Integer, primary_key=True),
#                       Column('name', String),
#                       Column('geom', Geometry('POINT'))
#                       )

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class spatial_table(Base):
    __tablename__ = 'spatial_table_2'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    # address = Column(String)
    # email = Column(String)
    geom = Column(Geometry('POINT'))




# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()
# session.


c1 = spatial_table(
    name='Ravi Kumar',
    # address='Station Road Nanded',
    # email='ravi@gmail.com',
    geom=Point(37.5, 55.4).wkt,
)

session.add_all([c1])
session.commit()

r = session.query(spatial_table).order_by(spatial_table.geom)

for i in r:
    print(i.geom)


from shapely.wkb import loads
from shapely.wkt import dumps

# Бинарное представление геометрии
binary_geometry = bytes.fromhex('01010000000000000000c042403333333333b34b40')

# Преобразование бинарного представления в объект Shapely
geometry = loads(binary_geometry)

# Преобразование объекта Shapely в формат WKT
wkt_geometry = dumps(geometry)

print(wkt_geometry)

def get_session():
    with Session() as session:
        return session



metadata.create_all(engine)

get_session().query()


# Пример выполнения запроса
# result = session.query(spatial_table).all()
#
# # Вывод результатов
# for row in result:
#     print(row.name, row.geom)
