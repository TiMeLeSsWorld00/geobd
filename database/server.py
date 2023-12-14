from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

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

connection = engine.connect()
