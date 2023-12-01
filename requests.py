from typing import List, Literal

import pandas as pd
from geoalchemy2 import functions
from geoalchemy2.shape import from_shape
from shapely.geometry import Point
from sqlalchemy import select, func

from hardest import kek
from server import Base, session, engine, connection
from models import Organizations, Apartments


def get_sql_aggr(aggr: str):
    if aggr == 'sum':
        return func.sum
    elif aggr == 'avg':
        return func.avg
    elif aggr == 'min':
        return func.min
    elif aggr == 'max':
        return func.max
    else:
        raise ValueError("No such aggregation type. Check validation Literal")


def hardest(organizations: List[str], city_id: int):
    return kek(organizations)


def get_organizations_list_request(city_id: int):
    stmt = select(Organizations)
    result = connection.execute(stmt)
    return {field.id: field.name for field in result}


def get_organization_request(organisation_id: int):
    stmt = select(Organizations).where(Organizations.id == organisation_id)
    result = connection.execute(stmt)
    return dict(list(result)[0]._mapping)


def aggregate_in_radius_request(point: Point, aggr: Literal['sum', 'avg', 'max', 'min'], radius: int):
    lon, lat = point['coordinates']
    sql_aggr = get_sql_aggr(aggr)
    point_str = f'POINT({lon} {lat})'
    # point_wkt = Point([(50.854457, 4.377184)])
    stmt = session.query(
        sql_aggr(Apartments.price_total).label('price_total'),
        sql_aggr(Apartments.floor).label('floor'),
        sql_aggr(Apartments.separated_wc_count).label('separated_wc_count'),
        sql_aggr(Apartments.total_area).label('total_area'),
        sql_aggr(Apartments.kitchen_area).label('kitchen_area'),
        sql_aggr(Apartments.rooms_count).label('rooms_count'),
        sql_aggr(Apartments.floors_count).label('floors_count'),
        sql_aggr(Apartments.build_year).label('build_year'),
        sql_aggr(Apartments.passenger_lifts_count).label('passenger_lifts_count'),
        sql_aggr(Apartments.price_per_unit).label('price_per_unit'),
    ).filter(
        functions.ST_DWithin(Apartments.geopos, point_str, radius)
    )
    result = list(session.execute(stmt))[0]
    return dict(result._mapping)
