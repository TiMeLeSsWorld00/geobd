from database.hardest import kek
from database.server import session, connection
from database.models import *
from database.CRUD import add_organisation

from sqlalchemy import select, func
from geoalchemy2 import functions
from shapely.geometry import Point

from typing import List, Literal


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
    return [{'id': field.id, 'name': field.name} for field in result]


def get_organization_request(organisation_id: int):
    stmt = select(Organizations).where(Organizations.id == organisation_id)
    result = connection.execute(stmt)
    return dict(list(result)[0]._mapping)


def get_apartment_request(apartment: int):
    stmt = select(Apartments).where(Apartments.id == apartment)
    result = connection.execute(stmt)
    k = dict(list(result)[0]._mapping)
    k.pop('geopos')
    return k


def aggregate_in_radius_request(point: Point, aggr: Literal['sum', 'avg', 'max', 'min'], radius: int):
    lon, lat = point['coordinates']
    sql_aggr = get_sql_aggr(aggr)
    point_str = f'POINT({lon} {lat})'
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
        functions.ST_DWithin(Apartments.geopos, point_str, radius/3000)
    )
    result = list(session.execute(stmt))[0]
    return dict(result._mapping)


def add_organisation_request(name=None, email=None, description=None, categories=None):
    add_organisation(
        name=name,
        email=email,
        description=description,
        categories=categories
    )


def add_apartment_request(address, geopos, description=None, city_id=None, url=None, price_total=None, floor=None,
                  total_area=None, kitchen_area=None, rooms_count=None, repair_type=None,
                  floors_count=None, build_year=None, price_per_unit=None):
    p = Point(*geopos.coordinates).wkt
    apartment = Apartments(
        address=address,
        geopos=p,
        description=description,
        city_id=city_id,
        url=url,
        price_total=price_total,
        floor=floor,
        total_area=total_area,
        kitchen_area=kitchen_area,
        rooms_count=rooms_count,
        repair_type=repair_type,
        floors_count=floors_count,
        build_year=build_year,
        price_per_unit=price_per_unit,
    )
    stmt = select(Apartments).where(Apartments.geopos == p)
    if not len(list(connection.execute(stmt))):
        session.add(apartment)
        session.commit()


def delete_organisation_requests():
    return 'deleted'


def delete_apartment_request():
    return 'deleted'