from api.schemas import ApartmentSchema
from database.hardest import kek
from database.server import session, connection
from database.models import *
from database.CRUD import add_organisation, add_apartment, get_organisation_by_id, get_apartment_by_id, \
    delete_organization_by_id, delete_apartment_by_id

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


def get_apartments_list_request(city_id: int):
    stmt = select(Apartments)
    result = connection.execute(stmt)
    return [{'id': field.id, 'address': field.address, 'description': field.description} for field in list(result)[:100]]


def get_organization_request(organisation_id: int):
    return dict(get_organisation_by_id(organisation_id)[0]._mapping)


def get_apartment_request(apartment_id: int):
    result = dict(get_apartment_by_id(apartment_id)[0]._mapping)
    result.pop('geopos')
    return result


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


def add_apartment_request(data: ApartmentSchema):
    p = Point(*data.geopos.coordinates).wkt
    add = add_apartment(
        address=data.address,
        geopos=p,
        description=data.description,
        city_id=data.city_id,
        url=data.url,
        price_total=data.price_total,
        floor=data.floor,
        total_area=data.total_area,
        kitchen_area=data.kitchen_area,
        rooms_count=data.rooms_count,
        repair_type=data.repair_type,
        floors_count=data.floors_count,
        build_year=data.build_year,
        price_per_unit=data.price_per_unit,
    )
    return 'added' if add else 'adding failed'

def delete_organisation_requests(organization_id):
    return "deleted successfully" if delete_organization_by_id(organization_id) else "deleting failed"


def delete_apartment_request(apartment_id):
    return "deleted successfully" if delete_apartment_by_id(apartment_id) else "deleting failed"
