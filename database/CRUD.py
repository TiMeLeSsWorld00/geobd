from database.server import session, connection
from database.models import *

from sqlalchemy import select, delete
from shapely.geometry import Point

import logging
from datetime import datetime
import os




# create
def add_organisation(**kwargs):
    if len(get_organisation_by_name(kwargs.get('name'))):
        logging.info("CREATE: Attempt to add existing organisation " +
                     str(kwargs.get('name')))
        return False
    try:
        new_organisation = Organizations(**kwargs)
        session.add(new_organisation)
        session.commit()
    except BaseException as error:
        logging.error(
            "CREATE: Exception Add organisation with parameters: " +
            str(kwargs.get('name'))
        )
        logging.error(f"CREATE: Exception: {error}")
        return False
    logging.info("CREATE: Add organisation with parameters: " +
                 str(kwargs.get('name')))
    return True


def add_apartment(**kwargs):
    if len(get_apartment_by_coords(kwargs.get('geopos'))):
        logging.info("CREATE: Attempt to add existing apartment " +
                     str(kwargs.get('address')) + " " +
                     str(kwargs.get('geopos')))
        return False
    try:
        new_apartment = Apartments(**kwargs)
        session.add(new_apartment)
        session.commit()
    except BaseException as error:
        logging.error(
            "CREATE: Exception Add apartment with parameters: " +
            str(kwargs.get('address')) + " " +
            str(kwargs.get('geopos'))
        )
        logging.error(f"CREATE: Exception: {error}")
        return False
    logging.info("CREATE: Add apartment with parameters: " +
                 str(kwargs.get('address')) + " " +
                 str(kwargs.get('geopos')))
    return True


# read
def get_organisation_by_name(name: str):
    try:
        stmt = select(Organizations).where(Organizations.name == name)
        result = connection.execute(stmt)
    except BaseException as error:
        logging.error(f"READ: Tried to get organization with name: {name}; Exception: {error}")
        return None
    return list(result)


def get_organisation_by_id(id: int):
    try:
        stmt = select(Organizations).where(Organizations.id == id)
        result = connection.execute(stmt)
    except BaseException as error:
        logging.error(f"READ: Tried to get organization with id: {id}; Exception: {error}")
        return None
    return list(result)


def get_apartment_by_coords(geopos: Point):
    try:
        stmt = select(Apartments).where(Apartments.geopos == geopos)
        result = connection.execute(stmt)
    except BaseException as error:
        logging.error(f"READ: Tried to get apartments with geopos: {geopos}; Exception: {error}")
        return None
    return list(result)


def get_apartment_by_id(id: int):
    try:
        stmt = select(Apartments).where(Apartments.id == id)
        result = connection.execute(stmt)
    except BaseException as error:
        logging.error(f"READ: Tried to get apartments with id: {id}; Exception: {error}")
        return None
    return list(result)

# update


# delete
def delete_apartment_by_id(id: int):
    try:
        stmt = delete(Apartments).where(Apartments.id == id)
        session.execute(stmt)
        session.commit()
    except BaseException as error:
        logging.error(f"DELETE: Tried to delete apartments with id: {id}; Exception: {error}")
        return False
    logging.info(f"DELETE: Delete apartments with id: {id}")
    return True


def delete_organization_by_id(id: int):
    try:
        stmt = delete(Organizations).where(Organizations.id == id)
        session.execute(stmt)
        session.commit()
    except BaseException as error:
        print(error)
        logging.error(f"DELETE: Tried to delete organization with id: {id}; Exception: {error}")
        return False
    return True

