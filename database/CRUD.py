from server import session, connection
from models import *

from sqlalchemy import select

import logging

logging.basicConfig(level=logging.INFO, filename="database.log", filemode="w")


# create
def add_organisation(**kwargs):
    stmt = select(Organizations).where(Organizations.name == kwargs.get('name'))
    if len(list(connection.execute(stmt))):
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
    stmt = select(Apartments).where(Apartments.geopos == kwargs.get('geopos'))
    if len(list(connection.execute(stmt))):
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

# update

# delete
