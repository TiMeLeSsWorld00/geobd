from server import session, engine
from models import *

from sqlalchemy import inspect, text


def main():
    session.execute(text("CREATE EXTENSION postgis;"))
    session.commit()
    if not inspect(engine).has_table("cities"):
        Cities.__table__.create(engine)
        spb = Cities(
            id=1,
            name='Санкт-Петербург',
        )
        session.add(spb)
        session.commit()

    if not inspect(engine).has_table("organizations"):
        Organizations.__table__.create(engine)

    if not inspect(engine).has_table("apartments"):
        Apartments.__table__.create(engine)


if __name__ == '__main__':
    main()
