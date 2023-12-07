from server import session, engine, connection
from models import *

from sqlalchemy import text, inspect


def main():
    # connection.execute(text("CREATE EXTENSION postgis;"))
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

    # if not inspect(engine).has_table("apartments"):
    #     Apartments.__table__.create(engine)

    if not inspect(engine).has_table("Вкусно - и точка"):
        Apartments.__table__.create(engine)


if __name__ == '__main__':
    main()
