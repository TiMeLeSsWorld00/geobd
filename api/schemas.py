import geojson_pydantic
from pydantic import BaseModel


class ApartmentSchema(BaseModel):
    address: str
    geopos: geojson_pydantic.Point
    description: str = None
    city_id: int = None
    url: str = None
    price_total: int = None
    floor: int = None
    total_area: float = None
    kitchen_area: float = None
    rooms_count: int = None
    repair_type: float = None
    floors_count: int = None
    build_year: int = None
    price_per_unit: float = None
