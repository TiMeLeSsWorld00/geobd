from typing import Literal, List, Dict

import uvicorn
from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware

import pandas as pd
import ast
# import h3
import geojson_pydantic

from requests import hardest, get_organizations_list_request, get_organization_request, aggregate_in_radius_request, \
    add_apartment_request, add_organisation_request, get_apartment_request

app = FastAPI(
    title="GeoBD"
)

# Настройка CORS
origins = [
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/check", status_code=200)
def check():
    return 'Good'


@app.post("/api/find_best_apartment", status_code=200, response_model=object,
          description="Find find best apartment for organizations")
def find_best_apartment(organizations: List[str], city_id: int = 1):
    assert city_id >= 0
    if len(organizations) > 5:
        return HTTPException(403, 'To many organizations')

    return hardest(organizations, city_id)


@app.get("/api/organizations", status_code=200, response_model=object,
         description="Get organizations list ")
def get_organizations_list(city_id: int = 1):
    assert city_id >= 0

    return get_organizations_list_request(city_id)


@app.get("/api/organization/{organization}", status_code=200, response_model=object,
          description="Get organization information by id")
def get_organization(organization: int = 1):
    assert organization >= 0

    return get_organization_request(organization)


@app.post("/api/aggregate_in_radius", status_code=200, response_model=object,
          description="Get organization information by id")
def aggregate_in_radius(
    point: geojson_pydantic.Point = Body(description='Point in geojson format'),
    aggr: Literal['sum', 'avg', 'max', 'min'] = Body(),
    radius: int = Body(ge=0, le=100000, description='h3 resolution')
):
    return aggregate_in_radius_request(point.model_dump(), aggr, radius)


@app.put("/api/add_organisation", status_code=200, response_model=object,
         description="Add organisation")
def add_organisation(name: str = None, email: str = None, description: str = None, categories: str = None):
    add_organisation_request(name, email, description, categories)
    return 'added'


@app.delete("/api/delete_organisation", status_code=200, response_model=object,
         description="Delete organisation")
def delete_organisation(organisation_id: int = 1):
    return "deleted"


@app.put("/api/add_apartment", status_code=200, response_model=object,
         description="Add apartment")
def add_apartment(address: str, geopos: geojson_pydantic.Point, description: str = None, city_id: int = 1, url: str = None, price_total: float = None, floor: float = None,
                  total_area: float = None, kitchen_area: float = None, rooms_count: float = None, repair_type: str = None,
                  floors_count: float = None, build_year: str = None, price_per_unit: float = None):
    add_apartment_request(address, geopos, description, city_id, url, price_total, floor,
                  total_area, kitchen_area, rooms_count, repair_type,
                  floors_count, build_year, price_per_unit)
    return 'added'


@app.delete("/api/delete_apartment", status_code=200, response_model=object,
         description="Delete apartment")
def delete_apartment(city_id: int = 1):
    return "deleted"


@app.get("/api/apartment/{apartment}", status_code=200, response_model=object,
          description="Get apartment information by id")
def get_apartment(apartment: int = 1):
    return get_apartment_request(apartment)


@app.get("/api/apartments", status_code=200, response_model=object,
         description="Get apartment list ")
def get_apartments_list(city_id: int = 1):
    pass


if __name__ == '__main__':
    uvicorn.run('app:app', host='localhost', port=8000, reload=True)
