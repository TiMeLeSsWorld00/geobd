from api.schemas import ApartmentSchema
from requests import *

import uvicorn
from fastapi import FastAPI, HTTPException, Body, Depends
from fastapi.middleware.cors import CORSMiddleware

import geojson_pydantic
import logging
from datetime import datetime

app = FastAPI(
    title="GeoBD"
)

# Настройка CORS
origins = [
    "http://localhost:5173",
    "http://localhost:8081",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

date_str = datetime.now().strftime("%m.%d.%Y-%H%M%S")
logging.basicConfig(filename=f"logs/{date_str}.log", filemode="w")


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
    return delete_organisation_requests(organisation_id)


@app.put("/api/add_apartment", status_code=200, response_model=object,
         description="Add apartment")
def add_apartment(data: ApartmentSchema = Depends()):
    return add_apartment_request(data)


@app.delete("/api/delete_apartment", status_code=200, response_model=object,
            description="Delete apartment")
def delete_apartment(apartment_id: int):
    return delete_apartment_request(apartment_id)


@app.get("/api/apartment/{apartment}", status_code=200, response_model=object,
         description="Get apartment information by id")
def get_apartment(apartment: int = 1):
    return get_apartment_request(apartment)


@app.get("/api/apartments", status_code=200, response_model=object,
         description="Get apartment list top 100")
def get_apartments_list(city_id: int = 1):
    return get_apartments_list_request(city_id)


if __name__ == '__main__':
    uvicorn.run('app:app', host='localhost', port=8000, reload=True)
