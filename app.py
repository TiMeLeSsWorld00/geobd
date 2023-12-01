from typing import Literal, List, Dict

import uvicorn
from fastapi import FastAPI, HTTPException, Body

import pandas as pd
import ast
# import h3
import geojson_pydantic

from requests import hardest, get_organizations_list_request, get_organization_request, aggregate_in_radius_request

app = FastAPI(
    title="Begemotic"
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


@app.get("/api/organizations", status_code=200, response_model=Dict[int, str],
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


if __name__ == '__main__':
    uvicorn.run(app, host='localhost', port=8000)
