from fastapi import APIRouter, HTTPException, Query
from . import schemas
from datetime import datetime
from typing import Optional
from shared.publisher import Publisher


router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)

def publish_request(request_type: str, data: dict):
    try:
        publisher = Publisher()
        response = publisher.publish(request_type, data)
        publisher.close()
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match"):
    return publish_request('search', {'query': query, 'size': size, 'search_type': search_type})


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float, radius: int):
    return publish_request('near', {'latitude': latitude, 'longitude': longitude, 'radius': radius})

@router.get("/temperature/values")
def get_temperature_values():
    return publish_request('temperature/values', {})


@router.get("/quantity_by_type")
# def get_sensors_quantity(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
def get_sensors_quantity():
    return publish_request('quantity_by_type', {})



@router.get("/low_battery")
def get_low_battery_sensors():
    return publish_request('low_battery', {})



# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors():
    return publish_request('get_sensors', {})


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate):
    return publish_request('create_sensor', {'sensor': sensor})


# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int):
    return publish_request('get_sensor_by_id', {'sensor_id': sensor_id})


# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int):
    return publish_request('delete_sensor_by_id', {'sensor_id': sensor_id})

    

# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, 
                data: schemas.SensorData):
    return publish_request('post_sensor_by_id_data', {'sensor_id': sensor_id, 'data':data})


# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int,              
             from_date: Optional[datetime] = Query(None, alias="from"),
             to_date: Optional[datetime] = Query(None, alias="to"),
             bucket: Optional[str] = None):
    return publish_request('get_sensor_by_id_data', {'sensor_id': sensor_id, 'from_date':from_date, 'to_date':to_date, 'bucket': bucket})

