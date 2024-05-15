from collections import defaultdict
from fastapi import HTTPException
from sqlalchemy.orm import Session
import json
import uuid
from typing import List, Optional
from . import models, schemas, last_data
from datetime import datetime
from collections import defaultdict

def get_sensor(db: Session, sensor_id: int, mongodb_client: Session) -> Optional[models.Sensor]:

    # get sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if not db_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found in postgreSQL")
    
    # get sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_sensor = mongodb_client.findOne({"sensor_id": sensor_id})

    if mongodb_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found in MongoDB")

    sensor = {"id": sensor_id, 
              "name": db_sensor.name, 
              "latitude": mongodb_sensor["location"]["coordinates"][0], 
              "longitude": mongodb_sensor["location"]["coordinates"][1],
              "type": mongodb_sensor["type"], 
              "mac_address": mongodb_sensor["mac_address"],
              "manufacturer": mongodb_sensor["manufacturer"],
              "model": mongodb_sensor["model"], 
              "serie_number": mongodb_sensor["serie_number"], 
              "firmware_version": mongodb_sensor["firmware_version"], 
              "description": mongodb_sensor["description"], 
            }

    return sensor

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate,  mongodb_client: Session, elastic_client: Session):
    # create a new sensor in postgresql
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    # create a new sensor in mongodb
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mydoc = {
            "sensor_id": db_sensor.id,
            "name": db_sensor.name, 
            "location": {
                "type": "Point",
                "coordinates": [sensor.longitude, sensor.latitude]
            },
            "type": sensor.type,
            "mac_address": sensor.mac_address,
            "manufacturer": sensor.manufacturer,
            "model": sensor.model,
            "serie_number": sensor.serie_number,
            "firmware_version": sensor.firmware_version,
            "description": sensor.description
        }
    mongodb_client.insertOne(mydoc)

    elastic_index_name = "sensors"
    
    if not elastic_client.index_exists(elastic_index_name):
        elastic_client.create_index(elastic_index_name)
        mapping = {
            "properties": {
                "id": {"type": "keyword"},
                "name": {"type": "keyword"},
                "type": {"type": "keyword"},
                "description": {"type": "text"},
            }
        }

        elastic_client.create_mapping(elastic_index_name, mapping)

    elastic_doc = {
        "id": db_sensor.id,
        "name": sensor.name,
        "type": sensor.type,
        "description": sensor.description,
    }

    elastic_client.index_document(elastic_index_name, elastic_doc)
    
    output = {
        "id": db_sensor.id,
        "name": sensor.name,
        "latitude": sensor.latitude,
        "longitude": sensor.longitude,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description
    }

    return output

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData, db: Session, mongodb_client: Session, timescale_client: Session, cassandra_client: Session) ->  Optional[schemas.Sensor]:
   # get sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if not db_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # conver db_sensor to json format let it store in redis
    sensor_json = json.dumps(data.dict())
    redis.set(f"sensor-{sensor_id}", sensor_json)

    # get sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    col_sensor = mongodb_client.findOne({"sensor_id": sensor_id})
    if not col_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")


    # if sensor exists, update the last data in redis
    sensor = schemas.Sensor(id=sensor_id, 
                        name=db_sensor.name, 
                        latitude=col_sensor["location"]["coordinates"][0], 
                        longitude=col_sensor["location"]["coordinates"][1],
                        type=col_sensor["type"], 
                        mac_address=col_sensor["mac_address"],
                        joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), 
                        temperature=data.temperature,
                        velocity=data.velocity, 
                        humidity=data.humidity,
                        battery_level=data.battery_level, 
                        last_seen=data.last_seen,
                        description=col_sensor["description"])
        

    # timescale_client.execute("CREATE TABLE IF NOT EXISTS sensor_data ( id SERIAL PRIMARY KEY, sensor_id INT NOT NULL, data JSONB, last_seen TIMESTAMPTZ NOT NULL);")
    data = data.dict()
    last_seen = data.pop('last_seen')
    data_json = json.dumps(data)
    timescale_client.insert(f"INSERT INTO sensor_data (sensor_id, data, last_seen) VALUES ({sensor_id}, '{data_json}', '{last_seen}');")

    #cassandra_client.get_session().set_keyspace('sensor')
    insert_stmt = cassandra_client.get_session_keyspace().prepare("""
    INSERT INTO sensor.sensor_data (id, sensor_id, data, last_seen, type_sensor)
    VALUES (?, ?, ?, ?, ?)
    """)
    data_str = json.dumps(data)
    cassandra_client.get_session_keyspace().execute(insert_stmt, [uuid.uuid4(), sensor_id, data_str, last_seen, str(col_sensor["type"])])
    #cassandra_client.execute(f"INSERT INTO sensor_data (sensor_id, data, last_seen) VALUES ({sensor_id}, '{data_json}', '{last_seen}');")

    return sensor

def get_data(redis: Session, sensor_id: int, db: Session, mongodb_client: Session, timescale_client:Session , from_date:  Optional[datetime] = None, to_date:  Optional[datetime]  = None, bucket: Optional[str] = None):

    # get sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if not db_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")
    
    # get sensor from redis with sensor_id
    redis_sensor = redis.get(f"sensor-{sensor_id}")
    if not redis_sensor:
        raise HTTPException(status_code=404, detail="Sensor not found")
    # convert redis sensor byte format to json format
    redis_sensor = schemas.SensorData.parse_raw(redis_sensor)

    # get sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_sensor = mongodb_client.findOne({"sensor_id": sensor_id})

    sensor = schemas.Sensor(id=sensor_id, name=db_sensor.name, 
                            latitude=mongodb_sensor["location"]["coordinates"][0], 
                            longitude=mongodb_sensor["location"]["coordinates"][1],
                            type=mongodb_sensor["type"], 
                            mac_address=mongodb_sensor["mac_address"],
                            joined_at=db_sensor.joined_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"), 
                            temperature=redis_sensor.temperature, 
                            velocity=redis_sensor.velocity,
                            humidity=redis_sensor.humidity, 
                            battery_level=redis_sensor.battery_level, 
                            last_seen=redis_sensor.last_seen,
                            description=mongodb_sensor["description"])

    timescale_client.execute(f"SELECT * FROM sensor_data WHERE sensor_id = {sensor_id};")
    rows = timescale_client.fetchall()

    
    listResult = []
    for row in rows:
        id, sensor_id, data, last_seen = row
        from_dt = datetime.strptime(str(from_date), "%Y-%m-%d %H:%M:%S%z")
        to_dt = datetime.strptime(str(to_date), "%Y-%m-%d %H:%M:%S%z")
        last_seen_dt = datetime.strptime(str(last_seen), "%Y-%m-%d %H:%M:%S%z")

        if bucket == "month":
            _last = last_seen_dt.strftime("%Y-%m")
            _from = from_dt.strftime("%Y-%m") 
            _to = to_dt.strftime("%Y-%m") 
        elif bucket == "week":
            _last = last_seen_dt.strftime("%Y-%U")  
            _from = from_dt.strftime("%Y-%U")  
            _to = to_dt.strftime("%Y-%U") 
        elif bucket == "day":
            _last = last_seen_dt.strftime("%Y-%m-%d") 
            _from = from_dt.strftime("%Y-%m-%d") 
            _to = to_dt.strftime("%Y-%m-%d") 
        elif bucket == "hour":
            _last = last_seen_dt.strftime("%Y-%m-%d %H")  
            _from = from_dt.strftime("%Y-%m-%d %H") 
            _to = to_dt.strftime("%Y-%m-%d %H") 


        if _from <= _last and _last <= _to:
            
            if(_last not in listResult):
                listResult.append(_last)

    return listResult

def get_sensors_near(latitude: float, longitude: float, radius: int, db: Session, mongodb_client: Session, redis_client: Session) -> list[schemas.Sensor]: 
    
    # get sensors from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    collection = mongodb_client.getCollection("sensorsCol")

    # find sensors near to a given location
    collection.create_index([("location", "2dsphere")])
    geoJSON = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    nearby_sensors = list(mongodb_client.findAllDocuments(geoJSON))
    sensors = []

    # find all sensors near to a given location
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_data(redis=redis_client, sensor_id=doc["sensor_id"], db=db, mongodb_client=mongodb_client)
        if sensor:
            sensors.append(sensor)
    
    if not sensors :
        return []
    
    return sensors


def delete_sensor(db: Session, sensor_id: int, mongodb_client: Session):

    # delete sensor from postgresql with sensor_id
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")

    # delete sensor from mongodb with sensor_id
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    mongodb_client.deleteOne({"sensor_id": sensor_id})

    db.delete(db_sensor)
    db.commit()
    
    return db_sensor


def search_sensors(db: Session,  mongodb: Session, es: Session, query: str, size: int, search_type: str) -> list[schemas.Sensor]: 

    query_dict = eval(query)
    elasic_index_name = 'sensors'
    
    query_type = search_type if search_type else 'match'
    search_type = list(query_dict.keys())[0]
    value = query_dict[search_type]

    if query_type == 'similar':
        search_query = {
            "query": {
                "fuzzy": {
                    search_type: {
                        "value": value,
                        "fuzziness": "AUTO"
                    }
                }
            }
        }
    else: 
        search_query = {
            "query": {
                query_type: {
                    search_type: value
                }
            }
        }

    results = es.search(index_name=elasic_index_name, query=search_query)

    sensors = []
    mongodb.getDatabase("sensors")
    mongodb.getCollection("sensorsCol")

    for hit in results['hits']['hits']:
        sensor_data = hit['_source']
        id = sensor_data['id']
        mongodb_sensor = mongodb.findOne({"sensor_id": id})

        if mongodb_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found in MongoDB")

        sensor = {"id": id, 
                "name": sensor_data['name'], 
                "latitude": mongodb_sensor["location"]["coordinates"][0], 
                "longitude": mongodb_sensor["location"]["coordinates"][1],
                "type": mongodb_sensor["type"], 
                "mac_address": mongodb_sensor["mac_address"],
                "manufacturer": mongodb_sensor["manufacturer"],
                "model": mongodb_sensor["model"], 
                "serie_number": mongodb_sensor["serie_number"], 
                "firmware_version": mongodb_sensor["firmware_version"], 
                "description": str(sensor_data["description"]),
                }
        sensors.append(sensor)



    return sensors[:size]



def get_values_sensor_temperatura(db:Session, redis:Session, mongodb_client:Session, timescale_client:Session, cassandra_client:Session):
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    result = list(mongodb_client.findAllDocuments())

    sensors_data = cassandra_client.get_session_keyspace().execute("SELECT * FROM sensor.sensor_data;")
    temperature_stats = {}
    for sensor_data in list(sensors_data):
        if sensor_data.type_sensor == 'Temperatura':

            data = json.loads(sensor_data.data)
            sensor_id = int(sensor_data.sensor_id)
            temperature = data['temperature']

            if sensor_id in temperature_stats:
                temperature_stats[sensor_id].append(temperature)
            else:
                temperature_stats[sensor_id] = [temperature]

    temperature_stats_res = {}
    for sensor_id, temperatures in temperature_stats.items():
        max_temperature = max(temperatures)
        min_temperature = min(temperatures)
        average_temperature = sum(temperatures) / len(temperatures)
        temperature_stats_res[sensor_id] = {"max_temperature": max_temperature, "min_temperature": min_temperature, "average_temperature": average_temperature}


    sensor_data_list = []
    sorted_temperature_stats_res = {k: temperature_stats_res[k] for k in sorted(temperature_stats_res)}
    for sensor_data in sorted_temperature_stats_res:
        data_low_battery = {
            "id": int(sensor_data),
            "name": result[int(str(sensor_data))-1]['name'],
            "latitude":  result[int(str(sensor_data))-1]['location']['coordinates'][0], 
            "longitude":  result[int(str(sensor_data))-1]['location']['coordinates'][1], 
            "type":  result[int(str(sensor_data))-1]['type'], 
            "mac_address":  result[int(str(sensor_data))-1]['mac_address'], 
            "manufacturer": result[int(str(sensor_data))-1]['manufacturer'], 
            "model": result[int(str(sensor_data))-1]['model'], 
            "serie_number":  result[int(str(sensor_data))-1]['serie_number'], 
            "firmware_version":  result[int(str(sensor_data))-1]['firmware_version'], 
            "description": result[int(str(sensor_data))-1]['description'], 
            "values": temperature_stats_res[sensor_data]
        }

        sensor_data_list.append(data_low_battery)

    return  {"sensors": sensor_data_list}


def get_quantity_by_type(db:Session, redis:Session, mongodb_client:Session, timescale_client:Session, cassandra_client:Session):

    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    result = mongodb_client.findAllDocuments()

    sensor_count = defaultdict(int)
    for sensor in result:
        sensor_count[sensor['type']] += 1

    result = {"sensors": [{"type": key, "quantity": value} for key, value in sensor_count.items()]}

    return result

def get_low_battery(db:Session, redis:Session, mongodb_client:Session, timescale_client:Session, cassandra_client:Session):
    mongodb_client.getDatabase("sensors")
    mongodb_client.getCollection("sensorsCol")
    result = list(mongodb_client.findAllDocuments())

    sensors_data = cassandra_client.get_session_keyspace().execute("SELECT * FROM sensor.sensor_data;")

    sensor_data_list = []

    for sensor_data in list(sensors_data):
        data = json.loads(sensor_data.data)
        if data['battery_level'] < 0.2:

            data_low_battery = {
                "id": int(sensor_data.sensor_id),
                "name": result[int(str(sensor_data.sensor_id))-1]['name'],
                "latitude":  result[int(str(sensor_data.sensor_id))-1]['location']['coordinates'][0], 
                "longitude":  result[int(str(sensor_data.sensor_id))-1]['location']['coordinates'][1], 
                "type":  result[int(str(sensor_data.sensor_id))-1]['type'], 
                "mac_address":  result[int(str(sensor_data.sensor_id))-1]['mac_address'], 
                "manufacturer": result[int(str(sensor_data.sensor_id))-1]['manufacturer'], 
                "model": result[int(str(sensor_data.sensor_id))-1]['model'], 
                "serie_number":  result[int(str(sensor_data.sensor_id))-1]['serie_number'], 
                "firmware_version":  result[int(str(sensor_data.sensor_id))-1]['firmware_version'], 
                "description": result[int(str(sensor_data.sensor_id))-1]['description'], 
                "battery_level": data['battery_level']
            }

            sensor_data_list.append(data_low_battery)

    sorted_sensors = sorted(sensor_data_list, key=lambda sensor: sensor['id'])

    return  {"sensors": sorted_sensors}