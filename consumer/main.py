import json
import pika

from shared.subscriber import Subscriber

subscriber = Subscriber()

import json
from fastapi import APIRouter, Depends, HTTPException, Query
from app.sensors import models, schemas, repository
from app.database import SessionLocal
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from app.elasticsearch_client import ElasticsearchClient 
from app.timescale import Timescale
from app.cassandra_client import CassandraClient

# Dependency to get db session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_timescale():
    ts = Timescale()
    try:
        yield ts
    finally:
        ts.close()

# Dependency to get redis client
def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client
def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    try:
        yield mongodb
    finally:
        mongodb.close()

# Dependency to get elastic_search client
def get_elastic_search():
    es = ElasticsearchClient(host="elasticsearch")
    try:
        yield es
    finally:
        es.close()

# Dependency to get cassandra client
def get_cassandra_client():
    cassandra = CassandraClient(hosts=["cassandra"])
    try:
        yield cassandra
    finally:
        cassandra.close()


def callback(ch, method, properties, body,  
             db = Depends(get_db), 
             mongodb_client: MongoDBClient = Depends(get_mongodb_client), 
             es: ElasticsearchClient = Depends(get_elastic_search),
             redis_client: RedisClient = Depends(get_redis_client),
             timescale_client = Depends(get_timescale),
             cassandra_client = Depends(get_cassandra_client)):
    message = json.loads(body)
    request_type = message['request_type']
    data = message['data']

    response = {'response'}
    if request_type == 'search':
        response  = repository.search_sensors(db=db,mongodb=mongodb_client, es=es, query=data['query'], size=data['size'], search_type=data['search_type'])
    elif request_type == 'near':
        response = repository.get_sensors_near(latitude=data['latitude'], longitude=data['longitude'], radius=data['radius'], db=db, mongodb_client=mongodb_client, redis_client=redis_client)
    elif request_type == 'temperature/values':
        response = repository.get_values_sensor_temperatura(db=db, redis=redis_client, mongodb_client=mongodb_client, timescale_client= timescale_client, cassandra_client=cassandra_client)
    elif request_type == 'quantity_by_type':
        response =  repository.get_quantity_by_type(db=db, redis=redis_client, mongodb_client=mongodb_client, timescale_client= timescale_client, cassandra_client=cassandra_client)
    elif request_type == 'low_battery':
        response = repository.get_low_battery(db=db, redis=redis_client, mongodb_client=mongodb_client, timescale_client= timescale_client, cassandra_client=cassandra_client)
    elif request_type == 'get_sensors':
        response = repository.get_sensors(db)
    elif request_type == 'create_sensor':
        db_sensor = repository.get_sensor_by_name(db, data['sensor'].name)
        if db_sensor:
            raise HTTPException(status_code=400, detail="Sensor with same name already registered")
        response = repository.create_sensor(db=db, sensor=data['sensor'], mongodb_client=mongodb_client, elastic_client=ElasticsearchClient)
    elif request_type == 'get_sensor_by_id':
        db_sensor = repository.get_sensor(db, data['sensor_id'], mongodb_client)
        if db_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        response = db_sensor
    elif request_type == 'delete_sensor_by_id':
        db_sensor = repository.get_sensor(db, data['sensor_id'], )
        if db_sensor is None:
            raise HTTPException(status_code=404, detail="Sensor not found")
        response = repository.delete_sensor(db=db, sensor_id=data['sensor_id'], mongodb_client=mongodb_client)
    elif request_type == 'post_sensor_by_id_data':
        response = repository.record_data(redis=redis_client, sensor_id=data['sensor_id'], data=data, db=db, mongodb_client=mongodb_client, timescale_client=timescale_client, cassandra_client=cassandra_client)

    elif request_type == 'get_sensor_by_id_data':
        response = repository.get_data(redis=redis_client, sensor_id=data['sensor_id'], from_date=data['from_date'], to_date=data['to_date'], bucket=data['bucket'], db=db, mongodb_client=mongodb_client, timescale_client= timescale_client)

                                                          

    ch.basic_publish(exchange='', routing_key=properties.reply_to, properties=pika.BasicProperties(correlation_id=properties.correlation_id), body=json.dumps(response))


subscriber.subscribe(callback)
