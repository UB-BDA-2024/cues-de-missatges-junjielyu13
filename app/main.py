import fastapi
from .sensors.controller import router as sensorsRouter
from yoyo import read_migrations, get_backend
import os

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

app.include_router(sensorsRouter)

# from app.cassandra_client import CassandraClient

# cassandra_client = CassandraClient(["cassandra"])
# cassandra_client.execute("""
# CREATE KEYSPACE IF NOT EXISTS sensor
# WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
# """)

# cassandra_client.get_session().set_keyspace('sensor')

# cassandra_client.execute("""
# CREATE TABLE IF NOT EXISTS sensor_data ( id uuid PRIMARY KEY, sensor_id INT, data TEXT, last_seen timestamp);
# """)

# backend = get_backend("postgresql://timescale:timescale@timescale:5433/timescale")
# migrations = read_migrations('migrations_time')
# with backend.lock():
#     migrations_to_apply = backend.to_apply(migrations)
#     if migrations_to_apply:
#         backend.apply_migrations(migrations_to_apply)


@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
