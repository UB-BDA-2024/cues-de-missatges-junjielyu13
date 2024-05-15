from yoyo import step
steps = [
   step(
       "CREATE TABLE IF NOT EXISTS sensor_data ( id SERIAL PRIMARY KEY, sensor_id INT NOT NULL, data JSONB, last_seen TIMESTAMPTZ NOT NULL);",
   )
]

# psql -h timescale -p 5344 -U timescale -d timesclae