import psycopg2
import os


class Timescale:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ.get("TS_HOST"),
            port=os.environ.get("TS_PORT"),
            user=os.environ.get("TS_USER"),
            password=os.environ.get("TS_PASSWORD"),
            database=os.environ.get("TS_DBNAME"))
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS sensor_data ( id SERIAL PRIMARY KEY, sensor_id INT NOT NULL, data JSONB, last_seen TIMESTAMPTZ NOT NULL);")
        
    def getCursor(self):
            return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def ping(self):
        return self.conn.ping()
    
    def execute(self, query):
       return self.cursor.execute(query)
    
    def insert(self, query):
        # Insert values into a table
        self.cursor.execute(query)
        self.conn.commit()

    def select(self, query):
        # Select values from a table
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def fetchall(self):
        # Fetch all results from the last executed statement
        return self.cursor.fetchall()
    
    def delete(self, table):
        self.cursor.execute("DELETE FROM " + table)
        self.conn.commit()

        
     
         
