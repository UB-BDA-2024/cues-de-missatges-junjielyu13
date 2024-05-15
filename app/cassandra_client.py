from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sensor
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
            """)
        # self.session.execute("""
        #                     CREATE KEYSPACE IF NOT EXISTS sensor
        #                     WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
        #                     """)
        
        self.session.set_keyspace('sensor')
        self.session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data ( id uuid PRIMARY KEY, sensor_id INT, data TEXT, last_seen TEXT, type_sensor TEXT);
        """)

    def get_session(self):
        return self.session

    def get_session_keyspace(self):
        return self.cluster.connect('sensor')

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)
