import psycopg2
from elasticsearch import Elasticsearch

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Connect to PostgreSQL
postgres_config = {
    'host': 'localhost',
    'database': 'fashion',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'  # Default PostgreSQL port
}

pg_conn = psycopg2.connect(**postgres_config)
pg_cursor = pg_conn.cursor()

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])
    #basic_auth=('elastic', 'NSFO6Y0TTCi7PhlaIQu2'),
    #verify_certs=False, 
    #request_timeout=300) #might need to inc timeout acc 

def index_postgresql_to_elasticsearch(table_name, index_name):
    # Retrieve data from PostgreSQL
    pg_cursor.execute(f"SELECT * FROM fashion1")    #1000 records
    columns = [desc[0] for desc in pg_cursor.description]
    rows = pg_cursor.fetchall()

    # Index data in Elasticsearch
    for row in rows:
        #print(row)
        doc = dict(zip(columns, row))
        es.index(index=index_name, body=doc)

if __name__ == '__main__':
    print("Started")
    # Specify the PostgreSQL table name and Elasticsearch index name
    postgresql_table = 'fashion1'
    elasticsearch_index = 'fashion_index_1'

    index_postgresql_to_elasticsearch(postgresql_table, elasticsearch_index)

    # Close connections
    pg_cursor.close()
    pg_conn.close()
    print("Done")