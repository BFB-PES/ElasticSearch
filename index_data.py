#elastic search with last index data

import psycopg2
from elasticsearch import Elasticsearch
import time

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Connect to PostgreSQL
postgres_config = {
    'host': 'localhost',
    'database': 'fashion',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'  
}

pg_conn = psycopg2.connect(**postgres_config)
pg_cursor = pg_conn.cursor()

# Connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

def get_last_indexed_id(index_name):
    # Query Elasticsearch to get the last record - saves state
    body = {
        "size": 1,
        "sort": [
            {
                "id": {
                    "order": "desc"
                }
            }
        ]
    }
    res = es.search(index=index_name, body=body)

    # Extract the ID of the last record
    last_indexed_id = res['hits']['hits'][0]['_source']['id']

    return last_indexed_id 

def index_postgresql_to_elasticsearch(table_name, index_name):
    # Retrieve the ID of the last indexed record
    last_indexed_id = get_last_indexed_id(index_name)

    # Retrieve new records from PostgreSQL
    pg_cursor.execute(f"SELECT * FROM {table_name} WHERE id > {last_indexed_id}")
    columns = [desc[0] for desc in pg_cursor.description]
    rows = pg_cursor.fetchall()

    # Index new records in Elasticsearch
    for row in rows:
        doc = dict(zip(columns, row))
        es.index(index=index_name, body=doc)


#check: no hit situation; when no record match occurs

if __name__ == '__main__':
    print("Started")
    # Specify the PostgreSQL table name and Elasticsearch index name
    postgresql_table = 'fashion0'
    elasticsearch_index = 'fashion_index_1'

    start_time = time.time()
    index_postgresql_to_elasticsearch(postgresql_table, elasticsearch_index)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"indexing took {elapsed_time} seconds to run.")

    # Close connections
    pg_cursor.close()
    pg_conn.close()
    print("Done")
