import psycopg2
from elasticsearch import Elasticsearch

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connect_to_postgresql():
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
    return pg_conn, pg_cursor

def connect_to_elasticsearch():
    # Connect to Elasticsearch
    es = Elasticsearch(['http://localhost:9200'],
    #basic_auth=('elastic', 'NSFO6Y0TTCi7PhlaIQu2'),
    verify_certs=False)
    return es

def run_elasticsearch_query(es, index_name, query_body):
    # Run Elasticsearch query
    result = es.search(index=index_name, body=query_body)
    return result

def main():
    try:
        # Connect to PostgreSQL
        pg_conn, pg_cursor = connect_to_postgresql()

        # Connect to Elasticsearch
        es = connect_to_elasticsearch()

        # Define Elasticsearch query
        index_name = 'fashion_index_1'
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"seller": "Apraa & Parma"}},
                        {"match": {"color": "Rose"}}
                    ]
                }
            },
            "size": 100
}



        # Run Elasticsearch query
        result = run_elasticsearch_query(es, index_name, query_body)

        # Process and print the result
        print("Elasticsearch Query Result:")
        print(result)

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close connections
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()

if __name__ == '__main__':
    main()
