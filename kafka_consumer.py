import json
import psycopg2
from confluent_kafka import Consumer, KafkaError
# import time

# time.sleep(10)

def consume_and_write_to_postgres(topic, bootstrap_servers, postgres_config):
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': 'csv-consumer',
        'auto.offset.reset': 'earliest'
    }

    # Set up PostgreSQL connection
    conn = psycopg2.connect(**postgres_config)
    cursor = conn.cursor()

    # Create Kafka consumer
    consumer = Consumer(consumer_config)

    # Subscribe to Kafka topic
    consumer.subscribe([topic])

    # Consume and write data to PostgreSQL
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust timeout as needed
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        if msg is not None:
            # Process the received message and write to PostgreSQL
            value = msg.value().decode('utf8').replace("'", '"')
            
            row_dict = json.loads(value)
            # Example: Insert data into PostgreSQL
            insert_query = "INSERT INTO fashion (id, name) VALUES (%s, %s)"
            cursor.execute(insert_query, (row_dict['id'], row_dict['name']))
            conn.commit()
    

    # Clean up
    cursor.close()
    conn.close()

postgres_config = {
    'host': 'localhost',
    'database': 'fashion',
    'user': 'postgres',
    'password': 'postgres',
    'port': '5432'  # Default PostgreSQL port
}
# Example usage
consume_and_write_to_postgres('BFB1', 'localhost:9092', postgres_config)
