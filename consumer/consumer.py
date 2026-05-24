import json
import time
from kafka import KafkaConsumer

print("=" * 60)
print("Starting Kafka CDC Consumer...")
print("=" * 60)
topic_name = "dbserver1.public.customers"
print(f"Connecting to Kafka broker at 'kafka:29092'...")
print(f"Waiting for topic '{topic_name}' to be created by Debezium...")

while True:
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='cdc-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        if topic_name in consumer.topics():
            consumer.subscribe([topic_name])
            break
        else:
            print(".", end="", flush=True)
            time.sleep(3)
    except Exception as e:
        print("x", end="", flush=True)
        time.sleep(3)

print("\nConnected to Kafka successfully!")
print(f"Now listening for real-time Postgres events on topic: {topic_name}")
print("Press Ctrl+C to exit.")
print("-" * 60)

try:
    for message in consumer:
        print(f"\n[EVENT RECEIVED] Partition: {message.partition} | Offset: {message.offset}")
        event_data = message.value
        if event_data and 'payload' in event_data:
            payload = event_data['payload']
            op = payload.get('op')
            before = payload.get('before')
            after = payload.get('after')
            op_names = {
                'c': 'CREATE (Insert)',
                'u': 'UPDATE',
                'd': 'DELETE',
                'r': 'READ (Snapshot)'
            }
            print(f"Operation Type: {op_names.get(op, op)}")
            if before:
                print(f"Before State: {json.dumps(before)}")
            if after:
                print(f"After State : {json.dumps(after)}")
        else:
            print(json.dumps(event_data, indent=2))
        print("-" * 60)
except KeyboardInterrupt:
    print("\nExiting consumer...")
