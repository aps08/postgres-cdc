import urllib.request
import json
import time
import sys

url = "http://localhost:8083/connectors"
connector_config = {
    "name": "postgres-cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres_user",
        "database.password": "postgres_password",
        "database.dbname": "inventory",
        "topic.prefix": "dbserver1",
        "table.include.list": "public.customers",
        "decimal.handling.mode": "double"
    }
}

print("Checking Debezium health...")
attempts = 0
max_attempts = 30
connected = False

while attempts < max_attempts:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                connected = True
                break
    except Exception:
        print(".", end="", flush=True)
        time.sleep(3)
        attempts += 1

if not connected:
    print("\nError: Debezium did not start in time. Please check your docker logs.")
    sys.exit(1)

print("\nDebezium is up and healthy! Registering PostgreSQL connector...")
data = json.dumps(connector_config).encode('utf-8')
req = urllib.request.Request(
    url, 
    data=data, 
    headers={"Content-Type": "application/json"}, 
    method="POST"
)

try:
    with urllib.request.urlopen(req) as response:
        print("\n" + "=" * 60)
        print("SUCCESS: PostgreSQL CDC Connector Registered Successfully!")
        print("=" * 60)
        print(response.read().decode('utf-8'))
except Exception as e:
    if hasattr(e, 'code') and e.code == 409:
        print("\nConnector is already registered.")
    else:
        print(f"\nError registering connector: {e}")
        sys.exit(1)
