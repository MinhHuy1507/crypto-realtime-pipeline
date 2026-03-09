import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_trades')

SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', "http://schema-registry:8081")
SCHEMA_PATH = os.getenv('SCHEMA_PATH', '/opt/airflow/config/schemas/crypto_trades.avsc')