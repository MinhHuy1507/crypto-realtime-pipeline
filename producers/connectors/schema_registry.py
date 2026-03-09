"""
Schema Registry client setup — loads the Avro schema from disk,
registers it with Confluent Schema Registry, and provides serializers.
"""
import os
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# configuration
from config.kafka import KAFKA_TOPIC, SCHEMA_REGISTRY_URL, SCHEMA_PATH

with open(SCHEMA_PATH, 'r') as f:
    schema_str = f.read()

# Create Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda obj, ctx: obj
)

# Key Serializer for Key as String (Symbol)
string_serializer = StringSerializer('utf_8')

# Serialization Context for Kafka Topic
serialization_context = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)