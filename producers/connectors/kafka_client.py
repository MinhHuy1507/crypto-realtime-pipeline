from confluent_kafka import Producer
from config.kafka import KAFKA_BOOTSTRAP_SERVERS

class KafkaClient:
    def __init__(self, client_id: str = "crypto_producer"):
        self.config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': client_id,
            'enable.idempotence': True,
            'acks': 'all',
            'retries': 1000000,
            'delivery.timeout.ms': 120000, # 2 minutes
            'linger.ms': 20,
            'compression.type': 'lz4',
            'batch.size': 16384, # 16KB
            'request.timeout.ms': 30000
        }
        self.producer = Producer(self.config)

    def produce(self, topic, key, value, callback=None):
        self.producer.produce(topic=topic, key=key, value=value, callback=callback)

    def poll(self, timeout=0):
        self.producer.poll(timeout)
    
    def flush(self):
        self.producer.flush()
