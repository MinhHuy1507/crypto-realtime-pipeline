"""
Crypto Trades Producer — Entry point for the real-time ingestion pipeline.
Binance WebSocket -> TradeTransformer -> Avro Serialization -> Kafka.
"""
import json
import signal
import sys
from config.kafka import KAFKA_TOPIC
from producers.connectors.logging_config import setup_logging
from producers.connectors.kafka_client import KafkaClient
from producers.connectors.binance_client import BinanceClient
from producers.domain.transformer import TradeTransformer
from producers.connectors.schema_registry import avro_serializer, string_serializer, serialization_context

logger = setup_logging()

def crypto_trades_producer():
    logger.info("Starting Crypto Realtime Producer...")
    
    kafka_client = KafkaClient()
    running = True

    def shutdown_handler(signum, frame):
        nonlocal running
        logger.info(f"Received signal {signum}. Shutting down gracefully...")
        running = False
        kafka_client.flush()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    def handle_message(raw_message: str):
        """
        Callback to process and send message to Kafka.
        """
        record = TradeTransformer.transform(raw_message)
        if record:
            kafka_client.produce(
                topic=KAFKA_TOPIC,
                key=string_serializer(record['symbol']),
                value=avro_serializer(record, serialization_context),
                callback=delivery_report
            )
            kafka_client.poll(0)

    def delivery_report(err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')

    # Initialize and start Binance Client
    binance_client = BinanceClient(on_message_callback=handle_message)
    binance_client.start()

if __name__ == "__main__":
    crypto_trades_producer()
