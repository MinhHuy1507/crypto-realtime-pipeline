"""
Kafka -> 1-min OHLCV aggregation -> PostgreSQL.
Reads Avro trade events, applies tumbling window aggregation, writes candles to raw.candles_log.
"""
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from dependencies.spark_session import create_spark_session
from dependencies.container import get_container
from dependencies.get_schema import get_latest_schema

from config.kafka import KAFKA_TOPIC
from config.s3 import S3_CHECKPOINT_DIR

from pyspark.sql.functions import col, expr, max, min, sum, count, window
from pyspark.sql.avro.functions import from_avro

CHECKPOINT_DIR = os.path.join(S3_CHECKPOINT_DIR, "ingest_postgres/crypto_candles/")
TABLE = "candles_log"
SCHEMA = "raw"


if __name__ == "__main__":
    # Spark setup
    spark = create_spark_session("PostgreSQL Ingestion")
    spark.sparkContext.setLogLevel("WARN")
    container = get_container(spark)
    df = container.kafka_reader.read(topic=KAFKA_TOPIC, starting_offsets="latest", kafka_group_id="postgres_group")
    
    # Deserialize: strip 5-byte Confluent wire format header (1 magic byte + 4 schema ID bytes)
    crypto_trades_schema = get_latest_schema()
    parsed_df = df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)")) \
        .withColumn("data", from_avro(col("fixed_value"), crypto_trades_schema)) \
        .select("data.*")

    # Window and aggregation
    agg_df = parsed_df.withWatermark(eventTime="event_time", delayThreshold="1 minute") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("symbol")
        ) \
        .agg(
            expr("min_by(price, event_time)").alias("open"),  
            max("price").alias("high"),                      
            min("price").alias("low"),                       
            expr("max_by(price, event_time)").alias("close"),
            sum("quantity").alias("volume"),
            count("*").alias("num_trades")
        )
    
    agg_df = agg_df.select(
        col("symbol"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
        col("num_trades")
    )
    
    # Ingest to PostgreSQL
    query = container.postgres_writer.write(
        df=agg_df,
        table_name=f"{SCHEMA}.{TABLE}",
        checkpoint_location=CHECKPOINT_DIR,
        outputMode="update"
    )
    query.awaitTermination()
    