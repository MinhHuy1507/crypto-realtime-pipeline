"""
Kafka -> MinIO S3 raw archive.
Reads Avro trade events, writes raw trades as Parquet partitioned by event_date and symbol.
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

from config.s3 import S3_CHECKPOINT_DIR, S3_OUTPUT_PATH
from config.kafka import KAFKA_TOPIC

from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

CHECKPOINT_DIR = os.path.join(S3_CHECKPOINT_DIR, "ingest_minio/raw")
OUTPUT_PATH = os.path.join(S3_OUTPUT_PATH, "raw")
INPUT_TOPIC = KAFKA_TOPIC


if __name__ == "__main__":
    # Spark setup
    spark = create_spark_session("MinIO S3 Ingestion")
    spark.sparkContext.setLogLevel("WARN")
    container = get_container(spark)
    df = container.kafka_reader.read(topic=KAFKA_TOPIC, starting_offsets="latest")
    
    # Deserialize: strip 5-byte Confluent wire format header (1 magic byte + 4 schema ID bytes)
    crypto_trades_schema = get_latest_schema()
    parsed_df = df.withColumn("fixed_value", expr("substring(value, 6, length(value)-5)")) \
        .withColumn("data", from_avro(col("fixed_value"), crypto_trades_schema)) \
        .select("data.*")
    parsed_df = parsed_df.withColumn(
        "event_date",
        expr("to_date(event_timestamp)")
    )

    df = parsed_df.select(
        col("symbol"),
        col("price"),
        col("quantity"),
        col("event_timestamp"),
        col("processing_timestamp"),
        col("event_date")
    )
    
    # Ingest to Minio S3, partitioned by event date and symbol.
    query = container.minio_s3_writer.write(
        df=df,
        output_path=OUTPUT_PATH,
        output_mode="append",
        checkpoint_location=CHECKPOINT_DIR,
        partition_by=["event_date", "symbol"],
        processing_time="1 minute"
    )
    query.awaitTermination()
