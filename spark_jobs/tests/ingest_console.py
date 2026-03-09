import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
root_dir = os.path.dirname(parent_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from pyspark.sql.functions import col, expr, window, max, min, sum, count
from pyspark.sql.avro.functions import from_avro

from dependencies.spark_session import create_spark_session
from dependencies.container import get_container
from dependencies.get_schema import get_latest_schema

from config.kafka import KAFKA_TOPIC, SCHEMA_REGISTRY_URL, KAFKA_BOOTSTRAP_SERVERS
from config.s3 import S3_CHECKPOINT_DIR

CHECKPOINT_LOCATION = S3_CHECKPOINT_DIR + "/console_debug"


if __name__ == "__main__":
    spark = create_spark_session("IngestConsoleDebug")
    spark.sparkContext.setLogLevel("WARN")

    container = get_container(spark)
    df = container.kafka_reader.read(topic=KAFKA_TOPIC, starting_offsets="latest")
    
    avro_schema_json = get_latest_schema()
    
    print(f"Loaded Schema: {avro_schema_json}")

    # Deserialize: strip 5-byte Confluent wire format header (1 magic byte + 4 schema ID bytes)
    parsed_df = df \
    .withColumn("fixed_value", expr("substring(value, 6, length(value)-5)")) \
    .withColumn("data", from_avro(col("fixed_value"), avro_schema_json)) \
    .select("data.*")
    
    # Avro logicalType timestamp-millis has been converted by Spark to TimestampType
    parsed_df = parsed_df \
        .withColumn("event_timestamp", col("event_time")) \
        .withColumn("processing_timestamp", col("processing_time"))

    # window and aggregation
    agg_df = parsed_df.withWatermark(eventTime="event_timestamp", delayThreshold="1 minute") \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("symbol")
        ) \
        .agg(
            expr("min_by(price, event_timestamp)").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            expr("max_by(price, event_timestamp)").alias("close"),
            sum("quantity").alias("volume"),
            count("*").alias("number_of_trades")
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
        col("number_of_trades")
    )
    
    # local testing to console
    query = agg_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    query.awaitTermination()