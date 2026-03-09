from pyspark.sql import DataFrame, SparkSession
from .base import BaseReader, BaseWriter

class KafkaReader(BaseReader):
    def __init__(self, spark: SparkSession, bootstrap_servers: str):
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers

    def read(self, topic: str, starting_offsets: str = "latest", fail_on_data_loss: str = "false", max_offsets_per_trigger: int = None,
             kafka_group_id: str | None = None) -> DataFrame:
        """
        Reads data from a Kafka topic into a Spark DataFrame.
        """
        reader = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", starting_offsets)
            .option("failOnDataLoss", fail_on_data_loss)
        )
        
        if kafka_group_id:
            reader = reader.option("kafka.group.id", kafka_group_id).option("commitOffsetsOnCheckpoints", "true")
        
        if max_offsets_per_trigger:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets_per_trigger)
            
        return reader.load()

class KafkaWriter(BaseWriter):
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers

    def write(self, df: DataFrame, topic: str, checkpoint_location: str):
        """
        Writes a Spark DataFrame to a Kafka topic.
        """
        (
            df.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", topic)
            .option("checkpointLocation", checkpoint_location)
            .start()
        )