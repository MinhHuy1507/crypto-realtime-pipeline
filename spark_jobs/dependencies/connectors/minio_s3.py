from pyspark.sql import DataFrame
from .base import BaseWriter

class MinioS3Writer(BaseWriter):
    def write(self, df: DataFrame, output_path: str, output_mode: str = "update", checkpoint_location: str = None, partition_by: list = None, processing_time: str = "1 minute"):
        """
        Writes a streaming DataFrame to MinIO S3 in Parquet format.
        """
        return (
            df.writeStream
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_location) \
            .outputMode(output_mode) \
            .partitionBy(partition_by) \
            .trigger(processingTime=processing_time) \
            .start()
        )