import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from .base import BaseWriter

logger = logging.getLogger(__name__)

class PostgresWriter(BaseWriter):
    def __init__(self, url, user, password, driver="org.postgresql.Driver"):
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver

    def _write_to_postgres(self, batch_df, batch_id, table_name):
        """
        Function to execute data writing micro-batch (called inside foreachBatch)
        """
        try:
            logger.info(f"Writing batch {batch_id} to table {table_name}")
            df = batch_df.withColumn("batch_id", lit(batch_id))
            (
                df.write.format("jdbc")
                .option("url", self.url)
                .option("dbtable", table_name)
                .option("user", self.user)
                .option("password", self.password)
                .option("driver", self.driver)
                .mode("append")
                .save()
            )
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")
            raise e 

    def write(self, df: DataFrame, table_name: str, checkpoint_location: str, outputMode: str = "update"):
        """
        Writes a Spark DataFrame to a Postgres table using foreachBatch.
        """
        write_func = lambda batch_df, batch_id: self._write_to_postgres(batch_df, batch_id, table_name)

        return (
            df.writeStream
            .foreachBatch(write_func)
            .trigger(processingTime="10 seconds")
            .option("checkpointLocation", checkpoint_location)
            .outputMode(outputMode)
            .start()
        )