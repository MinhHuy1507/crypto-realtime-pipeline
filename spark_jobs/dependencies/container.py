"""
Dependency Injection container — provides lazy-initialized connectors
(KafkaReader, PostgresWriter, MinioS3Writer) to Spark jobs.
"""
from pyspark.sql import SparkSession
from config.kafka import KAFKA_BOOTSTRAP_SERVERS
from config.postgres import POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DRIVER
from dependencies.connectors import KafkaReader, KafkaWriter, PostgresWriter, MinioS3Writer

class Container:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._kafka_reader = None
        self._kafka_writer = None
        self._postgres_writer = None
        self._minio_s3_writer = None

    @property
    def kafka_reader(self) -> KafkaReader:
        if not self._kafka_reader:
            self._kafka_reader = KafkaReader(
                spark=self.spark,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
            )
        return self._kafka_reader

    @property
    def kafka_writer(self) -> KafkaWriter:
        if not self._kafka_writer:
            self._kafka_writer = KafkaWriter(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
            )
        return self._kafka_writer

    @property
    def postgres_writer(self) -> PostgresWriter:
        if not self._postgres_writer:
            self._postgres_writer = PostgresWriter(
                url=POSTGRES_URL,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                driver=POSTGRES_DRIVER
            )
        return self._postgres_writer

    @property
    def minio_s3_writer(self) -> MinioS3Writer:
        if not self._minio_s3_writer:
            self._minio_s3_writer = MinioS3Writer()
        return self._minio_s3_writer
    

def get_container(spark: SparkSession) -> Container:
    return Container(spark)