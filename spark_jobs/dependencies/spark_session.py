from pyspark.sql import SparkSession
from config.s3 import S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY

def create_spark_session(app_name: str) -> SparkSession:
    """
    Create and return a SparkSession with the given application name.
    Args:
        app_name (str): The name of the Spark application.
        
    Returns:
    SparkSession: Configured Spark session with:
        - 2 shuffle partitions
        - Graceful shutdown enabled
        - Resource limits (max cores: 2, exec mem: 800m, driver mem: 512m)
        - MinIO S3-compatible storage configuration
    """
    spark = (SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.cores.max", "2")
        .config("spark.executor.memory", "512m")
        .config("spark.executor.memoryOverhead", "256m")
        .config("spark.executor.cores", "2")
        .config("spark.driver.memory", "512m")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.ui.prometheus.enabled", "true")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.metrics.conf.*.sink.prometheusServlet.class", "org.apache.spark.metrics.sink.PrometheusServlet")
        .config("spark.metrics.conf.*.sink.prometheusServlet.path", "/metrics/prometheus/")
        .config("spark.metrics.namespace", "spark")
        .config("spark.metrics.conf.*.sink.jmx.class", "org.apache.spark.metrics.sink.JmxSink")
        .config("spark.driver.extraJavaOptions", "-javaagent:/opt/jmx_prometheus_javaagent-0.19.0.jar=9101:/opt/airflow/monitoring/spark_jmx/spark-jmx-config.yml")
        .config("spark.executor.extraJavaOptions", "-javaagent:/opt/jmx_prometheus_javaagent-0.19.0.jar=9102:/opt/airflow/monitoring/spark_jmx/spark-jmx-config.yml")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def stop_spark_session(spark: SparkSession):
    """
    Stop the given SparkSession.
    Args:
        spark (SparkSession): The Spark session to stop.
    """
    if spark:
        spark.stop()