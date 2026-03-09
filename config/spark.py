SPARK_HOME = "/opt/spark"

SPARK_SUBMIT = f"{SPARK_HOME}/bin/spark-submit"

SPARK_MASTER = "spark://spark-master:7077"

JARS = ",".join([
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/kafka-clients-3.4.1.jar",
    "/opt/spark/jars/commons-pool2-2.11.1.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "/opt/spark/jars/postgresql-42.6.0.jar",
    "/opt/spark/jars/hadoop-aws-3.3.4.jar",
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    "/opt/spark/jars/spark-avro_2.12-3.5.0.jar"
])