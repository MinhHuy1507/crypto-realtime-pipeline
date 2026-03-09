from pyspark.sql import SparkSession

def test_spark_connection():
    spark = SparkSession.builder.appName("TestSparkConnection").getOrCreate()
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    df.show()

if __name__ == "__main__":
    test_spark_connection()