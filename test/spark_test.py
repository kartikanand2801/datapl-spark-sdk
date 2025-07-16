from spark_sdk.spark_table_writer import SparkTableWriter
from pyspark.sql import SparkSession
import os

def test_write_table():
    spark = SparkSession.builder \
        .appName("SparkTableWriterTest") \
        .master("local[*]") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .getOrCreate()

    # Sample DataFrame
    df = spark.createDataFrame([
        ("Alice", 34),
        ("Bob", 45)
    ], ["name", "age"])

    writer = SparkTableWriter(
        spark=spark,
        database="default",
        table="test_people",
        format="parquet",
        path="./tmp/test_people1",
        description="Test table for people",
    )

    writer.drop_table_if_exists()
    writer.write(df)

    spark.sql("SHOW TABLES").show()
    spark.sql("SELECT * FROM default.test_people").show()

    spark.stop()

if __name__ == "__main__":
    test_write_table()
