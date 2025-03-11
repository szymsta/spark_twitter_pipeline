# Libraries
from pyspark.sql import SparkSession


# Spark Session
spark = (
    SparkSession.builder
    .appName("spark_twtr")
    .master("local[*]")
    .getOrCreate()
)