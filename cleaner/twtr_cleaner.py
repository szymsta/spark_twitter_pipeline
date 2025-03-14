from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, split, col
from pyspark.sql.types import LongType, DateType


class TwtrCleaner:

    def __init__(self, spark_session : SparkSession):
        self.spark_session = spark_session


    def clean_dataset(self, df: DataFrame) -> DataFrame:
        return (df.withColumn("hashtags", split(regexp_replace(col("hashtags"), r'["\'\[\] _-]', ""), ","))
                .withColumn("date", col("date").cast(DateType()))
                .withColumn("user_created", col("user_created").cast(DateType()))
                .withColumn("user_followers", col("user_followers").cast(LongType()))
                .withColumn("user_friends", col("user_friends").cast(LongType()))
                .withColumn("user_favourites", col("user_favourites").cast(LongType()))
                )