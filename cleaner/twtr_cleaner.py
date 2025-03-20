from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, split, col
from pyspark.sql.types import LongType, DateType


class TwtrCleaner:
    """
    The TwtrCleaner class is responsible for cleaning and transforming the raw tweet data.
    It handles operations such as cleaning and formatting columns in the dataset.
    """

    def __init__(self, spark_session : SparkSession):
        """
        Initializes the TwtrCleaner object.

        Args:
            spark_session (SparkSession): An instance of SparkSession to handle data operations.
        """
        self.spark_session = spark_session


    def clean_dataset(self, df: DataFrame) -> DataFrame:
        """
        Cleans and transforms the input dataset by:
        - Removing unwanted characters from the "hashtags" column.
        - Casting the "date" and "user_created" columns to DateType.
        - Casting the "user_followers", "user_friends", and "user_favourites" columns to LongType.

        Args:
            df (DataFrame): The DataFrame containing the raw tweet data.

        Returns:
            DataFrame: A cleaned DataFrame with properly formatted columns.
        """
        return (df.withColumn("hashtags", split(regexp_replace(col("hashtags"), r'["\'\[\] _-]', ""), ","))
                .withColumn("date", col("date").cast(DateType()))
                .withColumn("user_created", col("user_created").cast(DateType()))
                .withColumn("user_followers", col("user_followers").cast(LongType()))
                .withColumn("user_friends", col("user_friends").cast(LongType()))
                .withColumn("user_favourites", col("user_favourites").cast(LongType()))
                )
