from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, split, col
from pyspark.sql.types import LongType, DateType


class TwtrCleaner:
    """
    The TwtrCleaner class is responsible for cleaning and transforming the raw tweet data.
    It handles operations such as cleaning and formatting columns in the dataset.

    Attributes:
        spark_session (SparkSession): The Spark session used for performing DataFrame operations.

    Constants:
        HASHTAG (str): The column name for hashtags.
        DATE (str): The column name for the tweet's date.
        USER_CREATED (str): The column name indicating when the user account was created.
        USER_FOLLOWERS (str): The column name for the number of followers of the user.
        USER_FRIENDS (str): The column name for the number of friends of the user.
        USER_FAVOURITES (str): The column name for the number of tweets the user has favourited.
    """

    HASHTAG = "hashtags"
    DATE = "date"
    USER_CREATED = "user_created"
    USER_FOLLOWERS = "user_followers"
    USER_FRIENDS = "user_friends"
    USER_FAVOURITES = "user_favourites"

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
        return (df.withColumn(self.HASHTAG, split(regexp_replace(col(self.HASHTAG), r'["\'\[\] _-]', ""), ","))
                .withColumn(self.DATE, col(self.DATE).cast(DateType()))
                .withColumn(self.USER_CREATED, col(self.USER_CREATED).cast(DateType()))
                .withColumn(self.USER_FOLLOWERS, col(self.USER_FOLLOWERS).cast(LongType()))
                .withColumn(self.USER_FRIENDS, col(self.USER_FRIENDS).cast(LongType()))
                .withColumn(self.USER_FAVOURITES, col(self.USER_FAVOURITES).cast(LongType()))
                )
