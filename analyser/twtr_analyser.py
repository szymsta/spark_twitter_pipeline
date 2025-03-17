from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode_outer, col, lower, avg, count, round


class TwtrAnalyser:
    """
    Class for analyzing Twitter data, such as calculating the most common hashtags, retweets, 
    sources, and average followers by location.
    """

    HASHTAG_COLUMN = "hashtags"
    IS_RETWEET_COLUMN = "is_retweet"
    SOURCE_COLUMN = "source"
    USER_FOLLOWERS = "user_followers"
    USER_NAME = "user_name"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the TwtrAnalyser object with a Spark session.

        :param spark_session: An instance of SparkSession to perform data analysis.
        """
        self.spark_session = spark_session
    

    def calculate_hashtags(self, df: DataFrame) -> DataFrame:
        """
        Calculates the most frequent hashtags in the dataset (transformation).
        This method performs the following transformations:
        - Explodes the 'hashtags' column into individual rows.
        - Filters out null or empty hashtags.
        - Converts hashtags to lowercase.
        - Groups by hashtag and counts occurrences.

        :param df: The DataFrame containing the raw tweet data.
        :return: A DataFrame with the counts of the most frequent hashtags (transformation).
        """
        return (df.withColumn(self.HASHTAG_COLUMN, explode_outer(col(self.HASHTAG_COLUMN)))
                .filter((col(self.HASHTAG_COLUMN).isNotNull()) & (col(self.HASHTAG_COLUMN) != ""))
                .withColumn(self.HASHTAG_COLUMN, lower(col(self.HASHTAG_COLUMN)))
                .groupBy(self.HASHTAG_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )


    def calculate_retwtr(self, df: DataFrame) -> DataFrame:
        """
        Calculates the number of retweets in the dataset (transformation).
        This method performs the following transformations:
        - Filters out null or empty retweet indicators.
        - Groups by the retweet column and counts occurrences.

        :param df: The DataFrame containing the raw tweet data.
        :return: A DataFrame with the count of retweets (transformation).
        """
        return (df.filter((col(self.IS_RETWEET_COLUMN).isNotNull()) & (col(self.IS_RETWEET_COLUMN) != ""))
                .groupBy(self.IS_RETWEET_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )
    

    def calculate_source(self, df: DataFrame) -> DataFrame:
        """
        Calculates the most common sources from which the tweets originate (transformation).
        This method performs the following transformations:
        - Filters out null or empty source values.
        - Groups by the source column and counts occurrences.

        :param df: The DataFrame containing the raw tweet data.
        :return: A DataFrame with the count of tweets per source (transformation).
        """
        return (df.filter((col(self.SOURCE_COLUMN).isNotNull()) & (col(self.SOURCE_COLUMN) != ""))
                .groupBy(self.SOURCE_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )


    def calculate_avg_followers_by_location(self, df: DataFrame) -> DataFrame:
        """
        Calculates the average number of followers per location (transformation).
        This method performs the following transformations:
        - Selects relevant columns: user name, user location, and user followers.
        - Filters out null or empty user names and locations.
        - Removes duplicates based on user name.
        - Groups by user location and calculates the average number of followers.

        :param df: The DataFrame containing the raw tweet data.
        :return: A DataFrame with the average number of followers per location (transformation).
        """
        return (df.select(self.USER_NAME, self.USER_LOCATION, self.USER_FOLLOWERS)
                .filter((col(self.USER_NAME).isNotNull()) & (col(self.USER_NAME) != ""))
                .filter((col(self.USER_LOCATION).isNotNull()) & (col(self.USER_LOCATION) != ""))
                .dropDuplicates([self.USER_NAME])
                .groupBy(self.USER_LOCATION)
                .agg(round(avg(self.USER_FOLLOWERS), 2).alias("avg_followers"))
                .orderBy(col("avg_followers").desc())
        )
