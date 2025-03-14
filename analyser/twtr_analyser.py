from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode_outer, col, lower, avg, count, round


class TwtrAnalyser:

    HASHTAG_COLUMN = "hashtags"
    IS_RETWEET_COLUMN = "is_retweet"
    SOURCE_COLUMN = "source"
    USER_FOLLOWERS = "user_followers"
    USER_NAME = "user_name"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
    

    def calculate_hashtags(self, df: DataFrame) -> DataFrame:
        return (df.withColumn(self.HASHTAG_COLUMN, explode_outer(col(self.HASHTAG_COLUMN)))
                .filter((col(self.HASHTAG_COLUMN).isNotNull()) & (col(self.HASHTAG_COLUMN) != ""))
                .withColumn(self.HASHTAG_COLUMN, lower(col(self.HASHTAG_COLUMN)))
                .groupBy(self.HASHTAG_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )


    def calculate_retwtr(self, df: DataFrame) -> DataFrame:
        return (df.filter((col(self.IS_RETWEET_COLUMN).isNotNull()) & (col(self.IS_RETWEET_COLUMN) != ""))
                .groupBy(self.IS_RETWEET_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )
    

    def calculate_source(self, df: DataFrame) -> DataFrame:
        return (df.filter((col(self.SOURCE_COLUMN).isNotNull()) & (col(self.SOURCE_COLUMN) != ""))
                .groupBy(self.SOURCE_COLUMN)
                .count()
                .orderBy(col("count").desc())
        )


    def calculate_avg_followers_by_location(self, df: DataFrame) -> DataFrame:
        return (df.select(self.USER_NAME, self.USER_LOCATION, self.USER_FOLLOWERS)
                .filter((col(self.USER_NAME).isNotNull()) & (col(self.USER_NAME) != ""))
                .filter((col(self.USER_LOCATION).isNotNull()) & (col(self.USER_LOCATION) != ""))
                .dropDuplicates([self.USER_NAME])
                .groupBy(self.USER_LOCATION)
                .agg(round(avg(self.USER_FOLLOWERS), 2).alias("avg_followers"))
                .orderBy(col("avg_followers").desc())
        )