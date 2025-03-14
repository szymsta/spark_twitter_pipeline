from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import explode_outer, col, lower


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
