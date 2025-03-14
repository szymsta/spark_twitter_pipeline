from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower
from functools import reduce


class TwtrSearcher:

    TEXT = "text"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        self,spark_session: spark_session
    

    def search_keyword(self, key_word: str, df: DataFrame) -> DataFrame:
        return df.filter(lower(col(self.TEXT)).contains(lower(key_word)))
    

    def search_keywords(self, key_words: list, df: DataFrame) -> DataFrame:
        search_criteria = [lower(col(self.TEXT)).contains(key_word.lower()) for key_word in key_words]
        return df.filter(reduce(lambda x, y: x | y, search_criteria ))