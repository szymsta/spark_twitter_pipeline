from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower
from functools import reduce


class TwtrSearcher:
    """
    A class for searching Twitter data based on keywords and location. 
    This class provides methods to search for tweets containing specific 
    keywords and tweets from a specific location in the dataset.

    Attributes:
    TEXT (str): The column name for tweet text.
    USER_LOCATION (str): The column name for the user's location.
    """

    TEXT = "text"
    USER_LOCATION = "user_location"

    def __init__(self, spark_session: SparkSession):
        """
        Initializes the TwtrSearcher object with a Spark session.

        :param spark_session: An instance of SparkSession to perform data analysis.
        """
        self.spark_session: spark_session
    

    def search_keyword(self, key_word: str, df: DataFrame) -> DataFrame:
        """
        Searches for tweets containing the specified keyword (transformation).
        This method performs the following transformations:
        - Converts the 'text' column to lowercase.
        - Filters the DataFrame to find rows where the 'text' column contains 
          the specified keyword (case-insensitive).

        :param key_word: The keyword to search for.
        :param df: The DataFrame containing the tweet data.
        :return: A DataFrame with tweets containing the specified keyword (transformation).
        """
        return df.filter(lower(col(self.TEXT)).contains(key_word.lower()))
    

    def search_keywords(self, key_words: list, df: DataFrame) -> DataFrame:
        """
        Searches for tweets containing any of the specified keywords (transformation).
        This method performs the following transformations:
        - Filters the DataFrame for each keyword in the 'text' column.
        - Uses logical OR to match tweets that contain at least one of the keywords.

        :param key_words: A list of keywords to search for.
        :param df: The DataFrame containing the tweet data.
        :return: A DataFrame with tweets containing at least one of the keywords (transformation).
        """
        search_criteria = [
            (col(self.TEXT).isNotNull() & (col(self.TEXT) != ""))
            & lower(col(self.TEXT)).contains(key_word.lower()) for key_word in key_words
            ]
            
        return df.filter(reduce(lambda x, y: x | y, search_criteria ))
    

    def search_location(self, location: str, df: DataFrame) -> DataFrame:
        """
        Searches for tweets from a specific location (transformation).
        This method performs the following transformations:
        - Filters the DataFrame to find rows where the 'user_location' column 
          matches the specified location (case-insensitive).
        
        :param location: The location to search for.
        :param df: The DataFrame containing the tweet data.
        :return: A DataFrame with tweets from the specified location (transformation).
        """
        return (df.filter(
                (col(self.USER_LOCATION).isNotNull()) &
                (col(self.USER_LOCATION) != "") &
                (lower(col(self.USER_LOCATION)) == location.lower()))
        )
        