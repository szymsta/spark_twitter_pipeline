from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, reduce


class TwtrLoader:
    """
    The TwtrLoader class is responsible for loading and processing tweet datasets.
    It supports loading CSV files, adding category labels, and merging datasets into a single DataFrame.
    """

    LABELS: dict[str, str] = {
        "covid19_tweets.csv" : "covid",
        "GRAMMYs_tweets.csv" : "grammys",
        "financial.csv" : "finance"
    }


    def __init__(self, spark_session: SparkSession):
        """
        Initializes the TwtrLoader object.

        :param spark_session: An instance of SparkSession to handle data operations.
        """
        self.spark_session = spark_session
    

    def load_datasets(self, file_name: str) -> DataFrame:
        """
        Loads a single dataset from a CSV file, adds a category column, and removes missing values.

        :param file_name: The name of the CSV file to load.
        :return: A DataFrame containing the dataset with an additional "category" column.
        """
        label = self.LABELS.get(file_name, "unknown")
        return (self.spark_session.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load(file_name)
                .withColumn("category", lit(label))
                .na.drop())
    

    def union_datasets(self) -> DataFrame:
        """
        Merges all datasets into a single DataFrame while preserving column consistency.

        :return: A DataFrame containing all merged datasets.
        """
        dfs = [self.load_datasets(file) for file in self.LABELS.keys()]
        return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
