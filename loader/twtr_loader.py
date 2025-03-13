from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from functools import reduce


class TwtrLoader:

    LABELS = {
        "covid19_tweets.csv" = "covid",
        "GRAMMYs_tweets.csv" = "grammys",
        "financial.csv" = "finance"
    }


    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
    

    def load_datasets(self, file_name: str) -> DataFrame:
        label = self.LABELS.get(filename, "unknown")
        return (self.spark.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load(filename)
                .withColumn("category", lit(label))
                .na.drop())
    

    def union_datasets(self) -> DataFrame:
        dfs = [self.load_datasets(file) for file in self.LABELS.keys()]
        return reduce(DataFrame.unionByName, dfs)