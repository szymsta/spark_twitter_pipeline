from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit
from functools import reduce


class TwtrLoader:

    LABELS = {
        "covid19_tweets.csv" : "covid",
        "GRAMMYs_tweets.csv" : "grammys",
        "financial.csv" : "finance"
    }


    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
    

    def load_datasets(self, file_name: str) -> DataFrame:
        label = self.LABELS.get(file_name, "unknown")
        return (self.spark_session.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load(file_name)
                .withColumn("category", lit(label))
                .na.drop())
    

    def union_datasets(self) -> DataFrame:
        dfs = [self.load_datasets(file) for file in self.LABELS.keys()]
        return reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)