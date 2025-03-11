from pyspark.sql import SparkSession, DataFrame


class TwtrLoader:

    COVID_LABEL = "covid"
    GRAMMYS_LABEL = "grammys"
    FINANCE_LABEL = "finance"

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session
    

    def load_covid(self) -> DataFrame:
        return (self.spark.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load("covid19_tweets.csv")
                .withColumn("category", lit(TwtrLoader.COVID_LABEL))
                .na.drop())