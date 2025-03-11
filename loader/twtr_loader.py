from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit


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
    

    def load_grammys(self) -> DataFrame:
        return (self.spark.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load("GRAMMYs_tweets.csv")
                .withColumn("category", lit(TwtrLoader.GRAMMYS_LABEL))
                .na.drop())


    def load_financial(self) -> DataFrame:
        return (self.spark.read.format("csv")
                .options(header=True, inferSchema=True, delimiter=",")
                .load("financial.csv")
                .withColumn("category", lit(TwtrLoader.FINANCE_LABEL))
                .na.drop())