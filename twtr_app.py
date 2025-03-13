# Libraries
from pyspark.sql import SparkSession
from loader.twtr_loader import TwtrLoader
from cleaner.twtr_cleaner import TwtrCleaner

def main():

    # Initialize a SparkSession. This is the entry point to Spark functionality.
    spark = (
        SparkSession.builder
        .appName("spark_twtr")  # Set the application name
        .master("local[*]")     # Run Spark locally with as many worker threads as there are cores on your machine
        .getOrCreate())         # Get or create a Spark session

    # Create and clean
    twtr_loader = TwtrLoader(spark)
    twtr_cleaner = TwtrCleaner(spark)

    # Load, merge and clean datasets
    load_twtr = twtr_loader.union_datasets()
    clean_twtr = twtr_cleaner.clean_dataset(load_twtr)

    # Display the merged DataFrame in the console
    clean_twtr.show(5, truncate=False)


if __name__ == "__main__":
    # Call the main function to execute the script
    main()