# Libraries
from pyspark.sql import SparkSession
from loader.twtr_loader import TwtrLoader

def main():

    # Initialize a SparkSession. This is the entry point to Spark functionality.
    spark = (
        SparkSession.builder
        .appName("spark_twtr")  # Set the application name
        .master("local[*]")     # Run Spark locally with as many worker threads as there are cores on your machine
        .getOrCreate())         # Get or create a Spark session

    # Create an instance of the TwtrLoader class, passing the SparkSession as an argument
    all_twtr = TwtrLoader(spark)    

    # Load datasets and merge them into a single DataFrame
    loader = all_twtr.union_datasets()

    # Display the merged DataFrame in the console
    loader.show(5, truncate=False)


if __name__ == "__main__":
    # Call the main function to execute the script
    main()