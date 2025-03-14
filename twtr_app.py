# Import libraries
from pyspark.sql import SparkSession

# Import internal modules
from loader.twtr_loader import TwtrLoader
from cleaner.twtr_cleaner import TwtrCleaner
from analyser.twtr_analyser import TwtrAnalyser
from analyser.twtr_searcher import TwtrSearcher


def main():

    # Initialize SparkSession.
    spark = (
        SparkSession.builder
        .appName("spark_twtr_pipeline")     # Set the application name
        .master("local[*]")                 # Run Spark locally with as many worker threads as there are cores on your machine
        .getOrCreate())                     # Get or create a Spark session

    print("Spark session initialized.")


    # Initialize modules
    try:
        loader = TwtrLoader(spark)
        cleaner = TwtrCleaner(spark)
        analyser = TwtrAnalyser(spark)
        searcher = TwtrSearcher(spark)
        print("Modules initialized.")
    except Exception as e:
        print(f"Error initializing modules: {e}")
        spark.stop()
        return


    # Load and clean data
    try:
        print("Loading data...")
        load_twtr = loader.union_datasets()

        print("Cleaning data...")
        clean_twtr = cleaner.clean_dataset(load_twtr)

        print("Data loaded and cleaned successfully.")
    except Exception as e:
        print(f"Error during data loading or cleaning: {e}")


    # Call the main function to execute the script
if __name__ == "__main__":
    main()