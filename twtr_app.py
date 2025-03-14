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
        loader = TwtrLoader(spark)          # Module responsible for loading data
        cleaner = TwtrCleaner(spark)        # Module responsible for cleaning data
        analyser = TwtrAnalyser(spark)      # Module responsible for analyzing data
        searcher = TwtrSearcher(spark)      # Module responsible for searching and querying the data
        print("Modules initialized.")       # Confirm successful initialization

    except Exception as e:
        print(f"Error initializing modules: {e}")   # Provide the error message for debugging
        spark.stop()                                # Stop Spark session if modules fail to initialize
        return


    # Load and clean data
    try:
        print("Loading data...")
        load_twtr = loader.union_datasets()                     # Load the data using the loader module

        print("Cleaning data...")
        clean_twtr = cleaner.clean_dataset(load_twtr)           # Clean the loaded data using the cleaner module

        print("Data loaded and cleaned successfully.")          # Inform the user that data has been successfully loaded and cleaned
    except Exception as e:
        print(f"Error during data loading or cleaning: {e}")    # Provide the error message for debugging


# Call the main function to execute the script
if __name__ == "__main__":
    main()