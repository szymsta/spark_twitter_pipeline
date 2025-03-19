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
        .getOrCreate()                      # Get or create a Spark session
        )

    print("Spark session initialized.")


    # Initialize modules
    try:
        loader = TwtrLoader(spark)          # Module responsible for loading data
        cleaner = TwtrCleaner(spark)        # Module responsible for cleaning data
        analyser = TwtrAnalyser(spark)      # Module responsible for analyzing data
        searcher = TwtrSearcher(spark)      # Module responsible for searching and querying the data
        print("Modules initialized.")       # Confirm successful initialization

    except Exception as e:
        # Handle errors during data loading or cleaning
        print(f"Error initializing modules: {e}")
        
        # Stop Spark session if modules fail to initialize
        spark.stop()
        return


    # Load and clean data
    try:
        print("Loading data...")
        # Load the data using the loader module and cache it for better performance
        load_twtr = loader.union_datasets().cache()

        print("Cleaning data...")
        # Clean the loaded data using the cleaner module
        clean_twtr = cleaner.clean_dataset(load_twtr)
        
        # Notify that data has been processed
        print("Data loaded and cleaned successfully.")

    except Exception as e:
        # Handle errors during data loading or cleaning
        print(f"Error during data loading/cleaning: {e}")

        # Stop Spark session if modules fail to initialize
        spark.stop()    
        return


    # Analyze data and show top 5
    try:
        # Usage 1 - Analyze most frequent hashtags in the dataset
        print("Analyzing most frequent hashtags...")
        hashtags_df = analyser.calculate_hashtags(clean_twtr)
        hashtags_df.show(5)
        
        # Usage 2 - Analyze retweet counts
        print("Analyzing retweet counts...")
        retweet_df = analyser.calculate_retwtr(clean_twtr)
        retweet_df.show(5)

        # Usage 3 - Analyze tweet sources
        print("Analyzing tweet sources...")
        source_df = analyser.calculate_source(clean_twtr)
        source_df.show(5)

        # Usage 4 - Analyze average followers by user location
        print("Analyzing average followers by location...")
        avg_followers_df = analyser.calculate_avg_followers_by_location(clean_twtr)
        avg_followers_df.show(5)

    except Exception as e:
        # Handle errors during the search operation
        print(f"Error during data search: {e}")

        # Stop Spark session if error occurs during the search
        spark.stop()
        return
    

    # Usage 5 - Search the data for tweets containing specific keywords and get the sources
    try:
        print("Searching for tweets containing keywords...")

        # Step 1: Define the list of keywords to search for
        key_words = ["elonmusk", "musk", "teslamotors", "tesla"]

        # Step 2: Search the cleaned data for tweets containing any of the specified keywords
        musk_df = analyser.calculate_source(searcher.search_keywords(key_words, clean_twtr))

        # Step 3: Display top 10 results
        musk_df.show(10)
    
    except Exception as e:
        # Handle errors during the search operation
        print(f"Error during data search: {e}")

        # Stop Spark session if error occurs during the search
        spark.stop()
        return
    

    # Usage 6 - Search tweets containing specific keyword and location
    try:
        print("Searching for tweets containing keyword and location...")

        # Step 1: Define keyword and location to search for
        key_word  = "Trump"
        location = "United States"

        # Step 2: Apply keyword search first
        filtered_by_keyword_df = searcher.search_keyword(key_word, clean_twtr)

        # Step 3: Apply location search on the filtered DataFrame
        trump_df = searcher.search_location(location, filtered_by_keyword_df)

        # Step 4: Display top 10 results
        trump_df.show(10)

    except Exception as e:
        # Handle errors during the search operation
        print(f"Error during data search: {e}")

        # Stop Spark session if error occurs during the search
        spark.stop()
        return


# Call the main function to execute the script
if __name__ == "__main__":
    main()