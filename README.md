# Spark Twitter Data Pipeline

This project implements a data processing pipeline for Twitter data using Apache Spark. The pipeline performs data loading, cleaning, and analysis, including searching for tweets containing specific keywords and locations. The project is designed to run on a local Spark setup, using PySpark for distributed data processing.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Modules](#modules)
  - [TwtrLoader](#twtrloader)
  - [TwtrCleaner](#twtrcleaner)
  - [TwtrAnalyser](#twtranalyser)
  - [TwtrSearcher](#twtrsearcher)


## Installation

To run this project, you need to install the required dependencies and set up Apache Spark on your local machine.

### Requirements

Detailed requirements for specific versions of Apache Spark can be found in the official documentation: [Apache Spark Documentation](https://spark.apache.org/documentation.html). The project uses verions as below:

- Python 3.11.9
- Apache Spark 3.4.4
- JDK (Java Development Kit) 11
- Required Python libraries:
  - `pyspark` 3.4.4

### Step-by-Step Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/szymsta/spark-twitter-pipeline.git
   cd spark-twitter-pipeline

2. Install the required Python dependencies:
   ```bash
   pip install -r requirements.txt

3. Make sure that Apache Spark and Java are correctly installed. You can verify this by running the following commands:
   ```bash
   spark-shell --version
   java --version

## Usage

To run the pipeline, execute the **twtr_app.py** script, which will initialize the Spark session, load data, clean the data, and search for tweets containing specific keywords.

1. Ensure your dataset is available and accessible by the script.
   
2. Run the script:
    ```bash
    python twtr_app.py

The pipeline will:

- Load and clean the data using the TwtrLoader and TwtrCleaner modules.
- Search for tweets containing specific keywords (elonmusk, musk, London etc.).
- Display the results using modules.

## File Structure

Here's a breakdown of the project files:
```bash
/spark-twitter-pipeline
  ├── twtr_app.py            # Main script that runs the pipeline
  ├── loader/                # Contains the TwtrLoader module
  │   └── twtr_loader.py     # Module for loading Twitter data
  ├── cleaner/               # Contains the TwtrCleaner module
  │   └── twtr_cleaner.py    # Module for cleaning the loaded data
  ├── analyser/              # Contains the TwtrAnalyser & TwtrLoader modules
  │   └── twtr_analyser.py   # Module for analyzing data
  │   └── twtr_searcher.py   # Module for searching specific tweets
  ├── requirements.txt       # Python dependencies file
  ├── README.md              # Project documentation
  ├── covid19_tweets.csv     # Sample dataset containing COVID-19 tweets
  ├── financial.csv          # Sample dataset containing financial tweets
  └── GRAMMYs_tweets.csv     # Sample dataset containing grammys tweets
```

## Modules

1. TwtrLoader module is responsible for loading raw Twitter data from multiple file sources and merging them into a single DataFrame. It uses the union_datasets method to combine datasets.
   
2. TwtrCleaner module processes the raw data, cleaning it by:
   - Removing unnecessary columns
   - Filtering out invalid or incomplete data
   - Normalizing text (e.g., lowercase conversion)

3.  TwtrAnalyser module analyzes the cleaned data. It calculates various insights, such as:
    - Finding the most frequent hashtags.
    - Performing basic statistical analysis of the data.

4. TwtrSearcher module provides search functionality for the cleaned data:

    - Searching for tweets that contain specific keywords.
    - Searching for tweets from a specific user location.

## Sample Data

This project includes sample datasets to test the pipeline and demonstrate its functionality. These sample datasets are stored in the main folder and can be used to run the pipeline and verify its functionality. The datasets contain Twitter data such as tweet text, user information, hashtags, and other relevant details.

Please ensure that the paths to these datasets are correctly set in the script for proper loading and processing.
