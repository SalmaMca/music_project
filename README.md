

# Music Project

    Music Project Using Pandas And Polars

# Project Context

In the context of a CRM campaign, we would like to send each user the top 50 songs of their country as well as their own personal top 50 songs of the last 7 days. For the purpose of the exercise, consider that we are receiving each day in a folder, a text file named listen-YYYYMMDD.log that contains the logs of all listening streams made on Deezer on that date. These logs are formatted as follows:
 - There is a row per stream (1 listening).
 - Each row is in the following format: sng_id|user_id|country
With:
 - sng_id: Unique song identifier, an integer. For your information, The catalog contains more than 80M songs, a number that is constantly increasing.
 - user_id: Unique user identifier, an integer. Deezer currently has millions of users, a number that is constantly increasing.
 - country: 2 characters string upper case that matches the country ISO
code (Ex: FR, GB, BE, ...). There are 249 existing country codes, this number rarely changes (only when there is massive geopolitical change).

# instalation

pip install -r requirements.txt

# Project Structure

The project contains 3 main scripts inside "src/music_project":
 * main_pandas.py: Contain ETL Function Implemented With Pandas.
 * main_polars.py: Contain ETL Function Implemented With Polars.
 * main.py: Main Entry Points. 

The data is stored in a separate folder outside the project folder. The main script takes a log file, the engine type (either Pandas or Polars), and an output path. The log file is  loaded into memory, and then the requested transformations are applied. The final results are saved using the given output path. For this exercise, I decided to provide the solution using two frameworks: Pandas and Polars. Pandas is chosen because it is easier to use and debug, with a simple API. Polars, on the other hand, has a similar API to Pandas but is much faster. While Polars is a relatively new framework and there aren't many examples or evidence of it being used in production, this solution fits better within the performance constraints.

# Data Quality
In order to ensure data quality in the CRM campaign's data processing pipeline, it is essential to apply a data quality check function to the dataframe obtained from the daily log file. This function helps filter out erroneous or incomplete data that could potentially affect the analysis and disrupt the proper reading of the log file. The data quality check function performs several checks, including filtering rows based on the length of the country column to keep only the rows where the length is equal to 2. This helps eliminate any incorrectly entered country codes. Additionally, the function drops null values to ensure the dataset is clean and complete. By applying these data quality checks, we can obtain a clean and reliable dataframe, ready for further analysis and the generation of personalized recommendations for the CRM campaign.

## Type Data columns
To ensure data integrity and avoid potential data corruption, it is important to specify the appropriate data types for the columns in the dataframe obtained from the daily log file. It is recommended to treat both the "song_id" and "user_id" columns as strings or categorical variables, rather than integers. By doing so, you can prevent issues that may arise from casting large integers and ensure the accuracy and integrity of the data throughout the CRM campaign's data pipeline. This approach helps maintain data quality by reducing the risk of data corruption and allows for smooth data processing and analysis.


# Performance
 we can observe some performance differences between Pandas and Polars. Polars demonstrates superior performance in handling large datasets due to its optimized processing capabilities and parallelization techniques. It efficiently handles operations on massive data frames, resulting in faster execution times. On the other hand, Pandas is a versatile library that offers a wide range of functionalities and is commonly used for moderate-sized datasets. While Pandas provides a more intuitive and flexible syntax, it may experience performance limitations when dealing with larger datasets compared to Polars. Therefore, if performance is a critical factor and you're working with substantial amounts of data, Polars would be a recommended choice for its ability to leverage parallel processing and deliver efficient computations. However, for smaller datasets or scenarios that prioritize flexibility and a rich set of features, Pandas remains a reliable option.
