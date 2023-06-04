

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

# Project Structur



The project contains 3 main scripts inside "src/music_project":
 * main_pandas.py: Contain ETL Function Implemented With Pandas.
 * main_polars.py: Contain ETL Function Implemented With Polars.
 * main.py: Main Entry Points. 

The data is stored in a separate folder outside the project folder. Since we only have one day's worth of data. The main script takes a list of log files, the engine type (either Pandas or Polars), and an output path. The log files are loaded into memory, and then the requested transformations are applied. The final results are saved using the given output path. For this exercise, I decided to provide the solution using two frameworks: Pandas and Polars. Pandas is chosen because it is easier to use and debug, with a simple API. Polars, on the other hand, has a similar API to Pandas but is much faster. While Polars is a relatively new framework and there aren't many examples or evidence of it being used in production, this solution fits better within the performance constraints.

# Data Quality



# Performance


## Pandas


## Polars

