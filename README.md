# Summary:

Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
How to run the Python scripts
An explanation of the files in the repository
State and justify your database schema design and ETL pipeline.
Provide example queries and results for song play analysis.




# The purpose of this database in the context of the startup, Sparkify, and their analytical goals:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis, and bring you on the project. Your role is to create a database schema and ETL pipeline for this analysis. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.




# Files explanation:

sql_queries.py-----this file has all the sql queries inlcuding create/insert/drop tables.

create_tables.py----this file run python files and import the queries from the "sql_queries.py" to create/insert/drop

etl.ipynb----this file run  single file from the database song_data and log_data and loads the data into your tables. The basic logic in the notebook file is below:
1. get the file path

2. load the data and save it as pandas dataframe

3. filter by requests and finally use cur.execute command to load the query you want by inputting the dataframe that the question asks

etl.py----the same logic with etl.ipynb, just this code can read multiple files




# Database schema design and ETL pipeline

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong

songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


Dimension Tables
users - users in the app

user_id, first_name, last_name, gender, level

songs - songs in music database

song_id, title, artist_id, year, duration

artists - artists in music database

artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units

start_time, hour, day, week, month, year, weekday




# run the Python scripts

1. open a terminal and type "python create_table.py"

2. open etl.ipynb and use (shift+enter) to run the cells one by one

3. open a termial and type "python etl.py" to process all the files

4. open test.ipynb to and use (shift+enter) to run the cells one by one, check if we have the table data






