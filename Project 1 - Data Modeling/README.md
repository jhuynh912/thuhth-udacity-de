# SPARKIFY DATABASE ETL PIPELINE

## DATABASE DESCRIPTION

### Introduction
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They have their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app

### Analytical goals
- Understanding what songs users are listening to
- Have an easy way to query the data

### Database Design

**Fact Table**
`songplays` - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

**Dimension Tables**
`users` - users in the app, including these columns: user_id, first_name, last_name, gender, level
`songs` - songs in music database, including these columns: song_id, title, artist_id, year, duration
`artists` - artists in music database, including these columns: artist_id, name, location, latitude, longitude
`time` - timestamps of records in songplays broken down into specific units, including these columns: start_time, hour, day, week, month, year, weekday

**ER Diagram for Database design**
![Image](/sparkifydb_erd.png)

## RUN PYTHON SCRIPTS
`test.ipynb` displays the first few rows of each table to let you check your database.
`create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
`etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
`etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
`sql_queries.py` contains all your sql queries, and is imported into the last three files above.

**Run `create_tables.py` before running `etl.py` to reset your tables. Run `test.ipynb` to confirm your records were successfully inserted into each table.**

## ETL Process 
1. Reset tables before each time ETL pipeline run with file `create_tables.py`:
- Drops (if exists) and Creates the sparkify database. 
- Establishes connection with the sparkify database and gets cursor to it.
- Drops all the tables.
- Creates all tables needed.
- Finally, closes the connection. 

2. Run ETL pipeline scripts:
- Connect to the sparkify database
- Extract data from directory of JSON files of songs and logs
- Insert data into each table clarified above with list of columns specified
- Close the connection to the sparkify database when done loading data

## REPOSITORY STRUCTURE

- /data: include logs and songs JSON files
- ETL Process: create_tables.py and etl.py
- Development and Testing: etp.ipynb and test.ipynb
- sql_queries.py for queries used to create and insert into tables

