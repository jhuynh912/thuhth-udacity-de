# SPARKIFY DATABASE ETL ON AWS REDSHIFT

## Introduction
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The data is in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Analytical goals
- Understanding what songs users are listening to
- Analytics queries are fast and easy

### Database Design

**Fact Table**
`songplays` - records in log data associated with song plays i.e. records with page NextSong

**Dimension Tables**
`users` - users in the app
`songs` - songs in music database
`artists` - artists in music database
`time` - timestamps of records in songplays broken down into specific units

**ER Diagram for Database design**
![Image](/sparkifydb_erd.png)

## REPOSITORY STRUCTURE

- ETL Process: 
    - `create_tables.py`: connect to sparkify database on Redshift cluster, then drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
    - `etl.py`: load data from S3 bucket - which includes the company's log and song data into staging tables. Then insert data from staging tables to fact and dimension tables
    
- `sql_queries.py` for queries used to create and insert into tables
- `dwh.cfg` : contains configuration details of Cluster/S3 bucket/IAM role that helps to connect to Redshift cluster and databse

## ETL Process 
1. Reset tables before each time ETL pipeline run with file `create_tables.py`:
- Read and get config details from `dwh.cfg`
- Establishes connection with the sparkify database on Redshift cluster and gets cursor to it.
- Drops all the tables.
- Creates all tables needed.
- Finally, closes the connection. 

2. Run ETL pipeline scripts with `etl.py`:
- Connect to the sparkify database on Redshift cluster 
- Copy data from S3 bucket for logs and song data into staging tables: staging_events and staging_songs
- Insert data into each analytics table clarified above with list of columns specified
- Close the connection to the sparkify database when done loading data

