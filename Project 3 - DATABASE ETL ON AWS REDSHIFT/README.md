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
    - `etl.py`: load data from S3 bucket - which includes the company's log and song data into Spark dataframes. Then process and load data back into S3 as a set of dimensional tables
    - `dl.cfg` : include credentials to connect to Spark cluster
    
## ETL Process 
Connect to EMR master node
Submit Spark script with command spark-submit etl.py - which will implement these following:
- Get credentials to connect to Spark cluster
- Initiate a spark session
- Read json files into dataframes and process data. Then write data into given output S3 bucket