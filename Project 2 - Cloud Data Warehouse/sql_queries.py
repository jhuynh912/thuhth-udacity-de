import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN=config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
  artist   VARCHAR(1000),
  auth      VARCHAR(1000),
  firstName   VARCHAR(1000) ,
  gender      VARCHAR(1000),
  itemInSession    BIGINT,
  lastName    VARCHAR(1000),
  length     FLOAT,
  level     VARCHAR(1000),
  location     VARCHAR(1000),
  method      VARCHAR(1000),
  page      VARCHAR(1000),
  registration      FLOAT,
  sessionId      BIGINT,
  song      VARCHAR(1000),
  status      BIGINT ,
  ts      BIGINT,
  userAgent      VARCHAR(1000),
  userId      BIGINT
  
)
diststyle even;
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
  num_song   INTEGER ,
  artist_id      VARCHAR(1000),
  artist_latitude   DOUBLE PRECISION,
  artist_longitude      DOUBLE PRECISION,
  artist_location    VARCHAR(1000),
  artist_name    VARCHAR(1000),
  song_id     VARCHAR(1000),
  title     VARCHAR(1000),
  duration      FLOAT,
  year      INTEGER
  
)
diststyle even;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id  INTEGER    IDENTITY(1,1), 
start_time      TIMESTAMP , 
user_id      INTEGER      distkey, 
level      VARCHAR(1000), 
song_id      VARCHAR(1000), 
artist_id      VARCHAR(1000), 
session_id      INTEGER, 
location      VARCHAR(1000), 
user_agent      VARCHAR(1000),
PRIMARY KEY(songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(user_id       INTEGER PRIMARY KEY sortkey, 
first_name       VARCHAR(1000), 
last_name       VARCHAR(1000), 
gender       VARCHAR(1000), 
level       VARCHAR(1000)
)
;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(song_id VARCHAR(1000) PRIMARY KEY sortkey, 
title VARCHAR(1000) , 
artist_id VARCHAR(1000), 
year INTEGER, 
duration FLOAT
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(artist_id VARCHAR(1000) PRIMARY KEY sortkey, 
name VARCHAR(1000) , 
location VARCHAR(1000), 
latitude DOUBLE PRECISION, 
longitude DOUBLE PRECISION
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(start_time TIMESTAMP PRIMARY KEY, 
hour VARCHAR(1000), 
day VARCHAR(1000), 
week VARCHAR(1000), 
month VARCHAR(1000), 
year VARCHAR(1000), 
weekday VARCHAR(1000)
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy {} from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
json {}
;
""").format("staging_events", LOG_DATA , DWH_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy {} from 's3://udacity-dend/song_data/A/A' 
credentials 'aws_iam_role={}'
region 'us-west-2'
format json 'auto';
""").format("staging_songs", DWH_ROLE_ARN)

# FINAL TABLES
# (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id , artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + SE.ts/1000 *INTERVAL '1 second' AS start_time, SE.userId, SE.level, SS.song_id, SS.artist_id, SE.sessionId, SE.location, SE.userAgent
FROM  staging_events SE
LEFT JOIN staging_songs SS
ON SE.song = SS.title
AND SE.artist = SS.artist_name
AND SE.length = SS.duration
WHERE SE.page = 'NextSong'

;
""")

user_table_insert = ("""
INSERT INTO users
(SELECT userId, firstName, lastName, gender, level
FROM staging_events
WHERE page = 'NextSong')
""")

song_table_insert = ("""
INSERT INTO songs
(
SELECT song_id, title, artist_id, year, duration
FROM staging_songs
)
""")

artist_table_insert = ("""
INSERT INTO artists
(
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
);
""")

time_table_insert = ("""
INSERT INTO time
(
SELECT start_time, 
EXTRACT(hour from start_time) as hour,
EXTRACT(day from start_time) as day,
EXTRACT(week from start_time) as week,
EXTRACT(month from start_time) as month,
EXTRACT(year from start_time) as year,
EXTRACT(weekday from start_time) as weekday

FROM songplays

)


""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]


drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
