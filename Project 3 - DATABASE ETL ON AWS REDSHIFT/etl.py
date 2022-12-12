import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql  import functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initiate a spark session with the given credentials
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - Get the song data S3 path for json files
    - Read files into a dataframe 
    - Extract required columns for songs and artists table
    - Write new files to S3 bucket
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id","year","duration").dropDuplicates(["song_id"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.option("header",True) \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(output_data +'songs')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name"\
                              ,"artist_location","artist_latitude"\
                              ,"artist_longitude").dropDuplicates(["artist_id"])
    
    # write artists table to parquet files
    artists_table.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_data +'artists')


def process_log_data(spark, input_data, output_data):
    """
    - Get the log data S3 path for json files
    - Read files into a dataframe 
    - Extract required columns for users, time and songplays table
    - Write new files to S3 bucket
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").dropDuplicates(["userId"])
    
    # write users table to parquet files
    users_table.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_data +'users')

    # create timestamp column from original timestamp column

    df = df.withColumn("timestamp",(F.col('ts')/1000).cast("timestamp"))
    
    # extract columns to create time table
    time_table = df.select("timestamp", hour("timestamp").alias("hour")\
                                , dayofmonth("timestamp").alias("day")\
                                , weekofyear("timestamp").alias("week")\
                                , month("timestamp").alias("month")\
                                , year("timestamp").alias("year")\
                                , dayofweek("timestamp").alias("weekday") )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.option("header",True) \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data +'time')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/A/A/*/*.json")


    #create temp view to join 2 tables
    song_df.createOrReplaceTempView("SONG")
    df.createOrReplaceTempView("LOG")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT SE.timestamp,
        EXTRACT(year from SE.timestamp) AS year,
        EXTRACT(month from SE.timestamp) AS month,
        SE.userId, SE.level, SS.song_id, SS.artist_id, SE.sessionId, SE.location, SE.userAgent
        FROM  LOG SE
        LEFT JOIN SONG SS
        ON SE.song = SS.title
        AND SE.artist = SS.artist_name
        AND SE.length = SS.duration
        """)


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.option("header",True) \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(output_data +'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-thuhth/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
