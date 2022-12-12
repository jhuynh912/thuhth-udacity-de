import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import configparser
import os
from pyspark.sql.types import *
import datetime

# Connect to AWS Credentials
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Initiate a spark session with the given credentials
    Connect to S3 Data lake
    """
    
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").\
        config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider').\
        enableHiveSupport().getOrCreate()
    #Test Spark connection to S3
    input_data = "s3a://sparkify-thuhth/capstone/"
    sample_data = input_data + "fact_immigration/*"

    # read song data file
    df = spark.read.parquet(sample_data).limit(10)
    print('Done reading sample file')
    df.show(2)
    
    return spark
    print("DONE getting AWS Credentials & Create Spark session")

@udf(TimestampType())
def sas_to_timestamp(x):
    try:
        if  x is not None:
            epoch = datetime.datetime(1960, 1, 1)
            return epoch + datetime.timedelta(days=int(float(x)))
        else:
            return pd.Timestamp('1900-1-1')
    except:
         return pd.Timestamp('1900-1-1')

def process_i94_data(spark,input_path, output_path):
    """
    - Get the i94 data from sas path for sas file
    - Read files into a dataframe 
    - Extract required columns for songs and artists table
    - Write new files to S3 bucket
    """

#     raw_df = spark.read.format('com.github.saurfang.sas.spark').load(input_path).limit(100)
    print("DONE Reading SAS file")

    #write to parquet
#     raw_df.write.parquet("i94_data")
    print("DONE Saving file in parquet format")

    immigration_df=spark.read.parquet("i94_data").limit(100)
    print("DONE Reading Immigration data")

    #Select columns and rename for better understanding. Then drop duplicates if any based on cicid column
    fact_immigration_df = immigration_df.select("cicid"\
                                    ,"i94yr"\
                                    ,"i94mon"\
                                    ,col("i94res").alias("CountryID")\
                                    ,col("i94port").alias("PortID")\
                                    ,col("arrdate").alias("ArrivalDate")\
                                    ,col("depdate").alias("DepartureDate")\
                                    ,col("i94addr").alias("DestinationState")\
                                    ,"i94mode"\
                                    ,col("i94visa").alias("VisaCat")\
                                    ,"visatype")\
                                    .dropDuplicates(["cicid"])

    fact_immigration_df = fact_immigration_df.withColumn("ArrivalDate",sas_to_timestamp(col("ArrivalDate")))\
                                        .withColumn("DepartureDate",sas_to_timestamp(col("DepartureDate")))
    
    fact_immigration_df.show(3)

    dim_immigrants = immigration_df.select("cicid"\
                            ,col("i94bir").alias("Age")\
                            ,col("biryear").alias("BirthYear")\
                            ,"gender"
                            )
    dim_immigrants.show(3)
    
    fact_immigration_df.write.option("header",True) \
        .partitionBy("i94yr", "i94mon") \
        .mode("overwrite") \
        .parquet(output_path +'fact_immigration')
    
    print("DONE Saving fact_immigration_df in S3 with parquet format")
    
    dim_immigrants.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_path +'dim_immigrants')
    print("DONE Saving dim_immigrants in S3 with parquet format")




def process_dim_airport(spark,input_path, output_path):
    """
    Read and process dim_airport table 
    Save dim_airport table as parquet file on S3
    """

    airport_df = spark.read.format("csv").option("header", "true").load(input_path).limit(100)
    print("DONE Reading Airport data")

    dim_airport = airport_df.select(regexp_extract(col("iso_region"), ".*-(.*)",1).alias("StateID")\
                          ,"type"\
                          ,"name"\
                          ,"iso_country"\
                          ,"gps_code"\
                          ,"coordinates"
                         ).dropDuplicates()
    dim_airport.show(3)

    dim_airport.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_path +'dim_airport')
    print("DONE Saving dim_airport in S3 with parquet format")

def process_dim_cities(spark,input_path, output_path):
    """
    Read and process dim_cities table 
    Save dim_cities table as parquet file on S3
    """

    uscity_df = spark.read.options(header='true', delimiter=";").csv(input_path).limit(50)
    print("DONE Reading US Cities data")

    dim_cities = uscity_df.select(col("State Code").alias("StateCode")\
                        ,col("State").alias("StateName")\
                        ,col("City")\
                        ,col("Median Age").alias("MedianAge")\
                        ,col("Total Population").alias("TotalPopulation") \
                        ,col("Foreign-born").alias("ForeignBorn")\
                        ,"Race"\
                        ,"Count"
                        )
    dim_cities.show(3)

    dim_cities.write.option("header",True) \
        .mode("overwrite") \
        .parquet(output_path +'dim_cities')
    print("DONE Saving dim_airport in S3 with parquet format")

    
def records_count_check(df_list):
    """
    Count records for each table
    """
    for df in df_list:
        dfname =[x for x in globals() if globals()[x] is df][0]
        count = df.count()
        if count < 1:
            print("ERROR: Table has 0 records")
        else:
            print(f"COUNT CHECKED: Table {dfname} has {count} records")
            
def col_dtype_check(df, cols, col_type):
    """
    Check datatype of each column for a given dataframe
    """
    for col in cols:
        if dict(df.dtypes)[col] == col_type:
            print(f"CORRECT DATA TYPE: column {col} checked! Type {col_type}")
        else:
            print(f"FAILED DATA TYPE CHECK: column {col} has wrong dtype: {col_type}")

    
    
def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    spark = create_spark_session()
    
    #Provide input paths
    input_i94 = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    input_airport = 'airport-codes_csv.csv'
    input_city = "us-cities-demographics.csv"
    
    #Specify output path
    output_data = 's3a://capstone-thuhth/'
    output_data_temp = 'S3_output/'
    
    #Ingest and transform data
    process_i94_data(spark, input_i94, output_data_temp)
    process_dim_airport(spark,input_airport, output_data_temp)
    process_dim_cities(spark,input_city, output_data)
    
    #Perform data quality check
    records_count_check([fact_immigration_df, dim_immigrants, dim_airport, dim_cities])
    col_dtype_check(fact_immigration_df, ['ArrivalDate','DepartureDate'],'timestamp')


if __name__ == "__main__":
    main()
