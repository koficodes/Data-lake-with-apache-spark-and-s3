import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = 
    
    # read song data file
    df = spark.read.json(input_data+"song-data/*/*/*/*.json")

    # extract columns to create songs table
    songs_table = df.select('song_id','title','artist_id','year','duration').dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(output_data+'songs.parquet','overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists.parquet','overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+"log-data/*.json"

    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(log_data['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId','firstName','lastName','gender','level').dropDuplicates(['userId'])
    
    # write users table to parquet files
    users_table.write.parquet(output_data +'users.parquet','overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://karikari-udacity/"
    output_data = "s3a://karikari-udacity/outputs/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
