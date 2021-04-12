
# -*- coding: utf-8 -*-
"""
    This script implements the ETL pipeline to execute the process using amazon AWS services and apache Spark
    
    Staging tables:
        - staging_events - Load the raw data from log events json files artist
        auth, firstName, gender, itemInSession,    lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId
        - staging_songs - num_songs artist_id artist_latitude artist_longitude artist_location artist_name song_id title duration year
       
    Dimension tables:
        - users - users in the app: user_id, first_name, last_name, gender, level
        - songs - songs in music database: song_id, title, artist_id, year, duration
        - artists - artists in music database: artist_id, name, location, latitude, longitude
        - time - timestamps of records in songplays: start_time, hour, day, week, month, year, weekday
    
    Fact Table:
        - songplays - records in log data associated with song plays.
        
    The pipeline is implemented using dataframes loading data from Postgres database with the psycopg2 connector.        
        
"""

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
        The function create_spark_session create the object session for the apache spark service

        Parameters:
            None

        Returns:
            spark session object

        Note:
            None
    """   
    
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        The function process_song_data read a json formatted file for songs from AWS S3 Bucket 
        and write the parquet file with the dimensions tables related (Songs and artists)
        
        Parameters:
            spark (obj): 
                spark object session
            input_data (str): 
                Path to the json songs file
            output_data (str): 
                Path to the s3 bucket to write the parquet files
                
        Returns:
            None
            
        Note:
             The function write direct to S3 bucket songs and artists tables in parket format.             
    """   
    # get filepath to song data file   
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song['song_id', 'title', 'artist_id', 'year', 'duration']
    
    # drop duplicated rows
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = df_song['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    
    # drop duplicated rows
    songs_table = songs_table.dropDuplicates(['artist_id'])    
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    
    """
        The function process_log_data read a json formatted file for events from AWS S3 Bucket 
        and write the parquet files with the dimensions tables
        
        Parameters:
            spark (obj): 
                spark object session
            input_data (str): 
                Path to the json events file
            output_data (str): 
                Path to the s3 bucket to write the parquet files
                
        Returns:
            None
            
        Note:
            The function write direct to S3 bucket users, time and songplays tables in parket format        
    """   
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log[df_log.page == 'NextSong']

    # extract columns for users table    
    df_users_table = df_log['userId', 'firstName', 'lastName', 'gender', 'level']
    
    # write users table to parquet files
    df_users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))    
    
    # extract columns to create time table
    df_time_table = df_log.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'),
        dayofweek('datetime').alias('weekday')
   )
            
    # write time table to parquet files partitioned by year and month
    df_time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data+'time')    
    
    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    df_song = spark.read.json(song_data)
   
    # JOIN the events and songs files
    df_songplays = df_log.join(df_song, (df_song.title == df_log.song) & (df_song.artist_name == df_log.artist))
    
    
    # extract columns from joined song and log datasets to create songplays table     
    songplays_table = df_songplays.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year')
    )
        
    # create the id for songplay table
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()
    
    # write songplays table to parquet files partitioned by year
    songplays_table.write.mode('overwrite').partitionBy('year').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalake-sparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
