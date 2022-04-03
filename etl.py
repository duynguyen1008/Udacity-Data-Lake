
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType, StringType, TimestampType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import *

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_table(spark, input_data, output_data):
 
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    song_df = spark.read.json(song_data)
#     print('dơn song 1')
    # extract columns to create songs table
    songs_column = ['song_id', 'title', 'artist_id','year', 'duration']
    songs_table = song_df.select(songs_column).dropDuplicates()
#     print('done song 2')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f'./{output_data}/song_table/', mode='overwrite', partitionBy=['year', 'artist_id'])
    print('Done for song table')


def process_artists_table(spark, input_data, output_data):
    # get filepath to song data file

    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    song_df = spark.read.json(song_data )
#     print('done artists 1')
    
#     extract columns to create artists table
    artists_columns = ['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']
    artists_table = song_df.select(artists_columns).dropDuplicates()
    # write artists table to parquet files
#     print('done artists 2')
    artists_table.write.parquet(f'./{output_data}/artist_table/', mode='overwrite') 
    print('Done for artists table')
def process_user_table(spark, input_data, output_data):
  
    # get filepath to log data file
#     log_data = input_data + 'log_data/*/*/*.json'
    log_data = input_data + 'log_data/*/*/*'


    # read log data file
    log_df = spark.read.json(log_data)
  
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table        
    users_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']   
    users_table = log_df.select(users_columns).dropDuplicates()
#     print('done user 2')     
    # write users table to parquet files
    users_table.write.parquet(f'./{output_data}/user_table/')
    print('Done for user table') 
    
def process_time_table(spark, input_data, output_data):
    
#     log_data = os.path.join(input_data, "log-data/")
    log_data = input_data + 'log_data/*/*/*'

    # read log data file
    df = spark.read.json(log_data)
 
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").dropDuplicates()
#     print('done T2')

    time_table.write.parquet(f'./{output_data}/times_table/', partitionBy=["year","month"])  
    print('Done for time table')
    
  
  

def process_songplay_table(spark, input_data, output_data):    

    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')
    print('process_songplay_table')
    
    log_df = spark.read.json(input_data + 'log_data/*/*/*')
    df = log_df.filter(log_df.page == "NextSong") 
    print('done songplay 1') 


    get_timestamp = udf(lambda x: str(int(int(x)/1000)))  
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    print('Get timestamp done')    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    print('Get datetime done')    

#-------------------------------------------------------------
  
    
    
    log_df = df.alias('log_df')
    song_df = song_df.alias('song_df')
    
    songplay_df = log_df.join(song_df, (song_df.title==log_df.song)&(song_df.artist_name==log_df.artist)).withColumn('songplay_id', monotonically_increasing_id())
    
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = songplay_df.select(
        col('datetime').alias('start_time'),
        col('userId').alias("user_id"),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias("session_id"),
        col('location'),
        col('userAgent').alias("user_agent"),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )
#     print('songlay done 6')

#     # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f'./{output_data}/songplays_table_01/', partitionBy=["year","month"])   

    print('Done for sonplay table')

    
# import shutil  
# def delete():
#     shutil.rmtree('s3a://udacity-dend/actual-output/songplays_table', ignore_errors=True)
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/actual-output/"

    
    process_song_table(spark, input_data, output_data) 
    process_artists_table(spark, input_data, output_data)    
    process_user_table(spark, input_data, output_data)
    process_time_table(spark, input_data, output_data)
    process_songplay_table(spark, input_data, output_data)
#     delete()


if __name__ == "__main__":
    main()
