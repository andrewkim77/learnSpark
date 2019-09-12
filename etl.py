import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
# config.read('dl.cfg')
config.read_file(open('dl.cfg'))

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
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    
    songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM song_data
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/2018/11/*.json"

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter("page = 'NextSong'")
    
    df_log.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
        SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level
        FROM log_data
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users")


    # get_timestamp and get_datetime udf
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    
    # create timestamp column from original dataframe
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original dataframe
    df_log = df_log.withColumn("datetime", get_datetime(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.select(
    'timestamp',
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    date_format('datetime', 'F').alias('weekday')
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = df_log.select('song','artist',year('datetime').alias('year'),'length')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        select 
          userid as user_id      ,
          level         ,
          song_id       ,
          artist_id     ,
          sessionid as session_id    ,
          location      ,
          useragent as user_agent,
          year,
          month('ts/1000.0') as month
        from log_data
        join song_data on ( log_data.song = song_data.title and log_data.length = song_data.duration 
                            and log_data.artist = song_data.artist_id )
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.overwrite.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

