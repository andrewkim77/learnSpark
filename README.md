# learnSpark
learn Spark System

Using Spark System
==================
       
This program consists of two part, 'process_song_data' and 'process_log_data'.
It's process of read from s3 and some dataprocessing, like extracting, and save data to filesystems.

### For using AWS
Configure the AWS access configuration in 'dl.cfg' file

### load data 
Loading data from AWS s3 bucket, "s3a://udacity-dend/".


process_song_data explanation
=============================

1. load song_data from "s3a://udacity-dend/song_data/*/*/*/*.json", json files.
   ```python
   song_data = input_data + "song_data/*/*/*/*.json"
   ```
1. dataframe creation, df. 
   ```python
   df = spark.read.json(song_data)
   ```
1. sql view creation, song_data.
   ```python
   df.createOrReplaceTempView("song_data")
   ```
1. sub dataframe creation, songs_table amd artists_table dataframe.
   ```python
   songs_table = spark.sql("""
        SELECT song_id, title, artist_id, year, duration
        FROM song_data
    """)
   ```
1. write(save) songs_table to parquet files at songs folder partitioned by year and artist. write(save) artists_table to parquet files at artists folder ( example : songs )
   ```python
   songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs")
   ```

process_log_data explanation
=============================

code samples only different to process_song_data

1. load song_data from "s3a://udacity-dend/log_data/2018/11/*.json", json files.
1. dataframe creation, df_log. 
1. filter by actions for song plays
   ```python
   df_log = df_log.filter("page = 'NextSong'")
   ```
1. sql view creation, log_data.
1. sub dataframe creation, user_table dataframe ( extract columns for users_table   )
1. write(save) users table to parquet files
1. get_timestamp and get_datetime udf
   ```python
   get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
   get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
   ```
1. create timestamp, datetime column from original dataframe ( sample timestamp )
   ```python
   df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
   ```
1. extract columns to create time table
   ```python
    time_table = df_log.select(
    'timestamp',
    hour('datetime').alias('hour'),
    dayofmonth('datetime').alias('day'),
    weekofyear('datetime').alias('week'),
    month('datetime').alias('month'),
    year('datetime').alias('year'),
    date_format('datetime', 'F').alias('weekday')
    )
   ```
1. write time table to parquet files partitioned by year and month
1. read in song data to use for songplays table
   ```python
   song_df = df_log.select('song','artist',year('datetime').alias('year'),'length')
   ```
1. extract columns from joined song and log datasets to create songplays table 
   ```python
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
   ```
1. write songplays table to parquet files partitioned by year and month
