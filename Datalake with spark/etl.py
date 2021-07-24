import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long, TimestampType as Ts
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Function to create spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Reads log json file and loads into dataframes

            Parameters:
                    spark (str): spark session
                    input_data (str): input file base path for json files
                    output_data (str): output file base path for parquet files
            Return
                    df (datframe): dataframe for songs data
    '''

    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),   
        Fld("duration", Dbl()),
        Fld("year", Int()),
        
    ])

    df = spark.read.json(song_data,schema=songSchema)

    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration') \
                    .dropDuplicates()

    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))

    artists_table = df.select('artist_id',
                              col('artist_name').alias('name'),
                              col('artist_location').alias('location'),
                              col('artist_latitude').alias('latitude'),
                              col('artist_longitude').alias('longitude')) \
                    .dropDuplicates()
    
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))
    
    return df


def process_log_data(spark, input_data, output_data,song_df):
    
    '''
    Reads log json file and loads into dataframes

            Parameters:
                    spark (str): spark session
                    input_data (str): input file base path for json files
                    output_data (str): output file base path for parquet files
                    song_df (datframe): dataframe to load songs data
    '''

    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    logSchema = R([
        Fld("artist", Str()),
        Fld("auth", Str()),
        Fld("firstName", Str()),
        Fld("gender", Str()),
        Fld("itemInSession", Long()),
        Fld("lastName", Str()),
        Fld("length", Dbl()),
        Fld("level", Str()),
        Fld("location", Str()),   
        Fld("method", Str()),
        Fld("page", Str()),
        Fld("registration", Dbl()),
        Fld("sessionId", Long()),
        Fld("song", Str()),
        Fld("status", Long()),
        Fld("ts", Long()),
        Fld("userAgent", Str()),
        Fld("userId", Str()),
        
    ])
    df = spark.read.json(log_data,schema=logSchema)
    
    df = df.filter(df.page == 'NextSong')
   
    users_table = df.select(col('userId').alias('user_id'),
                            col('firstName').alias('first_name'),
                            col('lastName').alias('last_name'),
                            'gender',
                            'level') \
                    .dropDuplicates()

    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0),Ts())
    date_time = get_timestamp(df.ts)
    df = df.withColumn("start_time",date_format(date_time,'HH:mm:ss')) \
    .withColumn("hour",hour(date_time)) \
    .withColumn("day",dayofmonth(date_time)) \
    .withColumn("week",weekofyear(date_time)) \
    .withColumn("month",month(date_time)) \
    .withColumn("year",year(date_time)) \
    .withColumn("weekday",dayofweek(date_time))

    time_table = df.select('start_time',
                           'hour',
                           'day',
                           'week',
                           'month',
                           'year',
                           'weekday') \
                    .dropDuplicates()

    time_table.write.partitionBy("year","month").mode('overwrite').parquet(os.path.join(output_data, 'time'))
    
    spec = Window.partitionBy().orderBy(df.year)

    songplays_table = df.join(song_df, (song_df.title==df.song)  & (song_df.artist_name==df.artist) & (song_df.duration==df.length), 'left_outer') \
    .withColumn("songplay_id", row_number().over(spec)) \
    .select('songplay_id',
            df.start_time,
            col('userId').alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            col('sessionId').alias('session_id'),
            df.location,
            col('useragent').alias('user_agent'),
            df.year,
            df.month
           )

    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))
    
    songsplay_df = spark.read.parquet(os.path.join(output_data, 'songplays'))
    
    songsplay_df = songsplay_df.filter((songsplay_df.song_id.isNotNull()) & (songsplay_df.artist_id.isNotNull()))
    
    songsplay_df.show(5)

def main():
    '''
    Main function to call process data function
    '''
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-songplay/"
    
    song_df=process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data,song_df)


if __name__ == "__main__":
    main()
