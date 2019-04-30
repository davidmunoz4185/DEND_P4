import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('aws/credentials.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates a spark session 
    Returns:
        spark {Spark Session} -- Session to use PySpark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """ Process Song Data S3
    
    Arguments:
        spark {Spark Session} -- Session to use PySpark
        input_data {source data} -- Source Dir where sources are allocated
        output_data {source data} -- Output Dir where outputs are stored
    """

    song_data = "{}/song_data/*/*/*/*.json".format(input_data)

    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_data")

    songs_table = spark.sql("""
        SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        FROM song_data
        WHERE song_id IS NOT NULL
    """)

    songs_table.write.partitionBy("year", "artist_id").parquet("{}/songs".format(output_data))

    artists_table = spark.sql("""
        SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        FROM song_data
        WHERE artist_id IS NOT NULL
    """)

    artists_table.write.parquet("{}/artists".format(output_data))


def process_log_data(spark, input_data, output_data):
    """ Process Log Data S3
    
    Arguments:
        spark {Spark Session} -- Session to use PySpark
        input_data {source data} -- Source Dir where sources are allocated
        output_data {source data} -- Output Dir where outputs are stored
    """

    log_data = "{}/log_data/*.json".format(input_data)

    df = spark.read.json(log_data)

    df.createOrReplaceTempView("log_data")
    df = spark.sql("""
        SELECT *
        FROM log_data
        WHERE page = 'NextSong'
    """)

    users_table = spark.sql("""
        SELECT DISTINCT userid,
        firstname,
        lastname,
        gender,
        level
        FROM log_data
        WHERE userid IS NOT NULL
    """)

    users_table.write.parquet("{}/users".format(output_data))

    spark.udf.register("get_hour", lambda x: int(datetime.fromtimestamp(x / 1000.0).hour))
    spark.udf.register("get_minute", lambda x: int(datetime.fromtimestamp(x / 1000.0).minute))
    spark.udf.register("get_second", lambda x: int(datetime.fromtimestamp(x / 1000.0).second))
    spark.udf.register("get_year", lambda x: int(datetime.fromtimestamp(x / 1000.0).year))
    spark.udf.register("get_month", lambda x: int(datetime.fromtimestamp(x / 1000.0).month))
    spark.udf.register("get_day", lambda x: int(datetime.fromtimestamp(x / 1000.0).day))

    time_table = spark.sql("""
        SELECT DISTINCT
        ts as ts,
        get_hour(ts) as hour,
        get_minute(ts) as minute,
        get_second(ts) as second,
        get_day(ts) as day,
        get_month(ts) as month,
        get_year(ts) as year
        FROM log_data
    """)

    time_table.write.partitionBy("year", "month").parquet("{}/time_table".format(output_data))

    songplays_table = spark.sql("""
        SELECT events.ts,
        get_year(ts) as year,
        get_month(ts) as month,
        events.userid,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionid,
        events.location,
        events.useragent
        FROM log_data events inner join song_data songs
        on songs.artist_name = events.artist
        and songs.title = events.song
        WHERE events.page = 'NextSong'
        and songs.artist_name is not null
        and songs.title is not null
    """)

    songplays_table.write.partitionBy("year", "month").parquet("{}/song_plays".format(output_data))


def main():
    spark = create_spark_session()
    input_data = config['S3']['INPUT']
    output_data = config['S3']['OUTPUT']
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
