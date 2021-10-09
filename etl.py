import configparser
import os
from datetime import datetime
from pathlib import Path
from typing import Union

import findspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from schemas import song_schema, log_schema

findspark.init()


def read_config(config_path: Union[str, Path]) -> None:
    """Read AWS credentials from disk."""

    config = configparser.ConfigParser()

    with open(config_path, 'r') as f:
        config.read_file(f)

    aws = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_DEFAULT_REGION']
    for k in aws:
        os.environ[k] = config['AWS'][k]


def create_spark_session() -> SparkSession:
    """Create spark session."""

    spark = SparkSession \
        .builder \
        .appName('Sparkify Data Lake') \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def process_song_data(spark: SparkSession,
                      input_data: Union[str, Path],
                      output_data: Union[str, Path]) -> None:
    """
    Read song data from S3 and write songs and artist tables as parquet.

    Parameters
    ----------
    spark : SparkSession
        A spark session
    input_data : PathLike
        A path to the S3 bucket
    output_data : PathLike
        The output directory for the parquet files

    """

    # Get filepath to song data file
    song_data_path = Path(input_data) / 'song_data'

    # Read song data file
    df = spark \
        .read \
        .option("recursiveFileLookup", "true") \
        .json(song_data_path, schema=song_schema)

    # extract columns to create songs table
    songs_fields = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_fields)

    # Write songs table to parquet files partitioned by year and artist
    songs_table.write \
        .partitionBy('year', 'artist_id') \
        .mode('overwrite') \
        .parquet(Path(output_data))

    # Extract columns to create artists table
    artists_fields_exprs = [
        'artist_id AS artist_id',
        'artist_name AS name',
        'artist_location AS location',
        'artist_latitude AS latitude',
        'artist_longitude AS longitude'
    ]
    artists_table = df.selectExpr(*artists_fields_exprs)

    # Write artists table to parquet files
    artists_table.write \
        .mode('overwrite') \
        .parquet(Path(output_data))


def process_log_data(spark: SparkSession,
                     input_data: Union[str, Path],
                     output_data: Union[str, Path]) -> None:
    # Get filepath to log data file
    log_data = Path(input_data) / 'log_data'

    # Read log data files
    df = spark \
        .read \
        .option("recursiveFileLookup", "true") \
        .json(log_data, schema=log_schema)

    # Filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # Extract columns for users table
    users_fields_exprs = [
        'userId AS user_id',
        'firstName AS first_name',
        'lastName AS last_name',
        'gender',
        'level'
    ]
    users_table = df.selectExpr(*users_fields_exprs).limit(5).toPandas()

    # Write users table to parquet files
    users_table.write \
        .mode('overwrite') \
        .parquet(Path(output_data))

    # Create timestamp from original timestamp (in ms) column
    df = df.withColumn('ts_timestamp', F.to_timestamp(df.ts/1000))

    # Create date column from new timestamp
    df = df.withColumn('ts_date', F.to_date(df.ts_timestamp))

    # extract columns to create time table
    time_exprs = [
        'ts_timestamp AS start_time',
        'HOUR(ts_timestamp) AS hour',
        'DAY(ts_date) AS day',
        'WEEKOFYEAR(ts_date) AS week',
        'MONTH(ts_date) AS month',
        'YEAR(ts_date) AS year',
        'WEEKDAY(ts_date) AS weekday'
    ]
    time_table = df.selectExpr(*time_exprs).limit(5).toPandas()

    # Write time table to parquet files partitioned by year and month
    time_table.write \
        .partitionBy('year', 'month') \
        .mode('overwrite') \
        .parquet(Path(output_data))

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    read_config('./aws.cfg')
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-lake-spark-rojo/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
