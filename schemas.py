from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)

# Song data schema
song_schema = StructType([
    StructField("num_songs", IntegerType()),
    StructField("artist_id", StringType()),
    StructField("artist_latitude", DoubleType()),
    StructField("artist_longitude", DoubleType()),
    StructField("artist_location", StringType()),
    StructField("artist_name", StringType()),
    StructField("song_id", StringType()),
    StructField("title", StringType()),
    StructField("duration", DoubleType()),
    StructField("year", IntegerType())
])
