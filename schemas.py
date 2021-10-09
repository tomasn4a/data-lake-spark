from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
    LongType
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

# Log data schema
# Records with 'auth' == 'Logged Out' have a 'userId' equal to ''
# so we need to load as strings and then convert
log_schema = StructType([
    StructField("artist", StringType()),
    StructField("auth", StringType()),
    StructField("firstName", StringType()),
    StructField("gender", StringType()),
    StructField("itemInSession", IntegerType()),
    StructField("lastName", StringType()),
    StructField("length", DoubleType()),
    StructField("level", StringType()),
    StructField("location", StringType()),
    StructField("method", StringType()),
    StructField("page", StringType()),
    StructField("registration", StringType()),
    StructField("sessionId", IntegerType()),
    StructField("song", StringType()),
    StructField("status", IntegerType()),
    StructField("ts", LongType()),
    StructField("userAgent", StringType()),
    StructField("userId", StringType())
])