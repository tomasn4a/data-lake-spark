# Data Lake with Spark

Fourth project for Udacity's Data Engineer Nano-Degree

## Situation

A music streaming startup, Sparkify, has grown their user base and song database
even more and want to move their data warehouse to a data lake. Their data
resides in S3, in a directory of JSON logs on user activity on the app, as well
as a directory with JSON metadata on the songs in their app.

## Task

The task involves building an ETL pipeline that extracts their data from S3,
processes them using Spark, and loads the data back into S3 as a set of
dimensional tables, allowing their analytics team to continue finding
insights into what songs their users are listening to.

## Analysis

We will use Spark to read json files directly from S3, process them to create
the desired analytical tables, and write them back to S3 as parquet files.

Once the log files have been read into a distributed dataframe the process of
creating the tables is straighforward, some simple select, filter, and casting
expressions.

## Results

The `etl.py` scripts creates a Spark session, loads the log files from S3,
performs the necessary operations and writes the analytical tables as parquet
files. Some of the tables are partitioned by certain fields, making future
queries more efficient by only loading the necessary slices.

## Outputs and Contents

Below is a list of the most relevant files with instructions on how to execute
the python scripts.

**Note**: This project was developed using python 3.9.7 and Spark 3.1.2. Some
of the packages may need to be downgraded if using a lower version but there
shouldn't be any breaking changes.

To run the scripts make sure you have all neccesary packages (as listed in
`requirements.txt`) installed. The easiest way is to create a virtual
environment with your tool of choice (venv, conda), activate it, and run
`pip install -r requirements.txt`.

* `aws_template.cfg`: Create a copy of this config file named `aws.cfg` (so that
  it'll be ignored by git) and fill in the necessary AWS information.

* `schemas.py`: Contains the schemas for the log and song tables to be used by
  Spark when loading the json files instead of having to rely on automatic type
  detection.

* `etl.py`: The main script which creates the Spark session, loads and
  transforms the data and saves the tables as parquet files back to S3.
