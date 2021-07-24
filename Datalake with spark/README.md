# Analyze Song Play data

Goal of this project is to create tables using **PySpark**.  
Then fetch from **S3 Json Files** into **Dataframes**. 
modify and filter the data using **PySpark**. 
using **Python** to insert data into **S3 Location** in **Parquet Format**.
Merging data across different dimension tables and loading into fact table for analysis

# Data Model

The schema consist of dimension tables users, songs, artists, time. 
Fact table data is stored in songplays table. 
It is stored in 3rd normal form to avoid duplication. 
The tables are connected through referential integrity via foreign keys. 
The ETL pipeline loads data into dimension tables and then into fact table. 
Data is modified in dimension table for correct data type to rectify any data loading issues in fact table.

# Schema Description

Following tables have been used in the project. Below is the column name along with the data type for each column

**Fact Table : songplays**
column_name	data_type
songplay_id	varchar
start_time	timestamp
user_id	varchar
level	varchar
song_id	varchar
artist_id	varchar
session_id	varchar
location	varchar
user_agent	varchar

**Dimension Table : users**
column_name	data_type
user_id	varchar
first_name	varchar
last_name	varchar
gender	varchar
level	varchar

**Dimension Table : songs**
column_name	data_type
song_id	varchar
title	varchar
artist_id	varchar
year	integer
duration	float

**Dimension Table : artists**
column_name	data_type
artist_id	varchar
name	varchar
location	varchar
latitude	float
longitude	float

**Dimension Table : time**
column_name	data_type
start_time	timestamp
hour	integer
day	integer
week	integer
month	integer
year	integer
weekday	integer

# S3 Location for tables

Tables are stored at below S3 location in parquet format

**Fact Table : songplays**
s3a://data-lake-songplay/songplays
**Dimension Table : users**
s3a://data-lake-songplay/users
**Dimension Table : songs**
s3a://data-lake-songplay/songs
**Dimension Table : artists**
s3a://data-lake-songplay/artists
**Dimension Table : time**
s3a://data-lake-songplay/time


# How to run the scripts

Follow the instructions to run the scripts. Please execute the scripts in below order

1. Open Terminal by clicking the + icon and selecting terminal
2. Make sure jupyter notebook kernel is shut down before running any command in terminal window
3. Modify dl.cfg to update aws keys and secret
4. For etl data pipeline execution with all files in data folder, please run "Python etl.py" in terminal window

# Files in Repo

Following files/Folder are available in repo

1. etl.py : it is the data pipeline etl script to process all the files  from data folder
2. data folder : the folder contains files locally for song and log data
3. dl.cfg : configuration file to hold aws key and secret
4. Log data : Log event data is stored at 's3a://udacity-dend/log_data' s3 location
5. Song Data : Song event data is stored at 's3a://udacity-dend/song_data' s3 location

# Output Data

Parquet files can be accessed from following location.
as well as etl.py shows output from songsplay parquet file

**Fact Table : songplays**
s3a://data-lake-songplay/songplays
**Dimension Table : users**
s3a://data-lake-songplay/users
**Dimension Table : songs**
s3a://data-lake-songplay/songs
**Dimension Table : artists**
s3a://data-lake-songplay/artists
**Dimension Table : time**
s3a://data-lake-songplay/time

