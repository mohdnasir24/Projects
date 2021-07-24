# Analyze Song Play data

Goal of this project is to create a cluster in **Redshift**.
Create an **IAM Role** and assign **Policy** to it to access **Redshift**.
Then create **Staging Tables**, **Fact Tables** and **Dimension Tables** with appropriate **Distribution Styles**
After that,fetch data from **S3 Buckets** into **Staging tables** using **COPY COMMAND**
Data from **Staging tables** is inserted into **Fact Tables** and **Dimension Tables**
Data can be analysed once it is loaded correctly into **Fact Tables** and **Dimension Tables**

# Data Model

The schema consist of dimension tables users, songs, artists, time. 
Fact table data is stored in songplays table. 
It is stored in 3rd normal form to avoid duplication. 
The tables are connected through referential integrity via foreign keys. 
The ETL pipeline loads data into dimension tables as data should be loaded in dimension table before fact table to avoid any timing issues. 
Data is modified in dimension table for correct data type to rectify any data loading issues in fact table.

# Schema Description

Following tables have been used in the project. Below is the column name along with the data type for each column

**Staging Event Table : staging_events**
column	data_type
artist	varchar
auth	varchar
firstname	varchar
gender	varchar
iteminsession	integer
lastname	varchar
length	float
level	varchar
location	varchar
method	varchar
page	varchar
registration	bigint
sessionid	integer
song	varchar
status	integer
ts	bigint
user_agent	varchar
userid	integer
*Distribution Style:Auto*

**Staging Song Table : staging_songs**
column	data_type
num_songs	integer
artist_id	varchar
artist_latitude	float
artist_longitude	float
artist_location	varchar
artist_name	varchar
song_id	varchar
title	varchar
duration	float
year	integer
*Distribution Style:Auto*


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
*Distribution Style:Even*

**Dimension Table : users**
column_name	data_type
user_id	varchar
first_name	varchar
last_name	varchar
gender	varchar
level	varchar
*Distribution Style:Even*

**Dimension Table : songs**
column_name	data_type
song_id	varchar
title	varchar
artist_id	varchar
year	integer
duration	float
*Distribution Style:Even*

**Dimension Table : artists**
column_name	data_type
artist_id	varchar
name	varchar
location	varchar
latitude	float
longitude	float
*Distribution Style:Even*

**Dimension Table : time**
column_name	data_type
start_time	timestamp
hour	integer
day	integer
week	integer
month	integer
year	integer
weekday	integer
*Distribution Style:Even*




# How to run the scripts

Follow the instructions to run the scripts. Please execute the scripts in below order

1. Create new IAM user with programtic access mentioned in step 0 of IaC.ipynb
2. Note down key and secret values from IaC.ipynb and add it to dwh.cfg file
3. run IaC.ipynb till step 4 to create redshift cluster
4. Don't run step 5 now as it is required to run at the end to delete cluster
5. Note down redshift host and arn details from IaC.ipynb step 4 and add it to dwh.cfg file
6. Open Terminal by clicking the + icon and selecting terminal
7. Make sure jupyter notebook kernel is shut down before running any command in terminal window
8. First create tables by running command "Python create_table.py" in terminal window
9. For testing successful table creation, check tables in redshift front end query editor
10. For etl data pipeline execution with all files in data folder, please run "Python etl.py" in terminal window
11. For testing successful data insertion, check tables in redshift front end query editor
12. After succesfull data insertion, run step 5 and onwards in IaC.ipynb to delete redshift cluster and iam role

# Files in Repo

Following files/Folder are available in repo

1. create_table.py : it contains the python script to create above tables in redshift database
2. sql_queries.py : it contains the python script to insert data into tables in redshift database
3. IaC.ipynb : Jupyter notebook to create cluster, create iam role, create vpc and assign security policy using aws sdk python
4. etl.py : it is the data pipeline etl script to process all the files  from data folder
5. Log data : Log event data is stored at 's3://udacity-dend/log_data' s3 location
6. Log Json layout file : Log json layout file is stored at 's3://udacity-dend/log_json_path.json' location
7. Song Data : Song event data is stored at 's3://udacity-dend/song_data' s3 location
8. dwh.cfg : configuration file to store aws configuration data


# Example queries

Please run below sample queries to query the data

1. SELECT * FROM staging_events LIMIT 5;
2. SELECT * FROM staging_songs LIMIT 5;
3. SELECT * FROM songplays LIMIT 5;
4. SELECT * FROM users LIMIT 5; 
5. SELECT * FROM songs LIMIT 5;
6. SELECT * FROM artists LIMIT 5;
7. SELECT * FROM time LIMIT 5;
8. SELECT * FROM songplays where song_id is NOT NULL and artist_id is NOT NULL LIMIT 5;

