# udacity-de-cloud-data-warehouses

## Purpose
Sparkify has grown their user base and song database and 
want to move their processes and data onto the cloud. 
The data resides in S3, in a directory 
of JSON logs on user activity on the app, as well as a 
directory with JSON metadata on the songs in their app.

This app is building an ETL pipeline that extracts the data 
from S3, stages them in Redshift, and transforms data into 
a set of dimensional tables for the analytics team to 
continue finding insights in what songs their users are 
listening to.

## Content
* create_redshift_cluster.py
  * creates the redshift cluster in AWS
* create_tables.py
  * includes the code to drop and create the redshift tables
* delete_redshift_cluster.py
  * deletes the redshift cluster in AWS
* etl.py
  * includes the code to load the staging, dimension and fact tables
* requirements.txt
  * all python dependencies for local development
* sql_queries.py
  * includes all queries for table creation, copying of staging
  data, drop and insert statements
* test_app.py
  * test script to run python scripts locally

## How-to
* create redshift cluster (if it doesn't exist already you
can run create_redshift_cluster.py)
* create tables by running create_tables.py
* run etl.py to copy data to stg tables and insert into
fact and dimension tables
* delete cluster by running delete_redshift_cluster.py

## Schema

Using the song and event datasets, we create a star schema 
optimized for queries on song play analysis. This includes 
the following tables.

### Fact Table
songplays - records in event data associated with song plays 
i.e. records with page NextSong

* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables
users - users in the app
* user_id, first_name, last_name, gender, level

songs - songs in music database
* song_id, title, artist_id, year, duration

artists - artists in music database
* artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into 
specific units
* start_time, hour, day, week, month, year, weekday

### field mapping

* see '# FINAL TABLES' in sql_queries.py

### ETL PROCESSING

* Data in dimension tables is updated as defined in sql_queries.py
  * user_table_insert
  * song_table_insert
  * artist_table_insert
  * time_table_insert 
* only insert of new data, no updates
* dimension tables do not contain null values for PKs

### FILTERING/MODIFICATION

* only logs with the action 'Next Song' are processed in the ETL
* all fields are inserted as in source files, only time dimensions have calculated date columns (hour, day, month, etc.)

## Dev Setup
```
source venv/bin/activate
python -m pip install -r requirements.txt
```
