# Udacity-Data-Warehouse-Project


## Project Motivation

A music streaming startup, Sparkify, has grown their user base and song database and wants to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, we are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. We'll be able to test their database and ETL pipeline by running queries given to us by the analytics team from Sparkify and compare our results with their expected results.


## Project Description

In this project, we'll apply what we've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, we will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.


## Prerequisites

1. AWS credentials (IAM user credentials, stored in dwh.cfg)

2. A Redshift cluster to run your queries


## Files in the Repository

We need two datasets that reside in S3. Here are the S3 links for each:

**Song data:** s3://udacity-dend/song_data

This directory contains a dataset that is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.

song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

**Log data:** s3://udacity-dend/log_data

This directory contains a collection of JSON log files based on the songs in the dataset located in the directory above. These simulate activity logs from a music streaming app based on specified configurations. The log files are partitioned by year and month. For example, here are filepaths to two files in this dataset.

log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json


`sql_queries.py`: Python script containing all queries and SQL statements used for this project.

`create_tables.py`: Python script responsing for dropping and creating the tables used for this project.

`etl.py`: Python script that copies data from S3, processes that data, and inserts it into SQL tables.
 
`dwh.cfg`: A file that contains my AWS credentials.

`ETL_screenshot.png`: A PNG file showing that the ETL pipeline works.


## Results

`ETL_screenshot.png`: A PNG file showing that the ETL pipeline works.


## Acknowledgements

https://aws.amazon.com/redshift/

https://www.w3schools.com/sql/sql_update.asp

https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_AS.html

https://www.techonthenet.com/sql/tables/create_table2.php

https://dba.stackexchange.com/questions/135186/cant-create-a-table-with-the-same-name-i-deleted-before

https://javarevisited.blogspot.com/2015/10/how-to-replace-null-with-empty-string-in-SQL-SERVER-isnull-collesce-example.html#axzz7irPpmzkU
