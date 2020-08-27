### Sparkify Amazon Redshift Data Warehouse

Sparkify is a music streaming startup, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project is an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

#### Project Datasets

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

#### Schema for song play analysis

Fact Table

`songplays` - records in event data associated with song plays i.e. records with page NextSong

* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables

`users` - users in the app
* user_id, first_name, last_name, gender, level

`songs` - songs in music database
* song_id, title, artist_id, year, duration

`artists` - artists in music database
* artist_id, name, location, lattitude, longitude

`time` - timestamps of records in songplays broken down into specific units
* start_time, hour, day, week, month, year, weekday

<div align='center'>
<img src="/images/schema.png" height="400" width="400">
</div>

#### Project Template

The project template includes four files:

- `dwhX.cfg` Insert Redshift and AWS configurations
- `Lauch and Tear down AWS Redshift Cluster using the AWS python SDK.ipynb` Create and delete AWS Redsift cluster Steps in boto3
- `images` Related images folder
-   `create_table.py` to create fact and dimension tables for the star schema in Redshift.
-   `etl.py`   load the data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
-   `sql_queries.py`  SQL statements to create, drop and insert data into redshift tables, which will be imported into the two other files above.
-   `README.md` 
- `Perfomring Analytics` Some sample queries run using the facts and dimension tables

#### Steps

Create AWS Redshift cluster

1. Run the cells until to create a Redshift cluster found in the `Lauch and Tear down AWS Redshift Cluster using the AWS python SDK.ipynb` with the configurations in the `dwhX.cfg`.
2. Copy the Redshift endpoint and iam role to paste in the dwhX.cfg for host and iam role.

Create Tables following Schema

3. Following the Schema discussed in the schema section, write SQL statements to create and drop new facts and dimension tables .
4. Run `create_table.py` to execute the create tables SQL statements (also by droppingif they already exists).

Insert the data into the tables and create facts and dimension tables

5. Firstly, insert the data into the staging tables `events_staging` and `songs_staging`. 
 
		If you encounter any error try to trouble shoot by printing the top 
        few rows in the stl_load_errors table as shown in the figure below:
<div align='center'>
<img src="/images/Troubleshoot.png" height="200" width="800">
</div>
<div align='center'>
<img src="/images/TroubleshootX.png" height="400" width="400">
</div>

6.  Now perform ETL on the staging tables to obtain the facts and dimension tables.

Finally, perform analytics using the data in the data warehouse such as the top songs of the month:

<div align='center'>
<img src="/images/TopSongsOfMonth.png" height="600" width="800">
</div>
