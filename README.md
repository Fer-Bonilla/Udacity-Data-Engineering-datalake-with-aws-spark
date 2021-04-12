# Data lake with AWS spark

Udacity Data Engineering 4th project, building a Data lake using AWS spark services. This project implements a Data Lake in spark AWS S3 Bucket and Amazon spark.

- Understanding the problem to solve
- data description
- Modeling the datalake
- Project structure
- ETL description
- Running the ETL pipeline


## Problem understanding

Build an ETL pipeline for a data lake hosted on S3. Load data from S3 bucket, process the data into analytics tables using Spark hosted in Aws services, and load them back into S3.

## Data description

The project uses data from [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) that is a freely-available collection of audio features and metadata for a million contemporary popular music tracks (300 GB). This data is open for exploration and research and for this project will only use a sample from the songs database and artist information in json format.
  
- **Song dataset**:  
  Json files are under */data/song_data* directory. The file format is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log dataset**: 
  Json File are under */data/log_data*. The file format is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

The data is available in the Udacity buckets 

```
  Song data: s3://udacity-dend/song_data
  Log data: s3://udacity-dend/log_data

```
Paths pointing to S3 buckets are defined in the etl.py


## Datalake Model

The data lake will be designed for analytics using Fact and Dimensions tables on a Star Schema architecture using the spark apy for python, the info is load from S3 using the Amazon s3 api.

**Fact Table**
```
  songplays - records in log data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
```

**Dimension Tables**

```
  users - users in the app: user_id, first_name, last_name, gender, level
  songs - songs in music database: song_id, title, artist_id, year, duration
  artists - artists in music database: artist_id, name, location, latitude, longitude
  time - timestamps of records in songplays broken down into specific units: start_time, hour, day, week, month, year, weekday
```

### Logic model

![Logic model](https://github.com/Fer-Bonilla/Udacity-Data-Engineering-datalake-with-aws-spark/blob/main/images/dataLake_model.png)


## Project structure

The project structure is based on the Udacity's project template:
1. **etl.py** reads and processes files from song_data and log_data and loads them into spark datalake parquet files
2. **README.md** Provides instructions about the project
3. **dl.cfg** configuration parameters (Access and secrect Key)

## ETL Pipeline description

### etl.py
The ETL process is developed in the etl.py script. Data is load from the JSON files and processed using the Pyspark framework creating the tables structure and saving into parquet files. The script process first the songs json files and create the artist and songs parquet files, then process the event logs files and create the users, time and songplays spark tables and write to the parket files in the S3 bucket created in AWS services.


### ETL pipeline diagram

![ETL pipeline diagram](https://github.com/Fer-Bonilla/Udacity-Data-Engineering-datawarehouse-with-aws-redshift/blob/main/images/ETL_pipeline.png)

## Instructions to run the pipeline

A. Components required

 1.	Create or login into AWS amazon account
 2.	User created on IAM AWS and administrative role to connect from remote connection
 3.	Jupyter notebooks environment available
 4.	Python packages: pyspark

B Running the pipeline

 1.	Clone the repository
 2.	Create IAM role and user
 3.	Create and S3 bucket and setup the url locater in the etl.py file
 4.	Configure the session values (Access key and identification key) in the dl.cfg file
 5.	Run the etl.py pipeline
 6.	Verify the S3 bucket, you need to see the parquet files for each table

## Author 
Fernando Bonilla [linkedin](https://www.linkedin.com/in/fer-bonilla/)
