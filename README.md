# Udacity-DENG-Project4-Data-Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I have been tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow the analytics team to continue finding insights in what songs the users are listening to.

The subject datasets are a set of files in .JSON format stored in AWS S3 buckets:

* **s3://udacity-dend/song_data**: data about artists and songs, example of row '0' of the data as follows:
  `{0: {'artist_id': 'ARDR4AC1187FB371A1', 'artist_latitude': None, 'artist_location': '', 'artist_longitude': None, 'artist_name': 'Montserrat Caballé;Placido Domingo;Vicente Sardinero;Judith Blegen;Sherrill Milnes;Georg Solti', 'duration': 511.16363, 'num_songs': 1, 'song_id': 'SOBAYLL12A8C138AF9', 'title': 'Sono andati? Fingevo di dormire', 'year': 0}}`

* **s3://udacity-dend/log_data**: data of logs of usage of the app, example of row '0' of the data as follows:
  `{0: {'artist': 'Harmonia', 'auth': 'Logged In', 'firstName': 'Ryan', 'gender': 'M', 'itemInSession': 0, 'lastName': 'Smith', 'length': 655.77751, 'level': 'free', 'location': 'San Jose-Sunnyvale-Santa Clara, CA', 'method': 'PUT', 'page': 'NextSong','registration': 1541016707796.0, 'sessionId': 583, 'song': 'Sehr kosmisch', 'status': 200, 'ts': 1542241826796, 'userAgent': '"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"', 'userId': '26'}}`

<br/>

> A data lake is a system or repository of data stored in its natural/raw format, usually object blobs or files. A data lake is usually a single store of all enterprise data including raw copies of source system data and transformed data used for tasks such as reporting, visualization, advanced analytics and machine learning. A data lake can include structured data from relational databases (rows and columns), semi-structured data (CSV, logs, XML, JSON), unstructured data (emails, documents, PDFs) and binary data (images, audio, video). A data lake can be established "on premises" (within an organization's data centers) or "in the cloud" (using cloud services from vendors such as Amazon, Google and Microsoft). [Wikipedia]

<br/>

## 1. Database Design Description
There are two source datasets, one called "song" and another "log". And from these two datasets the following star schema database will been created for optimized queries on song play analysis. The tables are as below:

### 1.1 Fact Table
The fact table in this star scheme will be named "songplays" and is designed to record "log" data associated with song plays. This fact table will have the
following columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, month and year. The month and year columns are used to partition the final table. 

### 1.2 Dimension Tables
The following tables in this star scheme are all dimension tables.
- users - This table will be used to record unique user details. This table will have the following columns:
            user_id, first_name, last_name, gender, level.
- songs - This table will be used to record unique song details. This table will have the following columns:
            song_id, title, artist_id, year, duration.
- artists - This table will be used to record unique artist details. This table will have the following columns:
            artist_id, name, location, latitude, longitude.
- time - This table will be used to record unique time details. This table will have the following columns: 
            start_time, hour, day, week, month, year, weekday

<br/>

## 2. Files in the repository
There are two source datasets, one called "song" and another "log" and these are located on the AWS S3 bucket as detailed in the introduction above. The following subsections contain brief descriptions of the rest of the files in this repository: 

### 2.1 etl.py
This script does the following:  
- Extracts, transforms and loads/saves data into, the above mentioned, fact and dimension tables.

### 2.2 dl.cfg
This config file is accessed by etl.py for file paths of the source datasets and where to save the fact and dimenion table outputs as well. Additionally the credentials of the user should also be written into this .cfg file. 

`[AWS]`<br/>
`AWS_ACCESS_KEY_ID=<YOUR_ACCESS_KEY_ID>`<br/>
`AWS_SECRET_ACCESS_KEY=<YOUR_SECRET_ACCESS_KEY>`<br/>
`INPUT_DATA            = s3a://<YOUR BUCKET>`<br/>
`OUTPUT_DATA           = s3a://<YOUR BUCKET>/output_data/`<br/>
<br/>
`[LOCAL]`<br/>
`INPUT_DATA            = data`<br/>
`OUTPUT_DATA           = data/output_data/`<br/>

### 2.3 etl.ipynb
This jupyter notedbook was used to develop the code used in the "etl.py" script.

<br/>

## 3. User Guide

### 3.1 Setup dl.cfg file
Before running the main script "etl.py", please ensure the config file is updated:
- With the file paths of the source data sets
- With the file paths to indicate to the script where the resulting fact and dimension output tables should be saved to.
- With your AWS credentials. 

### 3.2 Running etl.py
#### 3.2.1 Running locally
A feature has been built the "etl.py" script to allow it to be ran locally, such that the source datasets are downloadeded from AWS S3 and the fact and dimension tables be created and stored locally. Although this is not recommended, to to carry this out edit line 250 and 251 to as below:<br/>
`input_data = config['AWS']['INPUT_DATA']`<br/>
`output_data = config['LOCAL']['OUTPUT_DATA']`<br/>
<br/>
Subsquently:
- Type the following into the terminal window "Python etl.py", followed by the return key.<br/>
<br/>
After following the above commands the fact and dimension tables will be populated, and saved locally, with the source data and ready for queries to be performed to extract the necessary data for analysis.

#### 3.2.2 Running non locally
To run the  script "etl.py" as intended, download data from AWS S3 to EMR for ETL and then save the output tables back into AWS S3, ensure line 250 and 251 of the "etl.py" script is as below:<br/>
`input_data = config['AWS']['INPUT_DATA']`<br/>
`output_data = config['AWS']['OUTPUT_DATA']`<br/>
<br/>
Subsquently:
- Open up a terminal and use the "scp" command to copy the "etl.py" and "dl.cfg" file onto to your AWS EMR cluster.
- Install package boto3 if not installed already using the following command `pip install boto3 --user`.
- Commit and run the "etl.py" script by typing the following command in the termial `spark-commit --master yarn ./etl.py`.
<br/>
After following the above commands the fact and dimension tables will be populated, and saved onto AWS s3, with the source data and ready for queries to be performed to extract the necessary data for analysis.
