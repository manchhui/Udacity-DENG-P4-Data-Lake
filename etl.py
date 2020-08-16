import configparser
import boto3
from datetime import datetime
import os
import glob
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, lit, concat
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, LongType as Long 


#-----------------------
# setup configparser
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']

s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY_ID'],
                    aws_secret_access_key=config['AWS']['AWS_SECRET_ACCESS_KEY']
                    )

# setup log data schema
logdataSchema = R([
    Fld("artist",Str()),
    Fld("auth",Str()),
    Fld("firstName",Str()),
    Fld("gender",Str()),
    Fld("itemInSession",Long()),
    Fld("lastName",Str()),
    Fld("length",Dbl()),
    Fld("level",Str()),
    Fld("location",Str()),
    Fld("method",Str()),
    Fld("page",Str()),
    Fld("registration",Dbl()),
    Fld("sessionId",Long()),
    Fld("song",Str()),
    Fld("status",Long()),
    Fld("ts",Long()),
    Fld("userAgent",Str()),
    Fld("userId",Str()),
])

# setup song data schema
songdataSchema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Long()),
    Fld("song_id",Str()),
    Fld("title",Str()),
    Fld("year",Long()),
])
#-----------------------

def create_spark_session():
    """
    Gets an existing or Creates a new spark session and returns this. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def getFilepaths(prefix, input_data):
    """
    - Takes in prefix and input_data variables.
    - Determines from the input_data variable if filepaths are local or on 's3a://' servers .json.
    - Returns either local or non-local failpaths.
    """
    
    filepaths=[]
    if 's3a://' in input_data:
        my_bucket = s3.Bucket('udacity-dend')
        objects = my_bucket.objects.filter(Prefix=prefix)
        for obj in objects:
            path, filename = os.path.split(obj.key)
            if '.json' in filename:
                filespaths = filepaths.append(input_data+'/'+obj.key)
    else:
        for root, dirs, files in os.walk(input_data+'/'+prefix):
            files = glob.glob(os.path.join(root,'*.json'))
            for f in files:
                filespaths = filepaths.append(os.path.abspath(f))
    return filepaths
    
    
def process_song_data(spark, input_data, output_data):
    """
    - Takes spark session, input_data (source data) and output_data (where to be written to) paths.
    - Sets up spark context and s3a settings.
    - Calls the filepaths function and gets song data located at input_data path.
    - Transforms song data into the "artist" and "songs_table".
    - Writes both tables to location in the output_data variable in the .parquet format.
    - Returns table to the main function.
    """
    
    sc = spark.sparkContext
    spark.sql("set spark.sql.parquet.compression.codec=gzip")
    hdpConf = sc._jsc.hadoopConfiguration()
    user = os.getenv("USER")
    hdpConf.set("hadoop.security.credential.provider.path", "jceks://hdfs/user/{}/awskeyfile.jceks".format(user))
    hdpConf.set("fs.s3a.fast.upload", "true")
    hdpConf.set("fs.s3a.fast.upload.buffer", "bytebuffer")

    
    # setup empty dataframe
    song_data = spark.createDataFrame(sc.emptyRDD(), songdataSchema)
    
    # get filepaths and EXTRACT song data files
    start = datetime.now()
    print('Extracting .JSON song_data filepaths - In Progress...')
    songFilepaths = getFilepaths('song_data', input_data)
    stop = datetime.now()
    total = stop - start
    print('Extracting .JSON song_data filepaths - Completed, Time Taken: {}'.format(total))
    
    start = datetime.now()
    print('Extracting .JSON song_data - In Progress...')
    song_data = spark.read.json(songFilepaths, schema=songdataSchema)
    stop = datetime.now()
    total = stop - start
    print('Extracting .JSON song_data - Completed, Time Taken: {}'.format(total))
    
    
    # TRANSFORM song data file
    start = datetime.now()
    print('Transforming .JSON song_data files - In Progress, Count of: "{}" files...'.format(song_data.count()))
    df = song_data

    # extract columns to create SONGS table
    songs_table = df.select('song_id','title','artist_id','year','duration')
    songs_table = songs_table.dropDuplicates()
    
    # extract columns to create ARTISTS table
    artists_table = df.select('artist_id','artist_name','artist_location','artist_latitude','artist_longitude')
    artists_table = artists_table.dropDuplicates()
    stop = datetime.now()
    total = stop - start
    print('Transforming .JSON song_data - Completed, Time Taken: {}'.format(total))
    
    
    # LOAD/WRITE song data file
    start = datetime.now()
    print('Loading/Writing .JSON song_data files - In Progress, Count of: "{}" files...'.format(song_data.count()))
    
    # write songs table to parquet files partitioned by year and artist
    songs_tablewrite=songs_table.repartition('year')
    songs_tablewrite.write.mode("overwrite").partitionBy('year').parquet(output_data+'/'+'songs_table.parquet')

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data+'/'+'artists_table.parquet')

    stop = datetime.now()
    total = stop - start
    print('Loading/Writing .JSON song_data - Completed, Time Taken: {}'.format(total))

    return songs_table, artists_table

def process_log_data(spark, input_data, output_data, songs_table, artists_table):
    """
    - Takes spark session, input_data (source data) and output_data (where to be written to) paths.
    - Sets up spark context and s3a settings.
    - Calls the filepaths function and gets log data located at input_data path.
    - Transforms log data into the "songplays", "users" and times_table".
    - Writes tables to location in the output_data variable in the .parquet format.
    """
    
    sc = spark.sparkContext
    spark.sql("set spark.sql.parquet.compression.codec=gzip")
    hdpConf = sc._jsc.hadoopConfiguration()
    user = os.getenv("USER")
    hdpConf.set("hadoop.security.credential.provider.path", "jceks://hdfs/user/{}/awskeyfile.jceks".format(user))
    hdpConf.set("fs.s3a.fast.upload", "true")
    hdpConf.set("fs.s3a.fast.upload.buffer", "bytebuffer")
    
    # setup empty dataframe
    log_data = spark.createDataFrame(sc.emptyRDD(), logdataSchema)
    
    # get filepaths and extact log data files
    start = datetime.now()
    print('Extracting .JSON log_data filepaths - In Progress...')
    logFilepaths = getFilepaths('log_data', input_data)
    stop = datetime.now()
    total = stop - start
    print('Extracting .JSON log_data filepaths - Completed, Time Taken: {}'.format(total))
    
    start = datetime.now()
    print('Extracting .JSON log_data - In Progress...')
    log_data = spark.read.json(logFilepaths, schema=logdataSchema)
    stop = datetime.now()
    total = stop - start
    print('Extracting .JSON log_data - Completed, Time Taken: {}'.format(total))
    
    
    # TRANSFORM log data file
    start = datetime.now()
    print('Transforming .JSON log_data - In Progress, Count of: "{}" files...'.format(log_data.count()))
    df = log_data
    
    # filter by actions for song plays
    df=df.where(col("page") == "NextSong")

    # extract columns for USERS table    
    users_table = df.withColumn("row_number",F.row_number()\
                            .over(Window.partitionBy(df.userId, df.firstName, df.lastName, df.gender)\
                            .orderBy(df.ts.desc())))\
                            .filter(F.col("row_number")==1)\
                            .drop("row_number")\
                            .select('userId','firstName','lastName','gender','level')
    
    # create timestamp column from original timestamp column
    df = df.withColumn("timestamp", F.to_timestamp(df['ts']/1000))

    # extract transform data for SONGPLAYS table
    # song_df acts as one of two staging tables for the final songplays table
    col_list = ['song','artist','length']
    
    song_df = df.select(col("timestamp").alias("start_time"),
                        col("userid").alias("user_id"),
                        col("level"),
                        col("sessionid").alias("session_id"),
                        col("location"),
                        col("useragent").alias("user_agent"),
                        col("song"),
                        col("artist"),
                        col("length"),
                        col("ts"))
    song_df = song_df.withColumn('concat_id',concat(*col_list))
    
    # song_df2 acts as one of two staging tables for the final songplays table
    col_list = ['title','artist_name','duration']
    song_df2 = songs_table.withColumn("artist_id", col("artist_id"))\
        .join(artists_table.withColumn("artist_id", col("artist_id")), on="artist_id")\
        .select("song_id","title","artist_id","artist_name","duration")
    song_df2 = song_df2.withColumn('concat_id',concat(*col_list))
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.join(song_df2, song_df["concat_id"] == song_df2["concat_id"], "left")\
        .select("start_time","user_id","level","song_id","artist_id","session_id","location","user_agent")\
        .dropDuplicates()
    songplays_table = songplays_table.withColumn("month", F.month("start_time"))
    songplays_table = songplays_table.withColumn("year", F.year("start_time"))
    
    # extract columns to create TIME table, first column is from the songplays table
    time_table = songplays_table.select(col("start_time").alias("time_stamp"))
    time_table = time_table.withColumn("hour", F.hour("time_stamp"))
    time_table = time_table.withColumn("day", F.dayofyear("time_stamp"))
    time_table = time_table.withColumn("weekofyear", F.weekofyear("time_stamp"))
    time_table = time_table.withColumn("month", F.month("time_stamp"))
    time_table = time_table.withColumn("year", F.year("time_stamp"))
    time_table = time_table.withColumn("weekday", F.dayofweek("time_stamp"))
    
    stop = datetime.now()
    total = stop - start
    print('Transforming .JSON log_data - Completed, Time Taken: {}'.format(total))
    
    
    # LOAD/WRITE log data file
    # start of loading/writing users / songplays and time tables
    start = datetime.now()
    print('Loading/Writing .JSON log_data - In Progress, Count of: "{}" files...'.format(log_data.count()))
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+'/'+'users_table.parquet')
    
    # write songplays table to parquet files partitioned by year and month?
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'/'+'songplays_table.parquet')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data+'/'+'time_table.parquet')
    
    stop = datetime.now()
    total = stop - start
    print('Loading/Writing .JSON log_data - Completed, Time Taken: {}'.format(total))

def main():
    """
    - Obtains spark session.
    - Obtains input_data and output_data paths from "dl.cfg" file.
    - Passes three variables to the "process_song_data" and "process_log_data" functions.
    """
    spark = create_spark_session()
    
    
    input_data = config['AWS']['INPUT_DATA']
    output_data = config['AWS']['OUTPUT_DATA']
        
    songs_table, artists_table = process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data, songs_table, artists_table)
    
    spark.stop()
    
if __name__ == "__main__":
    main()
