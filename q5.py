rom pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, sum, desc, row_number, asc, max, month, dayofmonth, hour, dayofweek, date_format
from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window
import os
import sys, datetime
from time import time
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#details for setting up a spark standalone cluster and using the DataFrames API
# https://spark.apache.org/docs/latest/spark-standalone.html
# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html

spark = SparkSession.builder.master("spark://192.168.0.2:7077").getOrCreate()
print("spark session created")
df_trips = spark.read.parquet(  'yellow_tripdata/yellow_tripdata_2022-01.parquet',
                                'yellow_tripdata/yellow_tripdata_2022-02.parquet',
                                'yellow_tripdata/yellow_tripdata_2022-03.parquet',
                                'yellow_tripdata/yellow_tripdata_2022-04.parquet',
                                'yellow_tripdata/yellow_tripdata_2022-05.parquet',
                                'yellow_tripdata/yellow_tripdata_2022-06.parquet'
)
df_zones = spark.read.option("header", "true").option("inferSchema", "true").format("csv").csv('yellow_tripdata/taxi+_zone_lookup.csv')
rdd_trips = df_trips.rdd
rdd_zones = df_zones.rdd

df_trips = df_trips.withColumn("month", month('tpep_pickup_datetime')).filter(col('month')<=6).drop('month')
df_trips = df_trips.withColumn("year", year('tpep_pickup_datetime')).filter(col('year')==2022)

start = time()
# Calculate column tip = tip_amount / fare_amount and filter out some results from months > 6
tip_df = df_trips.withColumn("tip", col('tip_amount')/col('fare_amount')).withColumn("month", month('tpep_pickup_datetime')).withColumn("day", dayofmonth('tpep_pickup_datetime'))
# Calculate average tip over each day
tip_df = tip_df.groupby("month", "day").avg("tip")
# Window order in each month by tip
window = Window.partitionBy(tip_df['month']).orderBy(tip_df['avg(tip)'].desc(),tip_df['day'].desc())
# Apply window and select top 5 tips for each month 
sqlquery5 = tip_df.select('month', "day", "avg(tip)", rank().over(window).alias('rank')).filter(col('rank')<=5).drop('rank')
collection = sqlquery5.collect()
time_stamp = time()-start
sqlquery5.show(41)
print(f'Execution Time: {time_stamp:.3f}s')
