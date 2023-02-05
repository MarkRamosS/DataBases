from pyspark.sql import SparkSession
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

df_trips = df_trips.withColumn("month", month('tpep_pickup_datetime')).withColumn("year", year('tpep_pickup_datetime')).filter((col("month") <= 6) & (col("year") == 2022)).drop("year").drop("month")

start = time()
# Window: Partition by day of the week and sort desceding by passengers (hour_of_day to break equalities)
window = Window.partitionBy(['day_of_week']).orderBy(desc('avg(passenger_count)'),desc('hour_of_day'))
# Groupby (day of week, hour), average over passengers in each group. Then apply window and select top 3 hours for each day of the week
sqlquery4 = df_trips.groupby(date_format("tpep_pickup_datetime", "EEEE").alias('day_of_week'), hour("tpep_pickup_datetime").alias('hour_of_day')).avg('passenger_count').select('*', rank().over(window).alias('rank')).filter(col('rank')<=3).drop('rank')
collection = sqlquery4.collect()

time_stamp = time()-start
sqlquery4.show(41)
print(f'Execution Time: {time_stamp:.3f}s')
