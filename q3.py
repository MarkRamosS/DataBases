from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, avg, window, year
from pyspark.sql.window import Window
import os
import sys, datetime
import pyspark.sql.functions as F
from time import time

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#details for setting up a spark standalone cluster and using the DataFrames API
# https://spark.apache.org/docs/latest/spark-standalone.html
# https://spark.apache.org/docs/2.2.0/sql-programming-guide.html

spark = SparkSession.builder.master("spark://192.168.0.2:7077").getOrCreate()
print("spark session created")
df_trips = spark.read.parquet('yellow_tripdata/yellow_tripdata_2022-01.parquet',
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
df_trips =  df_trips.join(df_zones.select(col("Zone").alias("PUZone"),col("LocationID")), df_trips["PULocationID"] == df_zones["LocationID"])
df_trips =  df_trips.alias("a").join(df_zones.select(col("Zone").alias("DOZone"),col("LocationID")).alias("b"), col("a.DOLocationID") == col("b.LocationID")) 
sqlquery3 = df_trips.where((col("PULocationId") != col("DOLocationID")) & (col("PUZone") != col("DOZone"))).groupBy(window("tpep_pickup_datetime","15 days", startTime = "2 days 22 hours")).agg(avg(col("Trip_distance")).alias("Avg_distance"), avg(col("Total_amount")).alias("Avg_cost")).orderBy(col("window"))
collection = sqlquery3.collect()
time_stamp = time()-start
sqlquery3.show(truncate=False)
print(f'Execution Time: {time_stamp:.3f}s')
