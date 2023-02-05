from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, max, month, dayofmonth, hour, year
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

df_trips = df_trips.withColumn("month", month('tpep_pickup_datetime')).withColumn("year", year('tpep_pickup_datetime')).filter((col("month") <= 6) & (col("year") == 2022)).drop("month").drop("year")

df_trips.createOrReplaceTempView("tripdata")
df_zones.createOrReplaceTempView("zonedata")
start = time()
sqlquery1 = spark.sql("select * from tripdata as t join zonedata as z on t.DOLocationID = z.LocationID where Zone = 'Battery Park'and tip_amount=(select max(tip_amount)from tripdata where DOLocationID = 12 and month(tpep_pickup_datetime)= 3) and month(tpep_pickup_datetime)= 3 ")
collection = sqlquery1.collect()
time_stamp = time()-start
sqlquery1.show(41)
print(f'Execution Time: {time_stamp:.3f}s')
