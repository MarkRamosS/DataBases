from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, row_number, asc, unix_timestamp, max, month, dayofmonth, hour, avg, window, year, to_timestamp, lit
from pyspark.sql.window import Window
from operator import add
import os
import sys, datetime
import pyspark.sql.functions as F
from time import time
from pyspark.rdd import RDD

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

df_trips = df_trips.withColumn("month", month('tpep_pickup_datetime')).withColumn("year", year('tpep_pickup_datetime')).filter((col("month") <= 6) & (col("year") == 2022)).drop("year").drop("month")

rdd_trips = df_trips.rdd
rdd_zones = df_zones.rdd

start = time()

def fortnight(m,d,h):
        if m == 1 and d >= 1 and d < 16:
                return "2022-01-01 00:00 - 2022-01-16 00:00"
        elif m == 1 and d >= 16 and d < 31: 
                return "2022-01-16 00:00 - 2022-01-31 00:00"
        elif (m == 1 and d >= 31) or (m == 2 and d < 15):
                return "2022-01-31 00:00 - 2022-02-15 00:00"
        elif (m == 2 and d >= 15) or (m == 3 and d < 2):
                return "2022-02-15 00:00 - 2022-03-02 00:00"
        elif m == 3 and d >= 2 and d < 17:
                return "2022-03-02 00:00 - 2022-03-17 00:00"
        elif (m == 3 and d >= 17) or (m == 4 and d<=1 and h < 1):
                return "2022-03-17 00:00 - 2022-04-01 01:00"
        elif m == 4 and ((d >= 1 and d < 16 ) or (d == 16 and h < 1)): 
                return "2022-04-01 01:00 - 2022-04-16 01:00"
        elif (m ==4 and d >= 16) or (m == 5 and d <=1 and h <1):
                return "2022-04-16 01:00 - 2022-05-01 01:00"
        elif m == 5 and ((d >= 1 and d < 16) or (d == 16 and h < 1)):
                return "2022-05-01 01:00 - 2022-05-16 01:00"
        elif m == 5 and ((d >= 16 and d < 31) or (d == 31 and h < 1)):
                return "2022-05-16 01:00 - 2022-05-31 01:00"
        elif (m ==5 and d >= 31) or (m == 6 and (d < 15 or (d==15 and h <1))):
                return "2022-05-31 01:00 - 2022-06-15 01:00"
        elif  m == 6 and ((d >= 15 and d < 30) or (d == 30 and h < 1)):
                return "2022-06-15 01:00 - 2022-06-30 01:00"
        else:
                return "2022-06-30 01:00 - 2022-07-01 00:00"

# We attempted joining rdd_trips with rdd_zones to get the names of the zones, but a worker crashed because of memory.
# rdd_trips = rdd_trips.map(lambda x: (x.PULocationID,(x.DOLocationID,x.tpep_pickup_datetime.month,x.trip_distance,x.total_amount))).join(rdd_zones)
# rdd_trips = rdd_trips.map(lambda x: (x[1][1][0],(x[1][1],x[1][0][1],x[1][0][2],x[1][0][3]))).join(rdd_zones)
# rdd_trips = rdd_trips.filter(lambda x: x[1][1][0] != x[1][1])


routes = rdd_trips.filter(lambda x: x.PULocationID != x.DOLocationID).map(lambda x: (fortnight(x.tpep_pickup_datetime.month,x.tpep_pickup_datetime.day,x.tpep_pickup_datetime.hour), (int(x.trip_distance), int(x.total_amount), 1)))
rddq3 = routes.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1],x[2]+y[2])).map(lambda x: (x[0], x[1][0]/x[1][2], x[1][1]/x[1][2]))

for x in rddq3.collect():
        print(x)

time_stamp = time()-start
print(f'Execution Time: {time_stamp:.3f}s')


