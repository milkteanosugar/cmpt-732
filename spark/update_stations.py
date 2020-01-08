from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
import pandas as pd

conf = SparkConf().setAppName('tpch orders')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

spark = SparkSession(sc)

def main():
    trips_df = spark.read.format("csv").option("header", "true").load("tripsl.csv")
    stations = spark.read.format("csv").option("header", "true").load("station_df.csv")
    all_stations = trips_df.groupBy('start_station_id','start_station_name','start_station_longitude','start_station_latitude').agg(functions.count('start_station_name'))
    update_station = stations.join(all_stations,(all_stations['start_station_name'] == stations['name'])).withColumnRenamed('name','old_name').withColumnRenamed('start_station_name','name')
    update_station = update_station.select('id','name','latitude','longitude','capacity','num_bikes_available','num_bikes_disabled')
    update_station.show()
    update_station.repartition(1).toPandas().to_csv("data/q2_data/update_station.csv")



if __name__ == '__main__':
    main()