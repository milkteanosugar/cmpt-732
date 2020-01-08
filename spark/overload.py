from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
import sys
conf = SparkConf().setAppName('tpch orders')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

spark = SparkSession(sc)

def main():
    #trips_df = spark.read.format("csv").option("header", "true").load("trips.csv")
    #stations = spark.read.format("csv").option("header", "true").load("station_df.csv")
    trips_df = spark.read.csv("tripsl.csv", inferSchema=True, header=True)
    stations = spark.read.csv("update_station.csv", inferSchema=True, header=True)

    #sep starttime stoptimes' date and hour
    trips_df = trips_df.withColumn('start_date', trips_df['starttime'].cast('date'))\
        .withColumn('start_hour',functions.hour(trips_df['starttime']))\
        .withColumn('end_date', trips_df['stoptime'].cast('date'))\
        .withColumn('end_hour',functions.hour(trips_df['stoptime']))

    #bike in hourly
    hourly_end_station = trips_df.groupBy('end_date','end_station_name','end_hour').agg(functions.count('end_station_name'))

    #bike in hourly
    hourly_start_station = trips_df.groupBy('start_date','start_station_name','start_hour').agg(functions.count('start_station_name'))

    #join bike in bike out
    hourly_trip_count = hourly_start_station.join(
        hourly_end_station, (hourly_start_station['start_date'] == hourly_end_station['end_date'])
                            & (hourly_start_station['start_station_name']== hourly_end_station['end_station_name'])
                            & (hourly_start_station['start_hour']== hourly_end_station['end_hour']))


    # hourly storage = bike in - bike out
    station_hourly_storage = hourly_trip_count.withColumn('hourly_bike_movement',
                                                          hourly_trip_count['count(end_station_name)'] -
                                                          hourly_trip_count['count(start_station_name)'])

    #filter hour with no bike movement
    hourly_storage =station_hourly_storage.filter(station_hourly_storage['hourly_bike_movement'] !=0 )\
        .select('start_station_name', 'start_date', 'start_hour','hourly_bike_movement')

    #count daily movement
    station_daily_storage = hourly_storage.groupBy('start_station_name', 'start_date')\
        .agg(functions.sum('hourly_bike_movement')).withColumnRenamed("sum(hourly_bike_movement)",'bike_movement')


    #filter the over load
    station_overload = station_daily_storage.filter(station_daily_storage['bike_movement'] > 0)
    station_capacity_overload = station_overload.join(stations, stations['name'] ==
                                                      station_overload['start_station_name'])
    station_filled_overload = station_capacity_overload.withColumn("filled_rate",
                                                                   (station_capacity_overload['num_bikes_available']
                                                                    + station_capacity_overload['bike_movement'])
                                                                   / station_capacity_overload['capacity']).cache()
    filter_filled_over = station_filled_overload.filter(station_filled_overload['filled_rate'] >= 1)\
        .withColumnRenamed('start_date','date').select('name', 'date', 'filled_rate', 'latitude', 'longitude')
    # get hourly storage
    hourly_filled_overload = filter_filled_over.join(
        hourly_storage,(filter_filled_over['name'] == hourly_storage['start_station_name'])
                       & (filter_filled_over['date'] == hourly_storage['start_date']))

    hourly_filled_overload_final = hourly_filled_overload.select('name', 'date', 'start_hour',
                                                                 'hourly_bike_movement', 'latitude','longitude')\
        .withColumnRenamed('start_hour', 'hour')
    hourly_filled_overload_final.show()

    hourly_filled_overload_final.toPandas().to_csv('data/q2_data/overload_stations.csv')


if __name__ == '__main__':
    main()

