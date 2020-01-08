import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import acos, cos, sin, lit, toRadians,lag
spark = SparkSession.builder.appName('short taxi trips').getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
from pyspark.sql.window import Window
def dist(long_x, lat_x, long_y, lat_y):
    return acos(
        sin(toRadians(lat_x)) * sin(toRadians(lat_y)) + 
        cos(toRadians(lat_x)) * cos(toRadians(lat_y)) * 
            cos(toRadians(long_x) - toRadians(long_y))
    ) * lit(6371.0)

def main(bike_inputs,taxi_inputs,year):
    # read df from csv 
    bike_trips_df = spark.read.csv('tripsl.csv',header=True)
    taxi_trips_df = spark.read.csv('taxi.csv',header=True)
    #filter on given year
    bike_trips_df = bike_trips_df.withColumn('date',bike_trips_df['starttime'].cast('date'))
    bike_trips_df = bike_trips_df.withColumn('year',functions.year(bike_trips_df['date']))
    bike_trips_df = bike_trips_df.filter(bike_trips_df['year'] == 2016)

    #calculate the average distance travel by bikes this year
    bike_dist_df = bike_trips_df.groupby(['start_station_name','end_station_name','start_station_latitude','start_station_longitude','end_station_latitude','end_station_longitude']).count().withColumnRenamed('start_station_name','startStationName').withColumnRenamed('end_station_name','endStationName')
   
    #calculate distance
    w = Window().partitionBy(['start_station_name','end_station_name']).orderBy(['start_station_name','end_station_name'])
    bike_dist_df = bike_dist_df.withColumn("dist", dist("start_station_longitude", "start_station_latitude",'end_station_longitude', 'end_station_latitude').cast('decimal'))
    
    #calculate 50 percentile distance
    avergae_distance =  bike_dist_df.orderBy('dist').selectExpr('percentile_approx(dist, 0.5)').collect()[0][0]
    
    #convert km to miles
    average_distance = float(avergae_distance) * 0.621371 
    #filter out any taxi trips larger than this distance
    new_taxi_trips_df = taxi_trips_df.filter(taxi_trips_df['trip_distance'] < average_distance).filter(taxi_trips_df['pickup_latitude'].isNotNull()).filter(taxi_trips_df['pickup_longitude'].isNotNull()).filter(taxi_trips_df['dropoff_latitude'].isNotNull()).filter(taxi_trips_df['dropoff_longitude'].isNotNull())
    #calculate thr velocity of each trip in miles/s
    new_taxi_trips_df = new_taxi_trips_df.withColumn('velocity',new_taxi_trips_df['trip_distance']/new_taxi_trips_df['travel_time']).orderBy('velocity')
    traffic_velocity = new_taxi_trips_df.selectExpr('percentile_approx(velocity, 0.2)').collect()[0][0]
    new_taxi_trips_df = new_taxi_trips_df.filter(new_taxi_trips_df['velocity'] <= traffic_velocity)
    new_taxi_trips_df.repartition(1).toPandas().to_csv('data/q3_data/taxi_traffic.csv',header=True)




if __name__ == '__main__':
    main()                   