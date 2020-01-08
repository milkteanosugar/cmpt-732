import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, SQLContext, functions, types
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import sqrt
spark = SparkSession.builder.appName('nearby station').getOrCreate()
assert spark.version >= '2.4'  # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext
sqlContext = SQLContext(sc)

short_station = spark.read.csv("shortage_stations.csv", inferSchema=True, header=True)
over_station = spark.read.csv("overload_stations.csv", inferSchema=True, header=True)

# overload stations
over_station_coord = over_station.select('date', 'hour', 'name', 'latitude', 'longitude')

over_station_coord = over_station_coord.withColumnRenamed('name', "near_station_name")\
    .withColumnRenamed('latitude', "near_latitude").withColumnRenamed('longitude', "near_longitude")\
    .withColumnRenamed('date', "near_date").withColumnRenamed('hour', "near_hour")


# shortage stations
short_station_coord = short_station.select('date', 'hour', 'name', 'latitude', 'longitude')

short_station_coord = short_station_coord.withColumnRenamed('name', "start_station_name")\
    .withColumnRenamed('latitude', "start_latitude").withColumnRenamed('longitude', "start_longitude")


# join stortage stations and overload stations
df_join = over_station_coord.join(short_station_coord,
                                  ((over_station_coord['near_date'] == short_station_coord['date'])
                                   & (over_station_coord['near_hour'] == short_station_coord['hour'])))

# Distance
df_join = df_join.withColumn('latitude_distance', functions.radians(over_station_coord['near_latitude'])
                             - functions.radians(short_station_coord['start_latitude']))

df_join = df_join.withColumn('longitude_distance', functions.radians(over_station_coord['near_longitude'])
                             - functions.radians(short_station_coord['start_longitude']))

df_join = df_join.withColumn('a', (pow(functions.sin('latitude_distance'), 2) +
                                   functions.cos(functions.radians(short_station_coord['start_latitude'])) *
                                   functions.cos(functions.radians(over_station_coord['near_latitude'])) *
                                   (pow(functions.sin('longitude_distance'), 2))))

df_join = df_join.withColumn('distance', 6373 * 2 * functions.atan2(sqrt(df_join['a']), sqrt(1- df_join['a'])))

# distance less than 3 km
#df_join = df_join.filter(df_join['distance'] < 3)

df_join = df_join.select('date', 'hour', 'start_station_name', 'near_station_name', 'distance')

df_join = df_join.dropDuplicates(['date', 'hour', 'start_station_name', 'near_station_name'])

df_join = df_join.orderBy('date', 'hour', 'distance').select('date', 'hour', 'start_station_name', 'near_station_name')

df_join = df_join.groupBy(['date', 'hour', 'start_station_name']).agg(collect_list('near_station_name'))\
    .withColumnRenamed('collect_list(near_station_name)', 'nearby_stations')

df_join.show()

df_join.toPandas().to_csv('data/q2_data/nearby_stations.csv')