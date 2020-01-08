import sys
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os
import gzip
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, functions, types, Row
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('tpch normalize') \
    .config('spark.cassandra.connection.host', ','.join(cluster_seeds))\
    .config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

'''
Get the popularity count for each station.
Resulting dataframe contains the following fileds
name,latitude,longitude, count
'''
def get_station_count(trips_df):
    start_station_count_df = trips_df.groupby(['month','start_station_name','start_station_latitude','start_station_longitude']).count()
    end_station_count_df = trips_df.groupby(['month','end_station_name','end_station_latitude','end_station_longitude']).count()
    station_count_df = start_station_count_df.union(end_station_count_df).withColumnRenamed('start_station_name','station_name').withColumnRenamed('start_station_latitude','latitude').withColumnRenamed('start_station_longitude','longitude')
    
    return station_count_df

def main(year):
    trips_df = spark.read.csv('tripsl.csv',header=True)
    trips_df = trips_df.withColumn('date',trips_df['stoptime'].cast('date'))
    trips_df = trips_df.withColumn('month',functions.month(trips_df['date']))

    #add a hour column to trips_df
    trips_df = trips_df.withColumn('hour',functions.hour(trips_df['starttime']))

    #add a month column to trips_df
    trips_df = trips_df.withColumn('year',functions.year(trips_df['date']))

    trips_df = trips_df.filter(trips_df['year'] == year)
    

    #calculation each station's trip count
    top_station_count_df = get_station_count(trips_df)
    #calculate the max count for each month
    max_count_df = top_station_count_df.groupby(['month']).max('count').withColumnRenamed('max(count)','count')
    top_station_count_df = top_station_count_df.join(max_count_df,['month','count']).withColumnRenamed('station_name','name')
    
    #saving to the designated folder
    top_station_count_df.repartition(1).toPandas().to_csv('data/q1_data/{}/top.csv'.format(year),header=True)

    #get top 10 station in each time frame
    
    #Morning: 7:00am to 12:00pm 7-12
    m = trips_df.filter(trips_df.hour >= 7).filter(trips_df.hour <= 12)
    #Afternoon:12:pm to 7:pm 12-19
    a = trips_df.filter(trips_df.hour >= 12).filter(trips_df.hour <= 19 )
    #Evening : 7:00 pm to 7:am 19-23,0-7
    e1 = trips_df.filter(trips_df.hour >= 19).filter(trips_df.hour <= 23 )
    e2 = trips_df.filter(trips_df.hour >= 0).filter(trips_df.hour <= 7 )
    e = e1.union(e2)
    
    #counter to track which time frame processing
    c = 0
    frame = ['morning','afternoon','evening']
    for x in [m,a,e]:
        #get the station count by time frame
        top10_station_count_df = get_station_count(x)
        #get the top10 popular stations for each month
        window = Window.partitionBy(top10_station_count_df['month']).orderBy(top10_station_count_df['count'].desc())
        top10_station_count_df = top10_station_count_df.select('*', functions.rank().over(window).alias('rank')).filter(functions.col('rank') <= 10).withColumnRenamed('station_name','name')

        #output as csv file
        top10_station_count_df.repartition(1).toPandas().to_csv('data/q1_data/{}/top10_{}.csv'.format(year,frame[c]),header=True)
        c+=1



if __name__ == '__main__':
    year = sys.argv[1]
    main(year)
