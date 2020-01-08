#!/bin/sh
spark-submit top_spark.py 2013
spark-submit top_spark.py 2014
spark-submit top_spark.py 2015
spark-submit top_spark.py 2016
#get updated stations
spark-submit update_stations.py

#upload it to cluster for later use
hdfs dfs -copyFromLocal data/q2_data/update_station.csv

#get 2016 traffic taxi trip
spark-submit short-taxi_trips.py 
#upload it to cluster for later use
hdfs dfs -copyFromLocal data/q3_data/taxi_traffic.csv

#get new suggested stations according to cluster size
spark-submit taxi_ml_cluster.py

#get shortage stations
spark-submit shortage.py
#upload it to cluster for later use
hdfs dfs -copyFromLocal data/q2_data/shortage_stations.csv

#get overload stations
spark-submit overload.py
#upload it to cluster for later use
hdfs dfs -copyFromLocal data/q2_data/overload_stations.csv


#get nearby stations
spark-submit nearby_station.py
