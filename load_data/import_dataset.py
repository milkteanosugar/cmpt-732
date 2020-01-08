#import dataset from google cloud
import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
	
#don't save it in the directory having User
project_path = './'
credential_path = "732.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = project_path + credential_path
#get client
client = bigquery.Client() 

#get citibike_stations and citibike_trips tables using query

station_query = """
SELECT S.station_id AS id, S.name, S.latitude, S.longitude, S.region_id AS rid,S.capacity,S.num_bikes_available,
S.num_bikes_disabled 
FROM `bigquery-public-data.new_york.citibike_stations` AS S
"""

trip_query = """
SELECT * 
FROM `bigquery-public-data.new_york.citibike_trips` AS T
"""

taxi_query = '''
SELECT * FROM `bigquery-public-data.new_york.tlc_yellow_trips_2016` AS T 
LIMIT 30000000
'''

#safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
station_query_job = client.query(station_query)
trip_query_job = client.query(trip_query)
taxi_job = client.query(taxi_query)


#convert to pandas dataframe
citibike_stations_df = station_query_job.to_dataframe()
citibike_trips_df = trip_query_job.to_dataframe()
taxi_trips_df = taxi_job.to_dataframe()

#export to csv
td = (taxi_trips_df['dropoff_datetime']-taxi_trips_df['pickup_datetime'])
taxi_trips_df['travel_time'] = td.astype('timedelta64[s]')

#save to local
citibike_stations_df.to_csv("data/tripsl.csv",header=True)
citibike_trips_df.to_csv("data/station_df.csv",header=True)
taxi_trips_df.to_csv("data/taxi.csv",header=True)
