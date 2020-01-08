import pandas as pd

station_df = pd.read_csv(r'data/q2_data/update_station.csv')
new_station_df = pd.read_csv(r'data/q3_data/new_station.csv')
def get_location(station_name):
    global station_df
    selected_station_df = station_df.loc[station_df['name'] == station_name]
    lon = float(selected_station_df['longitude'].tolist()[0])
    lat = float(selected_station_df['latitude'].tolist()[0])
    return [lat,lon]

def all_stations():
    global station_df
    station_name = list(station_df['name'])
    station_lon = list(station_df['longitude'])
    station_lat = list(station_df['latitude'])
    return station_name,station_lon,station_lat

def get_new_stations(num):
    global new_station_df
    num_stations = new_station_df.loc[new_station_df['0'] == num]
    locations = num_stations['1'].tolist()[0]
    location = locations.split('[[')[1]
    location = location.split(']]')[0]
    location_list = location.split('],')
    lon_list = []
    lat_list = []
    for i in location_list:
        temp = i.split('[')[1]
        lat,lon = temp.split(',')
        lon_list.append(float(lon))
        lat_list.append(float(lat))
    return lon_list,lat_list



