import pandas as pd

stations = pd.read_csv(r'data/q2_data/shortage_stations.csv')
stations_overload = pd.read_csv(r'data/q2_data/overload_stations.csv')
station_df = pd.read_csv(r'data/q2_data/update_station.csv')

def get_location(station_name):
    global station_df
    selected_station_df = station_df.loc[station_df['name'] == station_name]
    if not selected_station_df.empty:
        lon = float(selected_station_df['longitude'].tolist()[0])
        lat = float(selected_station_df['latitude'].tolist()[0])
        name = selected_station_df['name'].tolist()[0]
        return name,lat,lon

def get_stations(date):
    global stations
    stations_df = stations.loc[stations['date'] == date]
    if not stations_df.empty:
        station_list = stations_df.groupby('name')['name'].apply(list)
        final_list = [x[0] for x in station_list]
        return final_list

def get_over_stations(date):
    global stations_overload
    stations_df = stations_overload.loc[stations_overload['date'] == date]
    if not stations_df.empty:
        station_list = stations_df.groupby('name')['name'].apply(list)
        final_list = [x[0] for x in station_list]
        return final_list

def get_hourly_storage(date,station):
    global stations
    global station_df
    select_station = stations.loc[stations['date'] == date]
    select_station = select_station.loc[select_station['name'] == station]
    if not select_station.empty:
        station_name = select_station['name'].tolist()[0]
        station_info = station_df.loc[station_df['name'] == station_name]
        capacity = int(station_info['num_bikes_available'].tolist()[0])
        hours = select_station['hour'].tolist()

        true_false = []
        hourly_movment = []
        total_filled = capacity
        for i in range(23):
            if i in hours:
                hourly_bike = select_station.loc[select_station['hour'] == i]
                bike_movement = hourly_bike['hourly_bike_movement'].tolist()[0]
                total_filled = total_filled + bike_movement;
                hourly_movment.append(total_filled)
            else:
                hourly_movment.append(total_filled)

            filled_rate = total_filled/capacity
            if (filled_rate <= 0.001):
                true_false.append(True)
            else:
                true_false.append(False)

    return [hourly_movment,true_false]






























