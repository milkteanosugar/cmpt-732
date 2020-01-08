import pandas as pd

stations = pd.read_csv(r'data/q2_data/nearby_stations.csv')


def get_nearby_stations(date, hour, station_name):
    global stations
    near_date = stations.loc[(stations.date == date)]
    near_hour = near_date.loc[(stations.hour == hour)]
    near_station = near_hour.loc[near_date['start_station_name'] == station_name]
    if not near_station.empty:
        nearby_str = near_station['nearby_stations'].tolist()[0]
        nearby_list = nearby_str.split('[')[1].split(']')[0]
        nearby_list2 = nearby_list.split(',')
        return_nearby = []
        for i in nearby_list2:
            temp = i.split('\'')
            return_nearby.append(temp[1])
            return_nearby = return_nearby[:5]
        return return_nearby


