import pandas as pd
import json
from datetime import datetime
import yaml

# config variable
config = {}

# import yaml file
with open('/home/godd/airflow/dags/workflow/modules/config.yml', 'r') as f:
    config = yaml.safe_load(f)

def transform_data(df: pd.DataFrame):
    try:
        # path to the locations file
        location_path = config['locations_path']

        # convert the date_time column to datetime
        df['date_time'] = pd.to_datetime(df['date_time'])

        df["date_time"] = df["date_time"].apply(convert_date)

        # rename the location
        df["location"] = df["location"].apply(rename_location)

        # read locations file and add the location id to the dataframe
        with open(location_path, 'r') as f:
            locations = json.load(f)['locations']
            for location in locations:
                location_name = location['name']
                location_id = "VN-" + str(location['code'])

                # add the location id to the dataframe
                df.loc[df['location'] == location_name, 'location_id'] = location_id
    

        return df
    except Exception as e:
        print(e)
        df.to_pickle("/home/godd/airflow/dags/workflow/modules/transform_error.pkl")
        return pd.DataFrame()

def convert_date(date):
    return datetime(date.year, date.month, date.day, date.hour, date.minute, date.second)

def rename_location(location):
    if location == "Phan Thiet":
        return "Binh Thuan"
    elif location == "Gia Nghia":
        return "Dac Nong"
    elif location == "Hanoi":
        return "Ha Noi"
    elif location == "Vi Thanh":
        return "Hau Giang"
    elif location == "Rach Gia":
        return "Kien Giang"
    elif location == "Da Lat":
        return "Lam Dong"
    elif location == "Vinh":
        return "Nghe An"
    elif location == "Phan Rang":
        return "Ninh Thuan"
    elif location == "Dong Hoi":
        return "Quang Binh"
    elif location == "My Tho":
        return "Tien Giang"
    elif location == "Thua Thien - Hue":
        return "Thua Thien Hue"
    else:
        return location