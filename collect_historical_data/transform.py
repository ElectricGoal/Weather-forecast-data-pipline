import pandas as pd
import os
import json
from datetime import datetime
import yaml

# config variable
config = {}

# import yaml file
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

def transform_data():

    # specify the directory path where the CSV files are located
    directory = config['save_path']

    # initialize an empty DataFrame to hold the data
    df = pd.DataFrame()

    # loop through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            # construct the full path of the CSV file
            filepath = os.path.join(directory, filename)

            location_df = pd.read_csv(filepath)

            # append location dataframe to the main dataframe
            df = pd.concat([df, location_df])

    # drop columns that are not needed
    df = df.drop(['truth_location'], axis=1)

    df['location'] = df['location'].str.replace('%20', ' ')
    
    # convert date column to datetime
    df['date_time'] = pd.to_datetime(df['date_time'])

    with open('locations.json', 'r') as f:
        locations = json.load(f)['locations']
        for location in locations:
            location_name = location['name']
            location_id = "VN-" + str(location['code'])

            # add the location id to the dataframe
            df.loc[df['location'] == location_name, 'location_id'] = location_id

    df["date_time"] = df["date_time"].apply(convert_date)
    df["location"] = df["location"].apply(rename_location)

    # return the dataframe
    return df

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


