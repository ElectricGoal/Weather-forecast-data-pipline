"""
This script is used to retrieve and transform weather data into single csv.
example API explorer: https://www.worldweatheronline.com/developer/premium-api-explorer.aspx

input: api_key, location_list, start_date, end_date, frequency

output: data/location_name.csv

Note: 
- location_name is the name of the location in location_list
- must replace spaces with '%20' in location names
"""

import urllib.request
import urllib.parse
import json
import pandas as pd
from datetime import datetime
import os

# function to unnest json for each month
def extract_monthly_data(data):
    """
    Extract data from json response for each month

    Parameters
    ----------
    data : json
        json response from worldweatheronline.com
    
    Returns
    -------
    df_month : pandas DataFrame
        weather data for the month
    """
    num_days = len(data)

    # initialize df_month to store return data
    df_month = pd.DataFrame()

    for i in range(num_days):
        # extract this day
        d = data[i]

        # astronomy data is the same for the whole day
        astr_df = pd.DataFrame(d['astronomy'])

        # hourly data; temperature for each hour of the day
        hourly_df = pd.DataFrame(d['hourly'])

        # this wanted_key will be duplicated and use 'ffill' to fill up the NAs
        wanted_keys = ['date', 'maxtempC', 'mintempC',
                       'totalSnow_cm', 'sunHour', 'uvIndex']  # The keys you want
        
        subset_d = dict((k, d[k]) for k in wanted_keys if k in d)
        this_df = pd.DataFrame(subset_d, index=[0])
        df = pd.concat([this_df.reset_index(drop=True), astr_df], axis=1)

        # concat selected astonomy columns with hourly data
        df = pd.concat([df, hourly_df], axis=1)
        df = df.fillna(method='ffill')

        # make date_time columm to proper format
        # fill leading zero for hours to 4 digits (0000-2400 hr)
        df['time'] = df['time'].apply(lambda x: x.zfill(4))

        # keep only first 2 digit (00-24 hr)
        df['time'] = df['time'].str[:2]

        # convert to pandas datetime
        df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])

        df['weather_status'] = df['weatherDesc'].apply(lambda x: x[0]['value'])
        # keep only interested columns
        col_to_keep = ['date_time', 'maxtempC', 'mintempC', 'totalSnow_cm', 'sunHour', 'uvIndex',
                       'moon_illumination', 'moonrise', 'moonset', 'sunrise', 'sunset',
                       'DewPointC', 'FeelsLikeC', 'HeatIndexC', 'WindChillC', 'WindGustKmph',
                       'cloudcover', 'humidity', 'precipMM', 'pressure', 'tempC', 'visibility',
                       'winddirDegree', 'windspeedKmph', 'weather_status']
        df = df[col_to_keep]

        df = df.loc[:, ~df.columns.duplicated()]
        
        df_month = pd.concat([df_month, df])
    return (df_month)


def retrieve_this_location(api_key, location, start_date, end_date, frequency, response_cache_path):
    """
    Retrieve weather data for a location between start_date and end_date
    Default frequency = 1 hr
    Each month costs 1 request (free trial 500 requests/key)

    Parameters
    ----------
    api_key : str
        API key for worldweatheronline.com
    location : str
        location name
    start_date : str
        start date in format 'YYYY-MMM-DD'
    end_date : str
        end date in format 'YYYY-MMM-DD'
    frequency : int
        frequency of data retrieval in hours
    response_cache_path : str
        path to store cached response

    Returns
    -------
    df_hist : pandas DataFrame
        historical weather data for the location
    """
    start_time = datetime.now()

    # create list of first day of month for range between start and end dates non-inclusive (open)
    list_mon_begin = pd.date_range(
        start_date, end_date, freq='MS', inclusive='right')
    
    # convert to Series and add start_date at beginning
    list_mon_begin = pd.concat([pd.Series(pd.to_datetime(
        start_date)), pd.Series(list_mon_begin)], ignore_index=True)

    # create list of month end dates for range between start and end dates non-inclusive (open)
    list_mon_end = pd.date_range(start_date, end_date, freq='M', inclusive='left')

    # convert to Series and add end_date at end
    list_mon_end = pd.concat([pd.Series(list_mon_end), pd.Series(
        pd.to_datetime(end_date))], ignore_index=True)

    # count number of months to be retrieved
    total_months = len(list_mon_begin)

    # initialize df_hist to store return data
    df_hist = pd.DataFrame()

    for m in range(total_months):
        # convert to string
        start_d = str(list_mon_begin[m])[:10]
        end_d = str(list_mon_end[m])[:10]

        # check if data is already cached
        file_path = f'{response_cache_path}/{location}_{start_d}_{end_d}'
        if response_cache_path and os.path.exists(file_path):
            print('Reading cached data for ' + location +
                  ': from ' + start_d + ' to ' + end_d)
            with open(f'{response_cache_path}/{location}_{start_d}_{end_d}', 'r') as f:
                json_data = json.load(f)
        else:
            print('Currently retrieving data for ' + location +
                  ': from ' + start_d + ' to ' + end_d)
            url_page = 'http://api.worldweatheronline.com/premium/v1/past-weather.ashx?key=' + api_key + '&q=' + location + '&format=json&date=' + start_d + '&enddate=' + end_d + '&tp=' + str(
                frequency)
            json_page = urllib.request.urlopen(url_page, timeout=10)
            json_data = json.loads(json_page.read().decode())

        if response_cache_path:
            with open(f'{response_cache_path}/{location}_{start_d}_{end_d}', 'w') as f:
                json.dump(json_data, f)

        data = json_data['data']['weather']
        truth_location = json_data['data']['request'][0]['query']

        # call function to extract json object
        df_this_month = extract_monthly_data(data)
        df_this_month['location'] = location
        df_this_month['truth_location'] = truth_location
        df_hist = pd.concat([df_hist, df_this_month])

        time_elapsed = datetime.now() - start_time
        print('Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed))
    return (df_hist)

# main function to retrive the data by location list
def retrieve_hist_data(api_key, location_list, start_date, end_date, frequency, save_path, location_label=False, export_csv=True,
                       store_df=False, response_cache_path=None):
    """
    Retrieve weather data for a list of locations between start_date and end_date

    Parameters
    ----------
    api_key : str
        API key for worldweatheronline.com
    location_list : list
        list of location names
    start_date : str
        start date in format 'YYYY-MMM-DD'
    end_date : str
        end date in format 'YYYY-MMM-DD'
    frequency : int
        frequency of data retrieval in hours
    save_path : str
        path to store the csv files
    location_label : bool
        whether to add location name as prefix to the colnames
    export_csv : bool
        whether to export the csv files
    store_df : bool
        whether to store the result as object in the work space
    response_cache_path : str
        path to store cached response

    Returns
    -------
    result_list : list
        list of pandas DataFrames of historical weather data for the locations
    """
    result_list = []
    for location in location_list:
        print('\n\nRetrieving weather data for ' + location + '\n\n')

        df_this_city = retrieve_this_location(
            api_key, location, start_date, end_date, frequency, response_cache_path)

        if (location_label == True):
            # add city name as prefix to the colnames
            df_this_city = df_this_city.add_prefix(location + '_')
            df_this_city.columns.values[0] = 'date_time'

        if (export_csv == True):
            df_this_city.to_csv(save_path + location +
                                '.csv', header=True, index=False)
            print('\n\nexport ' + location + ' completed!\n\n')

        if (store_df == True):
            # save result as object in the work space
            result_list = pd.concat([result_list, df_this_city], axis=1)

    return result_list
