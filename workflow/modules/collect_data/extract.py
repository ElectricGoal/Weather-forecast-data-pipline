import pandas as pd
import requests
import json
from datetime import datetime
import yaml

# config variable
config = {}

# import yaml file
with open('/home/godd/airflow/dags/workflow/modules/config.yml', 'r') as f:
    config = yaml.safe_load(f)

# function to extract data from the API
def extract_data():
    # api key
    API_KEY = config['API_KEY']

    # list of locations
    location_list = []

    # dataframe to store the data
    df = pd.DataFrame()

    time_index = 0

    # path to the locations file
    locations_extract_path = config['locations_extract_path']

    # path to the checkpoint file
    checkpoint_path = config['checkpoint_path']

    # read locations file
    with open(locations_extract_path, 'r') as f:
        locations = json.load(f)['locations']
        for location in locations:
            # replace spaces with '%20' in location names
            location_name = location['name'].replace(' ', '%20')
            location_list.append(location_name)

    # read checkpoint file
    with open(checkpoint_path, 'r') as f:
        checkpoint = json.load(f)
        time_index = checkpoint['time_index']

    # get the data for each location
    for location in location_list:
        url_page = config['url'] + 'key=' + API_KEY + '&q=' + location + '&format=json'
        
        # get the response
        response = requests.get(url_page)

        # check if the response is successful
        if response.status_code == 200:
            data = response.json()['data']
            current_condition = data['current_condition'][0]
            weather = data['weather'][0]
            hourlies = data['weather'][0]['hourly'][time_index]

            new_row = {
                'date_time': datetime.now(),
                'maxtempC': weather['maxtempC'],
                'mintempC': weather['mintempC'],
                'totalSnow_cm': weather['totalSnow_cm'],
                'sunHour': weather['sunHour'],
                'uvIndex': current_condition['uvIndex'],
                'moon_illumination': weather['astronomy'][0]['moon_illumination'],
                'moonrise': weather['astronomy'][0]['moonrise'],
                'moonset': weather['astronomy'][0]['moonset'],
                'sunrise': weather['astronomy'][0]['sunrise'],
                'sunset': weather['astronomy'][0]['sunset'],
                'DewPointC': hourlies['DewPointC'],
                'FeelsLikeC': current_condition['FeelsLikeC'],
                'HeatIndexC': hourlies['HeatIndexC'],
                'WindChillC': hourlies['WindChillC'],
                'WindGustKmph': hourlies['WindGustKmph'],
                'cloudcover': current_condition['cloudcover'],
                'humidity': current_condition['humidity'],
                'precipMM': current_condition['precipMM'],
                'pressure': current_condition['pressure'],
                'tempC': current_condition['temp_C'],
                'visibility': current_condition['visibility'],
                'winddirDegree': current_condition['winddirDegree'],
                'windspeedKmph': current_condition['windspeedKmph'],
                'weather_status': current_condition['weatherDesc'][0]['value'],
                'location': location.replace('%20', ' ')
            }
            
            # append the new row to the dataframe
            df = pd.concat([df, pd.DataFrame(new_row, index=[0])])
                
        else:
            # throw an error
            raise Exception('Error while fetching data from the API')
    
    # write checkpoint file
    with open(checkpoint_path, 'w') as f:
        checkpoint['time_index'] = (time_index + 1) % 24
        checkpoint['date_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        json.dump(checkpoint, f)

    return df
