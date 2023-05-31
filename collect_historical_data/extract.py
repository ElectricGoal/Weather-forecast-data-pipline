from helper import retrieve_hist_data
import json
import yaml

# config variable
config = {}

# import yaml file
with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

# initialize variables
frequency = config['frequency']
start_date = config['start_date']
end_date = config['end_date']
API_KEY = config['API_KEY']
save_path = config['save_path']
location_list = []

with open('locations_extract.json', 'r') as f:
    locations = json.load(f)['locations']
    for location in locations:
        # replace spaces with '%20' in location names
        location_name = location['name'].replace(' ', '%20')
        location_list.append(location_name)

# location_list = ['Hai%20Phong', 'Ha%20Nam', 'Hanoi', 'Ha%20Tinh', 'Vi%20Thanh', 'Hoa%20Binh', 'Ho%20Chi%20Minh', 'Hung%20Yen', 'Khanh%20Hoa', 'Rach%20Gia', 'Kon%20Tum', 'Lai%20Chau', 'Da%20Lat', 'Lang%20Son', 'Lao%20Cai', 'Long%20An', 'Nam%20Dinh', 'Vinh', 'Ninh%20Binh', 'Phan%20Rang', 'Phu%20Tho', 'Phu%20Yen', 'Dong%20Hoi', 'Quang%20Nam', 'Quang%20Ngai', 'Quang%20Ninh', 'Quang%20Tri', 'Soc%20Trang', 'Son%20La', 'Tay%20Ninh', 'Thai%20Binh', 'Thai%20Nguyen', 'Thanh%20Hoa', 'Thua%20Thien%20-%20Hue', 'My%20Tho', 'Tra%20Vinh', 'Tuyen%20Quang', 'Vinh%20Long', 'Vinh%20Phuc', 'Yen%20Bai']

# get the historical data
hist_weather_data = retrieve_hist_data(API_KEY,
                                location_list,
                                start_date,
                                end_date,
                                frequency,
                                save_path,
                                location_label = False,
                                export_csv = True,
                                store_df = False)
