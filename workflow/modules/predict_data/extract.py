import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import yaml

# config variable
config = {}

# import yaml file
with open('/home/godd/airflow/dags/workflow/modules/config.yml', 'r') as f:
    config = yaml.safe_load(f)

# function to extract data from the API
def extract_data():
    # connection string in mongodb
    connection_string = config['connection_string']

    # Create a new client and connect to the server
    client = MongoClient(connection_string, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        return

    db = client['weatherDB']

    # select the collection
    collection = db['weather']

    # get the data from the collection
    data = collection.find()

    # convert the data to dataframe
    df = pd.DataFrame(list(data))

    # drop the _id column
    df.drop('_id', axis=1, inplace=True)

    return df

df = extract_data()

df.to_csv('weather.csv', index=False)

print(df['location_id'].unique())