import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
from datetime import datetime

def load_data_to_mongo(df: pd.DataFrame):
    # load the data into a mongo database

    # connection string in mongodb
    connection_string = "mongodb+srv://grab:admin4321@cluster0.nqxzihj.mongodb.net/?retryWrites=true&w=majority"

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

    # convert dataframe to dictionary
    data = df.to_dict('records')

    # insert data into the collection
    collection.insert_many(data)


