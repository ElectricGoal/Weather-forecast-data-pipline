import pandas as pd
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def load_data(df: pd.DataFrame):

    # connection string in mongodb
    # connection_string = "mongodb+srv://grab:admin4321@cluster0.nqxzihj.mongodb.net/?retryWrites=true&w=majority"

    connection_string = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+1.8.2"

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
    collection = db['weather_test_1']

    # convert dataframe to dictionary
    data = df.to_dict('records')

    try:
        # insert data into the collection
        collection.insert_many(data)
        print("Data inserted successfully")
    except Exception as e:
        print(e)
        return