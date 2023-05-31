import pandas as pd
from transform import transform_data
from load import load_data_to_mongo

df = pd.DataFrame()

df = transform_data()

# load the data into a mongo database
load_data_to_mongo(df)