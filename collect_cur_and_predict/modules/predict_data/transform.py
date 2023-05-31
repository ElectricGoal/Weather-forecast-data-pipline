import pandas as pd
from modules.predict_data.train_model import train_model

def transform_data(df: pd.DataFrame):
    # select the columns
    df = df[["date_time", "location", "location_id", "tempC", "precipMM"]]

    df["date_time"] = pd.to_datetime(df["date_time"])

    # classify the precipitation
    df["precipMM"] = df["precipMM"].apply(lambda x: 1 if x > 0 else 0)

    return train_model(df, 12 * 24)