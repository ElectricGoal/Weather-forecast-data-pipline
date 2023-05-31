import pandas as pd
from prophet import Prophet

def train_model(df: pd.DataFrame, future_hours: int):
    print("Train model for temperature and precipitation: ")

    # create a list of location ids and location names
    locations = df[['location_id', 'location']].drop_duplicates().to_dict('records')

    result = pd.DataFrame(columns=['date_time', 'tempC', 'rainning'])

    for location in locations:
        print("Process location id: ", location["location_id"])
        temp = df[df["location_id"] == location["location_id"]]

        # create dataframe for precipitation
        precipitation = temp.reset_index()[["date_time", "precipMM"]].rename(
            {"date_time": "ds", "precipMM": "y"}, axis=1)

        # create dataframe for temperature
        temperature = temp.reset_index()[["date_time", "tempC"]].rename(
            {"date_time": "ds", "tempC": "y"}, axis=1)

        # create model
        precip_model = Prophet(changepoint_prior_scale=0.01)
        temp_model = Prophet()

        # fit the model
        precip_model.fit(precipitation)
        temp_model.fit(temperature)

        # make prediction
        future_precip = temp_model.make_future_dataframe(periods=future_hours, freq='H')
        forecast_precip = temp_model.predict(future_precip)

        future_temp = temp_model.make_future_dataframe(periods=future_hours, freq='H')
        forecast_temp = temp_model.predict(future_temp)

        # get the last future_hours
        forecast_precip = forecast_precip.tail(future_hours)
        forecast_temp = forecast_temp.tail(future_hours)

        # rename the columns
        forecast_precip = forecast_precip.rename({"ds": "date_time", "yhat": "rainning"}, axis=1)
        forecast_temp = forecast_temp.rename({"ds": "date_time", "yhat": "tempC"}, axis=1)

        # classify the precipitation forecast
        forecast_precip['rainning'] = forecast_precip['rainning'].apply(lambda x: 1 if x > 0.5 else 0)

        # concat two attributes
        forecast = pd.concat([forecast_temp[["date_time", "tempC"]], forecast_precip["rainning"]], axis=1)

        # round the temperature
        forecast["tempC"] = forecast["tempC"].apply(lambda x: round(x))

        # add location id
        forecast["location_id"] = location["location_id"]

        # add location name
        forecast["location"] = location["location"]

        # append to result
        result = pd.concat([result, forecast], axis=0)

    return result

