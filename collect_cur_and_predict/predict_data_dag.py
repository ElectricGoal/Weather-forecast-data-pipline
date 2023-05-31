import pandas as pd
from datetime import datetime, timedelta
import pendulum
import sys

sys.path.append("/home/godd/airflow/dags/workflow")

from modules.predict_data.extract import extract_data
from modules.predict_data.transform import transform_data
from modules.predict_data.load import load_data

from airflow.decorators import dag, task

default_args = {
    'owner': 'minhtu',
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='predict_weather_data_dag',
    default_args=default_args,
    start_date = pendulum.datetime(2023, 5, 15, 13, 30, tz='Asia/Ho_Chi_Minh'),
    # schedule_interval='0 0 * * 0',
    schedule_interval='@once',
    tags=['predict-weather-data'])

def data_etl():

    @task()
    def extract():
        return extract_data()
    
    @task()
    def transform(df: pd.DataFrame):
        return transform_data(df)
    
    @task()
    def load(df):
        load_data(df)

    df = pd.DataFrame()
    df = extract()
    df = transform(df)
    load(df)

data_etl()