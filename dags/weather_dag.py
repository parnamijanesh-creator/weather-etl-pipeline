from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import os

# Define the scheduling and retry logic for the whole pipeline
default_args = {
    'owner': 'data_engineer',
    'retries': 3, # The Reliability fix we talked about!
    'retry_delay': timedelta(minutes=1),
}

# The @dag decorator tells Airflow this is a workflow
# schedule_interval="@hourly" means it will run automatically every hour
@dag(
    dag_id='weather_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['portfolio']
)
def weather_etl():

    @task
    def extract_weather_data():
        """Hits the OpenWeatherMap API and returns JSON."""
        API_KEY = os.getenv("WEATHER_API_KEY")
        CITY = "Delhi"
        URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        
        response = requests.get(URL)
        response.raise_for_status() # Automatically raises an error if the API is down
        return response.json()

    @task
    def transform_weather_data(raw_data: dict):
        """Parses the nested JSON into a dictionary."""
        weather_dict = {
            "city_name": raw_data["name"],
            "temperature_celsius": raw_data["main"]["temp"],
            "humidity_percent": raw_data["main"]["humidity"],
            "weather_condition": raw_data["weather"][0]["main"],
            "extraction_timestamp": datetime.now().isoformat()
        }
        return weather_dict

    @task
    def load_weather_data(clean_data: dict):
        """Connects to Postgres and loads the data."""
        # Convert the dictionary back to a DataFrame for easy SQL loading
        df = pd.DataFrame([clean_data])
        
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        
        DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DB_URI)
        
        df.to_sql("current_weather", engine, if_exists="append", index=False)
        print("Successfully loaded to Postgres!")

    # Define the execution dependencies
    # The output of extract is passed to transform, and transform to load
    raw_data = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)

# Instantiate the DAG
weather_etl_dag = weather_etl()