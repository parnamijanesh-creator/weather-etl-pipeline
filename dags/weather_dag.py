from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import os
from sqlalchemy import create_engine, text

# Default arguments including our Reliability fix (Retries)
default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='weather_etl_pipeline_v2',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['portfolio', 'idempotent']
)
def weather_etl():

    @task
    def extract_weather_data():
        """Hits the OpenWeatherMap API and returns JSON."""
        API_KEY = os.getenv("WEATHER_API_KEY")
        CITY = "Delhi"
        URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        
        response = requests.get(URL)
        response.raise_for_status() 
        return response.json()

    @task
    def transform_weather_data(raw_data: dict):
        """Parses the JSON and extracts the Logical Date."""
        # FIX 1: We stop using datetime.now(). 
        # The API provides 'dt', which is the exact Unix timestamp of the weather calculation.
        import datetime
        logical_date = datetime.datetime.utcfromtimestamp(raw_data["dt"]).isoformat()
        
        weather_dict = {
            "city_name": raw_data["name"],
            "temperature_celsius": raw_data["main"]["temp"],
            "humidity_percent": raw_data["main"]["humidity"],
            "weather_condition": raw_data["weather"][0]["main"],
            "logical_date": logical_date
        }
        return weather_dict

    @task
    def load_weather_data(clean_data: dict):
        """Connects to Postgres and performs an UPSERT."""
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv("DB_PORT")
        DB_NAME = os.getenv("DB_NAME")
        
        DB_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DB_URI)
        
        # FIX 2: Create a table with a strict PRIMARY KEY
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_production (
            city_name VARCHAR(50),
            temperature_celsius FLOAT,
            humidity_percent INT,
            weather_condition VARCHAR(50),
            logical_date TIMESTAMP,
            PRIMARY KEY (city_name, logical_date)
        );
        """
        
        # FIX 3: The UPSERT statement (ON CONFLICT DO UPDATE)
        upsert_sql = """
        INSERT INTO weather_production (city_name, temperature_celsius, humidity_percent, weather_condition, logical_date)
        VALUES (:city_name, :temperature_celsius, :humidity_percent, :weather_condition, :logical_date)
        ON CONFLICT (city_name, logical_date) 
        DO UPDATE SET
            temperature_celsius = EXCLUDED.temperature_celsius,
            humidity_percent = EXCLUDED.humidity_percent,
            weather_condition = EXCLUDED.weather_condition;
        """
        
        # Execute the raw SQL using SQLAlchemy
        with engine.begin() as conn:
            conn.execute(text(create_table_sql))
            conn.execute(text(upsert_sql), clean_data)
            
        print(f"Successfully UPSERTED data for {clean_data['city_name']} at {clean_data['logical_date']}")

    # DAG Dependencies
    raw_data = extract_weather_data()
    clean_data = transform_weather_data(raw_data)
    load_weather_data(clean_data)

# Instantiate the DAG
weather_etl_dag = weather_etl()