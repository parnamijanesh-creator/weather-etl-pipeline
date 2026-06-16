import os
import requests
import pandas as pd
from sqlalchemy import create_engine
import datetime

# --- Configuration ---
API_KEY = os.environ.get("WEATHER_API_KEY")
CITY = "Delhi"
# We add &units=metric to automatically get Celsius
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# This matches the credentials we set in our docker-compose.yml
DB_URI = "postgresql://admin:password@db:5432/weather_db"

def extract():
    """Hits the OpenWeatherMap API and returns the JSON payload."""
    print(f"Extracting weather data for {CITY}...")
    response = requests.get(URL)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Extraction failed: {response.status_code} - {response.text}")

def transform(raw_data):
    """Parses the nested JSON and flattens it into a Pandas DataFrame."""
    print("Transforming data...")
    
    # We map the raw JSON fields to our clean target schema
    weather_dict = {
        "city_name": raw_data["name"],
        "temperature_celsius": raw_data["main"]["temp"],
        "humidity_percent": raw_data["main"]["humidity"],
        "weather_condition": raw_data["weather"][0]["main"],
        "extraction_timestamp": datetime.datetime.now()
    }
    
    # Convert the dictionary into a Pandas DataFrame
    df = pd.DataFrame([weather_dict])
    return df

def load(df):
    """Connects to Postgres and loads the DataFrame into a table."""
    print("Loading data into PostgreSQL...")
    engine = create_engine(DB_URI)
    
    # Write the data to a table called 'current_weather'
    # if_exists="append" means it will add a new row every time we run the script
    df.to_sql("current_weather", engine, if_exists="append", index=False)
    print("Data loaded successfully!")

# --- The Pipeline Execution ---
if __name__ == "__main__":
    try:
        raw_json = extract()
        clean_dataframe = transform(raw_json)
        load(clean_dataframe)
    except Exception as e:
        print(f"Pipeline Error: {e}")