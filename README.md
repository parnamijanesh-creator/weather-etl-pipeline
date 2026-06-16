# Weather API ETL Pipeline

A containerized data pipeline that extracts real-time weather data from the OpenWeatherMap API, transforms it using Pandas, and loads it into a PostgreSQL database.

## Architecture
* **Language:** Python
* **Database:** PostgreSQL
* **Infrastructure:** Docker & Docker Compose

## How to Run
1. Clone this repository.
2. Create a `.env` file in the root directory and add your OpenWeatherMap API key: `WEATHER_API_KEY=your_key_here`
3. Run `docker-compose up --build`
4. To check the data in the database, use the command: `docker exec -it weather_postgres psql -U admin -d weather_db -c "SELECT * FROM current_weather;"`