# 🌤️ Automated Weather ETL Pipeline

A production-grade, containerized data pipeline that extracts real-time weather data from the OpenWeatherMap API, transforms the JSON payload, and loads it into a PostgreSQL Data Warehouse. 

This project is fully orchestrated using **Apache Airflow** and focuses on data reliability and idempotency.

## 🏗️ Architecture & Tech Stack
* **Language:** Python 3.9
* **Orchestration:** Apache Airflow
* **Storage:** PostgreSQL
* **Infrastructure:** Docker & Docker Compose
* **Libraries:** `pandas`, `sqlalchemy`, `requests`

## 🚀 Key Engineering Features
* **Idempotency (UPSERTS):** The pipeline utilizes `ON CONFLICT DO UPDATE` SQL logic based on a composite Primary Key (City + Logical Date). Running the pipeline multiple times will never result in duplicate data.
* **Reliability (Retries):** Configured Airflow's `@dag` decorator with automated retries and exponential backoff to handle temporary API outages.
* **Security:** API keys and database credentials are fully abstracted from the codebase using `.env` files and Docker environment variables.

## 🛠️ How to Run Locally

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/weather-etl-pipeline.git](https://github.com/YOUR_USERNAME/weather-etl-pipeline.git)
   cd weather-etl-pipeline
   ```

2. **Setup Environment Variables:**
   Create a `.env` file in the root directory and add your API key:
   ```bash
   WEATHER_API_KEY=your_openweathermap_api_key_here
   DB_USER=admin
   DB_PASSWORD=password
   DB_HOST=db
   DB_PORT=5432
   DB_NAME=weather_db
   ```

3. **Start the Pipeline:**
   Run the following command in your terminal to build the Docker images and start the Airflow environment:
   ```bash
   docker-compose up -d
   ```

4. **Monitor in Airflow UI:**
   Open your browser and navigate to [http://localhost:8080](http://localhost:8080).
   * Log in with username admin (check your terminal logs for the auto-generated password).
   * Navigate to "Grid View" (or "Tree View") to see the `weather_etl_pipeline_v2` DAG.
   * Click the "Play" button (Manual Trigger) to start the ETL job.

5. **Verify Data:**
   Check the data in the PostgreSQL container:
   ```bash
   docker exec -it weather_postgres psql -U admin -d weather_db -c "SELECT * FROM weather_production;"
   ```

6. **Teardown:**
   To stop the services:
   ```bash
   docker-compose down
   ```