# Weather ETL Pipeline

This Apache Airflow pipeline fetches real-time weather data from the Open-Meteo API, transforms it, and loads it into a PostgreSQL database.

## Features

- API integration with Open-Meteo
- Data extraction, transformation, and loading (ETL)
- DAG scheduled to run daily
- Uses PostgreSQL for storage

## Tools & Technologies

- Apache Airflow
- PostgreSQL
- Python (requests, pendulum)
- Docker (via Astro CLI)

## DAG Workflow

1. **get_weather_data**: Fetches data from Open-Meteo API.
2. **transform_weather_data**: Cleans and formats the response.
3. **load_weather_data**: Writes the data into a PostgreSQL table.

## Sample API Used

```https://api.open-meteo.com/v1/forecast?latitude=38.80182&longitude=72.66183&current_weather=true```
