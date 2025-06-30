from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum
import requests
import json

### Configuration
Latitude = 38.80182
Longitude = 72.66183
POSTGRES_CONN_ID = 'postgres_default'
api_conn_id = 'open_mateo_api'



default_args = {
    'owner' : 'owner',
    'start_date' : pendulum.today("UTC").subtract(days=1)
}

with DAG(
    dag_id = 'weather-etl-pipeline',
    default_args = default_args,
    schedule = '@daily',
    catchup = False
 ) as dags:
    
    @task()
    def get_weather_data():
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast?latitude=38.80182&longitude=72.66183&current_weather=true"
        )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data {response.status_code}")
    @task()

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']

        transformed_data = {
        'latitude': weather_data['latitude'],
        'longitude': weather_data['longitude'],
        'temperature': current_weather['temperature'],
        'windspeed': current_weather['windspeed'],
        'winddirection': current_weather['winddirection'],
        'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
          """  
        )

        cursor.execute("""
        INSERT INTO weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    ## DAG Worflow- ETL Pipeline
    weather_data= get_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)