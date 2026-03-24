from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from datetime import datetime
import requests
import logging
import json

CITIES_COORDS = {
    'Lviv': {'lat': 49.8397, 'lon': 24.0297},
    'Kyiv': {'lat': 50.4501, 'lon': 30.5234},
    'Kharkiv': {'lat': 49.9935, 'lon': 36.2304},
    'Odesa': {'lat': 46.4825, 'lon': 30.7233},
    'Vyshhorod': {'lat': 50.5841, 'lon': 30.4894}
}


def _process_all_weather(data_interval_start, **kwargs):
    api_key = Variable.get("WEATHER_API_KEY")
    dt_timestamp = int(data_interval_start.timestamp())

    results = []
    for city, coords in CITIES_COORDS.items():
        try:
            url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"

            params = {
                "lat": coords['lat'],
                "lon": coords['lon'],
                "dt": dt_timestamp,
                "appid": api_key,
                "units": "metric"
            }

            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            weather_snapshot = data['data'][0]

            results.append((
                city,
                weather_snapshot.get('dt'),
                weather_snapshot.get('temp'),
                weather_snapshot.get('humidity'),
                weather_snapshot.get('clouds'),
                weather_snapshot.get('wind_speed')
            ))

        except Exception as e:
            logging.error(f"Error for {city} on {data_interval_start}: {e}")
            continue

    return results


def _check_response(response):
    if response.status_code == 200:
        logging.info("API Connection OK: 200 OK")
        return True
    if response.status_code == 401:
        logging.error("Error 401: Check API KEY")
    elif response.status_code == 404:
        logging.error("Error 404: Check endpoint")
    elif response.status_code == 429:
        logging.warning("Error 429: Rate Limit")
    else:
        logging.error(f"Error {response.status_code}: {response.text}")
    return False


with DAG(
        dag_id='weather_pipeline_v2',
        start_date=datetime(2023, 3, 16),
        schedule='@daily',
        catchup=True,
        tags=['weather', 'api_3.0_onecall']
) as dag:

    check_api_overall = HttpSensor(
        task_id="check_api_connection",
        http_conn_id="openweather_api",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": 50.58,
            "lon": 30.48,
            "appid": Variable.get("WEATHER_API_KEY")
        },
        response_check=_check_response,
        poke_interval=60,
        timeout=300,
        mode='reschedule'
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="weather_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_history (
            city TEXT,
            timestamp INTEGER,
            temp REAL,
            humidity INTEGER,
            clouds INTEGER,
            wind_speed REAL,
            PRIMARY KEY (city, timestamp)
        );
        """
    )

    fetch_data = PythonOperator(
        task_id="fetch_cities",
        python_callable=_process_all_weather
    )

    inject_data = SQLExecuteQueryOperator(
        task_id="inject_data",
        conn_id="weather_conn",
        sql="""
        INSERT OR REPLACE INTO weather_history (city, timestamp, temp, humidity, clouds, wind_speed)
        VALUES 
        {% for row in ti.xcom_pull(task_ids='fetch_cities') %}
        ('{{ row[0] }}', {{ row[1] }}, {{ row[2] }}, {{ row[3] }}, {{ row[4] }}, {{ row[5] }})
        {% if not loop.last %},{% endif %}
        {% endfor %};
        """,
    )


    [check_api_overall, create_table] >> fetch_data >> inject_data