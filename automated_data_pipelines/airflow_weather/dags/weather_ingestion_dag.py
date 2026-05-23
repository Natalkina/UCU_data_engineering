# ingestion dag for Lviv with dataset trigger
from airflow import DAG, Dataset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import json
import os

STORAGE_PATH = "/tmp/weather_pipeline"
os.makedirs(STORAGE_PATH, exist_ok=True)

LVIV_RAW_DATASET = Dataset(f"file://{STORAGE_PATH}/Lviv_raw.json")

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(minutes=5),
}


def _get_ds(**kwargs):
    if 'ds' in kwargs:
        return kwargs['ds']
    logical_date = kwargs.get('logical_date') or kwargs.get('data_interval_start')
    if logical_date:
        return logical_date.strftime('%Y-%m-%d')
    return datetime.utcnow().strftime('%Y-%m-%d')


def _extract_weather(**kwargs):
    city = kwargs['params']['city']
    lat = kwargs['params']['lat']
    lon = kwargs['params']['lon']
    ds = _get_ds(**kwargs)
    path = os.path.join(STORAGE_PATH, f"{city}_raw_{ds}.json")

    if os.path.exists(path):
        logging.info(f"Skipping extract for {city} - already completed")
        return

    api_key = Variable.get("WEATHER_API_KEY")
    logical_date = kwargs.get('logical_date') or kwargs.get('data_interval_start')
    dt_timestamp = int(logical_date.timestamp()) if logical_date else int(datetime.utcnow().timestamp())
    response = requests.get(
        "https://api.openweathermap.org/data/3.0/onecall/timemachine",
        params={"lat": lat, "lon": lon, "dt": dt_timestamp, "appid": api_key, "units": "metric"},
        timeout=15,
    )
    response.raise_for_status()
    raw = response.json()['data'][0]
    result = {
        'city': city,
        'timestamp': raw.get('dt'),
        'temp': raw.get('temp'),
        'humidity': raw.get('humidity'),
        'clouds': raw.get('clouds'),
        'wind_speed': raw.get('wind_speed'),
    }
    with open(path, 'w') as f:
        json.dump(result, f)
    logging.info(f"Stored raw data for {city} at {path}")


def _check_response(response):
    if response.status_code == 200:
        return True
    logging.error(f"API error {response.status_code}: {response.text}")
    return False


dag = DAG(
    dag_id="weather_ingestion_dag",
    start_date=datetime(2023, 3, 16),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    params={"city": "Lviv", "lat": 49.8397, "lon": 24.0297},
    tags=['weather', 'ingestion', 'Lviv'],
)

with dag:
    check_api = HttpSensor(
        task_id="check_api",
        http_conn_id="openweather_api",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": "{{ params.lat }}",
            "lon": "{{ params.lon }}",
            "appid": "{{ var.value.WEATHER_API_KEY }}",
        },
        response_check=_check_response,
        poke_interval=60,
        timeout=300,
        mode='reschedule',
    )

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=_extract_weather,
        outlets=[LVIV_RAW_DATASET],
    )

    check_api >> extract
