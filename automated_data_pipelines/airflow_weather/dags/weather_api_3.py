# dag factory for all cities

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import json
import os

STORAGE_PATH = "/tmp/weather_pipeline"
os.makedirs(STORAGE_PATH, exist_ok=True)

CITIES_COORDS = {
    'Lviv': {'lat': 49.8397, 'lon': 24.0297},
    'Kyiv': {'lat': 50.4501, 'lon': 30.5234},
    'Kharkiv': {'lat': 49.9935, 'lon': 36.2304},
    'Odesa': {'lat': 46.4825, 'lon': 30.7233},
    'Vyshhorod': {'lat': 50.5841, 'lon': 30.4894},
}

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


def _transform_weather(**kwargs):
    city = kwargs['params']['city']
    ds = _get_ds(**kwargs)
    raw_path = os.path.join(STORAGE_PATH, f"{city}_raw_{ds}.json")
    out_path = os.path.join(STORAGE_PATH, f"{city}_transformed_{ds}.json")

    if os.path.exists(out_path):
        logging.info(f"Skipping transform for {city} - already completed")
        with open(out_path) as f:
            return json.load(f)

    if not os.path.exists(raw_path):
        raise ValueError(f"No raw data for {city} on {ds}")

    with open(raw_path) as f:
        data = json.load(f)
    with open(out_path, 'w') as f:
        json.dump(data, f)
    return data


def _quality_check(**kwargs):
    city = kwargs['params']['city']
    ds = _get_ds(**kwargs)
    path = os.path.join(STORAGE_PATH, f"{city}_transformed_{ds}.json")
    if not os.path.exists(path):
        raise ValueError(f"No transformed data for {city}")

    with open(path) as f:
        data = json.load(f)
    errors = []
    if data.get('temp') is None or not (-80 <= data['temp'] <= 60):
        errors.append(f"Invalid temp: {data.get('temp')}")
    if data.get('humidity') is not None and not (0 <= data['humidity'] <= 100):
        errors.append(f"Invalid humidity: {data.get('humidity')}")
    if data.get('wind_speed') is not None and data['wind_speed'] < 0:
        errors.append(f"Invalid wind_speed: {data['wind_speed']}")
    if errors:
        raise ValueError(f"Quality check failed for {city}: {'; '.join(errors)}")
    logging.info(f"Quality check passed for {city}")


def _check_wind(**kwargs):
    city = kwargs['params']['city']
    ds = _get_ds(**kwargs)
    threshold = kwargs['params'].get('wind_speed_threshold', 15.0)
    path = os.path.join(STORAGE_PATH, f"{city}_transformed_{ds}.json")
    with open(path) as f:
        data = json.load(f)
    if data['wind_speed'] is not None and data['wind_speed'] > threshold:
        return "alert"
    return "load"


def _alert(**kwargs):
    city = kwargs['params']['city']
    ds = _get_ds(**kwargs)
    threshold = kwargs['params'].get('wind_speed_threshold', 15.0)
    path = os.path.join(STORAGE_PATH, f"{city}_transformed_{ds}.json")
    with open(path) as f:
        data = json.load(f)
    logging.warning(f"ALERT: {city} wind={data['wind_speed']} m/s > {threshold} m/s!")
    return data


def _load_data(**kwargs):
    city = kwargs['params']['city']
    ds = _get_ds(**kwargs)
    path = os.path.join(STORAGE_PATH, f"{city}_transformed_{ds}.json")
    with open(path) as f:
        return json.load(f)


def _check_response(response):
    if response.status_code == 200:
        return True
    logging.error(f"API error {response.status_code}: {response.text}")
    return False


def create_city_dag(city, coords):
    dag = DAG(
        dag_id=f"weather_pipeline_{city}",
        start_date=datetime(2023, 3, 16),
        schedule='@daily',
        catchup=False,
        default_args=default_args,
        params={"city": city, "lat": coords['lat'], "lon": coords['lon'], "wind_speed_threshold": 15.0},
        tags=['weather', 'factory', city],
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

        create_table = SQLExecuteQueryOperator(
            task_id="create_table",
            conn_id="weather_postgres_conn",
            sql="""
            CREATE TABLE IF NOT EXISTS weather_history (
                city TEXT, timestamp INTEGER, temp DOUBLE PRECISION,
                humidity INTEGER, clouds INTEGER, wind_speed DOUBLE PRECISION,
                PRIMARY KEY (city, timestamp));
            """,
        )

        extract = PythonOperator(task_id="extract", python_callable=_extract_weather)
        transform = PythonOperator(task_id="transform", python_callable=_transform_weather)
        quality = PythonOperator(task_id="quality_check", python_callable=_quality_check)
        branch = BranchPythonOperator(task_id="check_wind", python_callable=_check_wind)
        load = PythonOperator(task_id="load", python_callable=_load_data)
        alert = PythonOperator(task_id="alert", python_callable=_alert)

        inject = SQLExecuteQueryOperator(
            task_id="inject_data",
            conn_id="weather_postgres_conn",
            sql="""
            INSERT INTO weather_history (city, timestamp, temp, humidity, clouds, wind_speed)
            VALUES (
                '{{ params.city }}',
                {{ ti.xcom_pull(task_ids='load')['timestamp'] if ti.xcom_pull(task_ids='load') else ti.xcom_pull(task_ids='alert')['timestamp'] }},
                {{ ti.xcom_pull(task_ids='load')['temp'] if ti.xcom_pull(task_ids='load') else ti.xcom_pull(task_ids='alert')['temp'] }},
                {{ ti.xcom_pull(task_ids='load')['humidity'] if ti.xcom_pull(task_ids='load') else ti.xcom_pull(task_ids='alert')['humidity'] }},
                {{ ti.xcom_pull(task_ids='load')['clouds'] if ti.xcom_pull(task_ids='load') else ti.xcom_pull(task_ids='alert')['clouds'] }},
                {{ ti.xcom_pull(task_ids='load')['wind_speed'] if ti.xcom_pull(task_ids='load') else ti.xcom_pull(task_ids='alert')['wind_speed'] }}
            ) ON CONFLICT (city, timestamp) DO UPDATE SET
                temp=EXCLUDED.temp, humidity=EXCLUDED.humidity,
                clouds=EXCLUDED.clouds, wind_speed=EXCLUDED.wind_speed;
            """,
            trigger_rule='none_failed_min_one_success',
        )

        [check_api, create_table] >> extract >> transform >> quality >> branch >> [load, alert] >> inject

    return dag


for city, coords in CITIES_COORDS.items():
    globals()[f"weather_pipeline_{city}"] = create_city_dag(city, coords)
