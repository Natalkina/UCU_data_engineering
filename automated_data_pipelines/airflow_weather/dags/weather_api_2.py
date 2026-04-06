from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
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

WIND_SPEED_THRESHOLD = 15.0

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(minutes=5),
    'on_failure_callback': lambda ctx: logging.error(
        f"Task {ctx['task_instance'].task_id} failed. "
        f"Try {ctx['task_instance'].try_number}/{ctx['task_instance'].max_tries + 1}"
    ),
}


def _process_all_weather(city, lat, lon, data_interval_start, **kwargs):
    api_key = Variable.get("WEATHER_API_KEY")
    dt_timestamp = int(data_interval_start.timestamp())

    try:
        url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
        params = {
            "lat": lat,
            "lon": lon,
            "dt": dt_timestamp,
            "appid": api_key,
            "units": "metric"
        }
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        weather_snapshot = data['data'][0]

        return {
            'city': city,
            'timestamp': weather_snapshot.get('dt'),
            'temp': weather_snapshot.get('temp'),
            'humidity': weather_snapshot.get('humidity'),
            'clouds': weather_snapshot.get('clouds'),
            'wind_speed': weather_snapshot.get('wind_speed'),
        }
    except Exception as e:
        logging.error(f"Error for {city}: {e}")
        raise


def _transform_weather(city, ti, **kwargs):
    raw = ti.xcom_pull(task_ids=f"{city}.fetch_{city}")
    return {
        'city': raw['city'],
        'timestamp': raw['timestamp'],
        'temp': raw['temp'],
        'humidity': raw['humidity'],
        'clouds': raw['clouds'],
        'wind_speed': raw['wind_speed'],
    }


def _check_wind(city, ti, **kwargs):
    data = ti.xcom_pull(task_ids=f"{city}.transform_{city}")
    if data['wind_speed'] is not None and data['wind_speed'] > WIND_SPEED_THRESHOLD:
        return f"{city}.alert_and_load_{city}"
    return f"{city}.normal_load_{city}"


def _alert_and_load(city, ti, **kwargs):
    data = ti.xcom_pull(task_ids=f"{city}.transform_{city}")
    logging.warning(f"ALERT: {city} wind speed {data['wind_speed']} m/s exceeds {WIND_SPEED_THRESHOLD} m/s!")
    return data


def _normal_load(city, ti, **kwargs):
    return ti.xcom_pull(task_ids=f"{city}.transform_{city}")


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
        dag_id='weather_with_x_com_v2',
        start_date=datetime(2023, 3, 16),
        schedule='@daily',
        catchup=False,
        default_args=default_args,
        tags=['weather', 'api_3.0_onecall']
) as dag:

    check_api_overall = HttpSensor(
        task_id="check_api_connection",
        http_conn_id="openweather_api",
        endpoint="data/3.0/onecall",
        request_params={
            "lat": 50.58,
            "lon": 30.48,
            "appid": "{{ var.value.WEATHER_API_KEY }}"
        },
        response_check=_check_response,
        poke_interval=60,
        timeout=300,
        mode='reschedule'
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="weather_postgres_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS weather_history (
            city TEXT,
            timestamp INTEGER,
            temp DOUBLE PRECISION,
            humidity INTEGER,
            clouds INTEGER,
            wind_speed DOUBLE PRECISION,
            PRIMARY KEY (city, timestamp)
        );
        """
    )

    inject_data = SQLExecuteQueryOperator(
        task_id="inject_data",
        conn_id="weather_postgres_conn",
        sql="""
        {% set ns = namespace(rows=[]) %}
        {% for city in params.cities %}
            {% set data = ti.xcom_pull(task_ids=city ~ '.normal_load_' ~ city) or ti.xcom_pull(task_ids=city ~ '.alert_and_load_' ~ city) %}
            {% if data %}
                {% set _ = ns.rows.append("('" ~ data.city ~ "', " ~ data.timestamp ~ ", " ~ data.temp ~ ", " ~ data.humidity ~ ", " ~ data.clouds ~ ", " ~ data.wind_speed ~ ")") %}
            {% endif %}
        {% endfor %}
        {% if ns.rows %}
        INSERT INTO weather_history (city, timestamp, temp, humidity, clouds, wind_speed)
        VALUES {{ ns.rows | join(', ') }}
        ON CONFLICT (city, timestamp) DO UPDATE SET
            temp = EXCLUDED.temp,
            humidity = EXCLUDED.humidity,
            clouds = EXCLUDED.clouds,
            wind_speed = EXCLUDED.wind_speed;
        {% else %}
        SELECT 1;
        {% endif %}
        """,
        params={"cities": list(CITIES_COORDS.keys())},
        trigger_rule='none_failed_min_one_success',
    )

    for city, coords in CITIES_COORDS.items():
        with TaskGroup(group_id=city) as tg:
            fetch_data = PythonOperator(
                task_id=f"fetch_{city}",
                python_callable=_process_all_weather,
                op_kwargs={'city': city, 'lat': coords['lat'], 'lon': coords['lon']},
            )

            transform = PythonOperator(
                task_id=f"transform_{city}",
                python_callable=_transform_weather,
                op_kwargs={'city': city},
            )

            branch = BranchPythonOperator(
                task_id=f"check_wind_{city}",
                python_callable=_check_wind,
                op_kwargs={'city': city},
            )

            normal = PythonOperator(
                task_id=f"normal_load_{city}",
                python_callable=_normal_load,
                op_kwargs={'city': city},
            )

            alert = PythonOperator(
                task_id=f"alert_and_load_{city}",
                python_callable=_alert_and_load,
                op_kwargs={'city': city},
            )

            fetch_data >> transform >> branch >> [normal, alert]

        [check_api_overall, create_table] >> tg >> inject_data
