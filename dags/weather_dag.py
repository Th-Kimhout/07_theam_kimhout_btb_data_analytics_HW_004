from datetime import timedelta
from io import StringIO
import json
import pandas as pd
import pendulum

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from dotenv import load_dotenv
import os

load_dotenv()

# DAG definition
@dag(
    dag_id="weather_etl",
    # Run daily from today onwards, no backfill
    start_date=pendulum.today(tz="Asia/Phnom_Penh").subtract(days=1),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
)
def weather_dag():
    api_key = os.getenv("OPEN_WEATHER_API_KEY")
    bucket_name = os.getenv("GCS_BUCKET_NAME")

    """ETL pipeline for fetching weather data and uploading to GCS."""

    # 1. Wait until the Weather API is responsive
    is_weather_api_ready = HttpSensor(
        task_id="is_weather_api_ready",
        http_conn_id="open_weather_api_conn",
        endpoint="data/2.5/weather",
        request_params={
            "lat": "11.5564",
            "lon": "104.9282",
            "appid": "190204f2d3d9f381d04c875d3f5ccf80",
        },
        poke_interval=60,
        timeout=300,
    )

    # 2. Extract weather data from the API
    extract_weather_data = HttpOperator(
        task_id="extract_weather_data",
        http_conn_id="open_weather_api_conn",
        endpoint="data/2.5/weather", method="GET",
        data={
            "lat": "11.5564",
            "lon": "104.9282",
            "appid": api_key
        },
        response_check=lambda response: response.status_code == 200,
        log_response=True, )

    # 3. Transform data and upload directly to GCS
    @task
    def transform_and_load(weather_data: str):
        """Transform API JSON into CSV and upload to GCS."""
        data = json.loads(weather_data)

        # Convert times into Phnom Penh timezone
        tz = "Asia/Phnom_Penh"
        sunrise = pendulum.from_timestamp(data["sys"]["sunrise"], tz="UTC").in_timezone(tz)
        sunset = pendulum.from_timestamp(data["sys"]["sunset"], tz="UTC").in_timezone(tz)

        processed = {
            "city": data["name"],
            "description": data["weather"][0]["description"],
            "Temperature (C)": round(data["main"]["temp"] - 273.15, 2),
            "Feel Like (C)": round(data["main"]["feels_like"] - 273.15, 2),
            "Minimum Temperature (C)": round(data["main"]["temp_min"] - 273.15, 2),
            "Max Temperature (C)": round(data["main"]["temp_max"] - 273.15, 2),
            "Pressure": data["main"]["pressure"],
            "Humidity": data["main"]["humidity"],
            "Wind Speed": data["wind"]["speed"],
            "time_of_record": pendulum.now(tz).to_datetime_string(),
            "Sunrise (Local Time)": sunrise.to_datetime_string(),
            "Sunset (Local Time)": sunset.to_datetime_string(),
        }

        # Create CSV in memory
        csv_buffer = StringIO()
        pd.DataFrame([processed]).to_csv(csv_buffer, index=False)

        # Upload to GCS
        GCSHook(gcp_conn_id="google_cloud_default").upload(
            bucket_name=bucket_name,
            object_name=f"pp_daily_weather_report/weather_data_{pendulum.now(tz).format('YYYYMMDDHHmmss')}.csv",
            data=csv_buffer.getvalue(),
            mime_type="text/csv",
        )

    # Dependencies
    is_weather_api_ready >> extract_weather_data >> transform_and_load(extract_weather_data.output)


# Instantiate the DAG
weather_dag()
