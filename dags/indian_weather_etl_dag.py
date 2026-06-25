from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


DEFAULT_ARGS = {
    "owner": "bharath",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="indian_weather_etl_pipeline",
    description=(
        "Extract Indian weather forecasts, transform the data, "
        "and load it into PostgreSQL."
    ),
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026,
        6,
        25,
        tz="Asia/Kolkata",
    ),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=[
        "weather",
        "india",
        "etl",
        "business-intelligence",
    ],
)
def indian_weather_etl_pipeline():

    @task(task_id="extract_weather")
    def extract_task() -> str:
        from src.extract import extract_weather_data

        return extract_weather_data()

    @task(task_id="transform_weather")
    def transform_task(raw_file_path: str) -> str:
        from src.transform import transform_weather_data

        return transform_weather_data(raw_file_path)

    @task(task_id="load_weather")
    def load_task(processed_file_path: str) -> int:
        from src.load import load_weather_data

        return load_weather_data(processed_file_path)

    raw_file_path = extract_task()

    processed_file_path = transform_task(
        raw_file_path
    )

    load_task(
        processed_file_path
    )


indian_weather_etl_pipeline()