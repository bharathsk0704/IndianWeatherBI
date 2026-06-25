import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


PROCESSED_DATA_DIRECTORY = Path(
    os.getenv(
        "PROCESSED_DATA_DIRECTORY",
        "/opt/airflow/data/processed",
    )
)


def get_database_connection():
    """
    Create and return a connection to the weather PostgreSQL database.
    """

    return psycopg2.connect(
        host=os.getenv(
            "WEATHER_DB_HOST",
            "weather-postgres",
        ),
        port=os.getenv(
            "WEATHER_DB_PORT",
            "5432",
        ),
        dbname=os.getenv(
            "WEATHER_DB_NAME",
            "indian_weather",
        ),
        user=os.getenv(
            "WEATHER_DB_USER",
            "weather_user",
        ),
        password=os.getenv(
            "WEATHER_DB_PASSWORD",
            "weather_password",
        ),
    )


def load_weather_data(processed_file_path: str) -> int:
    """
    Load cleaned weather records into PostgreSQL.

    Existing city and forecast-time combinations are updated
    instead of inserting duplicate rows.

    Args:
        processed_file_path: Path to the processed CSV file.

    Returns:
        Number of records loaded or updated.
    """

    print("Starting PostgreSQL loading process...")

    csv_path = Path(processed_file_path)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Processed CSV file was not found: {csv_path}"
        )

    dataframe = pd.read_csv(csv_path)

    if dataframe.empty:
        raise ValueError(
            "The processed CSV file contains no records."
        )

    required_columns = [
        "city",
        "forecast_time",
        "temperature_c",
        "apparent_temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "rain_probability_pct",
        "wind_speed_kmh",
        "weather_code",
        "heat_risk",
        "rain_risk",
        "outdoor_suitability",
        "operational_risk",
        "extracted_at",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Required columns are missing: {missing_columns}"
        )

    dataframe["forecast_time"] = pd.to_datetime(
        dataframe["forecast_time"],
        errors="coerce",
    )

    dataframe["extracted_at"] = pd.to_datetime(
        dataframe["extracted_at"],
        errors="coerce",
        utc=True,
    )

    dataframe = dataframe.dropna(
        subset=[
            "city",
            "forecast_time",
        ]
    )

    selected_data = dataframe[required_columns].copy()

    
    selected_data = selected_data.astype(object).where(
        pd.notna(selected_data),
        None,
    )

    records = list(
        selected_data.itertuples(
            index=False,
            name=None,
        )
    )

    if not records:
        raise ValueError(
            "No valid records are available for loading."
        )

    insert_query = """
        INSERT INTO weather_forecast (
            city,
            forecast_time,
            temperature_c,
            apparent_temperature_c,
            humidity_pct,
            precipitation_mm,
            rain_probability_pct,
            wind_speed_kmh,
            weather_code,
            heat_risk,
            rain_risk,
            outdoor_suitability,
            operational_risk,
            extracted_at
        )
        VALUES %s

        ON CONFLICT (city, forecast_time)
        DO UPDATE SET
            temperature_c =
                EXCLUDED.temperature_c,

            apparent_temperature_c =
                EXCLUDED.apparent_temperature_c,

            humidity_pct =
                EXCLUDED.humidity_pct,

            precipitation_mm =
                EXCLUDED.precipitation_mm,

            rain_probability_pct =
                EXCLUDED.rain_probability_pct,

            wind_speed_kmh =
                EXCLUDED.wind_speed_kmh,

            weather_code =
                EXCLUDED.weather_code,

            heat_risk =
                EXCLUDED.heat_risk,

            rain_risk =
                EXCLUDED.rain_risk,

            outdoor_suitability =
                EXCLUDED.outdoor_suitability,

            operational_risk =
                EXCLUDED.operational_risk,

            extracted_at =
                EXCLUDED.extracted_at;
    """

    connection = get_database_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    insert_query,
                    records,
                    page_size=500,
                )

        print("----------------------------------------")
        print(f"Records loaded or updated: {len(records)}")
        print("Target table: weather_forecast")
        print("PostgreSQL loading completed successfully.")
        print("----------------------------------------")

        return len(records)

    except Exception as error:
        print(f"Database loading failed: {error}")
        raise

    finally:
        connection.close()


def find_latest_processed_file() -> Path:
    """
    Find the most recently created processed weather CSV.
    """

    processed_files = list(
        PROCESSED_DATA_DIRECTORY.glob(
            "indian_weather_processed_*.csv"
        )
    )

    if not processed_files:
        raise FileNotFoundError(
            "No processed Indian weather CSV file was found."
        )

    return max(
        processed_files,
        key=lambda file_path: file_path.stat().st_mtime,
    )


if __name__ == "__main__":
    latest_processed_file = find_latest_processed_file()

    print(
        f"Latest processed file selected: "
        f"{latest_processed_file}"
    )

    load_weather_data(
        str(latest_processed_file)
    )