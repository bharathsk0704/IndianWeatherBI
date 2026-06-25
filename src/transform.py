import json
import os
from pathlib import Path

import pandas as pd


RAW_DATA_DIRECTORY = Path(
    os.getenv("RAW_DATA_DIRECTORY", "/opt/airflow/data/raw")
)

PROCESSED_DATA_DIRECTORY = Path(
    os.getenv(
        "PROCESSED_DATA_DIRECTORY",
        "/opt/airflow/data/processed",
    )
)


def classify_heat_risk(temperature: float) -> str:
    """Create a heat-risk category from temperature."""

    if temperature >= 40:
        return "Extreme"
    if temperature >= 35:
        return "High"
    if temperature >= 30:
        return "Moderate"
    return "Low"


def classify_rain_risk(rain_probability: float) -> str:
    """Create a rain-risk category from rain probability."""

    if rain_probability >= 70:
        return "High"
    if rain_probability >= 30:
        return "Medium"
    return "Low"


def classify_outdoor_suitability(row: pd.Series) -> str:
    """Classify weather suitability for outdoor activities."""

    temperature = row["temperature_c"]
    rain_probability = row["rain_probability_pct"]
    wind_speed = row["wind_speed_kmh"]

    if (
        temperature >= 40
        or rain_probability >= 70
        or wind_speed >= 40
    ):
        return "Poor"

    if (
        temperature >= 35
        or rain_probability >= 30
        or wind_speed >= 25
    ):
        return "Caution"

    return "Good"


def classify_operational_risk(row: pd.Series) -> str:
    """Create an operational disruption-risk category."""

    suitability = row["outdoor_suitability"]
    precipitation = row["precipitation_mm"]

    if suitability == "Poor" or precipitation >= 10:
        return "High"

    if suitability == "Caution" or precipitation > 0:
        return "Medium"

    return "Low"


def transform_weather_data(raw_file_path: str) -> str:
    """
    Transform nested Open-Meteo JSON into clean tabular data.

    Args:
        raw_file_path: Path to the raw JSON file.

    Returns:
        Path to the cleaned CSV file.
    """

    print("Starting weather data transformation...")

    raw_path = Path(raw_file_path)

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Raw weather file was not found: {raw_path}"
        )

    with raw_path.open(
        mode="r",
        encoding="utf-8",
    ) as input_file:
        raw_data = json.load(input_file)

    transformed_rows = []

    for city_data in raw_data:
        city_name = city_data.get("city")
        requested_at = city_data.get("requested_at_utc")
        hourly = city_data.get("hourly", {})

        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m", [])
        apparent_temperatures = hourly.get(
            "apparent_temperature",
            [],
        )
        humidity_values = hourly.get(
            "relative_humidity_2m",
            [],
        )
        precipitation_values = hourly.get(
            "precipitation",
            [],
        )
        rain_probabilities = hourly.get(
            "precipitation_probability",
            [],
        )
        wind_speeds = hourly.get(
            "wind_speed_10m",
            [],
        )
        weather_codes = hourly.get(
            "weather_code",
            [],
        )

        number_of_records = len(times)

        for index in range(number_of_records):
            transformed_rows.append(
                {
                    "city": city_name,
                    "forecast_time": times[index],
                    "temperature_c": temperatures[index],
                    "apparent_temperature_c":
                        apparent_temperatures[index],
                    "humidity_pct": humidity_values[index],
                    "precipitation_mm":
                        precipitation_values[index],
                    "rain_probability_pct":
                        rain_probabilities[index],
                    "wind_speed_kmh": wind_speeds[index],
                    "weather_code": weather_codes[index],
                    "extracted_at": requested_at,
                }
            )

    dataframe = pd.DataFrame(transformed_rows)

    if dataframe.empty:
        raise ValueError(
            "Transformation produced no records."
        )

    print(f"Raw rows created: {len(dataframe)}")

    
    dataframe["forecast_time"] = pd.to_datetime(
        dataframe["forecast_time"],
        errors="coerce",
    )

    dataframe["extracted_at"] = pd.to_datetime(
        dataframe["extracted_at"],
        errors="coerce",
        utc=True,
    )

    
    numeric_columns = [
        "temperature_c",
        "apparent_temperature_c",
        "humidity_pct",
        "precipitation_mm",
        "rain_probability_pct",
        "wind_speed_kmh",
        "weather_code",
    ]

    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

    
    before_missing_check = len(dataframe)

    dataframe = dataframe.dropna(
        subset=[
            "city",
            "forecast_time",
            "temperature_c",
        ]
    )

    removed_missing_rows = (
        before_missing_check - len(dataframe)
    )

    
    dataframe["precipitation_mm"] = (
        dataframe["precipitation_mm"].fillna(0)
    )

    dataframe["rain_probability_pct"] = (
        dataframe["rain_probability_pct"].fillna(0)
    )

    
    dataframe = dataframe[
        dataframe["rain_probability_pct"].between(0, 100)
    ]

    dataframe = dataframe[
        dataframe["humidity_pct"].between(0, 100)
    ]

    dataframe = dataframe[
        dataframe["wind_speed_kmh"] >= 0
    ]

    
    before_duplicate_check = len(dataframe)

    dataframe = dataframe.drop_duplicates(
        subset=["city", "forecast_time"],
        keep="last",
    )

    removed_duplicates = (
        before_duplicate_check - len(dataframe)
    )

    
    dataframe["heat_risk"] = dataframe[
        "temperature_c"
    ].apply(classify_heat_risk)

    dataframe["rain_risk"] = dataframe[
        "rain_probability_pct"
    ].apply(classify_rain_risk)

    dataframe["outdoor_suitability"] = dataframe.apply(
        classify_outdoor_suitability,
        axis=1,
    )

    dataframe["operational_risk"] = dataframe.apply(
        classify_operational_risk,
        axis=1,
    )

    
    dataframe["forecast_date"] = dataframe[
        "forecast_time"
    ].dt.date

    dataframe["forecast_hour"] = dataframe[
        "forecast_time"
    ].dt.hour

    dataframe["day_name"] = dataframe[
        "forecast_time"
    ].dt.day_name()

    
    dataframe = dataframe[
        [
            "city",
            "forecast_time",
            "forecast_date",
            "forecast_hour",
            "day_name",
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
    ]

    dataframe = dataframe.sort_values(
        by=["city", "forecast_time"]
    )

    PROCESSED_DATA_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_filename = (
        raw_path.stem.replace("_raw_", "_processed_")
        + ".csv"
    )

    output_path = (
        PROCESSED_DATA_DIRECTORY / output_filename
    )

    dataframe.to_csv(
        output_path,
        index=False,
    )

    print("----------------------------------------")
    print(f"Missing rows removed: {removed_missing_rows}")
    print(f"Duplicate rows removed: {removed_duplicates}")
    print(f"Clean rows created: {len(dataframe)}")
    print(f"Cities included: {dataframe['city'].nunique()}")
    print(f"Processed CSV saved to: {output_path}")
    print("Weather data transformation completed.")
    print("----------------------------------------")

    return str(output_path)


def find_latest_raw_file() -> Path:
    """Find the most recently created raw JSON file."""

    raw_files = list(
        RAW_DATA_DIRECTORY.glob(
            "indian_weather_raw_*.json"
        )
    )

    if not raw_files:
        raise FileNotFoundError(
            "No Indian weather raw JSON file was found."
        )

    return max(
        raw_files,
        key=lambda file_path: file_path.stat().st_mtime,
    )


if __name__ == "__main__":
    latest_raw_file = find_latest_raw_file()

    print(f"Latest raw file selected: {latest_raw_file}")

    transform_weather_data(
        str(latest_raw_file)
    )