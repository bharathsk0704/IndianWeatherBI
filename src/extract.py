import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests



API_URL = "https://api.open-meteo.com/v1/forecast"



CITIES = {
    "Delhi": {
        "latitude": 28.6139,
        "longitude": 77.2090,
    },
    "Mumbai": {
        "latitude": 19.0760,
        "longitude": 72.8777,
    },
    "Bengaluru": {
        "latitude": 12.9716,
        "longitude": 77.5946,
    },
    "Chennai": {
        "latitude": 13.0827,
        "longitude": 80.2707,
    },
    "Kolkata": {
        "latitude": 22.5726,
        "longitude": 88.3639,
    },
}



RAW_DATA_DIRECTORY = Path(
    os.getenv("RAW_DATA_DIRECTORY", "/opt/airflow/data/raw")
)


def extract_weather_data() -> str:
    """
    Extract seven-day hourly weather forecasts for major Indian cities.

    Returns:
        The path of the raw JSON file created by the extraction.
    """

    extraction_time = datetime.now(timezone.utc)
    extracted_data = []

    print("Starting weather data extraction...")

    for city_name, coordinates in CITIES.items():
        print(f"Requesting forecast for {city_name}...")

        parameters = {
            "latitude": coordinates["latitude"],
            "longitude": coordinates["longitude"],
            "hourly": ",".join(
                [
                    "temperature_2m",
                    "apparent_temperature",
                    "relative_humidity_2m",
                    "precipitation",
                    "precipitation_probability",
                    "wind_speed_10m",
                    "weather_code",
                ]
            ),
            "forecast_days": 7,
            "timezone": "Asia/Kolkata",
            "temperature_unit": "celsius",
            "wind_speed_unit": "kmh",
            "precipitation_unit": "mm",
        }

        try:
            response = requests.get(
                API_URL,
                params=parameters,
                timeout=30,
            )

            
            response.raise_for_status()

            city_weather = response.json()

            # Basic validation
            if "hourly" not in city_weather:
                raise ValueError(
                    f"No hourly data returned for {city_name}."
                )

            if not city_weather["hourly"].get("time"):
                raise ValueError(
                    f"No forecast times returned for {city_name}."
                )

            
            city_weather["city"] = city_name
            city_weather["requested_at_utc"] = (
                extraction_time.isoformat()
            )

            extracted_data.append(city_weather)

            record_count = len(
                city_weather["hourly"]["time"]
            )

            print(
                f"Successfully received "
                f"{record_count} hourly records "
                f"for {city_name}."
            )

        except requests.RequestException as error:
            raise RuntimeError(
                f"API request failed for {city_name}: {error}"
            ) from error

    if not extracted_data:
        raise ValueError(
            "The API returned no weather data."
        )

    
    RAW_DATA_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True,
    )

    filename = (
        "indian_weather_raw_"
        f"{extraction_time.strftime('%Y%m%d_%H%M%S')}.json"
    )

    output_path = RAW_DATA_DIRECTORY / filename

    with output_path.open(
        mode="w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            extracted_data,
            output_file,
            indent=2,
            ensure_ascii=False,
        )

    print("----------------------------------------")
    print(f"Cities extracted: {len(extracted_data)}")
    print(f"Raw JSON saved to: {output_path}")
    print("Weather data extraction completed.")
    print("----------------------------------------")

    return str(output_path)



if __name__ == "__main__":
    extract_weather_data()