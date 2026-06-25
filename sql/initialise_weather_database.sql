CREATE TABLE IF NOT EXISTS weather_forecast (
    id SERIAL PRIMARY KEY,

    city VARCHAR(50) NOT NULL,
    forecast_time TIMESTAMP NOT NULL,

    temperature_c NUMERIC(6, 2),
    apparent_temperature_c NUMERIC(6, 2),
    humidity_pct INTEGER,

    precipitation_mm NUMERIC(8, 2),
    rain_probability_pct INTEGER,
    wind_speed_kmh NUMERIC(8, 2),
    weather_code INTEGER,

    heat_risk VARCHAR(20),
    rain_risk VARCHAR(20),
    outdoor_suitability VARCHAR(20),
    operational_risk VARCHAR(20),

    extracted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT unique_city_forecast
        UNIQUE (city, forecast_time)
);
