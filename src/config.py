from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bike_api_url: str = os.getenv(
        "BIKE_API_URL",
        "https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/vls-stations-etat-tr/records",
    )
    bike_api_limit: int = int(os.getenv("BIKE_API_LIMIT", "200"))
    weather_url: str = os.getenv(
        "WEATHER_URL",
        "https://www.meteoart.com/europe/france/brittany/rennes",
    )
    weather_city: str = os.getenv("WEATHER_CITY", "Rennes")
    raw_data_dir: str = os.getenv("RAW_DATA_DIR", "./data/raw")
    processed_data_dir: str = os.getenv("PROCESSED_DATA_DIR", "./data/processed")
    eda_output_dir: str = os.getenv("EDA_OUTPUT_DIR", "./data/eda")
    raw_bike_filename: str = os.getenv("RAW_BIKE_FILENAME", "star_bikes_raw.parquet")
    raw_weather_filename: str = os.getenv("RAW_WEATHER_FILENAME", "weather_scraped_raw.parquet")
    spark_app_name: str = os.getenv("SPARK_APP_NAME", "big-data-velostar-weather")
    enable_hdfs: bool = os.getenv("ENABLE_HDFS", "false").lower() == "true"
    hdfs_raw_dir: str = os.getenv("HDFS_RAW_DIR", "/datalake/raw/velostar")
    hdfs_processed_dir: str = os.getenv("HDFS_PROCESSED_DIR", "/datalake/processed/velostar")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: str = os.getenv("POSTGRES_PORT", "5432")
    postgres_db: str = os.getenv("POSTGRES_DB", "velostar_lakehouse")
    postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "postgres")
    postgres_schema: str = os.getenv("POSTGRES_SCHEMA", "public")
    postgres_table: str = os.getenv("POSTGRES_TABLE", "star_bikes_weather")
    streamlit_server_port: str = os.getenv("STREAMLIT_SERVER_PORT", "8501")

    @property
    def raw_bike_file_path(self) -> str:
        return os.path.join(self.raw_data_dir, self.raw_bike_filename)

    @property
    def raw_weather_file_path(self) -> str:
        return os.path.join(self.raw_data_dir, self.raw_weather_filename)

    @property
    def processed_file_path(self) -> str:
        return os.path.join(self.processed_data_dir, "star_bikes_weather.parquet")

    @property
    def postgres_url(self) -> str:
        return (
            f"postgresql+pg8000://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
