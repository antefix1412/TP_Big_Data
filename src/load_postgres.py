from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine, text

from config import settings


def load_to_postgres() -> None:
    if not os.path.exists(settings.processed_file_path):
        raise FileNotFoundError(
            f"Processed file not found: {settings.processed_file_path}. Run src/traitement.py first."
        )

    dataframe = pd.read_parquet(settings.processed_file_path)
    engine = create_engine(settings.postgres_url)

    with engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {settings.postgres_schema}.{settings.postgres_table} (
                    station_id TEXT PRIMARY KEY,
                    station_name TEXT,
                    available_bikes INTEGER,
                    available_docks INTEGER,
                    available_electric_bikes INTEGER,
                    total_capacity INTEGER,
                    bikes_used_proxy INTEGER,
                    usage_rate DOUBLE PRECISION,
                    station_status TEXT,
                    payment_terminal TEXT,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    record_timestamp TIMESTAMP,
                    collection_timestamp TIMESTAMP,
                    source TEXT,
                    station_status_index INTEGER,
                    payment_terminal_index INTEGER,
                    city TEXT,
                    temperature_c DOUBLE PRECISION,
                    rain_probability INTEGER,
                    weather_condition TEXT,
                    weather_summary TEXT,
                    scrape_timestamp TIMESTAMP,
                    scrape_status TEXT,
                    weather_url TEXT,
                    weather_error_message TEXT,
                    data_lineage TEXT
                )
                """
            )
        )
        connection.execute(
            text(f"TRUNCATE TABLE {settings.postgres_schema}.{settings.postgres_table}")
        )

    dataframe.to_sql(
        settings.postgres_table,
        engine,
        schema=settings.postgres_schema,
        if_exists="append",
        index=False,
    )

    print(f"Table loaded into PostgreSQL: {settings.postgres_schema}.{settings.postgres_table}")


if __name__ == "__main__":
    load_to_postgres()
