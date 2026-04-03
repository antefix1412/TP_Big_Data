from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests import RequestException

from config import settings


def ensure_directory(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def pick_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload.get(key) is not None:
            return payload.get(key)
    return None


def fetch_bike_stations() -> dict[str, Any]:
    candidate_limits = [settings.bike_api_limit, 100, 50, 20]
    attempted_limits: list[int] = []
    last_error: RequestException | None = None

    for limit in candidate_limits:
        if limit in attempted_limits:
            continue
        attempted_limits.append(limit)
        try:
            response = requests.get(
                settings.bike_api_url,
                params={"limit": limit},
                timeout=60,
            )
            response.raise_for_status()
            payload = response.json()
            payload["_used_limit"] = limit
            return payload
        except RequestException as error:
            last_error = error

    if last_error is None:
        raise RuntimeError("Bike API collection failed with no response.")
    raise last_error


def normalize_station(station: dict[str, Any], collected_at: str) -> dict[str, Any]:
    coordinates = station.get("coordonnees") or station.get("coordonneesxy") or {}
    return {
        "station_id": pick_value(station, "idstation", "id"),
        "station_name": pick_value(station, "nom", "name"),
        "available_bikes": pick_value(
            station,
            "nombrevelosdisponibles",
            "num_bikes_available",
        ),
        "available_docks": pick_value(
            station,
            "nombreemplacementsdisponibles",
            "nombreplacesdisponibles",
            "num_docks_available",
            "nombreemplacementsactuels",
        ),
        "available_electric_bikes": pick_value(
            station,
            "nombreveloselectriquesdisponibles",
            "num_ebikes_available",
        ),
        "station_status": pick_value(station, "etat", "is_installed"),
        "payment_terminal": pick_value(station, "paiement", "banking"),
        "latitude": pick_value(coordinates, "lat", "latitude"),
        "longitude": pick_value(coordinates, "lon", "lng", "longitude"),
        "record_timestamp": pick_value(station, "lastupdate", "duedate", "record_timestamp"),
        "collection_timestamp": collected_at,
        "source": "star_api",
    }


def save_raw_records(records: list[dict[str, Any]]) -> str:
    ensure_directory(settings.raw_data_dir)

    output_path = settings.raw_bike_file_path
    pd.DataFrame(records).to_parquet(output_path, index=False)
    return output_path


def upload_to_hdfs(local_path: str) -> None:
    if not settings.enable_hdfs:
        print("HDFS disabled, skipping upload.")
        return

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", settings.hdfs_raw_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, settings.hdfs_raw_dir], check=True)
    print(f"Raw bike file uploaded to HDFS: {settings.hdfs_raw_dir}")


def main() -> None:
    collected_at = datetime.now(timezone.utc).isoformat()
    try:
        payload = fetch_bike_stations()
    except RequestException as error:
        raise RuntimeError(f"Bike API collection failed: {error}") from error

    results = payload.get("results", [])
    records = [normalize_station(station, collected_at) for station in results]
    output_path = save_raw_records(records)

    print(f"Raw bike file created: {os.path.abspath(output_path)}")
    print(f"API limit used: {payload.get('_used_limit')}")
    print(f"Stations collected: {len(records)}")
    upload_to_hdfs(output_path)


if __name__ == "__main__":
    main()
