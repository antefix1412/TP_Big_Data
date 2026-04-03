from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests import RequestException

from config import settings


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)


def ensure_directory(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def build_weather_url(url: str) -> str:
    split_url = urlsplit(url)
    query = dict(parse_qsl(split_url.query, keep_blank_values=True))
    query.setdefault("page", "today")
    return urlunsplit(
        (split_url.scheme, split_url.netloc, split_url.path, urlencode(query), split_url.fragment)
    )


def parse_number(value: str | None) -> float | None:
    if value is None:
        return None

    match = re.search(r"-?\d+(?:[.,]\d+)?", value)
    if not match:
        return None

    return float(match.group(0).replace(",", "."))


def parse_hour_label(value: str) -> int | None:
    match = re.search(r"(\d{1,2}):(\d{2})", value)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2))
    return hour * 60 + minute


def extract_placeholder_value(soup: BeautifulSoup, css_selector: str) -> str | None:
    node = soup.select_one(css_selector)
    if node is None:
        return None

    value = node.get_text(strip=True) or node.get("data-temp") or node.get("data-value")
    if value is None:
        return None

    cleaned = value.strip()
    return cleaned or None


def extract_hourly_snapshot(soup: BeautifulSoup) -> dict[str, object]:
    table = soup.select_one("#day-table")
    if table is None:
        return {
            "temperature_c": None,
            "rain_probability": None,
            "weather_condition": None,
        }

    header_row = table.select_one("thead tr")
    if header_row is None:
        return {
            "temperature_c": None,
            "rain_probability": None,
            "weather_condition": None,
        }

    slots = []
    for cell in header_row.find_all("td"):
        label = cell.get_text(" ", strip=True)
        slot_minutes = parse_hour_label(label)
        if slot_minutes is not None:
            slots.append({"label": label, "minutes": slot_minutes})

    if not slots:
        return {
            "temperature_c": None,
            "rain_probability": None,
            "weather_condition": None,
        }

    paris_now = datetime.now(ZoneInfo("Europe/Paris"))
    current_minutes = paris_now.hour * 60 + paris_now.minute
    selected_index = min(
        range(len(slots)),
        key=lambda index: abs(slots[index]["minutes"] - current_minutes),
    )

    values_by_row: dict[str, list[str]] = {}
    for row in table.select("tbody tr"):
        header = row.find("th")
        if header is None:
            continue
        row_name = header.get_text(" ", strip=True)
        row_values = [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
        values_by_row[row_name] = row_values

    temperature_values = values_by_row.get("Température", [])
    rain_values = values_by_row.get("Risques de pluie", [])
    weather_values = values_by_row.get("Météo", [])

    temperature_c = None
    if selected_index < len(temperature_values):
        temperature_c = parse_number(temperature_values[selected_index])

    rain_probability = None
    if selected_index < len(rain_values):
        parsed_rain = parse_number(rain_values[selected_index])
        if parsed_rain is not None:
            rain_probability = int(parsed_rain)

    weather_condition = None
    if selected_index < len(weather_values):
        weather_condition = normalize_condition_label(weather_values[selected_index])

    return {
        "temperature_c": temperature_c,
        "rain_probability": rain_probability,
        "weather_condition": weather_condition,
    }


def normalize_condition_label(value: str) -> str | None:
    if value is None:
        return None

    lowered_value = value.lower()
    keywords = {
        "orage": "orage",
        "averse": "averse",
        "pluie": "pluie",
        "nuage": "nuageux",
        "couvert": "nuageux",
        "ensole": "ensoleille",
        "clair": "clair",
        "brume": "brouillard",
        "brouillard": "brouillard",
        "neige": "neige",
    }
    for keyword, label in keywords.items():
        if keyword in lowered_value:
            return label
    return None
def scrape_weather() -> dict[str, object]:
    headers = {"User-Agent": USER_AGENT}
    weather_url = build_weather_url(settings.weather_url)
    response = requests.get(weather_url, headers=headers, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else None
    summary_meta = soup.find("meta", attrs={"name": "description"})
    summary = summary_meta.get("content") if summary_meta else None
    hourly_snapshot = extract_hourly_snapshot(soup)

    return {
        "city": settings.weather_city,
        "weather_url": weather_url,
        "page_title": title,
        "weather_summary": summary,
        "temperature_c": hourly_snapshot["temperature_c"],
        "rain_probability": hourly_snapshot["rain_probability"],
        "weather_condition": hourly_snapshot["weather_condition"],
        "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
        "html_length": len(response.text),
        "scrape_status": "success",
        "error_message": None,
    }


def save_raw_record(record: dict[str, object]) -> str:
    ensure_directory(settings.raw_data_dir)
    output_path = settings.raw_weather_file_path
    pd.DataFrame([record]).to_parquet(output_path, index=False)
    return output_path


def upload_to_hdfs(local_path: str) -> None:
    if not settings.enable_hdfs:
        print("HDFS disabled, skipping upload.")
        return

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", settings.hdfs_raw_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_path, settings.hdfs_raw_dir], check=True)
    print(f"Raw weather file uploaded to HDFS: {settings.hdfs_raw_dir}")


def main() -> None:
    try:
        record = scrape_weather()
    except RequestException as error:
        record = {
            "city": settings.weather_city,
            "weather_url": settings.weather_url,
            "page_title": None,
            "weather_summary": None,
            "temperature_c": None,
            "rain_probability": None,
            "weather_condition": None,
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "html_length": None,
            "scrape_status": "error",
            "error_message": str(error),
        }

    output_path = save_raw_record(record)
    print(f"Raw weather file created: {os.path.abspath(output_path)}")
    upload_to_hdfs(output_path)


if __name__ == "__main__":
    main()
