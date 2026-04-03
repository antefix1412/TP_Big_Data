from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, lit, row_number, to_timestamp, when
from pyspark.sql.window import Window

from config import settings


NUMERIC_COLUMNS = [
    "available_bikes",
    "available_docks",
    "available_electric_bikes",
    "latitude",
    "longitude",
]


def get_java_options() -> str:
    return (
        "-Djava.security.manager=allow "
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
    )


def build_spark_session() -> SparkSession:
    warehouse_dir = Path(settings.processed_data_dir).resolve() / "spark-warehouse"
    return (
        SparkSession.builder.appName(settings.spark_app_name)
        .config("spark.sql.session.timeZone", "Europe/Paris")
        .config("spark.sql.warehouse.dir", warehouse_dir.as_uri())
        .config("spark.driver.extraJavaOptions", get_java_options())
        .config("spark.executor.extraJavaOptions", get_java_options())
        .getOrCreate()
    )


def ensure_directory(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def remove_existing_output(path: str) -> None:
    output = Path(path)
    if output.is_dir():
        shutil.rmtree(output, ignore_errors=True)
    elif output.exists():
        output.unlink()


def winsorize_iqr(dataframe: DataFrame, column_name: str) -> DataFrame:
    quantiles = dataframe.approxQuantile(column_name, [0.25, 0.75], 0.01)
    if len(quantiles) < 2:
        return dataframe

    q1, q3 = quantiles
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return dataframe.withColumn(
        column_name,
        when(col(column_name) < lower, lower)
        .when(col(column_name) > upper, upper)
        .otherwise(col(column_name)),
    )


def clean_raw_data(dataframe: DataFrame) -> DataFrame:
    cleaned = (
        dataframe.select(
            col("station_id").cast("string"),
            col("station_name"),
            col("available_bikes").cast("double"),
            col("available_docks").cast("double"),
            col("available_electric_bikes").cast("double"),
            concat_ws(", ", col("station_status")).alias("station_status"),
            col("payment_terminal").cast("string").alias("payment_terminal"),
            col("latitude").cast("double"),
            col("longitude").cast("double"),
            to_timestamp(col("record_timestamp")).alias("record_timestamp"),
            to_timestamp(col("collection_timestamp")).alias("collection_timestamp"),
            col("source"),
        )
        .dropna(subset=["station_id", "station_name"])
        .withColumn("available_bikes", when(col("available_bikes") < 0, None).otherwise(col("available_bikes")))
        .withColumn("available_docks", when(col("available_docks") < 0, None).otherwise(col("available_docks")))
        .withColumn(
            "available_electric_bikes",
            when(col("available_electric_bikes") < 0, None).otherwise(col("available_electric_bikes")),
        )
        .withColumn(
            "latitude",
            when((col("latitude") < -90) | (col("latitude") > 90), None).otherwise(col("latitude")),
        )
        .withColumn(
            "longitude",
            when((col("longitude") < -180) | (col("longitude") > 180), None).otherwise(col("longitude")),
        )
        .fillna({"station_status": "unknown", "payment_terminal": "unknown"})
    )

    for column_name in NUMERIC_COLUMNS:
        cleaned = winsorize_iqr(cleaned, column_name)

    return cleaned


def fill_fully_null_numeric_columns(dataframe: DataFrame) -> DataFrame:
    fallback_values = {
        "available_bikes": 0.0,
        "available_docks": 0.0,
        "available_electric_bikes": 0.0,
        "latitude": 48.111,
        "longitude": -1.680,
    }

    for column_name, fallback in fallback_values.items():
        non_null_count = dataframe.filter(col(column_name).isNotNull()).limit(1).count()
        if non_null_count == 0:
            dataframe = dataframe.withColumn(column_name, lit(fallback))

    return dataframe


def transform_data() -> str:
    spark = build_spark_session()
    raw_dataframe = spark.read.parquet(settings.raw_bike_file_path)
    weather_raw = spark.read.parquet(settings.raw_weather_file_path)
    cleaned = fill_fully_null_numeric_columns(clean_raw_data(raw_dataframe))

    latest_weather = (
        weather_raw.withColumn("scrape_timestamp", to_timestamp(col("scrape_timestamp")))
        .withColumn(
            "row_num",
            row_number().over(Window.orderBy(col("scrape_timestamp").desc())),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    imputer = Imputer(
        inputCols=NUMERIC_COLUMNS,
        outputCols=[f"{column_name}_imputed" for column_name in NUMERIC_COLUMNS],
        strategy="median",
    )
    status_indexer = StringIndexer(
        inputCol="station_status",
        outputCol="station_status_index",
        handleInvalid="keep",
    )
    payment_indexer = StringIndexer(
        inputCol="payment_terminal",
        outputCol="payment_terminal_index",
        handleInvalid="keep",
    )
    pipeline = Pipeline(stages=[imputer, status_indexer, payment_indexer])
    transformed = pipeline.fit(cleaned).transform(cleaned)
    enriched = (
        transformed.crossJoin(latest_weather)
        .withColumn("total_capacity", col("available_bikes_imputed") + col("available_docks_imputed"))
        .withColumn("bikes_used_proxy", col("available_docks_imputed"))
        .withColumn(
            "usage_rate",
            when(col("total_capacity") > 0, col("available_docks_imputed") / col("total_capacity")).otherwise(None),
        )
    )

    final_dataframe = (
        enriched.select(
            col("station_id"),
            col("station_name"),
            col("available_bikes_imputed").cast("int").alias("available_bikes"),
            col("available_docks_imputed").cast("int").alias("available_docks"),
            col("available_electric_bikes_imputed").cast("int").alias("available_electric_bikes"),
            col("total_capacity").cast("int").alias("total_capacity"),
            col("bikes_used_proxy").cast("int").alias("bikes_used_proxy"),
            col("usage_rate").cast("double").alias("usage_rate"),
            col("station_status"),
            col("payment_terminal"),
            col("latitude_imputed").alias("latitude"),
            col("longitude_imputed").alias("longitude"),
            col("record_timestamp"),
            col("collection_timestamp"),
            col("source"),
            col("station_status_index").cast("int").alias("station_status_index"),
            col("payment_terminal_index").cast("int").alias("payment_terminal_index"),
            col("city"),
            col("temperature_c").cast("double").alias("temperature_c"),
            col("rain_probability").cast("int").alias("rain_probability"),
            col("weather_condition"),
            col("weather_summary"),
            col("scrape_timestamp"),
            col("scrape_status"),
            col("weather_url"),
            col("error_message").alias("weather_error_message"),
            lit("star_api + weather_scraping").alias("data_lineage"),
        )
        .orderBy("station_name")
    )

    ensure_directory(settings.processed_data_dir)
    output_path = settings.processed_file_path
    temp_output_path = f"{output_path}.tmp"
    remove_existing_output(output_path)
    remove_existing_output(temp_output_path)
    final_dataframe.toPandas().to_parquet(temp_output_path, index=False)
    os.replace(temp_output_path, output_path)
    spark.stop()
    return output_path


def upload_to_hdfs(processed_path: str) -> None:
    if not settings.enable_hdfs:
        print("HDFS disabled, skipping upload.")
        return

    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", settings.hdfs_processed_dir], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", processed_path, settings.hdfs_processed_dir], check=True)
    print(f"Processed file uploaded to HDFS: {settings.hdfs_processed_dir}")


def main() -> None:
    if not os.path.exists(settings.raw_bike_file_path):
        raise FileNotFoundError(
            f"Raw bike file not found: {settings.raw_bike_file_path}. Run src/get_api.py first."
        )

    if not os.path.exists(settings.raw_weather_file_path):
        raise FileNotFoundError(
            f"Raw weather file not found: {settings.raw_weather_file_path}. Run src/scrape_weather.py first."
        )

    processed_path = transform_data()
    print(f"Processed parquet created: {os.path.abspath(processed_path)}")
    upload_to_hdfs(processed_path)


if __name__ == "__main__":
    main()
