from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


JAVA_OPTIONS = (
    '-Djava.security.manager=allow '
    '--add-opens=java.base/java.lang=ALL-UNNAMED '
    '--add-opens=java.base/java.util=ALL-UNNAMED '
    '--add-opens=java.base/java.io=ALL-UNNAMED '
    '--add-opens=java.base/sun.security.action=ALL-UNNAMED'
)

COMMON_ENV = {
    "PYTHONPATH": "/opt/project:/opt/project/src",
}

SPARK_ENV = {
    **COMMON_ENV,
    "PYSPARK_SUBMIT_ARGS": (
        f'--conf spark.driver.extraJavaOptions="{JAVA_OPTIONS}" '
        f'--conf spark.executor.extraJavaOptions="{JAVA_OPTIONS}" '
        "pyspark-shell"
    ),
}


def build_command(script_path: str) -> str:
    return f"cd /opt/project && python {script_path}"


with DAG(
    dag_id="velostar_pipeline",
    description="Pipeline Velostar automatise toutes les 2 minutes.",
    start_date=datetime(2026, 4, 3),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "codex",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["velostar", "etl", "postgres"],
) as dag:
    fetch_bikes = BashOperator(
        task_id="fetch_bikes",
        bash_command=build_command("src/get_api.py"),
        env=COMMON_ENV,
    )

    fetch_weather = BashOperator(
        task_id="fetch_weather",
        bash_command=build_command("src/scrape_weather.py"),
        env=COMMON_ENV,
    )

    run_eda = BashOperator(
        task_id="run_eda",
        bash_command=build_command("notebook.py"),
        env=COMMON_ENV,
    )

    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=build_command("src/traitement.py"),
        env=SPARK_ENV,
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=build_command("src/load_postgres.py"),
        env=COMMON_ENV,
    )

    [fetch_bikes, fetch_weather] >> run_eda >> transform_data >> load_postgres
