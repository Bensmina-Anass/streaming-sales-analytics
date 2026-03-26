from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta

import clickhouse_connect
from airflow import DAG
from airflow.operators.python import PythonOperator


CLICKHOUSE_HOST = os.getenv("CH_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CH_PORT", "8123"))
CLICKHOUSE_DATABASE = os.getenv("CH_DB", "ecommerce_dw")
CLICKHOUSE_USER = os.getenv("CH_USER", "admin")
CLICKHOUSE_PASSWORD = os.getenv("CH_PASSWORD", "admin")

SQL_BASE_PATH = Path("/opt/airflow/clickhouse")
BRONZE_TO_SILVER_SQL = SQL_BASE_PATH / "transform_bronze_to_silver.sql"
SILVER_TO_GOLD_SQL = SQL_BASE_PATH / "transform_silver_to_gold.sql"


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def read_sql_file(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8").strip()


def split_sql_statements(sql_text: str) -> list[str]:
    statements = []
    current = []

    for line in sql_text.splitlines():
        stripped = line.strip()

        if not stripped or stripped.startswith("--"):
            continue

        current.append(line)

        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            current = []

    if current:
        statement = "\n".join(current).strip().rstrip(";").strip()
        if statement:
            statements.append(statement)

    return statements


def execute_sql_script(sql_file_path: str, **context) -> None:
    sql_path = Path(sql_file_path)
    sql_text = read_sql_file(sql_path)
    statements = split_sql_statements(sql_text)

    if not statements:
        raise ValueError(f"No executable SQL statements found in {sql_path}")

    client = get_clickhouse_client()

    try:
        for idx, statement in enumerate(statements, start=1):
            print(f"Executing statement {idx}/{len(statements)} from {sql_path.name}")
            client.command(statement)
    finally:
        client.close()

    print(f"Completed SQL script: {sql_path.name}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="warehouse_pipeline",
    description="Minimal ClickHouse warehouse orchestration: bronze->silver->gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["warehouse", "clickhouse"],
) as dag:

    bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=execute_sql_script,
        op_kwargs={"sql_file_path": str(BRONZE_TO_SILVER_SQL)},
    )

    silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=execute_sql_script,
        op_kwargs={"sql_file_path": str(SILVER_TO_GOLD_SQL)},
    )

    bronze_to_silver >> silver_to_gold
