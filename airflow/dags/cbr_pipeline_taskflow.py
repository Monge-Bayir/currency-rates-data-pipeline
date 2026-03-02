from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import csv


@dag(
    dag_id='pipeline_taskflow',
    start_date=datetime(2026,3,1),
    schedule=None,
    catchup=False,
    tags=['cbr', 'taskflow']
)

def cbr_pipeline_taskflow():

    @task(retries=2)
    def extract() -> str:
        from src.extract import extract_bank
        ds = get_current_context()["ds"]
        raw_path = extract_bank(ds)
        return raw_path

    @task(retries=2)
    def transform(raw_path: str) -> str:
        from src.transform import transform_bank
        ds = get_current_context()["ds"]
        processed_path = transform_bank(raw_path, ds)

        if not processed_path:
            raise ValueError(f'transform_bank вернул пустой результат для raw_path={raw_path}')

        return processed_path

    @task(retries=2)
    def check_data_from_transform(processed_path: str):
        with open(processed_path, newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        if len(rows) < 50:
            raise ValueError(f'Слишком мало данных, их {rows}')

        for row in rows:
            if float(row["value_per_1"]) <= 0:
                raise ValueError("value_per_1 <= 0")

        return processed_path

    @task(retries=2)
    def migrate() -> int:
        from src.migrate import run_migrations
        return run_migrations(conn_id="rates_pg")

    @task(retries=2)
    def load_to_postgres(processed_path: str):
        from src.load import load_fact_rates
        ds = get_current_context()['ds']
        out = load_fact_rates(processed_path, ds)
        if not out:
            raise ValueError(f"load_fact_rates вернул пустой результат для processed_path={processed_path}")
        return out

    @task(retries=2)
    def analytics(processed_path: str) -> None:
        from src.analytics import analytics_check
        ds = get_current_context()["ds"]
        out = analytics_check(processed_path, ds)
        if not out:
            raise ValueError(f"analytics_check вернул пустой результат для processed_path={processed_path}")

    raw_path = extract()
    processed_path = transform(raw_path)
    checked_path = check_data_from_transform(processed_path)
    migrate()
    load_to_postgres(checked_path)
    analytics(checked_path)

cbr_pipeline_taskflow()