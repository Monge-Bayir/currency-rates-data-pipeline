from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

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
    def analytics(processed_path: str) -> None:
        from src.analytics import analytics_check
        ds = get_current_context()["ds"]
        out = analytics_check(processed_path, ds)
        if not out:
            raise ValueError(f"analytics_check вернул пустой результат для processed_path={processed_path}")

    raw_path = extract()
    processed_path = transform(raw_path)
    analytics(processed_path)

cbr_pipeline_taskflow()