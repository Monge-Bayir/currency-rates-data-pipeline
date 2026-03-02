import logging
import os
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)

def load_fact_rates(processed_data: str, run_date: str, conn_id: str = "rates_pg"):
    if not os.path.exists(processed_data):
        logging.warning(f"Файла {processed_data} не существует")
        return None

    hook = PostgresHook(postgres_conn_id=conn_id)
    rows = []

    with open(processed_data, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                rows.append((
                    row["rate_date"],
                    row["code"],
                    row["name"],
                    int(row["nominal"]),
                    float(row["value"]),
                    float(row["value_per_1"]),
                ))
            except (KeyError, ValueError, TypeError) as e:
                logging.warning(f"{row} невалидная строка: {e}")

    if not rows:
        logging.warning("Нет валидных строк для загрузки")
        return None

    sql = """
        INSERT INTO fact_rates (rate_date, code, name, nominal, value, value_per_1)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (rate_date, code)
        DO UPDATE SET
            name = EXCLUDED.name,
            nominal = EXCLUDED.nominal,
            value = EXCLUDED.value,
            value_per_1 = EXCLUDED.value_per_1;
    """

    conn = hook.get_conn()
    try:
        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()

    logging.info(f"Данные загружены в БД. rows={len(rows)}, run_date={run_date}")
    return len(rows)