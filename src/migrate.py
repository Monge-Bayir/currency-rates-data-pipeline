import os
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)

MIGRATIONS_DIR = "/opt/project/sql/migrations"

def run_migrations(conn_id: str = "rates_pg") -> int:
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()

    with conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """)

        applied = set()
        cur.execute("SELECT version FROM public.schema_migrations;")
        for (v,) in cur.fetchall():
            applied.add(v)

        files = sorted(f for f in os.listdir(MIGRATIONS_DIR) if f.endswith(".sql"))

        ran = 0
        for filename in files:
            version = filename.split(".sql")[0]
            if version in applied:
                continue

            path = os.path.join(MIGRATIONS_DIR, filename)
            with open(path, "r", encoding="utf-8") as f:
                sql = f.read()

            logging.info(f"Applying migration: {filename}")
            cur.execute(sql)
            cur.execute("INSERT INTO public.schema_migrations(version) VALUES (%s);", (version,))
            ran += 1

    logging.info(f"Migrations applied: {ran}")
    return ran

if __name__ == "__main__":
    run_migrations()