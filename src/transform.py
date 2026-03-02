import csv
import json
import logging
import os
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROC_DIR = "/opt/project/data/processed"
os.makedirs(PROC_DIR, exist_ok=True)

def transform_bank(raw_path: str) -> Optional[str]:
    path_name = f"{PROC_DIR}/processed_data_{datetime.now().strftime('%Y-%m-%d')}.csv"

    # идемпотентность: если уже есть — просто вернём
    if os.path.exists(path_name):
        logging.warning("Processed файл уже существует")
        return path_name

    try:
        if not raw_path.lower().endswith(".json"):
            raise ValueError("Файл должен быть формата json")

        with open(raw_path, "r", encoding="utf-8") as f:
            data = json.load(f)

    except FileNotFoundError:
        logging.error(f"Raw файл не найден: {raw_path}")
        return None
    except json.JSONDecodeError:
        logging.error("Некорректный JSON")
        return None
    except ValueError as e:
        logging.error(str(e))
        return None

    valute = data.get("Valute", {})
    rate_date = data.get("Date", "")[:10]

    records: list[dict] = []
    for code, row in valute.items(): #проходим по каждой валюте
        try:
            nominal = int(row["Nominal"])
            value = float(row["Value"])
            name = row["Name"]

            if nominal <= 0:
                raise ValueError("nominal <= 0")

            records.append({
                "rate_date": rate_date,
                "code": code,
                "name": name,
                "nominal": nominal,
                "value": value,
                "value_per_1": value / nominal
            })

        except (KeyError, ValueError, TypeError) as e:
            logging.warning(f"Плохая строка {code}: {e}")

    if not records:
        logging.warning("Нет валидных строк для записи в processed")
        return None

    with open(path_name, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["rate_date", "code", "name", "nominal", "value", "value_per_1"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logging.info(f"Processed записан: {path_name} (rows={len(records)})")
    return path_name

if __name__ == "__main__":
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    raw_path = f"/opt/project/data/raw/raw_bank_{today}.json"
    out = transform_bank(raw_path)
    print(out or "")