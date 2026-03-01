import logging
import os
import csv
import datetime

logging.basicConfig(level=logging.INFO)

PATH_NAME = "data/analytics"
os.makedirs(PATH_NAME, exist_ok=True)

def analytics_check(processed_path: str):
    if not os.path.exists(processed_path):
        logging.warning("Такого файла не существует")
        return

    records = []
    with open(processed_path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                row["value_per_1"] = float(row["value_per_1"])
                records.append(row)
            except ValueError:
                logging.warning(f"Строка некорректная: {row}")

    if not records:
        logging.warning("Нет данных для агрегации")
        return

    date = records[0]["rate_date"]
    avg = sum(r["value_per_1"] for r in records) / len(records)

    # 1) среднее значение
    path_avg_name = f"{PATH_NAME}/avg_analytics_{date}.txt"
    if not os.path.exists(path_avg_name):
        with open(path_avg_name, "w", encoding="utf-8") as file:
            file.write(f"{avg:.6f}")

    # 2) top-5 CSV
    top5 = sorted(records, key=lambda x: x["value_per_1"], reverse=True)[:5]
    path_top_name = f"{PATH_NAME}/top_analytics_{date}.csv"

    if not os.path.exists(path_top_name):
        with open(path_top_name, "w", newline="", encoding="utf-8") as file:
            fieldnames = ["name", "value_per_1"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for r in top5:
                writer.writerow({"name": r["name"], "value_per_1": r["value_per_1"]})

    logging.info("Аналитический слой выполнен")