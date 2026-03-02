import requests
import logging
import datetime
import os


logging.basicConfig(level=logging.INFO)

URL = 'https://www.cbr-xml-daily.ru/daily_json.js'
RAW_DIR = "/opt/project/data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

def extract_bank():
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status() #начинает работать, если запрос успешный
    except requests.RequestException:
        logging.error('Connection error from extract')
        return

    path_name = f"{RAW_DIR}/raw_bank_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    if not os.path.exists(path_name):
        with open(path_name, 'w', encoding='utf-8') as f:
            f.write(response.text)
    else:
        logging.warning('Этот файл уже существует')

    logging.info(f"Файл {path_name} загружен")
    return path_name


if __name__ == '__main__':
    extract_bank()