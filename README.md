# Currency Rates Data Pipeline (CBR)

Data pipeline для загрузки, обработки и аналитики курсов валют Центрального банка РФ  
с использованием **Python**, **PostgreSQL**, **Apache Airflow** и **Apache Superset**.

Проект демонстрирует полный цикл batch-обработки данных:
от источника → до аналитического слоя, витрин и BI-дашборда.

---

## Архитектура

Источник данных (ЦБ РФ)  
→ Raw JSON  
→ Processed CSV  
→ PostgreSQL (`fact_rates`)  
→ Datamart Views (`dm_currency_rates`, `dm_currency_rates_top`)  
→ Apache Superset Dashboard  

Дополнительный аналитический слой:
Processed CSV → `data/analytics` (avg / top-5)

Оркестрация: **Apache Airflow**

---

## Технологии

- Python 3.12
- Apache Airflow 2.9
- PostgreSQL 15
- Apache Superset
- Docker & Docker Compose
- CSV / JSON
- GitHub

---

## Структура проекта

```text
├── airflow/
│   ├── dags/
│   │   ├── pipeline_dag.py               # DAG на BashOperator
│   │   └── cbr_pipeline_taskflow.py      # DAG на TaskFlow API
│   └── docker-compose.yml
│
├── src/
│   ├── extract.py                        # Extract слой
│   ├── transform.py                      # Transform слой
│   ├── analytics.py                      # Analytics слой
│   ├── migrate.py                        # SQL-модуль для создания/обновления схемы
│   └── load.py                           # Load в PostgreSQL
│
├── data/
│   ├── raw/                              # Raw JSON файлы
│   ├── processed/                        # Processed CSV
│   └── analytics/                        # avg/top-5 артефакты
│
├── sql/
│   ├── 01_create_fact_rates.sql
│	├──	02_quality_checks.sql
│   ├── 03_datamart_currency_rates.sql
│   ├── 04_datamart_top_rates.sql
│   ├── 05_datamart.sql
│   └── 06_top_rates.sql
│
├── requirements.txt
└── README.md

```
---

## Логика пайплайна
---
### Extract
	•	Загружает курсы валют ЦБ РФ
	•	Сохраняет данные в data/raw в формате JSON
	•	Идемпотентен: файл за дату не перезаписывается
---

### Transform
	•	Валидирует входные данные
	•	Рассчитывает value_per_1 = value / nominal
	•	Сохраняет результат в data/processed (CSV)
	•	Проверяет корректность числовых значений
---

### Load (PostgreSQL)
	•	Загружает данные в таблицу fact_rates
	•	Использует INSERT ... ON CONFLICT
	•	Поддерживает повторные запуски без дубликатов
	•	Подключение через Airflow Connection rates_pg
---

### Datamart
	•	После загрузки в PostgreSQL DAG создаёт SQL-витрины:
	•	dm_currency_rates
	•	dm_currency_rates_top
	•	Эти витрины используются как BI-ready слой для визуализации
---

### Analytics
	•	Считает среднее значение курса
	•	Формирует TOP-5 валют с максимальным value_per_1
	•	Сохраняет результаты в data/analytics:
	•	avg_analytics_<date>.txt
	•	top_analytics_<date>.csv
---
### BI Layer
	•	На основе datamart views построен дашборд в Apache Superset
	•	Дашборд включает:
	•	средний курс валют
	•	топ-5 валют по курсу
	•	таблицу актуальных курсов

---

## Airflow DAG

### pipeline_taskflow
- Реализован с использованием **TaskFlow API**
- Передача данных между задачами через **XCom** (пути к файлам)
- Используется `ds` из execution context
- Добавлены проверки качества данных перед загрузкой в БД

Цепочка задач:
extract → transform → check_data → migrate → load_to_postgres → build_datamart → analytics

## Запуск проекта

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```
  - Airflow UI: http://localhost:8080
  - Логин: admin / admin
  - Superset login
  - admin / admin

## DAG
	  -	pipeline_taskflow
	  -	Extract → Transform → Load → Analytics 
	  -	Используется TaskFlow API
	  -	Передача данных между задачами через XCom (пути к файлам)

### Результат работы пайплайна
	•	Raw JSON с курсами валют
	•	Processed CSV с рассчитанным value_per_1
	•	PostgreSQL таблица fact_rates
	•	SQL-витрины для BI
	•	Файловый аналитический слой (avg, top-5)
	•	Дашборд в Apache Superset

## Почему Airflow
	  -	batch-обработка
	  -	ретраи
	  -	идемпотентность
	  -	контроль качества данных

### Почему Superset
	•	позволяет быстро подключить BI-слой к PostgreSQL
	•	работает поверх витрин данных
	•	подходит для построения дашбордов и мониторинга метрик

### Дашборд
<img width="1351" height="652" alt="Снимок экрана 2026-03-25 в 18 21 20" src="https://github.com/user-attachments/assets/05ea4f84-be1d-4545-9133-2e3b363aee8b" />

