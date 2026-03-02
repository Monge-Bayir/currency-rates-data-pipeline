# Currency Rates Data Pipeline (CBR)

Data pipeline для загрузки, трансформации и аналитики курсов валют ЦБ РФ  
с использованием **Python, PostgreSQL и Apache Airflow**.

## Архитектура

Источник данных → Raw → Processed → Analytics → PostgreSQL  
Оркестрация: Apache Airflow (TaskFlow API)

## Технологии
- Python 3.12
- Apache Airflow 2.9
- PostgreSQL 15
- Docker & Docker Compose
- CSV / JSON
- GitHub

## Структура проекта
```
├── airflow/  
│   ├── dags/
|       ├── pipeline_dag.py 
│   │   └── cbr_pipeline_taskflow.py  
│   └── docker-compose.yml  
├── src/  
│   ├── extract.py  
│   ├── transform.py  
│   ├── analytics.py
|   ├── migrate.py 
│   └── load.py  
├── data/  
│   ├── raw/  
│   ├── processed/  
│   └── analytics/  
├── sql/  
│   ├── create_tables.sql  
│   └── quality_checks.sql  
├── requirements.txt  
└── README.md
```

---

## Логика пайплайна

### Extract
- Загружает курсы валют ЦБ РФ
- Сохраняет данные в `data/raw` в формате JSON
- Идемпотентен: файл за дату не перезаписывается

---

### Transform
- Валидирует входные данные
- Рассчитывает `value_per_1 = value / nominal`
- Сохраняет результат в `data/processed` (CSV)
- Проверяет корректность числовых значений

---

### Analytics
- Считает среднее значение курса
- Формирует **TOP-5 валют** с максимальным `value_per_1`
- Результаты сохраняются в `data/analytics`:
  - `avg_analytics_<date>.txt`
  - `top_analytics_<date>.csv`

---

### Load (PostgreSQL)
- Загружает данные в таблицу `fact_rates`
- Использует `INSERT ... ON CONFLICT`
- Поддерживает повторные запуски без дубликатов
- Подключение через Airflow Connection `rates_pg`

---

## Airflow DAG

### pipeline_taskflow
- Реализован с использованием **TaskFlow API**
- Передача данных между задачами через **XCom** (пути к файлам)
- Используется `ds` из execution context
- Добавлены проверки качества данных перед загрузкой в БД

Цепочка задач:
## Запуск проекта

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```
  - Airflow UI: http://localhost:8080
  - Логин: admin / admin

## DAG
	  -	pipeline_taskflow
	  -	Extract → Transform → Load → Analytics 
	  -	Используется TaskFlow API
	  -	Передача данных между задачами через XCom (пути к файлам)

## Результат
	  -	CSV с обработанными курсами
	  -	Текстовый файл со средним значением
	  -	CSV с топ-5 валют
	  -	Данные готовы к загрузке в PostgreSQL

## Почему Airflow
	  -	batch-обработка
	  -	ретраи
	  -	идемпотентность
	  -	контроль качества данных
