# BigDataSpark

## Лабораторная работа №2

**Анализ больших данных — реализация ETL-пайплайна и аналитических витрин с использованием Apache Spark и ClickHouse**

Выполнил студент группы М8О-214СВ-24

Красавин М.А.

---

## Исходные данные

В качестве источника использовались CSV-файлы `MOCK_DATA*.csv`:

* всего **10 файлов**
* по **1000 строк** в каждом
* итоговый объём — **10000 записей**

Каждая запись содержит информацию о продаже:

- товар;
- покупатель;
- магазин;
- поставщик;
- дата продажи;
- цена, количество, рейтинг и отзывы.

---

## Архитектура решения

Решение развернуто с использованием Docker Compose и включает следующие компоненты:

- **PostgreSQL** — хранение staging-слоя и аналитической модели «звезда»;
- **Apache Spark** — реализация ETL и построение витрин;
- **ClickHouse** — хранение аналитических витрин (marts).

### Слои данных

- `stg` — staging-слой (сырые данные);
- `dw` — аналитическая модель данных (звезда);
- `marts` — аналитические витрины в ClickHouse.

---

## Модель данных в PostgreSQL

### Таблица фактов

- `dw.fact_sales`
  - количество продаж;
  - сумма продаж;
  - ссылки на таблицы измерений.

### Таблицы измерений

- `dw.dim_customer`;
- `dw.dim_product`;
- `dw.dim_store`;
- `dw.dim_supplier`;
- `dw.dim_date`.

Модель реализована в виде схемы «звезда», где таблица фактов связана с измерениями по surrogate-ключам.

---

## ETL-пайплайн

### 1. Загрузка данных в staging

CSV-файлы загружаются в таблицу `stg.mock_data`.

Проверка загрузки данных:

```sql
SELECT COUNT(*) FROM stg.mock_data;
```

---

### 2. ETL: STG → DW (Apache Spark)

Spark-приложение:

```
spark/jobs/01_etl_to_star.py
```

Основные шаги:

1. чтение данных из PostgreSQL (stg.mock_data);
2. формирование таблиц измерений;
3. заполнение таблицы фактов `dw.fact_sales`.

Запуск Spark-job:

```bash
docker exec -it bigdata_spark spark-submit \
  --jars /home/jovyan/work/jars/postgresql.jar,/home/jovyan/work/jars/clickhouse.jar \
  /home/jovyan/work/jobs/01_etl_to_star.py
```

Проверка результата:

```sql
SELECT COUNT(*) FROM dw.fact_sales;
SELECT COUNT(*) FROM dw.dim_customer;
SELECT COUNT(*) FROM dw.dim_product;
```

---

## Аналитические витрины в ClickHouse

DDL-скрипт создания витрин:

```
clickhouse/00_ddl_marts.sql
```

### Реализованные витрины

1. **mart_products** — витрина продаж по продуктам;
2. **mart_customers** — витрина продаж по клиентам;
3. **mart_time** — витрина продаж по времени;
4. **mart_stores** — витрина продаж по магазинам;
5. **mart_suppliers** — витрина продаж по поставщикам;
6. **mart_quality** — витрина качества продукции.

---

### Загрузка витрин (Apache Spark)

Spark-приложение:

```
spark/jobs/02_marts_to_clickhouse.py
```

Запуск:

```bash
docker exec -it bigdata_spark spark-submit \
  --jars /home/jovyan/work/jars/postgresql.jar,/home/jovyan/work/jars/clickhouse.jar \
  /home/jovyan/work/jobs/02_marts_to_clickhouse.py
```

---

## Проверка результатов (ClickHouse)

```sql
USE marts;

SELECT * FROM mart_products  ORDER BY revenue DESC LIMIT 10;
SELECT * FROM mart_customers ORDER BY revenue DESC LIMIT 10;
SELECT * FROM mart_time      ORDER BY year, month;
SELECT * FROM mart_stores    ORDER BY revenue DESC LIMIT 5;
SELECT * FROM mart_suppliers ORDER BY revenue DESC LIMIT 5;
SELECT * FROM mart_quality   ORDER BY rating DESC, reviews DESC LIMIT 10;
```

Наличие данных в таблицах подтверждает корректность работы ETL-пайплайна и аналитических витрин.

---

## Используемые технологии

- Apache Spark 3.5;
- PostgreSQL 16;
- ClickHouse 24;
- Docker / Docker Compose;
- SQL, PySpark.

---

## Итог

В ходе лабораторной работы был реализован полный ETL-пайплайн с использованием Apache Spark, данные преобразованы в аналитическую модель «звезда» в PostgreSQL и построены 6 аналитических витрин в ClickHouse. Решение соответствует требованиям лабораторной работы и готово к расширению дополнительными NoSQL СУБД (Cassandra, MongoDB, Neo4j, Valkey).
