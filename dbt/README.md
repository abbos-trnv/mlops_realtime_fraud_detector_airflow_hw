# ДЗ4 Аналитика транзакций в dbt и ClickHouse

Проект строит staging и шесть витрин из `transactions_db.transactions`.
Исходные данные — `train.csv` соревнования teta-ml-1-2025.
Для самостоятельного воспроизведения есть отдельный ClickHouse и загрузчик CSV.
Это bootstrap источника для ДЗ4; реализация Kafka-ingestion из ДЗ2 в него не входит.
Если таблица из ДЗ2 уже существует, достаточно указать свой ClickHouse через переменные окружения.

## Запуск на Windows

Нужны Docker Desktop с Linux Engine и Python 3.12, доступный через `py -3.12`.
Все команды ниже выполняются из корня репозитория в PowerShell.

```powershell
py -3.12 -m venv .venv-dbt
.\.venv-dbt\Scripts\python.exe -m pip install -r requirements-dbt.txt
docker compose -f compose.dbt.yaml up -d --wait
.\.venv-dbt\Scripts\python.exe scripts/load_dbt_data.py --csv data/train.csv
cd dbt
..\.venv-dbt\Scripts\dbt.exe deps --profiles-dir .
..\.venv-dbt\Scripts\dbt.exe debug --profiles-dir .
..\.venv-dbt\Scripts\dbt.exe build --profiles-dir .
..\.venv-dbt\Scripts\dbt.exe test --profiles-dir .
..\.venv-dbt\Scripts\dbt.exe docs generate --profiles-dir .
..\.venv-dbt\Scripts\dbt.exe docs serve --profiles-dir . --host 127.0.0.1 --port 8082
```

Документация откроется на http://localhost:8082; кнопка графа показывает lineage.
`build` создаёт модели и выполняет data tests и unit tests в порядке зависимостей.
Проверки можно повторять: модели полностью пересоздаются, а загрузчик не дописывает данные в непустой raw.
Если первоначальный импорт был прерван, проверьте число строк: загрузчик не объявляет частичную загрузку полной.
Для повторного импорта сначала осознанно очистите только учебную raw-таблицу.

Есть сокращённые команды `./scripts/dbt.ps1 setup`, `load`, `build`, `test`, `docs`, `serve`, `lint`.
Makefile содержит те же цели и выбирает Windows/Linux-пути автоматически.

## Подключение и ресурсы

ClickHouse HTTP доступен на `localhost:18123`, пользователь `dbt`, локальный пароль `local_dbt_password`.
Порт привязан к localhost. Эти значения предназначены для локального учебного запуска.
В `dbt/profiles.yml` поддерживаются `DBT_HOST`, `DBT_PORT`, `DBT_USER`, `DBT_PASSWORD`.
Загрузчик дополнительно читает `CLICKHOUSE_URL`.
Новый Compose использует отдельный проект `antifraud-dbt`, отдельный volume, лимиты 2 CPU / 2 ГБ RAM.
Он не запускает Airflow, Kafka и старые сервисы. dbt использует один поток.
Настройки ClickHouse ограничивают память сервера 1 ГБ, запросов 512 МБ и выполнение двумя потоками.

Для остановки без удаления данных:

```powershell
docker compose -f compose.dbt.yaml stop
```

## Модели и бизнес-правила

`raw.transactions` → `stg_transactions` → шесть витрин:

- `mart_daily_state_metrics`: дата и штат; count, оборот, средний чек, точный P95, процент крупных транзакций.
- `mart_fraud_by_category`: категория; count, fraud_count, fraud_rate, оборот и сумма фрода.
- `mart_fraud_by_state`: штат; fraud_rate, уникальные клиенты и мерчанты, оборот и сумма фрода.
- `mart_customer_risk_profile`: клиент; первая/последняя транзакция, count, средний чек, fraud_rate и риск.
- `mart_hourly_fraud_pattern`: ISO день недели и час UTC; count, fraud_rate, суммы и средний чек.
- `mart_merchant_analytics`: мерчант; оборот, count, fraud_rate и флаг подозрительности.

Все доли выражены в процентах (0–100). Макрос `amount_bucket`: SMALL <100,
MEDIUM от 100 включительно до 500, LARGE от 500 включительно.
Макрос `risk_level`: HIGH при fraud_rate >=5%, MEDIUM >=1%, иначе LOW.
Подозрительный мерчант: не менее 20 транзакций и fraud_rate >=5%.
Порог минимального числа операций снижает влияние единичных покупок.
Риск описывает историческую разметку; использовать его как прогноз без временного разделения нельзя.

В исходнике нет постоянного customer_id. Используется MD5 от имени, фамилии, улицы и штата
через `dbt_utils.generate_surrogate_key`. Это эвристика идентичности: один клиент может сменить адрес,
а разные люди могут иметь одинаковые поля. Прямые ФИО и улица не выводятся в витрины.
Staging сохраняет все исходные строки; произвольная дедупликация могла бы удалить настоящие покупки.
Даты интерпретируются в UTC. ISO день недели вычисляется через `dbt_date`.

## Тесты и качество

Все модели и колонки описаны в `models/schema.yml`, источник — в `models/sources/sources.yml`.
Стандартные тесты: not_null, unique, accepted_values.
Составные ключи дневной и часовой витрин проверяет `dbt_utils.unique_combination_of_columns`.
`dbt_expectations.expect_table_row_count_to_be_between` проверяет непустой staging.
Singular tests проверяют неотрицательные конечные суммы, границы fraud_rate,
согласованность количества строк в витринах и непревышение суммы/числа фродов над общими значениями.
Четыре unit tests из `models/unit_tests.yml` проверяют границы суммы, нормализацию, агрегацию категорий, разделение штатов,
долю крупных операций и точные границы риска 1%/5% на маленьких заданных наборах.

## Линтинг

```powershell
cd dbt
..\.venv-dbt\Scripts\sqlfluff.exe lint models tests --config .sqlfluff
cd ..
.\.venv-dbt\Scripts\pre-commit.exe install
.\.venv-dbt\Scripts\pre-commit.exe run --all-files
```

SQLFluff использует диалект ClickHouse и dbt templater, которому нужны `dbt deps` и работающий ClickHouse.
Версии закреплены в `requirements-dbt.txt` и `packages.yml`.
Правило регистра функций отключено: имена функций ClickHouse регистрозависимы и используют camelCase.
Для `quantileExact(0.95)(amount)` добавлен локальный `noqa` из-за ограничения парсера SQLFluff 3.4.2;
само выражение проверяется выполнением в ClickHouse.

## Автоматическая проверка

Workflow `.github/workflows/dbt.yml` поднимает чистый ClickHouse, создаёт raw-таблицу,
загружает 400 детерминированных синтетических записей и выполняет `build`, `test`, SQLFluff и генерацию Docs.
Синтетический набор предназначен только для CI. Локальная проверка выполняется на полном `train.csv`.
Артефакт `dbt-validation-and-docs` в GitHub Actions содержит логи и сгенерированную документацию.

## Что приложить к PR

Каталог `dbt/`, DDL, Compose, загрузчик, конфиги линтинга и Makefile.
В описании PR укажите dbt-core 1.9.8, dbt-clickhouse 1.9.5 и версии из `dbt --version`.
Приложите фактический вывод успешного `dbt test` и скриншот графа dbt Docs.
Артефакты выполнения находятся в `dbt/target/run_results.json`, `manifest.json`, `catalog.json`;
они генерируются локально и исключены из Git. В папку `evidence/` сохраняются результаты проверенного запуска.
Список реализованных витрин и проверок приведён выше.

Результаты успешного CI-прогона и скриншот графа: [evidence/README.md](evidence/README.md).
Полный локальный Kaggle-набор: 786431 строка; все 80 тестов прошли.
Фактический лог: [evidence/test-local.txt](evidence/test-local.txt).
