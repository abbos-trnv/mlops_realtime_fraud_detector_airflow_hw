# Подтверждение проверки ДЗ4

Успешный CI-прогон от 2 ноября 2026:
https://github.com/abbos-trnv/mlops_realtime_fraud_detector_airflow_hw/actions/runs/41515977359

- `build-ci.txt`: dbt build, 87/87 успешных шагов (7 моделей, 76 data tests, 4 unit tests).
- `test-ci.txt`: dbt test, 80/80 успешных тестов.
- В том же workflow успешно завершились SQLFluff и dbt docs generate.
- `dbt-docs-lineage.png`: скриншот настоящего dbt Docs, открытого из manifest/catalog этого прогона.

В CI используется детерминированный синтетический набор из 400 строк.
Полный Kaggle train.csv (786431 строка) загружен локально в ClickHouse.
Локальный полный build прерывался из-за сбоев Docker Engine; его нельзя считать успешным.
CSV и персональная конфигурация в Git не публикуются.
