# ДЗ5 Мониторинг потока фродовых транзакций

Проект наблюдает непрерывный поток Kafka, CPU-скоринг CatBoost, запись результатов
в PostgreSQL и горячую загрузку моделей. Конфигурации Prometheus, четыре дашборда
Grafana и три правила Grafana Alerting разработаны для этого проекта и хранятся
в `monitoring/`. JSON из семинара не копировались.

## Запуск в PowerShell

Нужны Docker Desktop с Linux Engine и размеченный `train.csv` из прежних ДЗ.
Из корня репозитория:

```powershell
Copy-Item .env.example .env
# В .env укажите существующий абсолютный путь, например:
# TRAIN_DATA_CSV="D:/antifraud/data/train.csv"
docker compose up --build -d
docker compose ps
```

При существующем .env сохраняйте его и проверьте только путь к CSV.
Новые параметры имеют значения по умолчанию и не требуют перезаписи .env.
Первая сборка скачивает CatBoost и образы сервисов. Поток начинается автоматически.
В старых учебных образах Kafka/ZooKeeper отключено определение ресурсов через
cgroup JVM; heap задан явно. Это устраняет падение Java на современных Linux-хостах.
Kafka healthcheck проверяет порт без запуска дополнительной JVM каждые несколько
секунд; kafka-setup отдельно проверяет API брокера и создаёт топики до старта потока.
Симулятор читает первые 20000 строк перемешанного CSV и бесконечно выбирает из них
транзакции, присваивая новый transaction_id и текущее время UTC. По умолчанию интервал
0.5 секунды, чтобы не перегружать ноутбук; это настраивается SEND_INTERVAL_SECONDS.
Поля score и fraud_flag по-прежнему вычисляет настоящая модель.

Сервисы:
- Grafana: http://localhost:3000, `admin / local_grafana_admin`.
- Prometheus: http://localhost:9090, targets: http://localhost:9090/targets.
- Метрики скоринга: http://localhost:8000/metrics.
- Метрики записи: http://localhost:8001/metrics.
- Airflow: http://localhost:8081, `admin / admin` (после запуска профиля training).
- Kafka UI: http://localhost:8080 (необязательный профиль tools).

Это локальные учебные пароли. Настроить другие можно через GRAFANA_ADMIN_USER,
GRAFANA_ADMIN_PASSWORD, GRAFANA_DB_USER, GRAFANA_DB_PASSWORD в .env.
Grafana создаёт пароль admin только при первом запуске с пустым volume:
изменение переменной не меняет пароль уже созданного пользователя.
Пользователь PostgreSQL `grafana_ro` создаётся writer автоматически и имеет только
SELECT на scores; это не административный пользователь БД.

## Автоматическая настройка

Prometheus каждые 1 секунду собирает три target: scorer, writer и node.
Grafana при старте подключает источники `fraud-prom` (Prometheus) и
`fraud-postgres` (PostgreSQL), четыре дашборда в папке Fraud monitoring
и три правила Alerting. Ручной импорт JSON и добавление источников не нужны.
UI обновляет графики каждые 5 секунд, по умолчанию показаны последние 15 минут.
Для rate и percentile дождитесь хотя бы 30 секунд после запуска потока.

По умолчанию запускается только поток с мониторингом. Переобучение доступно
через профиль training; запускать тяжёлый Airflow постоянно необязательно.
Все сервисы находятся в сети ml-scorer. Данные моделей, PostgreSQL, Prometheus
и Grafana хранятся в volumes. `docker compose stop` останавливает без потери данных.
`down -v` удаляет данные — для обычной остановки его использовать не нужно.

## Дашборды

1. `fraud-scoring` — **01 • Поток и модели**:
   TPS подтверждённых Kafka-скоров, p95 обработки, доля фрода,
   heatmap распределения скоров во времени, задержка от отправки до готового
   скора, ошибки и время с последнего успешного события.
   Аннотации показывают фактические загрузки файлов моделей и их версии.
2. `fraud-writer` — **02 • Запись результатов**:
   TPS committed-строк, p95 DB-транзакции, количество помеченного фрода,
   heatmap записанных скоров, ошибки и повторные доставки без дублей,
   разница между скоростью скоринга и записи.
3. `fraud-system` — **03 • Система Linux Docker host**:
   CPU, память, сетевой трафик, заполнение файловой системы и дисковый I/O.
   На Windows Node Exporter наблюдает Linux-среду Docker Desktop/WSL,
   а не память и диски Windows напрямую. Сетевые счётчики относятся
   к сетевому пространству Node Exporter в общей Docker-сети.
4. `fraud-business` — **04 • Бизнес и сегменты**, источник PostgreSQL:
   количество скоров и fraud_rate по версии модели, плотность распределения
   скоров и TPS с фильтрами **Штат / Мерчант**, доля фрода по категориям
   в последних 1000 записанных транзакциях, сумма операций, помеченных
   как фрод, для выбранного сегмента.

Плотность: 20 интервалов шириной 0.05, высота равна count/(N*0.05);
score=1 включается в последний интервал. Интеграл гистограммы равен 1.
Это гистограммная оценка плотности, а не KDE. Для пустого выбранного сегмента
данных закономерно нет.
TPS PostgreSQL измеряется по времени записи, а не по времени исходной покупки.
Данные фильтруются через SQL-экранирование Grafana `:sqlstring`; поддерживаются
несколько значений и All. Список мерчантов зависит от выбора штата.
Барплот последних 1000 берёт глобально последние 1000 строк по id до
агрегации по cat_id; это явно указано в заголовке, он не зависит от фильтров.
fraud_flag — решение модели, а не истинная метка target: панели не измеряют
precision/recall и не утверждают, что отмеченная сумма равна фактическим потерям.
Дополнительный бизнес-график суммы помогает оценивать объём средств,
направляемых на ручную проверку, при одинаковой доле фрода.

## Метрики

Скоринг:
- `fraud_scored_transactions_total`: число скоринговых результатов, подтверждённых Kafka.
- `fraud_scored_fraud_total`: из них с fraud_flag=1.
- `fraud_scoring_seconds`: Histogram полной обработки (preprocess, inference, Kafka ack).
- `fraud_scoring_score`: Histogram скоров 0..1 с шагом 0.05.
- `fraud_scoring_fraud_ratio`: накопленная доля фрода с запуска процесса;
  на дашборде используется доля за последние 30 секунд по rate counters.
- `fraud_scoring_errors_total`: ошибки обработки или отправки.
- `fraud_scoring_last_success_timestamp_seconds`: время последнего подтверждённого результата.
- **`fraud_event_age_seconds`**: собственная Histogram времени от emitted_at
  симулятора до готового подтверждённого скора. Её нет в семинарском скорере.
  На панели «От отправки события до скора — p95» видна задержка очереди,
  которая не видна из одного только времени inference.
- `fraud_model_reload_total{version}`: счётчик успешных фактических загрузок файла модели.
- `fraud_model_loaded_at_seconds{version}`: момент загрузки; используется аннотацией.
- `fraud_model_load_errors_total`: неудачные попытки загрузить модель.

Writer:
- `fraud_writer_scores_total`, `fraud_writer_fraud_total`: новые committed-строки/фрод.
- `fraud_writer_seconds`: Histogram времени записи до commit.
- `fraud_writer_score`: Histogram committed-скоров.
- `fraud_writer_errors_total`: ошибки декодирования/записи.
- `fraud_writer_duplicates_total`: повторные доставки, уже записанные в БД.
- `fraud_writer_last_success_timestamp_seconds`: время последней новой committed-строки.

Histogram автоматически экспортирует суффиксы _bucket, _count, _sum.
Классификатор использует порог score >0.5. Личные поля, transaction_id и мерчанты
не используются как Prometheus labels. Версии есть только в метриках загрузки.
Counters сбрасываются при рестарте процесса; Grafana использует rate, учитывающий reset.

## Сохранение бизнес-полей и повторы

В Kafka-результат добавлены необязательные us_state, merch, cat_id, amount,
event_time, scored_at; прежние transaction_id, score, fraud_flag и model_version
сохранены. Writer добавляет колонки и индексы без удаления существующих данных.
В старых строках новые поля могут быть NULL.
Offsets фиксируются вручную: scorer — после Kafka delivery ack, writer — после
commit в PostgreSQL. Ошибка останавливает процесс до продвижения offset.
При повторной доставке writer использует transaction_id и транзакционную advisory lock,
поэтому новые повторы не создают дубликатов и не увеличивают writer-success counters.
Исторические дубли из старой версии автоматически не удаляются.

## Проверка живого потока и панелей

```powershell
docker compose logs --tail 30 fraud_detector scoring_writer
# В Python-окружении проекта (дополнительные библиотеки не нужны):
.\.venv-dbt\Scripts\python.exe scripts/check_monitoring.py
```

Можно использовать обычный `python scripts/check_monitoring.py`.
Проверка ждёт targets UP, рост счётчика записи и выполняет запросы каждой панели,
включая SQL-панели через Grafana API; проверяет provisioning правил алертов.
Дважды откройте /metrics или используйте Prometheus expression
`rate(fraud_scored_transactions_total[30s])`: при живом потоке значение >0.
В PostgreSQL новые записи содержат бизнес-поля и model_version.

## Переобучение и отметка версии

```powershell
docker compose --profile training up --build -d airflow
```

Профиль загрузит размеченную витрину и стартовое окно. Откройте Airflow,
запустите DAG `retrain_catboost` кнопкой Trigger и дождитесь успешной публикации.
DAG по-прежнему имеет расписание */5 и catchup=False; по умолчанию при создании
он paused для снижения нагрузки. Включите переключатель DAG для запуска расписания.

На следующем событии scorer читает latest.txt и загружает новый .cbm.
Counter с новой version увеличивается только после успешной загрузки,
а Grafana показывает отметку «Модель <version>» примерно 30 секунд.
Контейнер скоринга перезапускать не нужно. В таблице scores появляются новые строки
с той же версией. Неудачная загрузка увеличит счётчик ошибок; метка успешной смены не появится.

Автоматическая проверка настоящего DAG и версии:
```powershell
python scripts/check_model_reload.py
```
Она выполняет все задачи реального DAG через airflow dags test, проверяет новую
версию, метрики загрузки и новые записи PostgreSQL, а также неизменность id контейнера.
Лог задач сохраняется в monitoring/evidence/retrain.log.
После демонстрации Airflow можно остановить отдельно:
`docker compose --profile training stop airflow`.

## Grafana Alerting

Правила провизионятся в Alerting → Alert rules, папка Fraud monitoring,
проверяются каждые 10 секунд. Доставка в Telegram/почту не настроена и не требуется.
- **fraud-flow-stopped**: нет подтверждённого скора >30 секунд или scorer недоступен;
  условие должно держаться ещё 30 секунд.
- **fraud-high-ratio**: доля фрода >20% за последнюю минуту, for=30s.
- **fraud-slow-scoring**: p95 полной обработки >0.5 сек за минуту, for=1m.
Для ratio/latency отсутствие наблюдений не считается фродом/задержкой;
остановку потока контролирует отдельное правило. Ошибки запросов видны как Error.

Проверка остановки потока с автоматическим восстановлением:
```powershell
python scripts/check_flow_alert.py
```
Скрипт pause-ит только transaction_simulator, ждёт обработки backlog и Firing,
сохраняет подтверждение и обязательно делает unpause.
Вручную: pause симулятора, подождать 60–120 секунд, увидеть Firing, unpause.

Проверка высокой доли фрода на реальном скоринге:
```powershell
docker compose pause transaction_simulator
docker compose run --rm --no-deps transaction_simulator python send_monitoring_probe.py --fraud-only --count 600 --interval 0.1
docker compose unpause transaction_simulator
```
В Alerting правило fraud-high-ratio должно перейти Pending → Firing,
если активная модель действительно помечает >20% выбранных fraud-строк.
Скрипт выбирает строки по истинной разметке target, но score всегда вычисляет модель.
Метрики и готовые флаги искусственно не подменяются.

## Проверки и сдача

- tests/test_monitoring.py: доставка/commit и фактическая смена версии модели.
- scripts/check_monitoring.py: три targets UP, рост потока, данные во всех панелях.
- scripts/check_model_reload.py: настоящая публикация из DAG и hot reload.
- scripts/check_flow_alert.py: реальный алерт остановки потока с восстановлением.
- .github/workflows/monitoring.yml: воспроизводимая проверка на синтетическом
  CSV из 6000 строк (только CI), настоящих Kafka, CatBoost, PostgreSQL, Prometheus,
  Grafana и Airflow. Ни метрики, ни ответы модели в интеграционной проверке не мокируются.

Результаты успешной интеграционной проверки сохранены в
[evidence/README.md](evidence/README.md). Проверены запросы всех 25 панелей через
настоящие Prometheus и Grafana API, публикация модели и алерт Firing.
Это проверка данных и конфигурации, а не визуальная проверка расположения панелей.
