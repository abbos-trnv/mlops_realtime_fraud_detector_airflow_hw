"""DAG регулярной публикации отчёта Data Quality."""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.dq.collect_metrics import collect_dq_metrics
from pipeline.dq.render_report import render_dq_report


@dag(
    dag_id="data_quality_report",
    description="Проверяет размеченную витрину и публикует HTML-отчёт",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=5),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "data-quality"],
)
def data_quality_report():
    """Связывает сбор DQ-метрик и публикацию HTML-отчёта."""
    metrics = task(collect_dq_metrics, task_id="collect_dq_metrics")()
    task(render_dq_report, task_id="render_dq_report")(metrics)


data_quality_report()
