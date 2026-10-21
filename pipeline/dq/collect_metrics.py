"""Сбор метрик качества данных из PostgreSQL."""

import json
import logging

from pipeline.db import get_conn
from pipeline.settings import DQ_REPORTS_DIR

logger = logging.getLogger(__name__)


def collect_dq_metrics() -> str:
    """Считает правила DQ для ``labeled_transactions`` и пишет JSON-снимок.

    Читает только витрину PostgreSQL; записывает ``dq_metrics.json`` в общий
    каталог отчётов. Повторный запуск заменяет снимок текущими значениями, не
    меняя витрину, поэтому безопасен при retry Airflow.
    """
    queries = {
        "row_count": "SELECT count(*) FROM labeled_transactions",
        "null_required": """
            SELECT count(*) FROM labeled_transactions
            WHERE transaction_time IS NULL OR merch IS NULL OR amount IS NULL OR target IS NULL
        """,
        "invalid_amount": """
            SELECT count(*) FROM labeled_transactions
            WHERE amount IS NULL OR amount <= 0 OR amount > 1000000
        """,
        "invalid_target": """
            SELECT count(*) FROM labeled_transactions
            WHERE target NOT IN (0, 1) OR target IS NULL
        """,
        "future_time": """
            SELECT count(*) FROM labeled_transactions
            WHERE transaction_time > CURRENT_TIMESTAMP + INTERVAL '1 day'
        """,
        "duplicate_rows": """
            SELECT COALESCE(sum(n - 1), 0) FROM (
                SELECT transaction_time, merch, amount, target, count(*) AS n
                FROM labeled_transactions
                GROUP BY transaction_time, merch, amount, target
                HAVING count(*) > 1
            ) grouped
        """,
        "class_balance": """
            SELECT COALESCE(avg(target::float), 0) FROM labeled_transactions
        """,
    }
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            metrics = {}
            for name, query in queries.items():
                cur.execute(query)
                metrics[name] = cur.fetchone()[0]
    finally:
        conn.close()

    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = DQ_REPORTS_DIR / "dq_metrics.json"
    path.write_text(json.dumps(metrics, default=str, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("DQ-снимок записан: %s", path)
    return str(path)
