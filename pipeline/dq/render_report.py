"""Рендер HTML-отчёта по метрикам Data Quality."""

import html
import json
from datetime import datetime, timezone
from pathlib import Path

from pipeline.settings import DQ_REPORTS_DIR


def render_dq_report(metrics_path: str) -> str:
    """Публикует HTML-отчёт с шестью правилами DQ рядом с model registry.

    Читает JSON-снимок предыдущей задачи и записывает ``latest.html`` в общий
    каталог DQ. Не изменяет данные и модели; повторная попытка лишь заменяет
    отчёт новой версией, что не может испортить витрину или публикацию модели.
    """
    metrics = json.loads(Path(metrics_path).read_text(encoding="utf-8"))
    rows = [
        ("Непустая витрина", "row_count > 0", metrics["row_count"] > 0, metrics["row_count"]),
        ("Обязательные поля", "transaction_time, merch, amount, target заполнены", metrics["null_required"] == 0, metrics["null_required"]),
        ("Диапазон суммы", "0 < amount <= 1 000 000", metrics["invalid_amount"] == 0, metrics["invalid_amount"]),
        ("Целевая метка", "target принадлежит {0, 1}", metrics["invalid_target"] == 0, metrics["invalid_target"]),
        ("Время транзакции", "нет дат более чем на сутки в будущем", metrics["future_time"] == 0, metrics["future_time"]),
        ("Дубли", "нет точных дублей времени, merchant, суммы и target", metrics["duplicate_rows"] == 0, metrics["duplicate_rows"]),
    ]
    rendered_rows = "".join(
        "<tr><td>{}</td><td>{}</td><td class='{}'>{}</td><td>{}</td></tr>".format(
            html.escape(rule), html.escape(expectation), "ok" if passed else "fail",
            "PASS" if passed else "FAIL", html.escape(str(value))
        )
        for rule, expectation, passed, value in rows
    )
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report = f"""<!doctype html><html lang='ru'><head><meta charset='utf-8'>
<title>Data Quality report</title><style>body{{font-family:Arial;margin:32px}}table{{border-collapse:collapse}}td,th{{border:1px solid #ccc;padding:8px;text-align:left}}.ok{{color:#167c3a;font-weight:bold}}.fail{{color:#b42318;font-weight:bold}}</style>
</head><body><h1>Data Quality report</h1><p>Сформирован: {created_at}. Строк в витрине: {metrics['row_count']}. Доля fraud: {float(metrics['class_balance']):.4%}.</p>
<table><thead><tr><th>Правило</th><th>Ожидание</th><th>Статус</th><th>Нарушений / значение</th></tr></thead><tbody>{rendered_rows}</tbody></table>
</body></html>"""
    DQ_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    output = DQ_REPORTS_DIR / "latest.html"
    output.write_text(report, encoding="utf-8")
    return str(output)
