"""Проверка кандидата перед публикацией в model registry."""

import json
import logging

from pipeline.settings import MODELS_DIR, MODEL_AUC_TOLERANCE

logger = logging.getLogger(__name__)


def evaluate_candidate(train_info: dict) -> dict:
    """Сравнивает ROC-AUC новой модели с последней опубликованной.

    Читает ``latest.txt`` и JSON с метриками активной версии из model registry.
    Ничего не записывает: решение передаётся в ``publish_model``. Кандидат
    разрешён к публикации, если активной модели ещё нет, метрики несопоставимы,
    либо ROC-AUC не ухудшился более чем на ``MODEL_AUC_TOLERANCE``. Повторный
    запуск детерминированен, поскольку он читает только опубликованные файлы.
    """
    result = dict(train_info)
    candidate_auc = train_info.get("roc_auc")
    latest_path = MODELS_DIR / "latest.txt"
    active_auc = None
    active_version = None

    if latest_path.exists():
        active_version = latest_path.read_text(encoding="utf-8").strip()
        metrics_path = MODELS_DIR / f"my_catboost_{active_version}.metrics.json"
        if metrics_path.exists():
            active_auc = json.loads(metrics_path.read_text(encoding="utf-8")).get("roc_auc")

    allowed = (
        active_version in (None, "", "initial")
        or active_auc is None
        or (candidate_auc is not None and candidate_auc >= active_auc - MODEL_AUC_TOLERANCE)
    )
    result.update(
        {
            "active_version": active_version,
            "active_roc_auc": active_auc,
            "publish_allowed": allowed,
            "publish_reason": (
                "Нет сопоставимой метрики активной модели"
                if active_auc is None
                else f"candidate_auc={candidate_auc:.4f}, active_auc={active_auc:.4f}, "
                f"tolerance={MODEL_AUC_TOLERANCE:.4f}"
            ),
        }
    )
    logger.info("Решение о публикации: %s. %s", allowed, result["publish_reason"])
    return result
