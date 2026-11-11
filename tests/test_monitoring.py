"""Focused regression tests for acknowledgement ordering and actual model reload accounting."""
import importlib.util
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
SERVICE = ROOT / 'fraud_detector' if (ROOT / 'fraud_detector').is_dir() else ROOT
sys.path.insert(0, str(SERVICE / 'src'))
spec = importlib.util.spec_from_file_location("scoring_app", SERVICE / 'app/app.py')
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)
import pandas as pd
import scorer


class ScoringTests(unittest.TestCase):
    def service(self, delivery_error=None):
        service = app.ProcessingService.__new__(app.ProcessingService)
        service.consumer = Mock()
        service.producer = Mock()
        service.output_topic = "scoring"
        service.producer.flush.return_value = 0
        def produce(*args, **kwargs):
            kwargs["on_delivery"](delivery_error, None)
        service.producer.produce.side_effect = produce
        return service

    def message(self):
        message = Mock()
        message.value.return_value = json.dumps({
            "transaction_id": "test-id",
            "data": {"us_state": "CA", "merch": "Shop", "cat_id": "food", "amount": 100},
        }).encode()
        return message

    @patch.object(app, "recorded")
    @patch.object(app, "make_pred")
    @patch.object(app, "run_preproc")
    def test_dimensions_and_metrics_after_ack(self, preprocess, predict, recorded):
        predict.return_value = (pd.DataFrame({"score": [.7], "fraud_flag": [1]}), [.7], "v2")
        service = self.service()
        self.assertTrue(service.process_message(self.message()))
        output = json.loads(service.producer.produce.call_args.kwargs["value"])[0]
        self.assertEqual((output["us_state"], output["merch"], output["cat_id"], output["model_version"]), ("CA", "Shop", "food", "v2"))
        recorded.assert_called_once()
        service.consumer.commit.assert_called_once()

    @patch.object(app, "recorded")
    @patch.object(app, "make_pred")
    @patch.object(app, "run_preproc")
    def test_no_success_or_commit_on_delivery_failure(self, preprocess, predict, recorded):
        predict.return_value = (pd.DataFrame({"score": [.7], "fraud_flag": [1]}), [.7], "v2")
        service = self.service("broker failed")
        with self.assertRaises(RuntimeError):
            service.process_message(self.message())
        recorded.assert_not_called()
        service.consumer.commit.assert_not_called()

    def test_count_actual_load_once_and_do_not_replace_on_failure(self):
        with patch.object(scorer, "_model", None), patch.object(scorer, "_version", None), patch.object(scorer, "_read_latest_version", return_value="v1") as latest, patch.object(scorer, "_load_model", return_value=object()) as load, patch.object(scorer, "model_loaded") as loaded:
            scorer._current_model()
            scorer._current_model()
            self.assertEqual(load.call_count, 1)
            latest.return_value = "v2"
            scorer._current_model()
            self.assertEqual(loaded.call_count, 2)
            self.assertEqual(loaded.call_args.args, ("v2",))
            active = scorer._model
            latest.return_value = "broken"
            load.side_effect = ValueError("corrupt file")
            with self.assertRaises(ValueError):
                scorer._current_model()
            self.assertIs(scorer._model, active)
            self.assertEqual(scorer._version, "v2")


if __name__ == "__main__":
    unittest.main()
