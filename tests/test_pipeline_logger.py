# tests/test_pipeline_logger.py
"""
Unit tests: structured pipeline logging (etl/pipeline_logger.py).

Coverage:
    - PipelineLogManager.log_event insert / read-back
    - PipelineLogManager.query_logs filtering by stage/status/scene_id
    - PipelineLogger.stage() context manager: COMPLETED and FAILED paths

Run:
    pytest tests/test_pipeline_logger.py -v
"""

from __future__ import annotations

import pytest


class TestLogEvent:
    def test_log_event_insert_and_read_back(self, plog, sample_dataset):
        log_id = plog.log_event(
            sample_dataset, "TEST_SCENE_1", "MODULE2_CROP", "CROP", "COMPLETED",
            "Cropped to study area", {"file_size_mb": 145.2},
        )
        assert log_id is not None

        logs, total = plog.query_logs(sample_dataset)
        assert total == 1
        assert logs[0]["log_id"] == log_id
        assert logs[0]["stage"] == "CROP"
        assert logs[0]["status"] == "COMPLETED"
        assert logs[0]["details"]["file_size_mb"] == 145.2

    def test_details_defaults_to_empty_dict(self, plog, sample_dataset):
        plog.log_event(sample_dataset, "TEST_SCENE_1", "ORCHESTRATOR", "SCENE_PIPELINE", "STARTED", "go")
        logs, _ = plog.query_logs(sample_dataset)
        assert logs[0]["details"] == {}


class TestQueryLogsFiltering:
    def _seed(self, plog, dataset_id):
        plog.log_event(dataset_id, "SCENE_A", "MODULE1_DOWNLOAD", "DOWNLOAD", "COMPLETED", "ok")
        plog.log_event(dataset_id, "SCENE_A", "MODULE2_CROP", "CROP", "COMPLETED", "ok")
        plog.log_event(dataset_id, "SCENE_B", "MODULE2_CROP", "CROP", "FAILED", "boom")

    def test_filter_by_stage(self, plog, sample_dataset):
        self._seed(plog, sample_dataset)
        logs, total = plog.query_logs(sample_dataset, stage="CROP")
        assert total == 2
        assert all(l["stage"] == "CROP" for l in logs)

    def test_filter_by_status(self, plog, sample_dataset):
        self._seed(plog, sample_dataset)
        logs, total = plog.query_logs(sample_dataset, status="FAILED")
        assert total == 1
        assert logs[0]["scene_id"] == "SCENE_B"

    def test_filter_by_scene_id(self, plog, sample_dataset):
        self._seed(plog, sample_dataset)
        logs, total = plog.query_logs(sample_dataset, scene_id="SCENE_A")
        assert total == 2
        assert all(l["scene_id"] == "SCENE_A" for l in logs)

    def test_limit_and_order(self, plog, sample_dataset):
        self._seed(plog, sample_dataset)
        logs, total = plog.query_logs(sample_dataset, limit=1, order="asc")
        assert total == 3
        assert len(logs) == 1
        assert logs[0]["stage"] == "DOWNLOAD"


class TestStageContextManager:
    def test_completed_path_logs_duration_and_output(self, db_client, sample_dataset, plog):
        from etl.pipeline_logger import PipelineLogger

        logger = PipelineLogger(db_client)
        with logger.stage(sample_dataset, "SCENE_A", "MODULE2_CROP", "CROP", "Cropping") as st:
            st.output(file_size_mb=12.3)

        logs, total = plog.query_logs(sample_dataset, stage="CROP")
        assert total == 2  # STARTED + COMPLETED
        completed = [l for l in logs if l["status"] == "COMPLETED"][0]
        assert completed["details"]["file_size_mb"] == 12.3
        assert completed["details"]["duration_seconds"] >= 0
        assert "memory_peak_mb" in completed["details"]

    def test_failed_path_logs_error_detail_and_reraises(self, db_client, sample_dataset, plog):
        from etl.pipeline_logger import PipelineLogger

        logger = PipelineLogger(db_client)
        with pytest.raises(ValueError):
            with logger.stage(sample_dataset, "SCENE_A", "MODULE9_FUSION", "FUSION", "Fusing"):
                raise ValueError("no S1 SILVER product found")

        logs, total = plog.query_logs(sample_dataset, stage="FUSION", status="FAILED")
        assert total == 1
        details = logs[0]["details"]
        assert details["error_type"] == "ValueError"
        assert "no S1 SILVER product found" in details["error_message"]
        assert "traceback" in details


class TestDatasetLogFile:
    """Regression cover for the run-log .txt files (logs/<dataset>.txt).

    These were empty (or FAILED-only): dataset_log_file attached its handler
    to the `etl.pipeline_logger` logger, which is NOTSET and so inherited
    root's WARNING — every INFO record was dropped before reaching the
    handler, and tracebacks from sibling etl modules were never in scope.
    """

    def test_info_records_reach_the_file(self, tmp_path):
        import logging

        from etl.pipeline_logger import dataset_log_file

        plog_logger = logging.getLogger("etl.pipeline_logger")
        with dataset_log_file("DS_INFO", logs_dir=str(tmp_path)) as path:
            plog_logger.info("[PLOG] %s", '{"stage": "DOWNLOAD", "status": "STARTED"}')

        text = path.read_text(encoding="utf-8")
        assert "DOWNLOAD" in text
        assert "STARTED" in text

    def test_sibling_module_traceback_reaches_the_file(self, tmp_path):
        import logging

        from etl.pipeline_logger import dataset_log_file

        orch_logger = logging.getLogger("etl.module5_orchestrator")
        with dataset_log_file("DS_TRACE", logs_dir=str(tmp_path)) as path:
            try:
                raise ValueError("boom")
            except ValueError:
                orch_logger.exception("[ORCH] pipeline gagal")

        text = path.read_text(encoding="utf-8")
        assert "ValueError: boom" in text
        assert "Traceback" in text

    def test_concurrent_datasets_do_not_interleave(self, tmp_path):
        import logging
        import threading

        from etl.pipeline_logger import adopt_dataset_log_scope, dataset_log_file

        plog_logger = logging.getLogger("etl.pipeline_logger")
        paths: dict[str, object] = {}

        def run(name: str, marker: str) -> None:
            with dataset_log_file(name, logs_dir=str(tmp_path)) as path:
                paths[name] = path

                def worker() -> None:
                    adopt_dataset_log_scope(path)
                    plog_logger.info("[PLOG] %s", marker)

                t = threading.Thread(target=worker)
                t.start()
                t.join()

        threads = [
            threading.Thread(target=run, args=("DS_A", "MARKER_A")),
            threading.Thread(target=run, args=("DS_B", "MARKER_B")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        text_a = paths["DS_A"].read_text(encoding="utf-8")
        text_b = paths["DS_B"].read_text(encoding="utf-8")
        assert "MARKER_A" in text_a and "MARKER_B" not in text_a
        assert "MARKER_B" in text_b and "MARKER_A" not in text_b

    def test_etl_logger_level_restored_on_exit(self, tmp_path):
        import logging

        from etl.pipeline_logger import dataset_log_file

        etl_logger = logging.getLogger("etl")
        before = etl_logger.level
        with dataset_log_file("DS_LEVEL", logs_dir=str(tmp_path)):
            assert etl_logger.isEnabledFor(logging.INFO)
        assert etl_logger.level == before
