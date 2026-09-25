# tests/test_report_generation.py
"""Tests for report.md's "Generate Report" feature: etl/report_generator.py
and GET /api/datasets/{id}/report.

Run:
    pytest tests/test_report_generation.py -v
"""
from __future__ import annotations

import time

import pytest
from pypdf import PdfReader

from etl import folder_manager as fm
from etl.report_generator import ReportGenerationError, ReportGenerator


def _pdf_text(path) -> str:
    reader = PdfReader(str(path))
    return "\n".join(page.extract_text() or "" for page in reader.pages)


def _seed_source_files(dataset_id: int, name: str, source: str, level: str, count: int = 2) -> None:
    """Tulis file dummy ke laci {source}/{RAW|PROCESSED}/ supaya
    fm.storage_breakdown() mendeteksinya, tanpa perlu pipeline sungguhan."""
    d = fm.ensure_source_level_dir(dataset_id, name, source, level)
    for i in range(count):
        (d / f"dummy_{level}_{i}.tif").write_bytes(b"x" * 1024)


class TestReportGenerator:

    def test_report_basic_generation(self, db_client, sample_dataset):
        path = ReportGenerator(sample_dataset, db_client).generate()
        assert path.exists()
        assert path.suffix == ".pdf"
        assert path.stat().st_size > 0

    def test_report_missing_data_handled_gracefully(self, db_client, sample_dataset):
        """Dataset tanpa source/produk apa pun tetap menghasilkan PDF, bukan
        exception -- report.md: 'Insufficient records: ... generate
        abbreviated report'."""
        path = ReportGenerator(sample_dataset, db_client).generate()
        text = _pdf_text(path)
        assert "TRINITY DATALAB REPORT" in text

    def test_report_unknown_dataset_raises(self, db_client):
        with pytest.raises(ReportGenerationError):
            ReportGenerator(999999, db_client).generate()

    def test_report_contains_mvp_sections(self, db_client, sample_dataset):
        path = ReportGenerator(sample_dataset, db_client).generate(force=True)
        text = _pdf_text(path)
        assert "Configuration & Data Ingestion Summary" in text
        assert "Data Quality & Warnings" in text
        assert "JSON Summary" in text

    def test_report_charts_embedded_when_data_present(self, db_client, sample_dataset):
        from etl.dataset_manager import DatasetManager

        info = DatasetManager(db_client).get_dataset(sample_dataset)
        _seed_source_files(sample_dataset, info["name"], "sentinel1", "RAW", count=4)
        _seed_source_files(sample_dataset, info["name"], "sentinel1", "PROCESSED", count=2)

        gen = ReportGenerator(sample_dataset, db_client)
        path = gen.generate(force=True)
        assert path.exists()

        chart_dir = fm.get_dataset_root(sample_dataset, info["name"]) / "reports" / "_charts"
        assert (chart_dir / "chart_storage.png").exists()
        assert (chart_dir / "chart_ablation.png").exists()

    def test_report_caching_reuses_existing_pdf(self, db_client, sample_dataset):
        gen = ReportGenerator(sample_dataset, db_client)
        first = gen.generate(force=True)
        time.sleep(0.05)
        second = gen.generate()  # no force, dataset unchanged since first
        assert second == first

    def test_report_force_regenerates(self, db_client, sample_dataset):
        gen = ReportGenerator(sample_dataset, db_client)
        first = gen.generate(force=True)
        time.sleep(1.1)  # mtime resolution
        second = gen.generate(force=True)
        assert second != first

    def test_report_performance(self, db_client, sample_dataset):
        start = time.monotonic()
        ReportGenerator(sample_dataset, db_client).generate(force=True)
        elapsed = time.monotonic() - start
        assert elapsed < 60, "Generasi laporan untuk dataset kecil seharusnya jauh di bawah 5 menit"


    def test_report_writes_json_export(self, db_client, sample_dataset):
        import json

        path = ReportGenerator(sample_dataset, db_client).generate(force=True)
        payload = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
        for key in ("report_metadata", "dataset", "temporal_range", "sources", "fusion_strategy",
                    "processing_levels", "warnings", "recommendations"):
            assert key in payload
        impl = payload["report_metadata"]["implementation"]
        assert impl["total_pages"] >= 1
        assert impl["queries"]

    def test_report_all_sections_present(self, db_client, sample_dataset):
        text = _pdf_text(ReportGenerator(sample_dataset, db_client).generate(force=True))
        for heading in ("Executive Summary", "2.3 Spatial & Temporal Coverage",
                        "3. Processing Level Comparison", "5.2 Summary Findings",
                        "9.3 Recommendations for Use", "11.1 Complete Metadata JSON"):
            assert heading in text


class TestReportStats:

    def test_linear_units_detected(self):
        from etl.report_stats import is_linear_units

        assert is_linear_units(0.15, 0.0)
        assert not is_linear_units(-11.3, -24.6)
        assert not is_linear_units(None, 0.0)

    def test_gaps_include_period_edges(self):
        from datetime import date

        from etl.report_stats import ReportStats

        st = ReportStats(period_start=date(2025, 1, 1), period_end=date(2025, 1, 31), period_days=31)
        st.obs_dates["GPM"] = [date(2025, 1, 15), date(2025, 1, 16)]
        gaps = st.gaps("GPM")
        assert gaps == [(date(2025, 1, 1), date(2025, 1, 14), 14), (date(2025, 1, 17), date(2025, 1, 31), 15)]
        assert st.completeness("GPM") == round(2 / 31 * 100, 1)


class TestReportForecast:

    @staticmethod
    def _stats(days: int):
        import math
        from datetime import date, timedelta

        from etl.report_stats import RasterObs, ReportStats

        start = date(2025, 1, 1)
        st = ReportStats(period_start=start, period_end=start + timedelta(days=days - 1), period_days=days)
        st.raster["GPM"] = {"RAIN_24H": [
            RasterObs(day=start + timedelta(days=i), band="RAIN_24H", mean=5 + 4 * math.sin(i / 5), std=None,
                      min=0.0, max=20.0, valid_frac=1.0) for i in range(days)
        ]}
        return st

    def test_horizon_is_one_third_of_period(self):
        from etl.report_forecast import horizon_for

        assert horizon_for(10) == 3
        assert horizon_for(90) == 30
        assert horizon_for(122) == 41

    def test_forecast_covers_horizon_with_ordered_bands(self):
        from datetime import timedelta

        from etl.report_forecast import build_forecasts

        st = self._stats(90)
        (fc,) = build_forecasts(st)
        assert fc.days[-1] == st.period_end + timedelta(days=30)
        assert fc.backtest["test_days"] == 30
        for lo95, lo80, m, hi80, hi95 in zip(fc.lo95, fc.lo80, fc.mean, fc.hi80, fc.hi95):
            assert 0 <= lo95 <= lo80 <= m <= hi80 <= hi95  # hujan tidak pernah negatif
        assert fc.confidence in ("Tinggi", "Sedang", "Rendah")

    def test_short_series_skipped(self):
        from etl.report_forecast import build_forecasts

        assert build_forecasts(self._stats(5)) == []


class TestReportEndpoint:

    def test_report_json_endpoint(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/report/json?force=true")
        assert resp.status_code == 200
        assert resp.json()["dataset"]["dataset_id"] == sample_dataset


    def test_report_endpoint_200_pdf(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/report")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
        assert len(resp.content) > 0

    def test_report_endpoint_404_for_unknown_dataset(self, api_client):
        resp = api_client.get("/api/datasets/999999/report")
        assert resp.status_code == 404

    def test_report_endpoint_force_query_param(self, api_client, sample_dataset):
        resp = api_client.get(f"/api/datasets/{sample_dataset}/report?force=true")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/pdf"
