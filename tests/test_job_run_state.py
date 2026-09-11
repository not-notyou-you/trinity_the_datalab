"""
Regresi dua bug yang membuat dataset yang sudah selesai tetap FAILED dan
unduhan GPM 404 padahal datanya ada:

1. Counter job/dataset hanya bertambah lintas retry/resume
   (DatasetManager.begin_job_run) dan scene yang terputus sesudah tahap antara
   yang COMPLETED dilewati selamanya (DatasetManager.scene_is_done).
2. Nama granule IMERG memakai huruf minor versi hardcoded "V07B", padahal
   GES DISC pindah ke V07C sejak Maret 2026 (module8._resolve_daily_granule).
"""
from __future__ import annotations

from datetime import datetime

import pytest
import requests

from etl import module8_gpm_download as m8
from etl.dataset_manager import DatasetManager


# ---------------------------------------------------------------------------
# Counter & state per eksekusi job
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "stage,status,expected",
    [
        ("CLEANUP", "COMPLETED", True),
        ("GOLD_EXPORT", "COMPLETED", False),
        ("CROP", "COMPLETED", False),
        ("CLEANUP", "FAILED", False),
        (None, "PENDING", False),
    ],
)
def test_scene_is_done_only_after_cleanup(stage, status, expected):
    assert DatasetManager.scene_is_done(stage, status) is expected


def test_begin_job_run_resets_counters_from_finished_scenes(db_client, sample_dataset):
    from etl.database_client import Dataset, DatasetJob, SceneJobState

    with db_client.session() as sess:
        job = DatasetJob(
            dataset_id=sample_dataset, job_type="CREATE", status="FAILED",
            downloaded_count=6, processed_count=2, failed_count=4, cleaned_count=2,
        )
        sess.add(job)
        sess.flush()
        job_id = job.job_id
        ds = sess.get(Dataset, sample_dataset)
        ds.completed_scenes, ds.failed_scenes = 2, 4
        sess.add_all([
            SceneJobState(job_id=job_id, product_identifier="S_DONE", current_stage="CLEANUP",
                          stage_status="COMPLETED", last_error="MemoryError lama"),
            SceneJobState(job_id=job_id, product_identifier="S_MID", current_stage="GOLD_EXPORT",
                          stage_status="COMPLETED", last_error="UniqueViolation lama"),
            SceneJobState(job_id=job_id, product_identifier="S_FAIL", current_stage="CALIBRATE",
                          stage_status="FAILED", last_error="MemoryError lama"),
        ])

    done = DatasetManager(db_client).begin_job_run(job_id)

    assert done == 1
    with db_client.session() as sess:
        job = sess.get(DatasetJob, job_id)
        assert (job.downloaded_count, job.processed_count, job.cleaned_count, job.failed_count) == (1, 1, 1, 0)
        ds = sess.get(Dataset, sample_dataset)
        assert (ds.completed_scenes, ds.failed_scenes) == (1, 0)
        errors = sess.query(SceneJobState.last_error).filter(SceneJobState.job_id == job_id).all()
        assert all(e[0] is None for e in errors)


def test_begin_job_run_unknown_job_is_noop(db_client):
    assert DatasetManager(db_client).begin_job_run(987654321) == 0


# ---------------------------------------------------------------------------
# Resolusi nama granule IMERG
# ---------------------------------------------------------------------------

DAY = datetime(2026, 3, 10)


def _name(run: str, minor: str, day: datetime = DAY) -> str:
    return m8._daily_granule_filename(day, run, minor=minor)


def test_resolver_picks_newest_minor_version_from_listing(tmp_path, monkeypatch):
    listing = {_name("L", "B"), _name("L", "C"), _name("L", "C", datetime(2026, 3, 11))}
    monkeypatch.setattr(m8, "_list_month_granules", lambda run, y, m: frozenset(listing))

    name, url = m8._resolve_daily_granule(DAY, "L", tmp_path)

    assert name.endswith("20260310-S000000-E235959.V07C.nc4")
    assert name.startswith("3B-DAY-L.")
    assert url == f"{m8.GES_DISC_ROOT}/GPM_3IMERGDL.07/2026/03/{name}"


def test_resolver_returns_none_when_run_not_published(tmp_path, monkeypatch):
    # Listing Final kosong (folder 404) -> None, bukan nama tebakan.
    monkeypatch.setattr(m8, "_list_month_granules", lambda run, y, m: frozenset())
    assert m8._resolve_daily_granule(DAY, "F", tmp_path) is None


def test_resolver_does_not_confuse_runs(tmp_path, monkeypatch):
    # Nama Late tidak boleh cocok sebagai Final (infix "-L" vs "").
    monkeypatch.setattr(m8, "_list_month_granules", lambda run, y, m: frozenset({_name("L", "C")}))
    assert m8._resolve_daily_granule(DAY, "F", tmp_path) is None


def test_resolver_uses_local_cache_without_listing(tmp_path, monkeypatch):
    (tmp_path / _name("L", "B")).write_bytes(b"x")

    def _boom(*a, **k):
        raise AssertionError("listing tidak boleh dipanggil kalau granule sudah di cache")

    monkeypatch.setattr(m8, "_list_month_granules", _boom)
    name, _ = m8._resolve_daily_granule(DAY, "L", tmp_path)
    assert name == _name("L", "B")


def test_resolver_falls_back_to_guess_when_listing_errors(tmp_path, monkeypatch):
    def _down(*a, **k):
        raise requests.ConnectionError("offline")

    monkeypatch.setattr(m8, "_list_month_granules", _down)
    name, _ = m8._resolve_daily_granule(DAY, "L", tmp_path)
    assert name == _name("L", "B")


def test_fetch_skips_unpublished_final_without_download_attempt(tmp_path, monkeypatch):
    listings = {"F": frozenset(), "L": frozenset({_name("L", "C")}), "E": frozenset({_name("E", "C")})}
    monkeypatch.setattr(m8, "_list_month_granules", lambda run, y, m: listings[run])
    attempted: list[str] = []

    def _fake_download(url, out_path, **kwargs):
        attempted.append(url)
        return "0" * 32

    monkeypatch.setattr(m8, "_download_with_retry", _fake_download)
    monkeypatch.setattr(m8, "_read_daily_precip", lambda p: ("data", "transform", "EPSG:4326"))

    data, transform, crs, checksum, run = m8._fetch_daily_precip(DAY, tmp_path, window_name="24h")

    assert run == "L"
    assert len(attempted) == 1 and "GPM_3IMERGDL.07" in attempted[0] and attempted[0].endswith("V07C.nc4")


def test_fetch_raises_when_no_run_has_the_day(tmp_path, monkeypatch):
    monkeypatch.setattr(m8, "_list_month_granules", lambda run, y, m: frozenset())
    with pytest.raises(RuntimeError, match="F/L/E"):
        m8._fetch_daily_precip(DAY, tmp_path, window_name="24h")
