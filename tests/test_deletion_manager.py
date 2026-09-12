# tests/test_deletion_manager.py
"""
Tests untuk penghapusan dataset (etl/deletion_manager.py dan
DatasetManager._spawn_deletion_runner): file yang ditulis job yang telat
berhenti dan scene placeholder NASA_AUX tidak boleh tertinggal yatim.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

import pytest

from etl import dataset_manager as dsm
from etl import folder_manager as fm
from etl.database_client import Dataset, SatelliteScene
from etl.deletion_manager import DeletionManager

BBOX_WKT = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    root = tmp_path / "data" / "datasets"
    monkeypatch.setattr(fm, "DATA_ROOT", root)
    return root


def _dataset_name(db_client, dataset_id):
    with db_client.session() as sess:
        return sess.get(Dataset, dataset_id).name


def _aux_scene(meta, region_id, pid):
    return meta.insert_satellite_scene(
        product_identifier=pid,
        acquisition_datetime=datetime(2024, 1, 15, tzinfo=timezone.utc),
        region_id=region_id,
        bbox_wkt=BBOX_WKT,
        orbit_direction="ASCENDING",
        resolution_m=250,
        instrument_mode="AUX",
    )


def test_delete_all_sweeps_files_written_after_manifest(
    db_client, sample_dataset, data_root, monkeypatch
):
    """Job yang telat berhenti menulis file setelah manifest dibuat; file
    itu harus ikut terhapus dan folder dataset hilang."""
    name = _dataset_name(db_client, sample_dataset)
    base = fm.get_dataset_root(sample_dataset, name)
    (base / "20240115" / "raw").mkdir(parents=True)
    (base / "20240115" / "raw" / "early.tif").write_bytes(b"x" * 10)

    original = DeletionManager._delete_files
    calls = {"n": 0}

    def delete_then_late_write(self, manifest, op_id):
        result = original(self, manifest, op_id)
        calls["n"] += 1
        if calls["n"] == 1:
            late = base / "_work" / "late_calibrated.tif"
            late.parent.mkdir(parents=True, exist_ok=True)
            late.write_bytes(b"y" * 20)
        return result

    monkeypatch.setattr(DeletionManager, "_delete_files", delete_then_late_write)

    result = DeletionManager(db_client, sample_dataset, name).delete_all()

    assert not base.exists()
    assert result["deleted_count"] == 2
    with db_client.session() as sess:
        assert sess.get(Dataset, sample_dataset) is None


def test_delete_all_removes_only_own_aux_placeholders(
    db_client, meta, sample_dataset, sample_region, data_root
):
    """Placeholder NASA_AUX dataset ini dihapus; milik dataset lain yang id-nya
    cocok dengan pola LIKE '_{id}_' (mis. 1{id}) tetap ada."""
    name = _dataset_name(db_client, sample_dataset)
    stamp = datetime.now().strftime("%H%M%S%f")[:8]
    own = [
        _aux_scene(meta, sample_region, f"NASA_AUX_MODIS_{sample_dataset}_{stamp}"),
        _aux_scene(meta, sample_region, f"NASA_AUX_GPM_{sample_dataset}_{stamp}"),
    ]
    other_pid = f"NASA_AUX_MODIS_1{sample_dataset}_{stamp}"
    other = _aux_scene(meta, sample_region, other_pid)

    try:
        DeletionManager(db_client, sample_dataset, name).delete_all()
        with db_client.session() as sess:
            assert all(sess.get(SatelliteScene, sid) is None for sid in own)
            assert sess.get(SatelliteScene, other) is not None
    finally:
        with db_client.session() as sess:
            leftover = sess.get(SatelliteScene, other)
            if leftover is not None:
                sess.delete(leftover)


def test_wait_thread_exit_waits_for_job_thread():
    stop = threading.Event()
    t = threading.Thread(target=stop.wait, daemon=True)
    dsm._register_thread("job-test-wait", t)
    t.start()
    try:
        assert dsm._wait_thread_exit("job-test-wait", timeout_s=0.05) is False
        threading.Timer(0.05, stop.set).start()
        started = time.monotonic()
        assert dsm._wait_thread_exit("job-test-wait", timeout_s=5) is True
        assert time.monotonic() - started < 5
    finally:
        stop.set()
    assert dsm._wait_thread_exit("tidak-terdaftar", timeout_s=0) is True
