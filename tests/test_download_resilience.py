"""Download macet, pemakaian ulang file antar-dataset, dan pemulihan job
setelah server restart."""
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from etl import download_guard as dg
from etl import folder_manager as fm


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t


# --- StallGuard -------------------------------------------------------------

def test_stall_guard_raises_when_rate_below_minimum():
    clock = FakeClock()
    guard = dg.StallGuard(min_bytes_per_sec=1000, window_s=10, clock=clock)
    clock.t = 10
    with pytest.raises(dg.DownloadStalledError):
        guard.update(500)  # 50 B/s < 1000 B/s


def test_stall_guard_passes_healthy_download_and_resets_window():
    clock = FakeClock()
    guard = dg.StallGuard(min_bytes_per_sec=1000, window_s=10, clock=clock)
    clock.t = 5
    guard.update(100)  # jendela belum penuh: tidak dinilai
    clock.t = 10
    guard.update(20_000)
    clock.t = 15
    guard.update(10)  # jendela baru, belum penuh


def test_stall_error_is_timeout_error_so_existing_retry_catches_it():
    assert issubclass(dg.DownloadStalledError, TimeoutError)
    assert issubclass(dg.DownloadStalledError, OSError)


# --- reuse granule MODIS/GPM ------------------------------------------------

# --- Listing LAADS (M7) --------------------------------------------------------

class _Resp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


def _patch_listing(monkeypatch, responses):
    import requests

    from etl import module7_modis_download as m7

    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        item = responses[min(len(calls), len(responses)) - 1]
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setenv("NASA_EARTHDATA_TOKEN", "x")
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(m7.time, "sleep", lambda s: None)
    return m7, calls


def test_laads_listing_retries_read_timeout(monkeypatch):
    """try1: satu ReadTimeout listing LAADS menghapus FLOOD 2025-01-08 penuh."""
    import requests

    html = '<a href="MCDWD_L3.A2025008.h28v09.061.1.hdf">x</a>'
    m7, calls = _patch_listing(
        monkeypatch, [requests.ReadTimeout("slow"), _Resp(503), _Resp(200, html)]
    )
    found = m7._discover_tile_files(datetime(2025, 1, 8), ["h28v09"], "MCDWD_L3")
    assert len(calls) == 3
    assert found[0]["file_name"] == "MCDWD_L3.A2025008.h28v09.061.1.hdf"


def test_laads_listing_network_error_becomes_runtime_error(monkeypatch):
    """RuntimeError, bukan exception requests: hanya RuntimeError yang memicu
    fallback NRT -> arsip standar di _discover_tile_files_with_fallback."""
    import requests

    m7, calls = _patch_listing(monkeypatch, [requests.ConnectionError("down")])
    with pytest.raises(RuntimeError):
        m7._discover_tile_files(datetime(2025, 1, 8), ["h28v09"], "MCDWD_L3")
    assert len(calls) == m7.MAX_RETRIES


def test_laads_listing_does_not_retry_404(monkeypatch):
    m7, calls = _patch_listing(monkeypatch, [_Resp(404)])
    with pytest.raises(RuntimeError):
        m7._discover_tile_files(datetime(2025, 1, 8), ["h28v09"], "MCDWD_L3")
    assert len(calls) == 1


def test_reuse_granule_links_file_from_other_dataset():
    root = fm.DATA_ROOT
    src = root / "11_other" / fm.GRANULE_CACHE_DIRNAME / "modis" / "MOD09A1.A2025001.hdf"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_bytes(b"granule")
    dst = root / "12_mine" / fm.GRANULE_CACHE_DIRNAME / "modis" / "MOD09A1.A2025001.hdf"
    dst.parent.mkdir(parents=True, exist_ok=True)

    assert dg.reuse_granule(dst, "modis", root, "[T]") is True
    assert dst.read_bytes() == b"granule"


def test_reuse_granule_ignores_missing_and_partial():
    root = fm.DATA_ROOT
    part = root / "13_other" / fm.GRANULE_CACHE_DIRNAME / "gpm" / "X.nc4.part"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"half")
    dst = root / "14_mine" / fm.GRANULE_CACHE_DIRNAME / "gpm" / "X.nc4"
    assert dg.reuse_granule(dst, "gpm", root, "[T]") is False
    assert not dst.exists()


# --- reuse ZIP Sentinel-1 ---------------------------------------------------

def _make_zip(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("x.SAFE/manifest.safe", "ok")


def test_find_reusable_scene_zip_accepts_complete_rejects_truncated(tmp_path):
    from etl.module1_download import find_reusable_scene_zip

    pid = "S1A_IW_GRDH_TEST_FA44.SAFE"
    # Layout nyata dataset lama: raw/sentinel1/{pid}/{pid}.zip
    good = tmp_path / "11_a" / "20250111" / "raw" / "sentinel1" / pid / f"{pid}.zip"
    _make_zip(good)
    bad = tmp_path / "10_b" / "_work" / pid / "raw" / "sentinel1" / f"{pid}.zip"
    bad.parent.mkdir(parents=True, exist_ok=True)
    bad.write_bytes(good.read_bytes()[:10])  # terpotong

    target = tmp_path / "18_c" / "_work" / pid / "raw" / "sentinel1" / f"{pid}.zip"
    assert find_reusable_scene_zip(pid, tmp_path, target) == good


def test_download_scene_reuses_zip_without_network(tmp_path, monkeypatch):
    import etl.module1_download as m1

    pid = "S1A_IW_GRDH_TEST_REUSE.SAFE"
    _make_zip(tmp_path / "11_a" / "20250111" / "raw" / "sentinel1" / f"{pid}.zip")
    out_dir = tmp_path / "18_c" / "_work" / pid / "raw" / "sentinel1"
    out_dir.mkdir(parents=True)
    (out_dir / f"{pid}.zip.part").write_bytes(b"stale")

    monkeypatch.setenv("COPERNICUS_USER", "u")
    monkeypatch.setenv("COPERNICUS_PASSWORD", "p")
    monkeypatch.setattr(m1, "_get_cdse_token", lambda *a: pytest.fail("tidak boleh unduh"))
    monkeypatch.setattr(m1, "_extract_bands", lambda z, o: (o / "vv.tif", o / "vh.tif"))

    result = m1.download_scene(
        {"product_identifier": pid, "download_url": "u",
         "acquisition_datetime": datetime(2025, 1, 11, tzinfo=timezone.utc)},
        output_dir=str(out_dir), keep_raw=True, reuse_root=tmp_path,
    )
    assert (out_dir / f"{pid}.zip").exists()
    assert not (out_dir / f"{pid}.zip.part").exists()
    assert result.zip_path.endswith(".zip")


class _FakeCdseResponse:
    """Balasan CDSE tiruan: cukup untuk jalur yang dipakai download_scene."""

    def __init__(self, status_code, headers=None, body=b""):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.exceptions.HTTPError(str(self.status_code), response=self)

    def iter_content(self, chunk_size):
        yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def test_download_scene_retries_429_with_backoff_not_max_retries_budget(tmp_path, monkeypatch):
    """CDSE membalas 429 sebelum akhirnya sukses: harus dijeda (bukan retry
    instan) dan tidak menghabiskan jatah MAX_RETRIES=3 seperti error jaringan
    biasa (okt_dec_2025_hybrid 2026-09-23: 12 scene sehat gagal permanen
    karena 3 worker retry 429 tanpa jeda dalam hitungan detik)."""
    import requests

    import etl.module1_download as m1

    pid = "S1A_IW_GRDH_TEST_429.SAFE"
    out_dir = tmp_path / "20_d" / "_work" / pid / "raw" / "sentinel1"
    out_dir.mkdir(parents=True)

    monkeypatch.setenv("COPERNICUS_USER", "u")
    monkeypatch.setenv("COPERNICUS_PASSWORD", "p")
    monkeypatch.setattr(m1, "_get_cdse_token", lambda *a: "tok")
    monkeypatch.setattr(m1, "_extract_bands", lambda z, o: (o / "vv.tif", o / "vh.tif"))
    slept = []
    monkeypatch.setattr(m1.time, "sleep", lambda s: slept.append(s))

    responses = [
        _FakeCdseResponse(429, headers={"Retry-After": "3"}),
        _FakeCdseResponse(429, headers={}),  # tanpa Retry-After: backoff eksponensial
        _FakeCdseResponse(200, body=b"zipbytes"),
    ]
    calls = []

    def fake_get(self, url, **kwargs):
        calls.append(url)
        return responses[len(calls) - 1]

    monkeypatch.setattr(requests.Session, "get", fake_get)

    result = m1.download_scene(
        {"product_identifier": pid, "download_url": "https://catalogue.dataspace.copernicus.eu/x",
         "acquisition_datetime": datetime(2025, 1, 11, tzinfo=timezone.utc)},
        output_dir=str(out_dir), keep_raw=True,
    )

    assert len(calls) == 3
    assert (out_dir / f"{pid}.zip").exists()
    assert result.zip_path.endswith(".zip")
    # Retry-After dihormati; sisanya backoff eksponensial -- keduanya dijeda,
    # tidak nol seperti retry instan yang lama.
    assert slept == [3.0] or slept[0] == pytest.approx(3.0, abs=0.01)
    assert len(slept) == 2
    assert all(s > 0 for s in slept)


def test_download_scene_429_does_not_consume_max_retries_budget(tmp_path, monkeypatch):
    """Setelah MAX_RATE_LIMIT_RETRIES kali 429, harus masih ada jatah untuk
    error jaringan (MAX_RETRIES) yang mengikuti -- keduanya dihitung
    terpisah."""
    import requests

    import etl.module1_download as m1

    pid = "S1A_IW_GRDH_TEST_429_BUDGET.SAFE"
    out_dir = tmp_path / "21_e" / "_work" / pid / "raw" / "sentinel1"
    out_dir.mkdir(parents=True)

    monkeypatch.setenv("COPERNICUS_USER", "u")
    monkeypatch.setenv("COPERNICUS_PASSWORD", "p")
    monkeypatch.setattr(m1, "_get_cdse_token", lambda *a: "tok")
    monkeypatch.setattr(m1.time, "sleep", lambda s: None)

    # MAX_RATE_LIMIT_RETRIES balasan 429 berturut-turut lalu satu sukses --
    # kalau 429 memakan jatah MAX_RETRIES=3 yang sama, ini akan gagal jauh
    # sebelum sukses.
    responses = [_FakeCdseResponse(429, headers={"Retry-After": "0"})] * m1.MAX_RATE_LIMIT_RETRIES
    responses.append(_FakeCdseResponse(200, body=b"zipbytes"))
    calls = []

    def fake_get(self, url, **kwargs):
        calls.append(url)
        return responses[len(calls) - 1]

    monkeypatch.setattr(requests.Session, "get", fake_get)
    monkeypatch.setattr(m1, "_extract_bands", lambda z, o: (o / "vv.tif", o / "vh.tif"))

    result = m1.download_scene(
        {"product_identifier": pid, "download_url": "https://catalogue.dataspace.copernicus.eu/x",
         "acquisition_datetime": datetime(2025, 1, 11, tzinfo=timezone.utc)},
        output_dir=str(out_dir), keep_raw=True,
    )
    assert len(calls) == m1.MAX_RATE_LIMIT_RETRIES + 1
    assert result.zip_path.endswith(".zip")


# --- pemulihan job setelah restart -----------------------------------------

def _job(db_client, dataset_id, status, kind="STANDARD"):
    from etl.database_client import Dataset, DatasetJob

    with db_client.session() as sess:
        ds = sess.get(Dataset, dataset_id)
        ds.dataset_kind = kind
        ds.status = status
        job = DatasetJob(dataset_id=dataset_id, status=status)
        sess.add(job)
        sess.flush()
        return job.job_id


def test_recover_interrupted_jobs_requeues_active_jobs(db_client, sample_dataset, monkeypatch):
    from etl.database_client import Dataset, DatasetJob
    from etl.dataset_manager import DatasetManager

    job_id = _job(db_client, sample_dataset, "DOWNLOADING")
    spawned = []
    mgr = DatasetManager(db_client)
    monkeypatch.setattr(mgr, "_spawn_job_runner", spawned.append)

    resumed = mgr.recover_interrupted_jobs()

    assert job_id in resumed and job_id in spawned
    with db_client.session() as sess:
        job = sess.get(DatasetJob, job_id)
        assert job.status == "QUEUED" and job.resume_count == 1
        assert sess.get(Dataset, sample_dataset).status == "QUEUED"


def test_recover_interrupted_jobs_skips_finished_jobs(db_client, sample_dataset, monkeypatch):
    from etl.dataset_manager import DatasetManager

    done_id = _job(db_client, sample_dataset, "COMPLETED")
    spawned = []
    mgr = DatasetManager(db_client)
    monkeypatch.setattr(mgr, "_spawn_job_runner", spawned.append)
    assert done_id not in mgr.recover_interrupted_jobs()
