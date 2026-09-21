"""Regresi run 2026-09-20: dua dataset gagal setelah pipeline dipercepat.

Keduanya kegagalan yang muncul justru KARENA optimasi, bukan karena datanya:

1. GDAL/rasterio dipakai dengan 24 core -> psutil melaporkan puncak CPU 1944%,
   dan kolom processing_jobs.cpu_usage_percent NUMERIC(5,2) cuma muat sampai
   999,99. UPDATE yang sama juga menulis status SUCCESS, jadi overflow-nya
   menggagalkan seluruh tahap scene: 9 tahap di 26_JAWA, 5 di
   25_jan_mar_2025_hybrid.

2. Unduhan S1 yang putus di tengah (IncompleteRead) dicoba lagi dengan header
   Range, dan endpoint /$value CDSE menjawab 501 Not Implemented. Karena 501
   bukan error jaringan, dua percobaan sisanya mengirim Range yang sama dan
   ditolak lagi -- scene sehat gagal permanen.
"""
from datetime import datetime, timezone

import pytest

from etl.metadata_manager import _fit_numeric


# --- 1. metrik multicore tidak boleh menggagalkan tahap ---------------------

def test_cpu_percent_above_one_core_fits_the_column():
    """24 core = sampai 2400%. Kolomnya (migrasi 022) muat sampai 99.999,99."""
    assert _fit_numeric(1944.0, 7, 2, "cpu_usage_percent", 1) == 1944.0


def test_absurd_metric_is_clamped_instead_of_raising():
    """Jaring pengaman: database yang belum dimigrasi atau mesin dengan lebih
    banyak core tidak boleh membuat status job hilang."""
    assert _fit_numeric(1e9, 7, 2, "cpu_usage_percent", 1) == 99999.99
    assert _fit_numeric(1e12, 10, 2, "memory_usage_mb", 1) == 99999999.99


def test_model_column_matches_the_migrated_schema():
    from etl.database_client import ProcessingJob

    col = ProcessingJob.__table__.c.cpu_usage_percent
    assert (col.type.precision, col.type.scale) == (7, 2)


# --- 2. server menolak resume -> unduh ulang dari awal ----------------------

class _Resp:
    """Respons streaming minimal yang cukup untuk download_scene."""

    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.exceptions.HTTPError(
                f"{self.status_code} Server Error", response=self
            )

    def iter_content(self, chunk_size=None):
        yield self._body


class _Session:
    """Mencatat header Range tiap permintaan, lalu menjawab dari `script`."""

    def __init__(self, script):
        self.headers = {}
        self._script = list(script)
        self.range_seen = []

    def get(self, url, **kwargs):
        self.range_seen.append(self.headers.get("Range"))
        return self._script.pop(0)


def _run_download(tmp_path, monkeypatch, script, part_bytes):
    import etl.module1_download as m1

    pid = "S1A_IW_GRDH_TEST_RESUME.SAFE"
    out_dir = tmp_path / "_work" / pid / "raw" / "sentinel1"
    out_dir.mkdir(parents=True)
    # .part sisa unduhan yang putus -> download_scene akan minta Range.
    (out_dir / f"{pid}.zip.part").write_bytes(part_bytes)

    session = _Session(script)
    # module1_download meng-import requests di dalam fungsi, jadi yang dipatch
    # modul requests-nya sendiri, bukan atribut module1_download.
    import requests

    monkeypatch.setenv("COPERNICUS_USER", "u")
    monkeypatch.setenv("COPERNICUS_PASSWORD", "p")
    monkeypatch.setattr(m1, "_get_cdse_token", lambda *a: "token")
    monkeypatch.setattr(requests, "Session", lambda: session)
    monkeypatch.setattr(m1, "_md5", lambda p: "0" * 32)
    monkeypatch.setattr(m1, "_extract_bands", lambda z, o: (o / "vv.tif", o / "vh.tif"))

    result = m1.download_scene(
        {"product_identifier": pid, "download_url": "https://example.invalid/x",
         "acquisition_datetime": datetime(2025, 12, 6, tzinfo=timezone.utc)},
        output_dir=str(out_dir), keep_raw=True,
    )
    return result, session, out_dir / f"{pid}.zip"


def test_range_rejected_restarts_download_from_zero(tmp_path, monkeypatch):
    full = b"FULL-FILE-CONTENT"
    result, session, zip_path = _run_download(
        tmp_path, monkeypatch,
        script=[
            _Resp(501),  # server menolak Range
            _Resp(200, full, {"Content-Length": str(len(full))}),
        ],
        part_bytes=b"PARTIAL",
    )

    # Permintaan pertama membawa Range, yang kedua tidak.
    assert session.range_seen[0] is not None
    assert session.range_seen[1] is None
    # Isi .part lama dibuang, bukan ditempeli -> ZIP-nya utuh, bukan gabungan.
    assert zip_path.read_bytes() == full
    assert result.zip_path.endswith(".zip")


def test_range_rejection_does_not_consume_the_retry_budget(tmp_path, monkeypatch):
    """Penolakan Range bukan kegagalan transfer, jadi tidak boleh memakan
    jatah 3 percobaan. Dengan satu 501 + dua kali putus jaringan, percobaan
    ketiga masih tersedia dan unduhan berakhir sukses; sebelum perbaikan, 501
    memakai percobaan pertama dan putus kedua menghabiskan sisanya."""
    import requests

    full = b"FULL-FILE-CONTENT"

    class _Broken(_Resp):
        def iter_content(self, chunk_size=None):
            raise requests.exceptions.ConnectionError("Connection broken")

    result, session, zip_path = _run_download(
        tmp_path, monkeypatch,
        script=[
            _Resp(501),                                                  # Range ditolak
            _Broken(200, full, {"Content-Length": str(len(full))}),      # putus 1
            _Broken(200, full, {"Content-Length": str(len(full))}),      # putus 2
            _Resp(200, full, {"Content-Length": str(len(full))}),        # berhasil
        ],
        part_bytes=b"PARTIAL",
    )

    assert zip_path.read_bytes() == full
    assert result.file_size_mb == pytest.approx(len(full) / 1024 ** 2)
