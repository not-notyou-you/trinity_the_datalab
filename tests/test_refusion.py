"""Tes pemetaan raster S1 yang dipakai perakitan ulang (etl/refusion.py).

Bagian inilah yang menentukan perbaikan berjalan tanpa unduhan: kalau raster
PROCESSED di disk gagal dicocokkan dengan product_identifier-nya, frame itu
lenyap dari mosaik dan stack hasil perbaikan cuma memuat sebagian AOI — persis
penyakit yang s1_mosaic dibuat untuk menyembuhkan.
"""
from __future__ import annotations

import pytest

from etl.refusion import _match_cogs, _s1_cogs_by_pid

PID_A = "S1A_IW_GRDH_1SDV_20251204T222544_20251204T222609_062171_07C81E_541C.SAFE"
PID_B = "S1A_IW_GRDH_1SDV_20251204T222609_20251204T222637_062171_07C81E_4B33.SAFE"


@pytest.fixture
def proc_dir(tmp_path, monkeypatch):
    from etl import folder_manager as fm

    root = tmp_path / "27_JAWA_A"
    proc = root / "sentinel-1" / "PROCESSED"
    proc.mkdir(parents=True)
    for stem in ("S1A_IW_GRDH_1SDV_20251204T222544_20",
                 "S1A_IW_GRDH_1SDV_20251204T222609_20"):
        for band in ("VV", "VH"):
            (proc / f"{stem}_calibrated_{band}_lee.tif").write_bytes(b"x")
    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: root)
    return proc


def test_kedua_band_tiap_frame_terpetakan(proc_dir):
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    assert len(cogs) == 2
    for bands in cogs.values():
        assert set(bands) == {"VV", "VH"}


def test_pid_utuh_cocok_dengan_nama_berkas_yang_terpotong(proc_dir):
    """Nama COG cuma memuat potongan pid, jadi pencocokannya lewat awalan."""
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    bands = _match_cogs(PID_A, cogs)
    assert set(bands) == {"VV", "VH"}
    assert "20251204T222544" in bands["VV"]


def test_dua_frame_satu_tanggal_tidak_saling_tertukar(proc_dir):
    """Frame satu tanggal beda hanya di detik akuisisi; tertukar di sini
    berarti mosaiknya menyusun frame yang salah."""
    cogs = _s1_cogs_by_pid(27, "JAWA_A")
    a = _match_cogs(PID_A, cogs)
    b = _match_cogs(PID_B, cogs)
    assert "20251204T222544" in a["VV"]
    assert "20251204T222609" in b["VV"]
    assert a["VV"] != b["VV"]


def test_pid_tanpa_raster_mengembalikan_kosong(proc_dir):
    hilang = "S1A_IW_GRDH_1SDV_20251299T000000_20251299T000030_000000_000000_0000.SAFE"
    assert _match_cogs(hilang, _s1_cogs_by_pid(27, "JAWA_A")) == {}


def test_folder_processed_tidak_ada_bukan_error(tmp_path, monkeypatch):
    from etl import folder_manager as fm

    monkeypatch.setattr(fm, "get_dataset_root", lambda did, name: tmp_path / "kosong")
    assert _s1_cogs_by_pid(99, "TIDAK_ADA") == {}
