"""Regresi dari run dataset 25 'jan_mar_2025_hybrid' (2026-09-19).

1. Semua NDVI/NDWI MODIS gagal: "can't compare offset-naive and
   offset-aware datetimes" -- tanggal awal periode MOD09A1 dibuat naive
   padahal tanggal dari orchestrator tz-aware.
2. Semua scene S1 gagal di CALIBRATE (FileNotFoundError): path ZIP di _work/
   264 karakter > MAX_PATH Windows karena product_identifier muncul dua kali.
"""
from datetime import datetime, timezone

import pytest

from etl import folder_manager as fm
from etl import module7_modis_download as m7

PID = "S1A_IW_GRDH_1SDV_20250111T111502_20250111T111532_057395_0710A1_5469.SAFE"
PID_SIBLING = "S1A_IW_GRDH_1SDV_20250111T111532_20250111T111557_057395_0710A1_FA44.SAFE"


@pytest.mark.parametrize("tz", [None, timezone.utc])
def test_mod09a1_periods_accept_aware_and_naive_dates(tz):
    periods = m7._mod09a1_periods(datetime(2025, 1, 1, tzinfo=tz))
    assert periods, "harus ada minimal satu periode"
    assert all(p.tzinfo == tz for p in periods)
    # Lintas tahun: periode sebelum 1 Jan ada di tahun sebelumnya.
    assert any(p.year == 2024 for p in periods)


def test_scratch_slug_short_and_unique():
    a, b = fm.scratch_slug(PID), fm.scratch_slug(PID_SIBLING)
    assert len(a) <= fm.SCRATCH_SLUG_MAX and len(b) <= fm.SCRATCH_SLUG_MAX
    assert a != b
    assert fm.scratch_slug(PID) == a  # deterministik
    assert fm.scratch_slug("20250111") == "20250111"


def test_s1_raw_zip_path_fits_max_path_with_long_dataset_name():
    zip_path = (
        fm.get_scene_dir(25, "jan_mar_2025_hybrid", "raw", "sentinel1", PID)
        / f"{PID}.zip"
    )
    # Anggaran 260 dikurangi root repo yang wajar (~80 karakter).
    rel = zip_path.relative_to(fm.get_dataset_root(25, "jan_mar_2025_hybrid").parent.parent)
    assert len(str(rel)) + 80 < 260, len(str(rel))
