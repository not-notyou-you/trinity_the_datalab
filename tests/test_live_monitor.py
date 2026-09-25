# tests/test_live_monitor.py
"""Live Monitoring: penjaga penghapusan, retensi, dan aturan murni.

Tidak butuh database: bagian yang menyentuh DB diuji lewat _LiveFiles dan
fungsi murni. Root dataset diarahkan ke tmp oleh conftest (_isolate_output_dirs).
"""
from __future__ import annotations

from datetime import date

import pytest

from etl import folder_manager as fm
from etl import live_monitor as lm


def _touch(p, size=10):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"x" * size)
    return p


@pytest.fixture
def live_root():
    files = lm._LiveFiles(901, "live_padang", "LIVE_AREA")
    root = files.root
    d = "20260924"
    other = "20260912"
    made = {
        "s1_vv": _touch(root / "sentinel-1/PROCESSED" / f"S1A_IW_GRDH_1SDV_{d}T112233_x_VV_lee.tif"),
        "s1_vh": _touch(root / "sentinel-1/PROCESSED" / f"S1A_IW_GRDH_1SDV_{d}T112233_x_VH_lee.tif"),
        "modis": _touch(root / "modis/PROCESSED" / f"modis_{d}_ndwi.tif"),
        "gpm": _touch(root / "gpm-imerg/PROCESSED" / f"gpm_rain_72h_{d}.tif"),
        "png": _touch(root / "live" / d / "s1_vh.png"),
        "keep_s1": _touch(root / "sentinel-1/PROCESSED" / f"S1A_IW_GRDH_1SDV_{other}T112233_x_VH_lee.tif"),
        "keep_gpm": _touch(root / "gpm-imerg/PROCESSED" / f"gpm_rain_72h_{other}.tif"),
        "old_granule": _touch(root / "_granule_cache/gpm" / "3B-DAY.MS.MRG.3IMERG.20260801-S000000-E235959.V07B.nc4"),
        "new_granule": _touch(root / "_granule_cache/gpm" / "3B-DAY.MS.MRG.3IMERG.20260910-S000000-E235959.V07B.nc4"),
        "modis_granule": _touch(root / "_granule_cache/modis" / "MCDWD_L3.A2026200.h29v09.061.2026201000000.hdf"),
    }
    return files, made


def test_rejects_non_live_dataset():
    with pytest.raises(lm.UnsafeDeletion):
        lm._LiveFiles(1, "wajo", "STANDARD")


def test_files_for_date_only_that_date(live_root):
    files, made = live_root
    got = set(files.files_for_date(date(2026, 9, 24)))
    assert got == {made[k] for k in ("s1_vv", "s1_vh", "modis", "gpm", "png")}


def test_delete_refuses_path_outside_root(live_root, tmp_path):
    files, _ = live_root
    outsider = _touch(tmp_path / "standard_dataset" / "precious.tif")
    with pytest.raises(lm.UnsafeDeletion):
        files.delete([outsider])
    assert outsider.exists()
    # '..' yang keluar dari root juga ditolak setelah resolve.
    sneaky = files.root / ".." / outsider.relative_to(tmp_path)
    with pytest.raises(lm.UnsafeDeletion):
        files.delete([sneaky])


def test_delete_reports_bytes(live_root):
    files, made = live_root
    deleted, freed = files.delete(files.files_for_date(date(2026, 9, 24)))
    assert len(deleted) == 5 and freed == 50
    assert made["keep_s1"].exists() and made["keep_gpm"].exists()


def test_stale_granules_keep_7_day_window(live_root):
    files, made = live_root
    stale = set(files.stale_granules(date(2026, 9, 12)))
    # 2026-08-01 dan MODIS hari ke-200 (19 Jul) < 12 Sep - 7 hari.
    assert stale == {made["old_granule"], made["modis_granule"]}


def test_other_dataset_root_untouched(live_root):
    files, _ = live_root
    std_root = fm.get_dataset_root(37, "wajo_jan_apr_2025")
    keep = _touch(std_root / "sentinel-1/PROCESSED" / "S1A_IW_GRDH_1SDV_20260924T000000_VH_lee.tif")
    files.delete(files.files_for_date(date(2026, 9, 24)))
    assert keep.exists()


@pytest.mark.parametrize("n,expected", [(1, 1), (3, 1), (4, 2), (6, 2), (7, 3), (9, 3), (10, 4), (12, 4)])
def test_forecast_steps_table(n, expected):
    assert lm.forecast_steps(n) == expected


@pytest.mark.parametrize("bad", [0, 13, "x", None])
def test_retention_bounds(bad):
    with pytest.raises(ValueError):
        lm._check_retention(bad)


def test_granule_date_parsing():
    assert lm._granule_date("MCDWD_L3.A2026001.h29v09.061.x.hdf") == date(2026, 1, 1)
    assert lm._granule_date("3B-DAY-L.MS.MRG.3IMERG.20260924-S000000-E235959.V07B.nc4") == date(2026, 9, 24)
    assert lm._granule_date("random.txt") is None
