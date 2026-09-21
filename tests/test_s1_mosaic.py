# tests/test_s1_mosaic.py
"""
Tests etl/s1_mosaic.py — penyatuan frame Sentinel-1 satu tanggal.

Skenario acuannya dataset 22_try6: dua frame satu lintasan di 2025-01-23,
satu menutup separuh selatan AOI (cakupan 51,7%), satu separuh utara (68,7%),
dan yang selesai belakangan menimpa yang duluan.
"""

from __future__ import annotations

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from etl import s1_mosaic

# AOI uji: 0,8 x 0,8 derajat, piksel 0,01 derajat -> 80 x 80 piksel penuh.
PIX = 0.01
WEST, NORTH = 106.4, -5.9


def _frame(path, *, row_from, row_to, value, res=PIX, cols=80, rows=80):
    """Raster 80x80 di grid AOI; hanya baris row_from..row_to yang berisi
    `value`, sisanya NaN — meniru frame yang cuma menutup sebagian AOI."""
    array = np.full((rows, cols), np.nan, dtype="float32")
    array[row_from:row_to, :] = value
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        path, "w", driver="GTiff", height=rows, width=cols, count=1,
        dtype="float32", crs="EPSG:4326",
        transform=from_origin(WEST, NORTH, res, res), nodata=np.nan,
    ) as dst:
        dst.write(array, 1)
    return str(path)


def _read(path):
    with rasterio.open(path) as src:
        return src.read(1)


class TestComplementaryFrames:
    def test_two_halves_become_one_full_coverage_raster(self, tmp_path):
        north = {"VV": _frame(tmp_path / "north_vv.tif", row_from=0, row_to=40, value=1.0)}
        south = {"VV": _frame(tmp_path / "south_vv.tif", row_from=40, row_to=80, value=2.0)}

        out = s1_mosaic.mosaic_frames(
            [north, south], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )

        data = _read(out["VV"])
        assert np.isfinite(data).all(), "masih ada lubang setelah mosaik"
        assert (data[:40] == 1.0).all()
        assert (data[40:] == 2.0).all()

    def test_result_does_not_depend_on_input_order(self, tmp_path):
        north = {"VV": _frame(tmp_path / "n.tif", row_from=0, row_to=40, value=1.0)}
        south = {"VV": _frame(tmp_path / "s.tif", row_from=40, row_to=80, value=2.0)}

        a = s1_mosaic.mosaic_frames(
            [north, south], tmp_path / "wa", date_key="20250123", level="PROCESSED"
        )
        b = s1_mosaic.mosaic_frames(
            [south, north], tmp_path / "wb", date_key="20250123", level="PROCESSED"
        )
        assert np.array_equal(_read(a["VV"]), _read(b["VV"]))

    def test_overlap_goes_to_the_frame_with_more_data(self, tmp_path):
        # Frame besar (60 baris) dan kecil (30 baris) bertumpang tindih di
        # baris 30-60. Yang menang harus yang cakupannya lebih besar, bukan
        # yang kebetulan disebut belakangan.
        big = {"VV": _frame(tmp_path / "big.tif", row_from=0, row_to=60, value=1.0)}
        small = {"VV": _frame(tmp_path / "small.tif", row_from=30, row_to=80, value=2.0)}

        out = s1_mosaic.mosaic_frames(
            [small, big], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        data = _read(out["VV"])
        assert (data[30:60] == 1.0).all(), "daerah tumpang tindih diambil dari frame kecil"
        assert (data[60:] == 2.0).all()

    def test_each_band_mosaicked_independently(self, tmp_path):
        a = {"VV": _frame(tmp_path / "a_vv.tif", row_from=0, row_to=40, value=1.0),
             "VH": _frame(tmp_path / "a_vh.tif", row_from=0, row_to=40, value=3.0)}
        b = {"VV": _frame(tmp_path / "b_vv.tif", row_from=40, row_to=80, value=2.0),
             "VH": _frame(tmp_path / "b_vh.tif", row_from=40, row_to=80, value=4.0)}

        out = s1_mosaic.mosaic_frames(
            [a, b], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        assert set(out) == {"VV", "VH"}
        assert np.isfinite(_read(out["VV"])).all()
        assert np.isfinite(_read(out["VH"])).all()

    def test_differing_resolution_keeps_the_finest(self, tmp_path):
        fine = {"VV": _frame(tmp_path / "fine.tif", row_from=0, row_to=80,
                             value=1.0, res=PIX / 2, cols=160, rows=160)}
        coarse = {"VV": _frame(tmp_path / "coarse.tif", row_from=0, row_to=80, value=2.0)}

        out = s1_mosaic.mosaic_frames(
            [fine, coarse], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        with rasterio.open(out["VV"]) as src:
            assert src.res[0] == pytest.approx(PIX / 2)


class TestDegenerateInput:
    def test_single_frame_is_returned_untouched(self, tmp_path):
        only = {"VV": _frame(tmp_path / "only.tif", row_from=0, row_to=40, value=1.0)}

        out = s1_mosaic.mosaic_frames(
            [only], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        assert out == only
        assert not (tmp_path / "work").exists(), "mosaik satu frame tidak boleh menyalin"

    def test_missing_file_is_skipped_not_fatal(self, tmp_path):
        present = {"VV": _frame(tmp_path / "ok.tif", row_from=0, row_to=40, value=1.0)}
        gone = {"VV": str(tmp_path / "tidak_ada.tif")}

        out = s1_mosaic.mosaic_frames(
            [present, gone], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        assert out == present

    def test_band_present_in_only_one_frame_still_survives(self, tmp_path):
        both = {"VV": _frame(tmp_path / "a_vv.tif", row_from=0, row_to=40, value=1.0),
                "VH": _frame(tmp_path / "a_vh.tif", row_from=0, row_to=40, value=3.0)}
        vv_only = {"VV": _frame(tmp_path / "b_vv.tif", row_from=40, row_to=80, value=2.0)}

        out = s1_mosaic.mosaic_frames(
            [both, vv_only], tmp_path / "work", date_key="20250123", level="PROCESSED"
        )
        assert set(out) == {"VV", "VH"}
        assert out["VH"] == both["VH"]
        assert np.isfinite(_read(out["VV"])).all()

    def test_mismatched_crs_falls_back_to_largest_frame(self, tmp_path):
        big = _frame(tmp_path / "big.tif", row_from=0, row_to=60, value=1.0)
        other = tmp_path / "other.tif"
        array = np.full((80, 80), 2.0, dtype="float32")
        with rasterio.open(
            other, "w", driver="GTiff", height=80, width=80, count=1,
            dtype="float32", crs="EPSG:32748",
            transform=from_origin(600000, 9300000, 10, 10), nodata=np.nan,
        ) as dst:
            dst.write(array, 1)

        # Frame ber-CRS asing tidak boleh ikut dimosaikkan (merge tidak
        # memeriksa CRS dan akan menaruh piksel di tempat yang salah tanpa
        # error). Yang dipakai frame dengan cakupan terbesar, apa adanya.
        forward = s1_mosaic.mosaic_frames(
            [{"VV": big}, {"VV": str(other)}], tmp_path / "wa",
            date_key="20250123", level="PROCESSED",
        )
        backward = s1_mosaic.mosaic_frames(
            [{"VV": str(other)}, {"VV": big}], tmp_path / "wb",
            date_key="20250123", level="PROCESSED",
        )
        assert forward["VV"] == backward["VV"] == str(other), "pilihan tidak deterministik"
        assert not (tmp_path / "wa").exists(), "tidak boleh ada mosaik lintas-CRS"
