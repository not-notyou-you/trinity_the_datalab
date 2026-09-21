# tests/test_gpm_native_resolution.py
"""
Tests penyimpanan raster hujan GPM (etl/module8_gpm_download._crop_to_aoi).

Berkas rainfall dulu di-resample ke grid Sentinel-1 ~10 m supaya bisa
ditumpuk langsung dengan S1. Satu sel IMERG 0,1 derajat karena itu digandakan
jutaan kali: di dataset 22_try6 satu berkas berisi 64 nilai unik ditulis
sebagai 8906x8906 piksel, dan sebulan data menghabiskan 214 MB.
"""

from __future__ import annotations

# etl diimpor SEBELUM rasterio: etl/__init__ membersihkan PROJ_LIB/GDAL_DATA
# yang diset PostgreSQL/PostGIS system-wide.
import etl  # noqa: F401  (efek samping yang disengaja, lihat etl/__init__.py)

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from etl import module8_gpm_download as m8

AOI = (106.4, -6.7, 107.2, -5.9)
IMERG_RES = 0.1


@pytest.fixture
def imerg_grid():
    """Petak akumulasi IMERG yang jauh lebih luas dari AOI, seperti aslinya."""
    rows, cols = 60, 60
    array = np.arange(rows * cols, dtype="float64").reshape(rows, cols)
    transform = from_origin(103.0, -3.0, IMERG_RES, IMERG_RES)
    return array, transform


def _write(imerg_grid, tmp_path):
    array, transform = imerg_grid
    out = tmp_path / "gpm_rain_24h_20250102.tif"
    m8._crop_to_aoi(array, transform, "EPSG:4326", AOI, out)
    return out


class TestResolution:
    def test_keeps_the_native_imerg_resolution(self, imerg_grid, tmp_path):
        out = _write(imerg_grid, tmp_path)

        with rasterio.open(out) as src:
            assert src.res[0] == pytest.approx(IMERG_RES)
            assert src.res[1] == pytest.approx(IMERG_RES)

    def test_pixel_count_matches_the_cells_that_touch_the_aoi(self, imerg_grid, tmp_path):
        """AOI 0,8 x 0,8 derajat menyentuh sekitar 8-10 sel di tiap sisi —
        bukan ribuan."""
        out = _write(imerg_grid, tmp_path)

        with rasterio.open(out) as src:
            assert src.width <= 12 and src.height <= 12, (src.width, src.height)

    def test_file_is_kilobytes_not_megabytes(self, imerg_grid, tmp_path):
        out = _write(imerg_grid, tmp_path)
        assert out.stat().st_size < 50_000, out.stat().st_size


class TestCoverage:
    def test_covers_the_whole_aoi(self, imerg_grid, tmp_path):
        """all_touched dipertahankan: sel tepi yang pusatnya di luar bbox tetap
        ikut, kalau tidak 12,5% AOI jadi nodata."""
        out = _write(imerg_grid, tmp_path)

        with rasterio.open(out) as src:
            b = src.bounds
        assert b.left <= AOI[0] and b.bottom <= AOI[1]
        assert b.right >= AOI[2] and b.top >= AOI[3]

    def test_values_are_preserved_not_resampled(self, imerg_grid, tmp_path):
        """Tidak ada interpolasi: tiap nilai di keluaran harus ada di sumber."""
        array, _ = imerg_grid
        out = _write(imerg_grid, tmp_path)

        with rasterio.open(out) as src:
            written = src.read(1)
            nodata = src.nodata
        real = written[written != nodata]
        assert real.size > 0
        assert set(np.unique(real)).issubset(set(np.unique(array).astype("float32")))

    def test_georeferencing_stays_epsg4326(self, imerg_grid, tmp_path):
        out = _write(imerg_grid, tmp_path)
        with rasterio.open(out) as src:
            assert src.crs.to_string() == "EPSG:4326"
