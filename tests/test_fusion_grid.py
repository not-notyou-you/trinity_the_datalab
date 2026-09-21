# tests/test_fusion_grid.py
"""
Tests grid referensi stack FUSION (etl/module9_fusion._dataset_fusion_grid).

Invarian yang dijaga: SEMUA tanggal satu dataset dirakit di grid yang sama,
apa pun jejak scene Sentinel-1 hari itu. Tanpa itu deret waktunya tidak bisa
ditumpuk jadi satu array — di dataset 22_try6 sebulan data menghasilkan lima
bentuk berbeda (8789x1488, 6067x8752, 8790x1483, 6068x8752, 8790x1492),
sebagian cuma menutup 17% AOI.
"""

from __future__ import annotations

# etl diimpor SEBELUM rasterio: etl/__init__ membersihkan PROJ_LIB/GDAL_DATA
# yang diset PostgreSQL/PostGIS system-wide, dan rasterio mengunci nilainya
# saat pertama kali dimuat.
import etl  # noqa: F401  (efek samping yang disengaja, lihat etl/__init__.py)

import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin

from etl import module9_fusion as m9

AOI = (106.4, -6.7, 107.2, -5.9)
RES = 0.01


@pytest.fixture
def reference(monkeypatch):
    """Ganti pencarian raster S1 dataset dengan grid yang ditentukan test."""
    state: dict = {"grid": None}

    def fake_reference(db, dataset_id, tier):
        return state["grid"]

    monkeypatch.setattr(m9, "_dataset_s1_reference_grid", fake_reference)

    def set_grid(res=RES, west=106.5, north=-6.0, shape=(11, 13)):
        state["grid"] = (from_origin(west, north, res, res),
                         CRS.from_epsg(4326), shape)

    state["set"] = set_grid
    return state


def _grid(dataset_id=1):
    return m9._dataset_fusion_grid(None, dataset_id, "COG", AOI)


class TestExtent:
    def test_covers_the_whole_aoi_not_the_scene_footprint(self, reference):
        # Raster acuan cuma menutup sepotong kecil AOI di tengah.
        reference["set"](west=106.9, north=-6.2, shape=(5, 5))

        transform, _, (height, width) = _grid()

        west, north = transform * (0, 0)
        east, south = transform * (width, height)
        assert (west, north) == pytest.approx((AOI[0], AOI[3]))
        assert east == pytest.approx(AOI[2], abs=RES)
        assert south == pytest.approx(AOI[1], abs=RES)

    def test_same_shape_for_every_date_whatever_the_footprint(self, reference):
        """Inti perbaikannya: jejak scene yang berbeda-beda tidak boleh lagi
        mengubah bentuk stack."""
        shapes = []
        for west, north, shape in (
            (106.5, -6.0, (11, 13)),   # sepotong utara
            (107.0, -6.6, (3, 40)),    # jalur sempit di timur
            (106.4, -6.7, (80, 80)),   # AOI penuh
        ):
            reference["set"](west=west, north=north, shape=shape)
            shapes.append(_grid()[2])

        assert len(set(shapes)) == 1, shapes

    def test_shape_follows_the_aoi_and_resolution(self, reference):
        reference["set"](res=0.1)
        assert _grid()[2] == (8, 8)

        reference["set"](res=0.01)
        assert _grid()[2] == (80, 80)


class TestResolution:
    def test_takes_resolution_from_the_dataset_s1_raster(self, reference):
        reference["set"](res=0.02)
        transform, _, _ = _grid()
        assert abs(transform.a) == pytest.approx(0.02)
        assert abs(transform.e) == pytest.approx(0.02)

    def test_falls_back_to_the_constant_without_any_s1_raster(self, reference):
        from etl.module8_gpm_download import S1_RESOLUTION_DEG

        reference["grid"] = None
        transform, crs, _ = _grid()
        assert abs(transform.a) == pytest.approx(S1_RESOLUTION_DEG)
        assert crs == CRS.from_epsg(4326)


class TestStackability:
    def test_two_footprints_reproject_onto_identical_arrays(self, reference, tmp_path):
        """Uji ujungnya: dua raster dengan jejak berbeda, direproject ke grid
        dataset, menghasilkan array sebentuk yang bisa langsung ditumpuk."""
        reference["set"](res=0.05)
        transform, crs, shape = _grid()

        def _raster(name, west, north, rows, cols, value):
            path = tmp_path / name
            with rasterio.open(
                path, "w", driver="GTiff", height=rows, width=cols, count=1,
                dtype="float32", crs="EPSG:4326",
                transform=from_origin(west, north, 0.05, 0.05), nodata=np.nan,
            ) as dst:
                dst.write(np.full((rows, cols), value, dtype="float32"), 1)
            return path

        north_half = _raster("a.tif", 106.4, -5.9, 8, 16, 1.0)
        east_strip = _raster("b.tif", 107.0, -5.9, 16, 4, 2.0)

        arrays = [
            m9._reproject_to_grid(
                path, transform, crs, shape, rasterio.enums.Resampling.nearest,
                float("nan"),
            )
            for path in (north_half, east_strip)
        ]

        assert arrays[0].shape == arrays[1].shape == shape
        stacked = np.stack(arrays)
        assert stacked.shape == (2, *shape)
        # Masing-masing tetap membawa datanya sendiri di tempat yang benar.
        assert np.nanmax(arrays[0]) == 1.0
        assert np.nanmax(arrays[1]) == 2.0


class TestLayerCoverage:
    """Cakupan piksel valid dulu tidak pernah diukur: scene yang cuma
    menyerempet AOI menghasilkan stack 96% NaN dan tetap dilaporkan sukses."""

    def test_float_layer_counts_finite_pixels(self):
        array = np.full((10, 10), np.nan, dtype="float32")
        array[:2, :] = 1.0
        assert m9._valid_fraction(array) == pytest.approx(0.2)

    def test_categorical_layer_counts_non_nodata(self):
        array = np.full((10, 10), m9.MODIS_NODATA_U8, dtype="uint8")
        array[0, :] = 1
        assert m9._valid_fraction(array) == pytest.approx(0.1)

    def test_empty_and_full_layers_are_the_extremes(self):
        assert m9._valid_fraction(np.full((4, 4), np.nan, dtype="float32")) == 0.0
        assert m9._valid_fraction(np.zeros((4, 4), dtype="float32")) == 1.0

    def test_try6_sliver_lands_below_the_warning_threshold(self):
        """Tiga dari lima tanggal 22_try6 berisi 3,6-3,7% piksel valid."""
        array = np.full((1000, 1), np.nan, dtype="float32")
        array[:37] = 1.0
        assert m9._valid_fraction(array) < m9.LOW_COVERAGE_FRACTION


class TestLayerWriter:
    """Lapisan ditulis satu per satu supaya stack seukuran AOI tidak perlu
    ditampung seluruhnya di memori."""

    def test_layers_are_compressed_and_shuffled(self, tmp_path):
        import h5py

        writer = m9._FusionH5Layers(tmp_path / "stack.h5", (64, 64))
        writer.add("sentinel1/VV", np.zeros((64, 64), dtype="float32"))
        writer.add("modis/FLOOD", np.zeros((64, 64), dtype="uint8"))
        writer.close()

        with h5py.File(tmp_path / "stack.h5", "r") as f:
            vv = f["sentinel1/VV"]
            assert vv.compression == "gzip"
            assert vv.shuffle is True
            # NoData tiap lapisan ikut tertulis, dan bentuknya beda untuk
            # lapisan kategorikal.
            assert vv.attrs["nodata"] == "NaN"
            assert f["modis/FLOOD"].attrs["nodata"] == m9.MODIS_NODATA_U8

    def test_names_are_collected_in_write_order(self, tmp_path):
        writer = m9._FusionH5Layers(tmp_path / "stack.h5", (8, 8))
        for name in ("sentinel1/VV", "sentinel1/VH", "gpm/rainfall_24h"):
            writer.add(name, np.zeros((8, 8), dtype="float32"))
        writer.close()

        assert writer.names == ["sentinel1/VV", "sentinel1/VH", "gpm/rainfall_24h"]
