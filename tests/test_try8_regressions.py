# tests/test_try8_regressions.py
"""
Regresi dari audit dataset 24_try8 -- tiap kelas di sini menjaga satu akar
masalah yang membuat berkas keluaran belum layak diserahkan ke konsumen ML:

  - fusion_20250114 berisi GPM/MODIS 15 Jan (hari SETELAH akuisisi S1 22:25 UTC)
  - satu sidecar fusion_metadata_processed.json untuk tiga HDF5 (tertimpa)
  - berkas GPM 9x8 dengan satu kolom nodata di luar AOI (drift float32)
  - scene S1 yang cuma menutup 0,5% AOI tetap diunduh (1,6 GB) dan difusikan
"""

from __future__ import annotations

# etl diimpor SEBELUM rasterio: etl/__init__ membersihkan PROJ_LIB/GDAL_DATA
# yang diset PostgreSQL/PostGIS system-wide.
import etl  # noqa: F401  (efek samping yang disengaja, lihat etl/__init__.py)

from datetime import date, datetime, timezone

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from etl import module1_download as m1
from etl import module5_orchestrator as m5
from etl import module8_gpm_download as m8
from etl import module9_fusion as m9

AOI = (106.4, -6.7, 107.2, -5.9)
AOI_WKT = box(*AOI).wkt


class TestAuxDayNeverAfterFeatureDate:
    @pytest.fixture
    def aux_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            m9.fm, "get_scene_dir", lambda *a, **k: tmp_path
        )
        return tmp_path

    @staticmethod
    def _name(d: date) -> str:
        return f"gpm_rain_24h_{d.strftime('%Y%m%d')}.tif"

    def _find(self, feature_date):
        return m9._find_aux_daily_file(1, "x", "gpm", self._name, feature_date)

    def test_prefers_the_feature_date_itself(self, aux_dir):
        for d in (date(2025, 1, 13), date(2025, 1, 14), date(2025, 1, 15)):
            (aux_dir / self._name(d)).touch()
        path, used = self._find(date(2025, 1, 14))
        assert used == date(2025, 1, 14)
        assert path.name == self._name(date(2025, 1, 14))

    def test_never_uses_the_next_day(self, aux_dir):
        """24_try8: S1 14 Jan 22:25 UTC dipasangkan dengan GPM 15 Jan karena
        tengah malam 15 Jan 'lebih dekat'. Hujan hari itu turun setelah citra
        diambil."""
        (aux_dir / self._name(date(2025, 1, 15))).touch()
        assert self._find(date(2025, 1, 14)) is None

    def test_falls_back_to_the_previous_day(self, aux_dir):
        (aux_dir / self._name(date(2025, 1, 13))).touch()
        path, used = self._find(date(2025, 1, 14))
        assert used == date(2025, 1, 13)


class TestHoursAfterAcquisition:
    def test_descending_pass_late_in_the_utc_day(self):
        acq = datetime(2025, 1, 14, 22, 25, 51, tzinfo=timezone.utc)
        assert m9._hours_after_acquisition("2025-01-15T00:00:00Z", acq) == pytest.approx(1.57, abs=0.01)

    def test_timezone_of_the_acquisition_does_not_matter(self):
        from datetime import timedelta
        wib = timezone(timedelta(hours=7))
        acq = datetime(2025, 1, 15, 5, 25, 51, tzinfo=wib)  # = 14 Jan 22:25 UTC
        assert m9._hours_after_acquisition("2025-01-15T00:00:00Z", acq) == pytest.approx(1.57, abs=0.01)

    def test_missing_inputs(self):
        assert m9._hours_after_acquisition(None, datetime.now(timezone.utc)) is None
        assert m9._hours_after_acquisition("2025-01-15T00:00:00Z", None) is None


class TestSidecarPerDate:
    def test_every_date_gets_its_own_sidecar(self):
        names = {
            m9.fusion_metadata_name(d, "PROCESSED", "HYBRID")
            for d in ("20250111", "20250114", "20250123")
        }
        assert len(names) == 3

    def test_sidecar_shares_the_h5_stem(self):
        h5 = m9.fusion_h5_name("20250111", "PROCESSED", "HYBRID")
        meta = m9.fusion_metadata_name("20250111", "PROCESSED", "HYBRID")
        assert meta == h5.removesuffix(".h5") + "_metadata.json"


class TestImergGridSnap:
    """Grid IMERG global dengan koordinat float32, persis seperti granule asli."""

    @pytest.fixture
    def global_grid(self):
        lon = (np.arange(3600) * 0.1 - 179.95).astype("float32").astype("float64")
        lat = (np.arange(1800) * 0.1 - 89.95).astype("float32").astype("float64")
        res_x = m8._snap_resolution(abs(lon[-1] - lon[0]) / (lon.size - 1))
        res_y = m8._snap_resolution(abs(lat[-1] - lat[0]) / (lat.size - 1))
        transform = from_origin(
            m8._snap_edge(lon[0] - res_x / 2, res_x),
            m8._snap_edge(lat[-1] + res_y / 2, res_y), res_x, res_y,
        )
        data = np.random.default_rng(0).random((1800, 3600))
        return data, transform

    def test_resolution_is_exactly_the_official_one(self, global_grid):
        _, transform = global_grid
        assert transform.a == 0.1 and transform.e == -0.1
        assert transform.c == -180.0 and transform.f == 90.0

    def test_aoi_crop_is_8x8_without_nodata(self, global_grid, tmp_path):
        data, transform = global_grid
        out = tmp_path / "gpm.tif"
        m8._crop_to_aoi(data, transform, "EPSG:4326", AOI, out)
        with rasterio.open(out) as src:
            arr = src.read(1)
            assert (src.width, src.height) == (8, 8)
            assert not (arr == src.nodata).any()
            assert src.bounds.left == pytest.approx(106.4)
            assert src.bounds.right == pytest.approx(107.2)

    def test_unaligned_aoi_still_includes_every_touched_cell(self, global_grid, tmp_path):
        data, transform = global_grid
        out = tmp_path / "gpm.tif"
        m8._crop_to_aoi(data, transform, "EPSG:4326", (106.45, -6.65, 107.15, -5.95), out)
        with rasterio.open(out) as src:
            assert (src.width, src.height) == (8, 8)

    def test_window_tags_describe_the_utc_span(self, global_grid, tmp_path):
        data, transform = global_grid
        out = tmp_path / "gpm.tif"
        tags = m8._window_tags(datetime(2025, 1, 14), "72h", 3, ["F"])
        m8._crop_to_aoi(data, transform, "EPSG:4326", AOI, out, tags=tags)
        with rasterio.open(out) as src:
            written = src.tags()
        assert written["WINDOW_START_UTC"] == "2025-01-12T00:00:00Z"
        assert written["WINDOW_END_UTC"] == "2025-01-15T00:00:00Z"


def _scene(pid: str, day: date, footprint):
    return {
        "product_identifier": pid,
        "acquisition_datetime": datetime(day.year, day.month, day.day, 11, tzinfo=timezone.utc),
        "footprint_wkt": footprint.wkt if footprint is not None else None,
    }


class TestS1AoiCoverageFilter:
    def test_aoi_coverage_of_a_union(self):
        west = box(106.4, -6.7, 106.8, -5.9)
        east = box(106.8, -6.7, 107.2, -5.9)
        assert m1.aoi_coverage([west.wkt, east.wkt], AOI_WKT) == pytest.approx(1.0)
        assert m1.aoi_coverage([west.wkt], AOI_WKT) == pytest.approx(0.5)

    def test_date_barely_touching_the_aoi_is_dropped(self):
        sliver = box(107.19, -6.7, 107.5, -5.9)  # ~1% AOI
        scenes = [_scene("S1_A", date(2025, 1, 14), sliver)]
        assert m5._drop_dates_barely_covering_aoi(scenes, AOI_WKT, 0) == []

    def test_frames_of_one_pass_are_judged_together(self):
        """Dua frame 11 Jan menutup 36% + 64% AOI: keduanya harus lolos, dan
        frame yang kecil tidak boleh dinilai sendirian."""
        south = box(106.0, -7.0, 107.5, -6.41)
        north = box(106.0, -6.41, 107.5, -5.5)
        scenes = [
            _scene("S1_south", date(2025, 1, 11), south),
            _scene("S1_north", date(2025, 1, 11), north),
        ]
        kept = m5._drop_dates_barely_covering_aoi(scenes, AOI_WKT, 0)
        assert [s["product_identifier"] for s in kept] == ["S1_south", "S1_north"]

    def test_unknown_footprint_is_kept(self):
        scenes = [_scene("S1_unknown", date(2025, 1, 14), None)]
        assert m5._drop_dates_barely_covering_aoi(scenes, AOI_WKT, 0) == scenes

    def test_footprint_parsed_from_cdse_geojson(self):
        item = {"GeoFootprint": {
            "type": "Polygon",
            "coordinates": [[[106, -7], [108, -7], [108, -5], [106, -5], [106, -7]]],
        }}
        assert m1.aoi_coverage([m1._footprint_wkt(item)], AOI_WKT) == pytest.approx(1.0)

    def test_footprint_parsed_from_cdse_odata_string(self):
        item = {"Footprint": "geography'SRID=4326;POLYGON ((106 -7, 108 -7, 108 -5, 106 -5, 106 -7))'"}
        assert m1.aoi_coverage([m1._footprint_wkt(item)], AOI_WKT) == pytest.approx(1.0)
