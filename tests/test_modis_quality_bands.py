# tests/test_modis_quality_bands.py
"""
Band kualitas MODIS (etl/module7_modis_download.py), dari audit 24_try8:

  - FLOOD: komposit 2 hari cuma menutup 0-6,5% AOI per hari di musim hujan;
    celahnya diisi komposit 1 hari CS dan asal tiap piksel dicatat di band 2.
  - NDVI/NDWI: satu periode MOD09A1 cuma 0,1-0,15% cerah; sekarang komposit
    observasi cerah terbaru lintas periode (maks INDEX_LOOKBACK_DAYS), umur
    tiap piksel di band 2, dan observasi setelah tanggal fitur dibuang.
"""

from __future__ import annotations

import etl  # noqa: F401  (efek samping yang disengaja, lihat etl/__init__.py)

from datetime import datetime
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from etl import module7_modis_download as m7


def _grid(shape=(4, 4)):
    height, width = shape
    return {
        "crs": "EPSG:4326",
        "transform": from_origin(106.4, -5.9, 0.2, 0.2),
        "width": width, "height": height,
        "bounds": (106.4, -5.9 - 0.2 * height, 106.4 + 0.2 * width, -5.9),
        "nodata": 255,
    }


class TestPeriods:
    def test_newest_first_and_within_lookback(self):
        periods = m7._mod09a1_periods(datetime(2025, 1, 11), 32)
        assert periods[0] == datetime(2025, 1, 9)
        assert [p.date().isoformat() for p in periods] == [
            "2025-01-09", "2025-01-01", "2024-12-26", "2024-12-18", "2024-12-10",
        ]

    def test_year_boundary_uses_the_previous_year_grid(self):
        """Periode MOD09A1 dimulai ulang di DOY 1 tiap tahun: sebelum 1 Jan
        2025 adalah DOY 361 tahun 2024 (26 Des), bukan 24 Des."""
        periods = m7._mod09a1_periods(datetime(2025, 1, 2), 8)
        assert periods[:2] == [datetime(2025, 1, 1), datetime(2024, 12, 26)]


class TestObservationDates:
    def test_doy_wrapping_into_next_year(self, monkeypatch):
        doy = np.array([[361, 365], [2, 65535]], dtype="uint16")
        monkeypatch.setattr(m7, "_read_eos_grid_field", lambda *a, **k: (doy, _grid((2, 2))))
        obs = m7._observation_ordinals(
            Path("x.hdf"), "MOD09A1", (2, 2), datetime(2024, 12, 26), None
        )
        as_dates = [
            datetime.fromordinal(int(v)).date().isoformat() if np.isfinite(v) else None
            for v in obs.ravel()
        ]
        assert as_dates == ["2024-12-26", "2024-12-30", "2025-01-02", None]

    def test_daily_product_uses_the_observation_date(self):
        obs = m7._observation_ordinals(
            Path("x.hdf"), "MOD09GA_NRT", (2, 2), None, datetime(2025, 1, 11)
        )
        assert (obs == datetime(2025, 1, 11).toordinal()).all()


def _period_tif(path: Path, values, observed):
    with rasterio.open(
        path, "w", driver="GTiff", height=2, width=2, count=2, dtype="float32",
        crs="EPSG:4326", transform=from_origin(106.4, -5.9, 0.4, 0.4), nodata=np.nan,
    ) as dst:
        dst.write(np.asarray(values, dtype="float32"), 1)
        dst.write(np.asarray(observed, dtype="float32"), 2)
    return path


class TestLatestClearComposite:
    def test_picks_the_most_recent_clear_observation(self, tmp_path):
        d = datetime(2025, 1, 11)
        o = lambda day: datetime(2025, 1, day).toordinal()  # noqa: E731
        nan = np.nan
        newer = _period_tif(tmp_path / "p0.tif", [[0.1, nan], [0.3, nan]], [[o(10), nan], [o(9), nan]])
        older = _period_tif(tmp_path / "p1.tif", [[0.9, 0.5], [0.8, nan]], [[o(3), o(4)], [o(2), nan]])
        out = tmp_path / "out.tif"
        m7._composite_latest_clear([newer, older], d, out, 32)
        with rasterio.open(out) as src:
            value, age = src.read(1), src.read(2)
        assert value[0, 0] == pytest.approx(0.1) and age[0, 0] == 1
        assert value[0, 1] == pytest.approx(0.5) and age[0, 1] == 7
        assert value[1, 0] == pytest.approx(0.3) and age[1, 0] == 2
        assert np.isnan(value[1, 1]) and np.isnan(age[1, 1])

    def test_future_and_too_old_observations_are_ignored(self, tmp_path):
        d = datetime(2025, 1, 11)
        future = datetime(2025, 1, 14).toordinal()
        too_old = datetime(2024, 11, 1).toordinal()
        p = _period_tif(tmp_path / "p.tif", [[0.5, 0.6], [np.nan, np.nan]],
                        [[future, too_old], [np.nan, np.nan]])
        out = tmp_path / "out.tif"
        m7._composite_latest_clear([p], d, out, 32)
        with rasterio.open(out) as src:
            assert np.isnan(src.read(1)).all()


class TestFloodGapFill:
    def test_two_day_wins_and_one_day_cs_fills_gaps(self, monkeypatch, tmp_path):
        two_day = np.array([[0, 255], [3, 255]], dtype="uint8")
        one_day_cs = np.array([[1, 2], [1, 255]], dtype="uint8")

        def fake_read(_path, name):
            return (two_day if name == m7.FLOOD_SUBDATASET else one_day_cs), _grid((2, 2))

        monkeypatch.setattr(m7, "_read_eos_grid_field", fake_read)
        out = tmp_path / "flood.tif"
        info = m7._flood_tile(Path("x.hdf"), out)
        with rasterio.open(out) as src:
            classes, source = src.read(1), src.read(2)
        assert info == {"filled": True}
        assert classes.tolist() == [[0, 2], [3, 255]]
        assert source.tolist() == [
            [m7.FLOOD_SOURCE_2DAY, m7.FLOOD_SOURCE_1DAY_CS],
            [m7.FLOOD_SOURCE_2DAY, 255],
        ]

    def test_nrt_granule_without_the_fill_field(self, monkeypatch, tmp_path):
        two_day = np.array([[0, 255], [3, 255]], dtype="uint8")

        def fake_read(_path, name):
            if name != m7.FLOOD_SUBDATASET:
                raise RuntimeError("SDS tidak ada")
            return two_day, _grid((2, 2))

        monkeypatch.setattr(m7, "_read_eos_grid_field", fake_read)
        out = tmp_path / "flood.tif"
        assert m7._flood_tile(Path("x.hdf"), out) == {"filled": False}
        with rasterio.open(out) as src:
            assert src.read(1).tolist() == two_day.tolist()


class TestStaleFilesAreRebuilt:
    def test_single_band_file_is_not_current(self, tmp_path):
        path = tmp_path / "old.tif"
        with rasterio.open(
            path, "w", driver="GTiff", height=1, width=1, count=1, dtype="float32",
            crs="EPSG:4326", transform=from_origin(0, 0, 1, 1),
        ) as dst:
            dst.write(np.zeros((1, 1), dtype="float32"), 1)
        assert not m7._is_current_format(path)
