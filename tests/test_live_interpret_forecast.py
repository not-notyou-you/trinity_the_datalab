# tests/test_live_interpret_forecast.py
"""Kalimat kondisi (etl/live_interpret.py) dan forecast (etl/live_forecast.py)."""
from __future__ import annotations

from datetime import date, timedelta

import pytest

from etl import live_forecast as lf
from etl import live_interpret as li


def _m(vh_pct=5.0, vv=-12.0, ndwi=10.0, ndvi=0.6, flood=0.0, valid=80.0, r24=2.0, r72=10.0, r7d=20.0):
    return {
        "sentinel1": {"vv_mean_db": vv, "vh_mean_db": -18.0, "vh_water_pct": vh_pct},
        "modis": {
            "flood": {"valid_pct": valid, "cloud_pct": 100 - valid, "flood_pct": flood},
            "ndvi": {"valid_pct": valid, "cloud_pct": 100 - valid, "mean": ndvi},
            "ndwi": {"valid_pct": valid, "cloud_pct": 100 - valid, "water_pct": ndwi},
        },
        "gpm": {"rain_24h": {"mean_mm": r24}, "rain_72h": {"mean_mm": r72}, "rain_7d": {"mean_mm": r7d}},
    }


def test_all_eight_sentences_follow_format():
    out = li.interpret_scene(_m(), _m())
    assert set(out) == set(li.PREVIEW_KEYS)
    for v in out.values():
        assert v["text"].startswith("Menampilkan ")
        assert " dalam kondisi " in v["text"] and " karena " in v["text"]
        assert any(ch.isdigit() for ch in v["because"])  # selalu ada angka


def test_vh_increase_is_flood_indication_with_comparison():
    out = li.interpret_scene(_m(vh_pct=18.0), _m(vh_pct=6.0))
    s = out["s1_vh"]
    assert s["category"] == li.GENANGAN
    assert "18% area memiliki VH < −20 dB" in s["text"]
    assert "naik dari 6% pada scene sebelumnya" in s["text"]


def test_rain_72h_high_mentions_threshold():
    s = li.interpret_scene(_m(r72=142.0), None)["gpm_rain_72h"]
    assert s["category"] == li.TINGGI
    assert "142 mm" in s["text"] and "ambang 100 mm" in s["text"]


def test_cloudy_modis_is_unavailable_with_cloud_percent():
    s = li.interpret_scene(_m(valid=8.0), None)["modis_ndwi"]
    assert s["category"] == li.NA
    assert "tutupan awan 92%" in s["text"]


def test_missing_source_explained():
    m = _m()
    m["gpm"] = {}
    s = li.interpret_scene(m, None, {"gpm": {"status": "FAILED"}})["gpm_rain_24h"]
    assert s["category"] == li.NA and "dicoba ulang" in s["text"]


def test_area_status_combines_three_signals():
    it = li.interpret_scene(_m(vh_pct=20.0, r72=150.0), _m(vh_pct=5.0))
    st = li.area_status(it)
    assert st["label"] == "Tinggi"
    assert "hujan tinggi" in st["text"] and "area genangan meningkat" in st["text"]
    calm = li.area_status(li.interpret_scene(_m(), _m()))
    assert calm["label"] == "Normal"


def _pts(vals, start=date(2026, 1, 1), step=12):
    return [(start + timedelta(days=i * step), v) for i, v in enumerate(vals)]


@pytest.mark.parametrize("n,method", [(1, "persistence"), (2, "ses"), (3, "ses"), (4, "holt"), (12, "holt")])
def test_method_by_length(n, method):
    fc = lf.forecast_series(_pts([10.0 + i for i in range(n)]))
    assert fc["method"] == method
    assert len(fc["points"]) == lf.forecast_steps(n)


def test_single_point_persistence_wide_band():
    fc = lf.forecast_series(_pts([30.0]), bounds=(0, None), fallback_sigma=25.0)
    p = fc["points"][0]
    assert p["mean"] == 30.0 and fc["note"] == "data belum cukup"
    assert p["lo"] == 0.0 and p["hi"] > 60  # pita lebar, hujan tidak negatif


def test_forecast_dates_follow_average_interval():
    fc = lf.forecast_series(_pts([1, 2, 3, 4, 5, 6], step=6))
    assert fc["interval_days"] == 6
    assert fc["points"][0]["date"] == (date(2026, 1, 1) + timedelta(days=36)).isoformat()


def test_rain_never_negative():
    fc = lf.forecast_series(_pts([80, 60, 40, 20, 5, 1]), bounds=(0, None), fallback_sigma=25)
    assert all(p["lo"] >= 0 and p["mean"] >= 0 for p in fc["points"])


def test_area_forecast_steps_from_stored_scenes():
    scenes = [(d, _m(r72=v)) for d, v in _pts([10, 20, 30, 40, 50])]
    scenes[2][1]["modis"] = {}  # satu scene tanpa MODIS
    fc = lf.build_area_forecast(scenes)
    assert fc["steps"] == 2
    assert len(fc["series"]["modis"]["forecast"]["points"]) == 2
    assert fc["series"]["gpm"]["thresholds"]["tinggi"] == li.THRESHOLDS["rain_72h_high_mm"]
