# etl/live_forecast.py
"""Forecast ringan per Daerah Live (LIVE_MONITORING.md bagian 5).

Deret sangat pendek (1-12 scene), tanpa training:

    1 titik    persistence (nilai terakhir), pita lebar, label "data belum cukup"
    2-3 titik  simple exponential smoothing (alpha tetap)
    >= 4 titik Holt (level + tren) dengan tren teredam (phi) supaya 4 titik
               tidak diekstrapolasi jadi garis lurus tak terbatas

Parameter smoothing TETAP (bukan dioptimasi): dengan <= 12 titik, optimasi
alpha/beta hanya menghafal derau. Jumlah langkah = ceil(n/3) (tabel 5.1).

Pita ketidakpastian (80%): z * sigma * sqrt(h) * inflasi, di mana sigma =
RMSE galat satu-langkah (in-sample) dan inflasi = 1 + 2/n (deret pendek =>
pita lebih lebar). Tanpa galat yang bisa dihitung (1-2 titik) sigma diambil
dari FALLBACK_SIGMA variabel itu. Hasil dijepit ke rentang fisik (hujan >= 0,
persen 0..100).

Tanggal langkah ke-k = tanggal terakhir + k * rata-rata interval antar scene
(12 hari kalau baru ada 1 scene, revisit S1 yang umum).

Modul murni: tanpa DB, tanpa disk.
"""
from __future__ import annotations

import math
from datetime import date, timedelta

from etl.live_interpret import THRESHOLDS

ALPHA = 0.5
BETA = 0.3
PHI = 0.8
Z80 = 1.2816
DEFAULT_INTERVAL_DAYS = 12

# Deret yang digrafikkan: satu per satelit (4.4). key -> cara mengambil nilai
# dari live_scenes.metrics.
SERIES: dict[str, dict] = {
    "sentinel1": {
        "label": "Rata-rata VH", "unit": "dB",
        "path": ("sentinel1", "vh_mean_db"), "bounds": (-40.0, 10.0),
        "fallback_sigma": 1.5, "chart": "line",
    },
    # MODIS: % area air NDWI. LST tidak ada di pipeline (lihat catatan di
    # etl/live_interpret.py); indeks air adalah konfirmasi optik genangan.
    "modis": {
        "label": "Area air (NDWI > 0)", "unit": "%",
        "path": ("modis", "ndwi", "water_pct"), "bounds": (0.0, 100.0),
        "fallback_sigma": 5.0, "chart": "line",
    },
    "gpm": {
        "label": "Hujan 72 jam", "unit": "mm",
        "path": ("gpm", "rain_72h", "mean_mm"), "bounds": (0.0, None),
        "fallback_sigma": 25.0, "chart": "bar",
    },
}


def forecast_steps(n: int) -> int:
    return math.ceil(n / 3) if n > 0 else 0


def _get(d, path):
    for k in path:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d if isinstance(d, (int, float)) and math.isfinite(d) else None


def _clip(v, bounds):
    lo, hi = bounds
    if lo is not None:
        v = max(lo, v)
    if hi is not None:
        v = min(hi, v)
    return v


def _ses(y: list[float]) -> tuple[list[float], float]:
    """(prediksi satu-langkah untuk y[1:], level akhir)."""
    level = y[0]
    preds = []
    for v in y[1:]:
        preds.append(level)
        level = ALPHA * v + (1 - ALPHA) * level
    return preds, level


def _holt(y: list[float]) -> tuple[list[float], float, float]:
    level, trend = y[0], y[1] - y[0]
    preds = []
    for v in y[1:]:
        pred = level + PHI * trend
        preds.append(pred)
        new_level = ALPHA * v + (1 - ALPHA) * pred
        trend = BETA * (new_level - level) + (1 - BETA) * PHI * trend
        level = new_level
    return preds, level, trend


def forecast_series(points: list[tuple[date, float]], bounds=(None, None),
                    fallback_sigma: float = 1.0, steps: int | None = None) -> dict:
    """Forecast satu deret. points: [(tanggal, nilai)] urut naik. `steps`
    default ceil(n/3); pemanggil mengoper jumlah dari scene TERSIMPAN supaya
    sumber yang bolong di satu scene tetap mengikuti tabel 5.1."""
    pts = [(d, float(v)) for d, v in points if v is not None]
    n = len(pts)
    if n == 0:
        return {"method": None, "points": [], "note": "tidak ada data"}
    y = [v for _, v in pts]
    h = steps if steps else forecast_steps(n)

    if n >= 2:
        gaps = [(pts[i + 1][0] - pts[i][0]).days for i in range(n - 1)]
        interval = max(1, round(sum(gaps) / len(gaps)))
    else:
        interval = DEFAULT_INTERVAL_DAYS

    if n == 1:
        method, note = "persistence", "data belum cukup"
        means = [y[-1]] * h
        sigma = fallback_sigma * 2
    elif n <= 3:
        method, note = "ses", "data sedikit, pita lebar"
        preds, level = _ses(y)
        means = [level] * h
        sigma = _rmse(y[1:], preds, fallback_sigma)
    else:
        method, note = "holt", None
        preds, level, trend = _holt(y)
        means = []
        damp = 0.0
        for k in range(1, h + 1):
            damp += PHI ** k
            means.append(level + damp * trend)
        sigma = _rmse(y[1:], preds, fallback_sigma)

    inflate = 1 + 2 / n
    out = []
    last = pts[-1][0]
    for k, m in enumerate(means, start=1):
        half = Z80 * sigma * math.sqrt(k) * inflate
        out.append({
            "date": (last + timedelta(days=interval * k)).isoformat(),
            "mean": round(_clip(m, bounds), 3),
            "lo": round(_clip(m - half, bounds), 3),
            "hi": round(_clip(m + half, bounds), 3),
        })
    return {"method": method, "note": note, "interval_days": interval,
            "sigma": round(sigma, 3), "points": out}


def _rmse(actual, preds, fallback):
    errs = [a - p for a, p in zip(actual, preds)]
    if len(errs) < 2:
        # Satu galat bukan perkiraan sebaran; jangan lebih sempit dari fallback.
        return max(fallback, abs(errs[0]) if errs else 0.0)
    rmse = math.sqrt(sum(e * e for e in errs) / len(errs))
    return max(rmse, fallback * 0.25)


def build_area_forecast(scenes: list[tuple[date, dict]]) -> dict:
    """{steps, series: {key: {label, unit, actual:[...], forecast:{...}}}}
    dari [(scene_date, metrics)] urut naik."""
    result = {"steps": forecast_steps(len(scenes)), "n_scenes": len(scenes), "series": {}}
    for key, cfg in SERIES.items():
        actual = [(d, _get(m, cfg["path"])) for d, m in scenes]
        valid = [(d, v) for d, v in actual if v is not None]
        fc = forecast_series(valid, cfg["bounds"], cfg["fallback_sigma"],
                             steps=result["steps"])
        result["series"][key] = {
            "label": cfg["label"], "unit": cfg["unit"], "chart": cfg["chart"],
            "actual": [{"date": d.isoformat(), "value": v} for d, v in actual],
            "forecast": fc,
        }
    # Garis ambang grafik GPM (4.4) dari tempat ambang yang sama dengan
    # kalimat kondisi.
    result["series"]["gpm"]["thresholds"] = {
        "waspada": THRESHOLDS["rain_72h_warn_mm"],
        "tinggi": THRESHOLDS["rain_72h_high_mm"],
    }
    return result
