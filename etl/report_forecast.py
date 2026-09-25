# etl/report_forecast.py
"""Prakiraan jangka pendek per dataset untuk laporan (etl/report_generator.py).

Prinsip -- sengaja konservatif karena satu dataset hanya berisi beberapa bulan:

* Horizon = sepertiga panjang periode dataset (10 hari -> 3 hari, 90 hari ->
  30 hari). Hanya data dataset ini yang dipakai; tidak ada data luar.
* Beberapa model sederhana bersaing: naif (nilai terakhir), rata-rata periode,
  exponential smoothing (SES) dan Holt damped-trend. Model dipilih lewat
  **backtest**: dilatih pada 2/3 awal, diuji pada 1/3 akhir -- panjang uji
  sama dengan horizon prakiraan yang sesungguhnya, jadi error-nya jujur
  mewakili apa yang akan terjadi.
* Skill = 1 - MAE(model) / MAE(naif). Model yang tidak mengalahkan metode
  naif tidak dipakai: yang dilaporkan adalah pilihan terbaik termasuk naif
  itu sendiri, dengan tingkat keyakinan rendah.
* Pita ketidakpastian 80%/95% dikalibrasi dari error backtest (bukan hanya
  asumsi normal), lalu dipotong ke rentang fisik variabel.
* Curah hujan dimodelkan di ruang log1p (distribusinya miring dan banyak nol).

Modul ini murni numerik (numpy saja) -- tidak menyentuh DB atau disk.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, timedelta
from statistics import mean, pstdev

import numpy as np

from etl import report_stats as rs

HEAVY_RAIN_MM = 20.0  # hujan lebat (BMKG: 20-50 mm/hari)


@dataclass
class Forecast:
    key: str
    label: str
    unit: str
    source: str
    obs: list[tuple[date, float]]            # observasi asli yang dipakai
    days: list[date] = field(default_factory=list)
    mean: list[float] = field(default_factory=list)
    lo80: list[float] = field(default_factory=list)
    hi80: list[float] = field(default_factory=list)
    lo95: list[float] = field(default_factory=list)
    hi95: list[float] = field(default_factory=list)
    model: str = "-"
    horizon_days: int = 0
    backtest: dict = field(default_factory=dict)
    confidence: str = "Rendah"
    direction: str = "stabil"
    recent_mean: float | None = None
    recent_days: int = 0
    period_mean: float | None = None
    period_std: float | None = None
    forecast_mean: float | None = None
    notes: list[str] = field(default_factory=list)


# -- variabel ------------------------------------------------------------------

# key, label, satuan, source, (batas bawah, batas atas), log-transform
_VARIABLES = [
    ("vv", "Sentinel-1 VV", "dB", "SENTINEL1", (-40.0, 5.0), False),
    ("vh", "Sentinel-1 VH", "dB", "SENTINEL1", (-40.0, 5.0), False),
    ("ndvi", "MODIS NDVI", "", "MODIS", (-1.0, 1.0), False),
    ("ndwi", "MODIS NDWI", "", "MODIS", (-1.0, 1.0), False),
    ("flood", "Luas banjir MODIS", "% piksel", "MODIS", (0.0, 100.0), False),
    ("rain", "Curah hujan GPM", "mm/hari", "GPM", (0.0, None), True),
]


def horizon_for(period_days: int) -> int:
    return max(1, round(period_days / 3))


def observations(st: rs.ReportStats) -> dict[str, list[tuple[date, float]]]:
    """Deret observasi harian per variabel. S1: scene PASS bersatuan dB
    (rata-rata per tanggal). MODIS: hanya hari dengan >= 50% piksel valid,
    supaya hari berawan tidak dibaca sebagai perubahan nyata."""
    out: dict[str, list[tuple[date, float]]] = {}
    for band, key in (("VV", "vv"), ("VH", "vh")):
        by_day: dict[date, list[float]] = {}
        for m in st.s1_metrics:
            if (m["band"] == band and m["date"] and m["mean_db"] is not None
                    and m["flag"] == "PASS" and not m["linear"]):
                by_day.setdefault(m["date"], []).append(m["mean_db"])
        out[key] = sorted((d, mean(v)) for d, v in by_day.items())
    for band, key in (("NDVI", "ndvi"), ("NDWI", "ndwi"), ("FLOOD", "flood")):
        out[key] = [(o.day, o.mean) for o in st.raster_band("MODIS", band)
                    if o.mean is not None and o.valid_frac >= 0.5]
    out["rain"] = [(o.day, o.mean) for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
    return out


def build_forecasts(st: rs.ReportStats) -> list[Forecast]:
    if not st.period_end or not st.period_days:
        return []
    h = horizon_for(st.period_days)
    obs = observations(st)
    out = []
    for key, label, unit, source, bounds, log in _VARIABLES:
        pts = obs.get(key) or []
        if len(pts) < 6:
            continue
        fc = _forecast_one(key, label, unit, source, pts, bounds, log, st.period_end, h)
        if fc is not None:
            out.append(fc)
    return out


# -- inti ------------------------------------------------------------------------

def _daily_grid(pts: list[tuple[date, float]]) -> tuple[list[date], np.ndarray, int]:
    """Observasi tak beraturan -> grid harian dengan interpolasi linier.
    Mengembalikan juga celah terpanjang (hari) untuk dicatat."""
    days = [d for d, _ in pts]
    x = np.array([(d - days[0]).days for d in days], dtype=float)
    y = np.array([v for _, v in pts], dtype=float)
    grid = np.arange(0, x[-1] + 1)
    max_gap = int(np.max(np.diff(x))) if len(x) > 1 else 0
    return [days[0] + timedelta(days=int(i)) for i in grid], np.interp(grid, x, y), max_gap


def _forecast_one(key, label, unit, source, pts, bounds, log, period_end, h) -> Forecast | None:
    grid_days, y_raw, max_gap = _daily_grid(pts)
    y = np.log1p(np.clip(y_raw, 0, None)) if log else y_raw
    n = len(y)
    last = grid_days[-1]
    steps = (period_end - last).days + h  # prakiraan selalu berakhir di period_end + h
    if steps <= 0:
        return None

    fc = Forecast(key=key, label=label, unit=unit, source=source, obs=pts, horizon_days=h)
    vals = [v for _, v in pts]
    fc.period_mean, fc.period_std = mean(vals), (pstdev(vals) if len(vals) > 1 else 0.0)
    recent_n = max(7, h // 2)
    recent = [v for d, v in pts if d > last - timedelta(days=recent_n)]
    fc.recent_mean, fc.recent_days = (mean(recent) if recent else vals[-1]), recent_n

    # Backtest: latih 2/3 awal, uji 1/3 akhir (panjang uji ~ horizon sungguhan).
    n_test = max(1, min(h, n // 3))
    n_train = n - n_test
    results = {}
    if n_train >= 8 and n_test >= 2:
        train, test = y[:n_train], y[n_train:]
        for name, fit in _MODELS.items():
            model = fit(train)
            pred = model.predict(n_test)
            results[name] = {
                "model": model, "pred": pred,
                "mae": float(np.mean(np.abs(_back(pred, log) - _back(test, log)))),
                "err": test - pred,
            }
        naive_mae = results["naif"]["mae"]
        best = min(results, key=lambda k: results[k]["mae"])
        skill = 1 - results[best]["mae"] / naive_mae if naive_mae > 0 else 0.0
        fc.backtest = {
            "train_days": n_train, "test_days": n_test,
            "mae": round(results[best]["mae"], 4), "mae_naive": round(naive_mae, 4),
            "skill": round(skill, 3),
            "mae_by_model": {k: round(v["mae"], 4) for k, v in results.items()},
        }
    else:
        best = "rata-rata"
        fc.notes.append("Deret terlalu pendek untuk backtest; dipakai rata-rata periode dengan keyakinan rendah.")

    final = _MODELS[best](y)
    pred = final.predict(steps)
    base_sigma = final.sigma(steps)

    # Kalibrasi pita dari error backtest: z = kuantil |error| / sigma model.
    z80, z95 = 1.2816, 1.96
    coverage = None
    if best in results:
        bt = results[best]
        s_bt = bt["model"].sigma(len(bt["err"]))
        ratio = np.abs(bt["err"]) / np.maximum(s_bt, 1e-9)
        z80 = max(z80, float(np.quantile(ratio, 0.8)))
        z95 = max(z95, float(np.quantile(ratio, 0.95)))
        coverage = float(np.mean(ratio <= 1.2816))
        fc.backtest["coverage80_uncalibrated"] = round(coverage, 2)

    lo_b, hi_b = bounds
    def clip(a):
        a = _back(a, log)
        return np.clip(a, lo_b if lo_b is not None else -np.inf, hi_b if hi_b is not None else np.inf)

    fc.days = [last + timedelta(days=i + 1) for i in range(steps)]
    fc.mean = clip(pred).tolist()
    fc.lo80, fc.hi80 = clip(pred - z80 * base_sigma).tolist(), clip(pred + z80 * base_sigma).tolist()
    fc.lo95, fc.hi95 = clip(pred - z95 * base_sigma).tolist(), clip(pred + z95 * base_sigma).tolist()
    fc.model = best

    # Rata-rata prakiraan hanya di jendela horizon (setelah period_end).
    horizon_vals = [v for d, v in zip(fc.days, fc.mean) if d > period_end]
    fc.forecast_mean = mean(horizon_vals) if horizon_vals else fc.mean[-1]

    skill = fc.backtest.get("skill")
    if skill is None or skill < 0.05:
        fc.confidence = "Rendah"
    elif skill >= 0.2 and n >= 30:
        fc.confidence = "Tinggi"
    else:
        fc.confidence = "Sedang"
    if skill is not None and skill <= 0:
        fc.notes.append("Tidak ada model yang mengalahkan metode naif pada backtest; prakiraan setara 'kondisi terakhir berlanjut'.")
    if max_gap > 10:
        fc.notes.append(f"Ada celah observasi {max_gap} hari yang diisi interpolasi linier.")
    if last < period_end:
        fc.notes.append(f"Observasi terakhir {last}; prakiraan dimulai dari tanggal itu.")

    # Arah: bandingkan rata-rata prakiraan dengan kondisi terkini; perubahan
    # di bawah 0,25 sigma periode dianggap tidak bermakna (stabil).
    diff = fc.forecast_mean - fc.recent_mean
    thr = 0.25 * (fc.period_std or 0)
    fc.direction = "stabil" if abs(diff) <= thr else ("naik" if diff > 0 else "turun")
    return fc


def _back(a, log):
    return np.expm1(a) if log else a


# -- model --------------------------------------------------------------------

class _Model:
    def predict(self, h: int) -> np.ndarray: ...
    def sigma(self, h: int) -> np.ndarray: ...


class _Naive(_Model):
    def __init__(self, y):
        self.last = float(y[-1])
        d = np.diff(y)
        self.s = float(np.std(d)) if len(d) > 1 else 0.0

    def predict(self, h):
        return np.full(h, self.last)

    def sigma(self, h):
        return self.s * np.sqrt(np.arange(1, h + 1))  # random walk


class _Mean(_Model):
    def __init__(self, y):
        self.m = float(np.mean(y))
        self.s = float(np.std(y))

    def predict(self, h):
        return np.full(h, self.m)

    def sigma(self, h):
        return np.full(h, self.s)


class _SES(_Model):
    """Simple exponential smoothing; alpha dipilih dari grid (SSE one-step)."""

    def __init__(self, y):
        best = None
        for a in np.arange(0.05, 1.0, 0.05):
            level, sse, res = y[0], 0.0, []
            for v in y[1:]:
                e = v - level
                res.append(e)
                sse += e * e
                level += a * e
            if best is None or sse < best[0]:
                best = (sse, a, level, res)
        _, self.a, self.level, res = best
        self.s = float(np.std(res)) if res else 0.0

    def predict(self, h):
        return np.full(h, float(self.level))

    def sigma(self, h):
        k = np.arange(1, h + 1)
        return self.s * np.sqrt(1 + (k - 1) * self.a ** 2)


class _HoltDamped(_Model):
    """Holt linear trend dengan redaman phi=0.9 -- tren tidak diekstrapolasi
    lurus tanpa batas, melainkan melandai (lebih aman untuk data pendek)."""
    PHI = 0.9

    def __init__(self, y):
        best = None
        for a in (0.1, 0.2, 0.3, 0.5, 0.7):
            for b in (0.05, 0.1, 0.2):
                level, trend, sse, res = y[0], y[1] - y[0], 0.0, []
                for v in y[1:]:
                    f = level + self.PHI * trend
                    e = v - f
                    res.append(e)
                    sse += e * e
                    new_level = f + a * e
                    trend = self.PHI * trend + b * a * e
                    level = new_level
                if best is None or sse < best[0]:
                    best = (sse, a, b, level, trend, res)
        _, self.a, self.b, self.level, self.trend, res = best
        self.s = float(np.std(res)) if res else 0.0

    def predict(self, h):
        k = np.arange(1, h + 1)
        damp = np.cumsum(self.PHI ** k)
        return self.level + damp * self.trend

    def sigma(self, h):
        k = np.arange(1, h + 1)
        return self.s * np.sqrt(1 + (k - 1) * (self.a ** 2) * (1 + self.b) ** 2)


_MODELS = {
    "naif": _Naive,
    "rata-rata": _Mean,
    "SES": _SES,
    "Holt damped": _HoltDamped,
}


# -- analisis turunan ---------------------------------------------------------

def rain_outlook(st: rs.ReportStats, h: int) -> dict | None:
    """Prospek hujan berbasis distribusi empiris dataset: peluang hari hujan
    lebat dan rentang akumulasi h-hari dari jendela bergulir historis."""
    vals = [o.mean for o in st.raster_band("GPM", "RAIN_24H") if o.mean is not None]
    if len(vals) < 10:
        return None
    p_heavy = sum(1 for v in vals if v >= HEAVY_RAIN_MM) / len(vals)
    out = {
        "horizon_days": h,
        "heavy_threshold_mm": HEAVY_RAIN_MM,
        "p_heavy_day": p_heavy,
        "expected_heavy_days": p_heavy * h,
        "p_at_least_one_heavy": 1 - (1 - p_heavy) ** h,
        "p_rain_day": sum(1 for v in vals if v >= rs.RAINY_THRESHOLD_MM) / len(vals),
    }
    if h < len(vals):
        sums = np.convolve(np.array(vals), np.ones(h), mode="valid")
        out.update(total_p10=float(np.quantile(sums, .1)), total_p50=float(np.quantile(sums, .5)),
                   total_p90=float(np.quantile(sums, .9)), windows=len(sums))
    return out


def flood_scenario(st: rs.ReportStats, rain_fc: Forecast | None) -> dict | None:
    """Regresi linier %banjir MODIS ~ hujan 7 hari (hari cerah saja). Dipakai
    hanya kalau hubungannya cukup kuat (r >= 0,3, n >= 10)."""
    flood = {o.day: o.mean for o in st.raster_band("MODIS", "FLOOD") if o.mean is not None and o.valid_frac >= 0.5}
    r7 = {o.day: o.mean for o in st.raster_band("GPM", "RAIN_7D") if o.mean is not None}
    common = sorted(set(flood) & set(r7))
    if len(common) < 10 or rain_fc is None:
        return None
    x = np.array([r7[d] for d in common])
    y = np.array([flood[d] for d in common])
    r = rs.pearson(x.tolist(), y.tolist())
    if r is None or r < 0.3:
        return {"r": r, "n": len(common), "usable": False}
    slope, intercept = np.polyfit(x, y, 1)
    resid = float(np.std(y - (slope * x + intercept)))
    rain7_fc = 7 * (rain_fc.forecast_mean or 0)
    rain7_now = float(np.mean([r7[d] for d in sorted(r7)[-7:]]))
    pred = lambda v: float(np.clip(slope * v + intercept, 0, 100))  # noqa: E731
    return {
        "usable": True, "r": r, "n": len(common), "slope": float(slope), "intercept": float(intercept),
        "resid_std": resid, "rain7_forecast": rain7_fc, "rain7_recent": rain7_now,
        "flood_forecast": pred(rain7_fc), "flood_recent_model": pred(rain7_now),
        "flood_lo": max(0.0, pred(rain7_fc) - 1.2816 * resid), "flood_hi": min(100.0, pred(rain7_fc) + 1.2816 * resid),
    }


def fmt(v: float | None, unit: str = "", d: int = 2) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "—"
    return f"{v:.{d}f}{(' ' + unit) if unit else ''}"
