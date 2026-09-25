# etl/live_metrics.py
"""Metrik ringkas satu scene Daerah Live, dihitung dari COG PROCESSED.

Satu fungsi publik, scene_inputs() + compute_metrics(). Hasilnya angka-angka
kecil (rata-rata, persen) yang disimpan di live_scenes.metrics dan dipakai
untuk kalimat kondisi (etl/live_interpret.py), grafik, dan forecast. Karena
itu harus tetap bisa dihitung ulang dari log walau berkasnya sudah dihapus
-- yang disimpan adalah angkanya, bukan pointer ke raster.

Nilai dihitung dari raster ASLI (bukan dari PNG preview). Satu pengecualian:
raster Sentinel-1 dibaca turun ke sisi terpanjang S1_METRIC_MAX_SIDE (rata-rata
di ranah linear) supaya AOI besar tidak memuat ratusan juta piksel ke memori.
Raster yang sudah difilter Lee tidak berubah berarti oleh agregasi ini.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling

from etl import folder_manager as fm
from etl import module7_modis_download as m7
from etl import module8_gpm_download as m8
from etl.live_interpret import THRESHOLDS
from etl.module9_fusion import AUX_DAY_OFFSETS

logger = logging.getLogger(__name__)

S1_METRIC_MAX_SIDE = 2048
MODIS_BANDS = ("FLOOD", "NDVI", "NDWI")
GPM_WINDOWS = ("24h", "72h", "7d")


# ---------------------------------------------------------------------------
# Pencarian berkas
# ---------------------------------------------------------------------------

def s1_frames(root: Path, scene_date: date) -> list[dict[str, Path]]:
    """[{VV: path, VH: path}] per frame S1 tanggal itu (COG _lee)."""
    dk = scene_date.strftime("%Y%m%d")
    base = root / fm.SOURCE_DIR_NAMES["sentinel1"] / "PROCESSED"
    frames: dict[str, dict[str, Path]] = {}
    if base.is_dir():
        for p in sorted(base.glob(f"*_{dk}T*_lee.tif")):
            for band in ("VV", "VH"):
                tag = f"_{band}_lee.tif"
                if p.name.endswith(tag):
                    frames.setdefault(p.name[: -len(tag)], {})[band] = p
    return [f for f in frames.values() if f]


def aux_file(root: Path, source: str, band: str, scene_date: date) -> tuple[Path, date] | None:
    """Berkas MODIS/GPM untuk tanggal scene, memakai urutan pencocokan yang
    sama dengan fusion (AUX_DAY_OFFSETS: hari itu, lalu D-1)."""
    sub = fm.SOURCE_DIR_NAMES[source]
    for off in AUX_DAY_OFFSETS:
        d = scene_date + timedelta(days=off)
        dk = d.strftime("%Y%m%d")
        name = m7.band_filename(band, dk) if source == "modis" else m8.band_filename(band, dk)
        p = root / sub / "PROCESSED" / name
        if p.exists():
            return p, d
    return None


def scene_inputs(root: Path, scene_date: date) -> dict:
    """Semua input satu scene: {'s1': [frames], 'modis': {band: (path, d)},
    'gpm': {window: (path, d)}} -- yang tidak ada tidak muncul."""
    return {
        "s1": s1_frames(root, scene_date),
        "modis": {b: f for b in MODIS_BANDS if (f := aux_file(root, "modis", b, scene_date))},
        "gpm": {w: f for w in GPM_WINDOWS if (f := aux_file(root, "gpm", w, scene_date))},
    }


# ---------------------------------------------------------------------------
# Pembacaan
# ---------------------------------------------------------------------------

def _read(path: Path, band: int = 1, max_side: int | None = None) -> tuple[np.ndarray, dict]:
    """Band sebagai float64 dengan NaN di NoData, plus tag berkasnya."""
    with rasterio.open(path) as src:
        shape = None
        if max_side and max(src.width, src.height) > max_side:
            s = max_side / max(src.width, src.height)
            shape = (max(1, int(src.height * s)), max(1, int(src.width * s)))
        data = src.read(band, out_shape=shape, masked=True,
                        resampling=Resampling.average if shape else Resampling.nearest)
        tags = src.tags()
    arr = np.asarray(data.astype(np.float64).filled(np.nan), dtype=np.float64)
    arr[~np.isfinite(arr)] = np.nan
    return arr, tags


def _r(v, nd=2):
    return None if v is None or not np.isfinite(v) else round(float(v), nd)


def _to_db(lin: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        out = 10.0 * np.log10(np.where(lin > 0, lin, np.nan))
    return out


# ---------------------------------------------------------------------------
# Metrik per sumber
# ---------------------------------------------------------------------------

def s1_metrics(frames: list[dict[str, Path]]) -> dict | None:
    """Rata-rata VV/VH (dB) dan % piksel VH di bawah ambang air, dikumpulkan
    dari semua frame hari itu."""
    vv_all, vh_all = [], []
    for f in frames:
        for band, bucket in (("VV", vv_all), ("VH", vh_all)):
            if band in f:
                arr, _ = _read(f[band], max_side=S1_METRIC_MAX_SIDE)
                # GOLD menyimpan sigma0 linear (0 = di luar swath). Data yang
                # sudah dB (instalasi dengan cog_convert_db) punya median
                # negatif; linear tidak pernah.
                valid = arr[np.isfinite(arr)]
                if valid.size and np.median(valid) > 0:
                    arr = _to_db(arr)
                bucket.append(arr[np.isfinite(arr)].ravel())
    if not vv_all and not vh_all:
        return None
    vv = np.concatenate(vv_all) if vv_all else np.array([])
    vh = np.concatenate(vh_all) if vh_all else np.array([])
    thr = THRESHOLDS["s1_vh_water_db"]
    return {
        "vv_mean_db": _r(vv.mean()) if vv.size else None,
        "vh_mean_db": _r(vh.mean()) if vh.size else None,
        "vh_water_pct": _r((vh < thr).mean() * 100) if vh.size else None,
        "vh_water_threshold_db": thr,
        "frames": len(frames),
        "valid_pixels": int(vh.size),
    }


def modis_metrics(files: dict[str, tuple[Path, date]]) -> dict:
    out: dict = {}
    if "FLOOD" in files:
        path, d = files["FLOOD"]
        arr, tags = _read(path)
        total = arr.size
        valid = np.isfinite(arr) & (arr != 255)
        n = int(valid.sum())
        v = arr[valid]
        out["flood"] = {
            "matched_date": d.isoformat(),
            "observation_date": tags.get("OBSERVATION_DATE"),
            "valid_pct": _r(n / total * 100 if total else 0),
            "cloud_pct": _r(100 - n / total * 100 if total else 100),
            # Persen dari piksel yang TERAMATI, bukan dari seluruh AOI.
            "flood_pct": _r((v == 3).mean() * 100) if n else None,
            "recurring_pct": _r((v == 2).mean() * 100) if n else None,
            "water_pct": _r(np.isin(v, (1, 2, 3)).mean() * 100) if n else None,
        }
    for band in ("NDVI", "NDWI"):
        if band not in files:
            continue
        path, d = files[band]
        arr, tags = _read(path)
        age, _ = _read(path, band=2)
        valid = np.isfinite(arr)
        n = int(valid.sum())
        v = arr[valid]
        periods = [p for p in (tags.get("PERIODS_USED") or "").split(",") if p]
        entry = {
            "matched_date": d.isoformat(),
            "valid_pct": _r(n / arr.size * 100 if arr.size else 0),
            "cloud_pct": _r(100 - n / arr.size * 100 if arr.size else 100),
            "mean": _r(v.mean(), 3) if n else None,
            "age_days_median": _r(np.nanmedian(age[valid]), 1) if n else None,
            # MOD09A1 = komposit 8 hari; tanggal awal periode terbaru dipakai.
            "composite_period": periods[0] if periods else None,
            "lookback_days": int(tags["LOOKBACK_DAYS"]) if tags.get("LOOKBACK_DAYS") else None,
        }
        if band == "NDWI":
            entry["water_pct"] = _r((v > THRESHOLDS["ndwi_water"]).mean() * 100) if n else None
        out[band.lower()] = entry
    return out


def gpm_metrics(files: dict[str, tuple[Path, date]]) -> dict:
    out: dict = {}
    for window, (path, d) in files.items():
        arr, tags = _read(path)
        v = arr[np.isfinite(arr)]
        v = v[v >= 0]
        out[f"rain_{window}"] = {
            "matched_date": d.isoformat(),
            "mean_mm": _r(v.mean(), 1) if v.size else None,
            "max_mm": _r(v.max(), 1) if v.size else None,
            "imerg_runs": tags.get("IMERG_RUNS"),
            "window_start": tags.get("WINDOW_START_UTC"),
            "window_end": tags.get("WINDOW_END_UTC"),
        }
    return out


def compute_metrics(inputs: dict, scene_date: date | None = None) -> dict:
    """{'sentinel1': {...}|None, 'modis': {...}, 'gpm': {...}}. Satu sumber
    yang gagal dibaca tidak menggugurkan yang lain.

    Entri MODIS/GPM yang berkasnya diambil dari tanggal lain (D-1) diberi
    `nearest: True` supaya UI dan kalimat kondisi menyebutnya "terdekat"."""
    result: dict = {}
    for key, fn, arg in (
        ("sentinel1", s1_metrics, inputs.get("s1") or []),
        ("modis", modis_metrics, inputs.get("modis") or {}),
        ("gpm", gpm_metrics, inputs.get("gpm") or {}),
    ):
        try:
            result[key] = fn(arg) if arg else ({} if key != "sentinel1" else None)
        except Exception:
            logger.exception("[LIVE] metrik %s gagal", key)
            result[key] = {} if key != "sentinel1" else None
    if scene_date is not None:
        iso = scene_date.isoformat()
        for key in ("modis", "gpm"):
            for entry in (result.get(key) or {}).values():
                entry["nearest"] = entry.get("matched_date") != iso
    return result


def source_status(inputs: dict, metrics: dict) -> dict:
    """Status tiap sumber: OK | UNAVAILABLE (berkas ada, tapi tidak ada piksel
    valid -- awan) | FAILED (berkas tidak ada: unduh/proses gagal, bisa dicoba
    ulang)."""
    st: dict = {}
    s1 = metrics.get("sentinel1")
    st["sentinel1"] = {"status": "OK" if s1 and s1.get("valid_pixels") else "FAILED",
                       "frames": len(inputs.get("s1") or [])}

    mod = metrics.get("modis") or {}
    missing = [b for b in MODIS_BANDS if b not in (inputs.get("modis") or {})]
    if missing:
        m_status = "FAILED"
    elif all((mod.get(k) or {}).get("valid_pct") in (0, None) for k in ("flood", "ndvi", "ndwi")):
        m_status = "UNAVAILABLE"
    else:
        m_status = "OK"
    st["modis"] = {"status": m_status, "missing": missing}

    g_missing = [w for w in GPM_WINDOWS if w not in (inputs.get("gpm") or {})]
    st["gpm"] = {"status": "FAILED" if g_missing else "OK", "missing": g_missing}
    return st
