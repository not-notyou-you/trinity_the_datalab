# etl/report_stats.py
"""Pengumpul statistik untuk etl/report_generator.py.

Semua angka di laporan berasal dari sini, dan hanya dari dua sumber nyata:

1. Query agregat ke database (GROUP BY bulan/band/tier/stage, AVG, STDDEV,
   MIN/MAX, COUNT DISTINCT) -- Sentinel-1 punya statistik backscatter skalar
   di `quality_metrics`, semua source punya jejak tier/ukuran di
   `data_products`, dan fusi tercatat di `fusion_products`.
2. Statistik piksel dari GeoTIFF COG MODIS/GPM milik dataset ini. Nilai
   NDVI/NDWI/kelas banjir/curah hujan TIDAK disimpan sebagai kolom skalar di
   DB, jadi dihitung langsung dari raster saat laporan dibuat (read-only,
   ~10 ms per file).

Modul ini murni membaca -- tidak menulis ke DB atau ke direktori data.
Setiap bagian yang datanya tidak ada dikembalikan kosong/None, bukan diisi
angka contoh dari spesifikasi (lihat docstring report_generator.py).
"""
from __future__ import annotations

import logging
import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from statistics import mean, pstdev

from sqlalchemy import and_, func, select

from etl.database_client import (
    DatabaseClient,
    DataProduct,
    FusionProduct,
    NasaScene,
    ProcessingJob,
    ProcessingStage,
    QualityMetric,
    SatelliteScene,
)

logger = logging.getLogger(__name__)

# Musim untuk wilayah monsun Indonesia (dataset default country_code = ID).
SEASONS: list[tuple[str, str, tuple[int, ...]]] = [
    ("DJF", "Musim hujan (Des-Feb)", (12, 1, 2)),
    ("MAM", "Peralihan I (Mar-Mei)", (3, 4, 5)),
    ("JJA", "Musim kemarau (Jun-Agu)", (6, 7, 8)),
    ("SON", "Peralihan II (Sep-Nov)", (9, 10, 11)),
]
MONTH_NAMES = ["", "Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli",
               "Agustus", "September", "Oktober", "November", "Desember"]

# Urutan tier lineage per source (etl/tier_names.py) -- dipakai untuk
# menghitung "loss" antar level pemrosesan di Section 3.
TIER_ORDER: dict[str, list[str]] = {
    "SENTINEL1": ["RAW", "ALIGNED", "DESPECKLED", "COG"],
    "MODIS": ["ALIGNED", "INDICES", "COG"],
    "GPM": ["ALIGNED", "ACCUMULATED", "COG"],
}

# MCDWD (MODIS Global Flood Product) -- kode kelas piksel.
FLOOD_CLASSES = {0: "Tidak ada air", 1: "Air permukaan", 2: "Banjir berulang", 3: "Banjir (anomali)"}
FLOOD_NODATA = 255

RAINY_THRESHOLD_MM = 0.1
GAP_THRESHOLD_DAYS = 10

_DATE_RE = re.compile(r"(20\d{6})")


def season_of(month: int) -> str:
    for key, _, months in SEASONS:
        if month in months:
            return key
    return "?"


def _f(x) -> float | None:
    return None if x is None else float(x)


@dataclass
class RasterObs:
    """Statistik satu raster COG (satu band, satu tanggal) atas AOI."""
    day: date
    band: str
    mean: float | None
    std: float | None
    min: float | None
    max: float | None
    valid_frac: float
    classes: dict[int, float] | None = None  # FLOOD saja: fraksi per kelas (dari piksel valid)
    file_name: str = ""
    pixels: int = 0


@dataclass
class ReportStats:
    period_start: date | None
    period_end: date | None
    period_days: int
    tiers: dict[str, list[dict]] = field(default_factory=dict)
    stage_jobs: list[dict] = field(default_factory=list)
    s1_scenes: list[dict] = field(default_factory=list)
    s1_metrics: list[dict] = field(default_factory=list)
    s1_monthly: list[dict] = field(default_factory=list)
    s1_orbit: list[dict] = field(default_factory=list)
    quality_flags: dict[str, dict[str, int]] = field(default_factory=dict)
    raster: dict[str, dict[str, list[RasterObs]]] = field(default_factory=dict)
    obs_dates: dict[str, list[date]] = field(default_factory=dict)
    fusion: list[dict] = field(default_factory=list)
    nasa_products: dict[str, list[str]] = field(default_factory=dict)
    queries: list[str] = field(default_factory=list)  # label query yang dipakai (Implementation Summary)

    # -- turunan ---------------------------------------------------------

    def completeness(self, source: str) -> float | None:
        days = self.obs_dates.get(source) or []
        if not days or not self.period_days:
            return None
        return round(len(days) / self.period_days * 100, 1)

    def gaps(self, source: str, threshold: int = GAP_THRESHOLD_DAYS) -> list[tuple[date, date, int]]:
        """Celah (tanpa observasi) lebih dari `threshold` hari, termasuk celah
        di awal/akhir periode dataset."""
        days = sorted(self.obs_dates.get(source) or [])
        if not days or self.period_start is None or self.period_end is None:
            return []
        out = []
        bounds = [self.period_start - timedelta(days=1)] + days + [self.period_end + timedelta(days=1)]
        for a, b in zip(bounds, bounds[1:]):
            gap = (b - a).days - 1
            if gap > threshold:
                out.append((a + timedelta(days=1), b - timedelta(days=1), gap))
        return out

    def revisit_days(self, source: str) -> float | None:
        days = sorted(self.obs_dates.get(source) or [])
        if len(days) < 2:
            return None
        return round(mean((b - a).days for a, b in zip(days, days[1:])), 1)

    def raster_band(self, source: str, band: str) -> list[RasterObs]:
        return sorted(self.raster.get(source, {}).get(band, []), key=lambda o: o.day)

    def monthly_raster(self, source: str, band: str) -> list[dict]:
        """Agregasi per bulan dari rata-rata AOI harian: n hari, mean, std
        (antar hari), max/min (piksel), rata-rata fraksi valid."""
        groups: dict[tuple[int, int], list[RasterObs]] = defaultdict(list)
        for o in self.raster_band(source, band):
            groups[(o.day.year, o.day.month)].append(o)
        rows = []
        for (y, m), obs in sorted(groups.items()):
            means = [o.mean for o in obs if o.mean is not None]
            rows.append({
                "year": y, "month": m, "n": len(obs),
                "mean": mean(means) if means else None,
                "std": pstdev(means) if len(means) > 1 else 0.0 if means else None,
                "max": max((o.max for o in obs if o.max is not None), default=None),
                "min": min((o.min for o in obs if o.min is not None), default=None),
                "valid": mean(o.valid_frac for o in obs),
            })
        return rows

    def seasonal_raster(self, source: str, band: str) -> list[dict]:
        groups: dict[str, list[RasterObs]] = defaultdict(list)
        for o in self.raster_band(source, band):
            groups[season_of(o.day.month)].append(o)
        rows = []
        for key, label, _ in SEASONS:
            obs = groups.get(key) or []
            means = [o.mean for o in obs if o.mean is not None]
            rows.append({
                "season": key, "label": label, "n": len(obs),
                "mean": mean(means) if means else None,
                "std": pstdev(means) if len(means) > 1 else None,
                "max_mean": max(means) if means else None,
                "max_px": max((o.max for o in obs if o.max is not None), default=None),
                "valid": mean(o.valid_frac for o in obs) if obs else None,
                "sum": sum(means) if means else None,
                "wet_days": sum(1 for v in means if v >= RAINY_THRESHOLD_MM),
            })
        return rows


# -- collection ---------------------------------------------------------------

def collect(db: DatabaseClient, dataset_id: int, dataset: dict, root_path=None) -> ReportStats:
    start, end = dataset.get("date_start"), dataset.get("date_end")
    if isinstance(start, datetime):
        start = start.date()
    if isinstance(end, datetime):
        end = end.date()
    period_days = (end - start).days + 1 if start and end else 0
    st = ReportStats(period_start=start, period_end=end, period_days=period_days)

    with db.session() as sess:
        _collect_tiers(sess, dataset_id, st)
        _collect_stage_jobs(sess, dataset_id, st)
        _collect_s1(sess, dataset_id, st)
        _collect_fusion(sess, dataset_id, st)
        _collect_nasa_products(sess, dataset_id, st)
        cog_rows = sess.execute(
            select(DataProduct.source, DataProduct.band_name, DataProduct.file_name, DataProduct.file_path)
            .where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.source.in_(["MODIS", "GPM"]),
                DataProduct.product_tier == "COG",
                DataProduct.is_latest == True,  # noqa: E712
                DataProduct.is_valid == True,  # noqa: E712
            )
        ).all()
    st.queries.append("data_products: COG MODIS/GPM (is_latest, is_valid) -> path raster")
    _collect_rasters(cog_rows, st)

    s1_days = sorted({s["date"] for s in st.s1_scenes if s["date"]})
    st.obs_dates["SENTINEL1"] = s1_days
    for src in ("MODIS", "GPM"):
        st.obs_dates[src] = sorted({o.day for obs in st.raster.get(src, {}).values() for o in obs})
    _derive_nasa_flags(st)
    return st


def _collect_tiers(sess, dataset_id: int, st: ReportStats) -> None:
    date_key = func.substring(DataProduct.file_name, r"(20\d{6})")
    rows = sess.execute(
        select(
            DataProduct.source, DataProduct.product_tier, DataProduct.processing_level,
            func.count(DataProduct.product_id),
            func.count(func.distinct(func.coalesce(date_key, func.cast(DataProduct.scene_id, _text_type())))),
            func.sum(DataProduct.file_size_mb), func.avg(DataProduct.file_size_mb),
            func.stddev(DataProduct.file_size_mb),
            func.count(func.distinct(DataProduct.band_name)),
            func.sum(func.cast(~DataProduct.is_valid, _int_type())),
        )
        .where(DataProduct.dataset_id == dataset_id)
        .group_by(DataProduct.source, DataProduct.product_tier, DataProduct.processing_level)
    ).all()
    st.queries.append(
        "data_products GROUP BY source, product_tier, processing_level: COUNT, COUNT DISTINCT tanggal, "
        "SUM/AVG/STDDEV(file_size_mb), COUNT DISTINCT band, SUM(invalid)"
    )
    for src, tier, level, n, n_dates, s, a, sd, n_bands, n_invalid in rows:
        st.tiers.setdefault(src, []).append({
            "tier": getattr(tier, "value", tier), "level": getattr(level, "value", level),
            "files": int(n), "dates": int(n_dates), "size_mb": _f(s) or 0.0,
            "avg_mb": _f(a) or 0.0, "std_mb": _f(sd), "bands": int(n_bands), "invalid": int(n_invalid or 0),
        })
    for src, items in st.tiers.items():
        order = TIER_ORDER.get(src, [])
        items.sort(key=lambda r: (order.index(r["tier"]) if r["tier"] in order else 99, r["level"] or ""))


def _text_type():
    from sqlalchemy import String
    return String


def _int_type():
    from sqlalchemy import Integer
    return Integer


def _collect_stage_jobs(sess, dataset_id: int, st: ReportStats) -> None:
    job_ids = select(DataProduct.job_id).where(DataProduct.dataset_id == dataset_id, DataProduct.job_id.isnot(None))
    dur = func.extract("epoch", ProcessingJob.completed_at - ProcessingJob.started_at)
    rows = sess.execute(
        select(
            ProcessingStage.stage_name, ProcessingStage.stage_order,
            func.count(ProcessingJob.job_id), func.avg(dur), func.stddev(dur), func.max(dur),
            func.percentile_cont(0.5).within_group(dur),
            func.sum(func.cast(ProcessingJob.status == "FAILED", _int_type())),
            func.avg(ProcessingJob.output_size_mb),
        )
        .join(ProcessingStage, ProcessingStage.stage_id == ProcessingJob.stage_id)
        .where(ProcessingJob.job_id.in_(job_ids))
        .group_by(ProcessingStage.stage_name, ProcessingStage.stage_order)
        .order_by(ProcessingStage.stage_order)
    ).all()
    st.queries.append(
        "processing_jobs JOIN processing_stages GROUP BY stage: COUNT, AVG/STDDEV/MAX/MEDIAN(durasi), "
        "SUM(FAILED), AVG(output_size_mb)"
    )
    for name, _, n, a, sd, mx, med, failed, out_mb in rows:
        st.stage_jobs.append({
            "stage": name, "jobs": int(n), "avg_sec": _f(a), "std_sec": _f(sd), "max_sec": _f(mx),
            "median_sec": _f(med), "failed": int(failed or 0), "avg_out_mb": _f(out_mb),
        })


def _collect_s1(sess, dataset_id: int, st: ReportStats) -> None:
    s1_filter = and_(DataProduct.dataset_id == dataset_id, DataProduct.source == "SENTINEL1")
    scenes = sess.execute(
        select(
            SatelliteScene.scene_id, SatelliteScene.product_identifier, SatelliteScene.acquisition_datetime,
            SatelliteScene.orbit_direction, SatelliteScene.relative_orbit,
            SatelliteScene.incidence_angle_near, SatelliteScene.incidence_angle_far,
            SatelliteScene.raw_file_size_mb,
        )
        .where(SatelliteScene.scene_id.in_(select(DataProduct.scene_id).where(s1_filter)))
        .order_by(SatelliteScene.acquisition_datetime)
    ).all()
    st.queries.append("satellite_scenes WHERE scene_id IN (produk S1 dataset) -- tanggal, orbit, incidence angle")
    for sid, ident, acq, orbit, rel, near, far, raw_mb in scenes:
        st.s1_scenes.append({
            "scene_id": sid, "product_identifier": ident,
            "date": acq.date() if acq else None, "datetime": acq,
            "orbit": getattr(orbit, "value", orbit) or "UNKNOWN", "relative_orbit": rel,
            "inc_near": _f(near), "inc_far": _f(far), "raw_mb": _f(raw_mb),
        })

    metrics = sess.execute(
        select(
            QualityMetric.scene_id, SatelliteScene.acquisition_datetime, SatelliteScene.orbit_direction,
            QualityMetric.band_name, QualityMetric.backscatter_mean_db, QualityMetric.backscatter_std_db,
            QualityMetric.backscatter_min_db, QualityMetric.backscatter_max_db, QualityMetric.speckle_index,
            QualityMetric.valid_pixels, QualityMetric.total_pixels, QualityMetric.quality_score,
            QualityMetric.quality_flag, QualityMetric.radiometric_consistency, SatelliteScene.product_identifier,
        )
        .join(DataProduct, DataProduct.product_id == QualityMetric.product_id)
        .join(SatelliteScene, SatelliteScene.scene_id == QualityMetric.scene_id)
        .where(s1_filter)
        .order_by(SatelliteScene.acquisition_datetime)
    ).all()
    st.queries.append("quality_metrics JOIN data_products JOIN satellite_scenes -- statistik backscatter per scene/band")
    for (sid, acq, orbit, band, m, sd, mn, mx, spk, valid, total, score, flag, consistent, ident) in metrics:
        st.s1_metrics.append({
            "scene_id": sid, "date": acq.date() if acq else None, "orbit": getattr(orbit, "value", orbit),
            "band": band, "mean_db": _f(m), "std_db": _f(sd), "min_db": _f(mn), "max_db": _f(mx),
            "speckle": _f(spk), "valid_frac": (valid / total) if total else None,
            "score": _f(score), "flag": flag, "consistent": consistent, "product_identifier": ident,
            "linear": is_linear_units(_f(m), _f(mn)),
        })

    month = func.date_trunc("month", SatelliteScene.acquisition_datetime)
    monthly = sess.execute(
        select(
            month, QualityMetric.band_name, func.count(QualityMetric.metric_id),
            func.avg(QualityMetric.backscatter_mean_db), func.stddev(QualityMetric.backscatter_mean_db),
            func.min(QualityMetric.backscatter_mean_db), func.max(QualityMetric.backscatter_mean_db),
            func.avg(QualityMetric.speckle_index),
            func.avg(func.cast(QualityMetric.valid_pixels, _float_type()) / func.nullif(QualityMetric.total_pixels, 0)),
            func.avg(QualityMetric.quality_score),
        )
        .join(DataProduct, DataProduct.product_id == QualityMetric.product_id)
        .join(SatelliteScene, SatelliteScene.scene_id == QualityMetric.scene_id)
        .where(s1_filter, ~and_(QualityMetric.backscatter_min_db >= 0, QualityMetric.backscatter_mean_db > -1))
        .group_by(month, QualityMetric.band_name)
        .order_by(month, QualityMetric.band_name)
    ).all()
    st.queries.append(
        "quality_metrics (tanpa produk bersatuan linear) GROUP BY DATE_TRUNC('month'), band: COUNT, AVG/STDDEV/MIN/MAX(backscatter_mean_db), "
        "AVG(speckle_index), AVG(valid/total), AVG(quality_score)"
    )
    for mth, band, n, a, sd, mn, mx, spk, valid, score in monthly:
        st.s1_monthly.append({
            "year": mth.year, "month": mth.month, "band": band, "n": int(n), "mean": _f(a),
            "std": _f(sd), "min": _f(mn), "max": _f(mx), "speckle": _f(spk), "valid": _f(valid), "score": _f(score),
        })

    orbit_rows = sess.execute(
        select(
            SatelliteScene.orbit_direction, func.count(func.distinct(SatelliteScene.scene_id)),
            func.min(SatelliteScene.incidence_angle_near), func.max(SatelliteScene.incidence_angle_far),
            func.count(func.distinct(SatelliteScene.relative_orbit)),
            func.min(SatelliteScene.acquisition_datetime), func.max(SatelliteScene.acquisition_datetime),
        )
        .where(SatelliteScene.scene_id.in_(select(DataProduct.scene_id).where(s1_filter)))
        .group_by(SatelliteScene.orbit_direction)
    ).all()
    st.queries.append("satellite_scenes GROUP BY orbit_direction: COUNT DISTINCT scene, MIN/MAX incidence, rentang waktu")
    for orbit, n, near, far, n_rel, first, last in orbit_rows:
        st.s1_orbit.append({
            "orbit": getattr(orbit, "value", orbit) or "UNKNOWN", "scenes": int(n),
            "inc_near": _f(near), "inc_far": _f(far), "relative_orbits": int(n_rel),
            "first": first, "last": last,
        })

    flag_rows = sess.execute(
        select(QualityMetric.quality_flag, func.count())
        .join(DataProduct, DataProduct.product_id == QualityMetric.product_id)
        .where(s1_filter)
        .group_by(QualityMetric.quality_flag)
    ).all()
    st.queries.append("quality_metrics GROUP BY quality_flag (distribusi flag S1)")
    if flag_rows:
        st.quality_flags["SENTINEL1"] = {flag: int(n) for flag, n in flag_rows}


def is_linear_units(mean_db: float | None, min_db: float | None) -> bool:
    """Backscatter sigma0 dalam dB atas AOI darat selalu punya minimum negatif
    dan rata-rata jauh di bawah 0. Minimum >= 0 dengan rata-rata > -1 berarti
    nilainya masih satuan linear (daya) yang tersimpan di kolom *_db -- produk
    seperti ini dikeluarkan dari statistik dB dan dilaporkan sebagai isu."""
    return mean_db is not None and min_db is not None and min_db >= 0 and mean_db > -1


def _float_type():
    from sqlalchemy import Float
    return Float


def _collect_fusion(sess, dataset_id: int, st: ReportStats) -> None:
    rows = sess.execute(
        select(
            FusionProduct.feature_date, FusionProduct.processing_level, FusionProduct.fusion_strategy,
            FusionProduct.s1_scene_id, FusionProduct.modis_scene_id, FusionProduct.gpm_scene_id,
            FusionProduct.s1_offset_days, FusionProduct.temporal_offset_modis, FusionProduct.temporal_offset_gpm,
            FusionProduct.days_since_s1, FusionProduct.feature_stack_path, FusionProduct.created_at,
        )
        .where(FusionProduct.dataset_id == dataset_id)
        .order_by(FusionProduct.feature_date, FusionProduct.processing_level)
    ).all()
    st.queries.append("fusion_products WHERE dataset_id -- tanggal, kelengkapan source, offset temporal")
    for (d, level, strat, s1, modis, gpm, s1_off, mod_off, gpm_off, since, path, created) in rows:
        st.fusion.append({
            "date": d, "level": level, "strategy": strat, "s1_scene_id": s1, "modis_scene_id": modis,
            "gpm_scene_id": gpm, "s1_offset_days": s1_off, "modis_offset_days": mod_off,
            "gpm_offset_days": gpm_off, "days_since_s1": since, "path": path, "created_at": created,
            "n_sources": sum(x is not None for x in (s1, modis, gpm)),
        })


def _collect_nasa_products(sess, dataset_id: int, st: ReportStats) -> None:
    ids = select(FusionProduct.modis_scene_id).where(FusionProduct.dataset_id == dataset_id).union(
        select(FusionProduct.gpm_scene_id).where(FusionProduct.dataset_id == dataset_id)
    )
    rows = sess.execute(
        select(NasaScene.source, NasaScene.product_short_name, func.count())
        .where(NasaScene.nasa_scene_id.in_(ids))
        .group_by(NasaScene.source, NasaScene.product_short_name)
    ).all()
    st.queries.append("nasa_scenes GROUP BY source, product_short_name (produk NASA yang dipakai fusi)")
    for src, name, _ in rows:
        st.nasa_products.setdefault(src, []).append(name)


def _collect_rasters(rows, st: ReportStats) -> None:
    try:
        import numpy as np
        import rasterio
    except ImportError:  # pragma: no cover - rasterio selalu ada di requirements
        logger.warning("[report] rasterio tidak tersedia; statistik MODIS/GPM dilewati")
        return
    for src, band, name, path in rows:
        m = _DATE_RE.search(name or "") or _DATE_RE.search(path or "")
        if not m:
            continue
        day = datetime.strptime(m.group(1), "%Y%m%d").date()
        try:
            with rasterio.open(path) as r:
                raw = r.read(1)
                nodata = r.nodata
        except Exception:
            logger.debug("[report] raster tidak terbaca: %s", path)
            continue
        total = raw.size
        if band == "FLOOD":
            valid = raw[raw != FLOOD_NODATA]
            classes = {c: float((valid == c).sum()) / valid.size for c in FLOOD_CLASSES} if valid.size else {}
            obs = RasterObs(
                day=day, band=band,
                # "mean" FLOOD = % piksel valid berkelas banjir (2 atau 3)
                mean=float(((valid == 2) | (valid == 3)).mean() * 100) if valid.size else None,
                std=None, min=None, max=None, valid_frac=valid.size / total if total else 0.0,
                classes=classes, file_name=name, pixels=total,
            )
        else:
            arr = raw.astype("float64")
            mask = np.isfinite(arr)
            if nodata is not None and not (isinstance(nodata, float) and math.isnan(nodata)):
                mask &= arr != nodata
            vals = arr[mask]
            obs = RasterObs(
                day=day, band=band,
                mean=float(vals.mean()) if vals.size else None,
                std=float(vals.std()) if vals.size else None,
                min=float(vals.min()) if vals.size else None,
                max=float(vals.max()) if vals.size else None,
                valid_frac=vals.size / total if total else 0.0, file_name=name, pixels=total,
            )
        st.raster.setdefault(src, {}).setdefault(band, []).append(obs)


def _derive_nasa_flags(st: ReportStats) -> None:
    """MODIS/GPM tidak punya quality_flag di DB -- flag diturunkan dari fraksi
    piksel valid per raster (GOOD >= 90%, ACCEPTABLE >= 70%, MARGINAL >= 50%,
    POOR < 50%). Labelnya ditandai 'derived' di laporan."""
    for src, bands in st.raster.items():
        counts = {"GOOD": 0, "ACCEPTABLE": 0, "MARGINAL": 0, "POOR": 0}
        for obs in bands.values():
            for o in obs:
                counts[valid_flag(o.valid_frac)] += 1
        st.quality_flags[src] = counts


def valid_flag(frac: float) -> str:
    if frac >= 0.9:
        return "GOOD"
    if frac >= 0.7:
        return "ACCEPTABLE"
    if frac >= 0.5:
        return "MARGINAL"
    return "POOR"


def linear_trend(points: list[tuple[date, float]]) -> float | None:
    """Kemiringan regresi linier (unit per 30 hari). None kalau < 3 titik."""
    if len(points) < 3:
        return None
    x0 = points[0][0]
    xs = [(d - x0).days for d, _ in points]
    ys = [v for _, v in points]
    mx, my = mean(xs), mean(ys)
    den = sum((x - mx) ** 2 for x in xs)
    if den == 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den * 30


def pearson(a: list[float], b: list[float]) -> float | None:
    if len(a) < 3 or len(a) != len(b):
        return None
    ma, mb = mean(a), mean(b)
    sa = math.sqrt(sum((x - ma) ** 2 for x in a))
    sb = math.sqrt(sum((y - mb) ** 2 for y in b))
    if sa == 0 or sb == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(a, b)) / (sa * sb)
