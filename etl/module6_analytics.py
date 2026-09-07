# etl/module6_analytics.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio

logger = logging.getLogger(__name__)

VALID_BACKSCATTER_MIN = -35.0
VALID_BACKSCATTER_MAX = 5.0


@dataclass
class BandMetrics:
    band_name: str
    total_pixels: int
    valid_pixels: int
    nodata_pixels: int
    nodata_percent: float
    backscatter_mean_db: float
    backscatter_std_db: float
    backscatter_min_db: float
    backscatter_max_db: float
    speckle_index: float
    radiometric_consistency: bool
    quality_score: float
    quality_flag: str


def compute_quality_score(
    nodata_percent: float,
    speckle_index: float,
    radiometric_ok: bool,
) -> float:
    nodata_component = 50.0 * (1.0 - min(nodata_percent / 100.0, 1.0))
    speckle_component = 30.0 * max(0.0, 1.0 - speckle_index)
    radiometric_component = 20.0 if radiometric_ok else 0.0
    return round(min(100.0, nodata_component + speckle_component + radiometric_component), 2)


def compute_band_metrics(
    file_path: str,
    band_name: str,
    nodata_value: float = -9999.0,
    cloud_threshold: float = 20.0,
    min_quality_score: float = 60.0,
) -> BandMetrics:
    with rasterio.open(file_path) as src:
        data = src.read(1).astype(np.float32)
        nodata = src.nodata if src.nodata is not None else nodata_value

    total = int(data.size)
    if isinstance(nodata, float) and np.isnan(nodata):
        valid_mask = ~np.isnan(data)
    else:
        valid_mask = data != nodata
    valid = data[valid_mask]
    nodata_count = total - int(valid.size)
    nodata_percent = round((nodata_count / total) * 100, 2) if total else 0.0

    if valid.size == 0:
        mean_db = std_db = min_db = max_db = 0.0
        speckle = 1.0
        radiometric_ok = False
    else:
        mean_db = float(np.mean(valid))
        std_db = float(np.std(valid))
        min_db = float(np.min(valid))
        max_db = float(np.max(valid))
        speckle = round(std_db / abs(mean_db), 4) if mean_db != 0 else 1.0
        radiometric_ok = VALID_BACKSCATTER_MIN <= mean_db <= VALID_BACKSCATTER_MAX

    score = compute_quality_score(nodata_percent, speckle, radiometric_ok)
    flag = "PASS" if score >= min_quality_score else "FAIL"

    logger.info("[M6] %s band=%s score=%.2f flag=%s", Path(file_path).name, band_name, score, flag)

    return BandMetrics(
        band_name=band_name,
        total_pixels=total,
        valid_pixels=int(valid.size),
        nodata_pixels=nodata_count,
        nodata_percent=nodata_percent,
        backscatter_mean_db=round(mean_db, 4),
        backscatter_std_db=round(std_db, 4),
        backscatter_min_db=round(min_db, 4),
        backscatter_max_db=round(max_db, 4),
        speckle_index=speckle,
        radiometric_consistency=radiometric_ok,
        quality_score=score,
        quality_flag=flag,
    )


def generate_quality_plot(
    metrics: list[BandMetrics],
    output_path: str,
    scene_id: int,
) -> str:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, len(metrics), figsize=(6 * len(metrics), 5))
    if len(metrics) == 1:
        axes = [axes]
    for ax, m in zip(axes, metrics):
        ax.bar(
            ["mean", "std", "min", "max"],
            [m.backscatter_mean_db, m.backscatter_std_db, m.backscatter_min_db, m.backscatter_max_db],
        )
        ax.set_title(f"{m.band_name} score={m.quality_score}")
    fig.suptitle(f"scene_{scene_id}")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return output_path


def run(
    scene_id: int,
    gold_products: dict[str, str],
    db,
    analytics_dir: str = "analytics",
) -> list[BandMetrics]:
    from etl.metadata_manager import MetadataManager

    meta = MetadataManager(db)
    results = []
    for band_name, file_path in gold_products.items():
        m = compute_band_metrics(file_path, band_name)
        results.append(m)
        products = meta.get_products_by_scene(scene_id, tier="GOLD")
        product_id = next((p["product_id"] for p in products if p["band_name"] == band_name), None)
        if not product_id:
            logger.warning("[M6] No GOLD product for scene=%d band=%s", scene_id, band_name)
            continue
        meta.insert_quality_metrics(
            scene_id=scene_id,
            product_id=product_id,
            band_name=m.band_name,
            total_pixels=m.total_pixels,
            valid_pixels=m.valid_pixels,
            nodata_pixels=m.nodata_pixels,
            quality_score=m.quality_score,
            backscatter_mean_db=m.backscatter_mean_db,
            backscatter_std_db=m.backscatter_std_db,
            backscatter_min_db=m.backscatter_min_db,
            backscatter_max_db=m.backscatter_max_db,
            radiometric_consistency=m.radiometric_consistency,
            speckle_index=m.speckle_index,
            quality_flag=m.quality_flag,
        )
    return results