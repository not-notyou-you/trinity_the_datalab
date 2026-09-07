# api/routes/quality.py
"""
GET /api/quality/{scene_id}                        — quality metrics for all bands of a scene
GET /api/quality/summary/stats                     — aggregated quality statistics
GET /api/quality/dataset/{dataset_id}/by-source    — per-source quality for one dataset
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, select

from api.schemas import (
    DatasetQualityBySourceResponse,
    QualityMetricItem,
    QualityResponse,
    SourceQualityItem,
)
from api.deps import get_db
from etl.database_client import (
    DataProduct,
    DatabaseClient,
    ProductTierEnum,
    QualityMetric,
    SatelliteScene,
)
from etl.metadata_manager import MetadataManager

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/{scene_id}",
    response_model=QualityResponse,
    summary="Scene quality metrics",
    description=(
        "Retrieve radiometric quality metrics for all processed bands of a scene. "
        "Returns per-band backscatter statistics, nodata percentage, speckle index, "
        "and composite quality score."
    ),
)
async def get_quality(
    scene_id: int,
    db: DatabaseClient = Depends(get_db),
) -> QualityResponse:
    with db.session() as sess:
        scene = sess.get(SatelliteScene, scene_id)
        if not scene:
            raise HTTPException(404, f"Scene {scene_id} not found")

        metrics = sess.scalars(
            select(QualityMetric)
            .where(QualityMetric.scene_id == scene_id)
            .order_by(QualityMetric.band_name)
        ).all()

    if not metrics:
        raise HTTPException(
            404,
            f"No quality metrics found for scene {scene_id}. "
            "Run Module 6 (QUALITY_ANALYTICS) first."
        )

    band_items = [
        QualityMetricItem(
            metric_id               = m.metric_id,
            scene_id                = m.scene_id,
            product_id              = m.product_id,
            band_name               = m.band_name,
            assessed_at             = m.assessed_at,
            total_pixels            = m.total_pixels,
            valid_pixels            = m.valid_pixels,
            nodata_pixels           = m.nodata_pixels,
            nodata_percent          = None,  # DB generated column, read via raw SQL if needed
            backscatter_mean_db     = float(m.backscatter_mean_db) if m.backscatter_mean_db else None,
            backscatter_std_db      = float(m.backscatter_std_db) if m.backscatter_std_db else None,
            backscatter_min_db      = float(m.backscatter_min_db) if m.backscatter_min_db else None,
            backscatter_max_db      = float(m.backscatter_max_db) if m.backscatter_max_db else None,
            cloud_threshold_percent = float(m.cloud_threshold_percent),
            radiometric_consistency = m.radiometric_consistency,
            speckle_index           = float(m.speckle_index) if m.speckle_index else None,
            quality_score           = float(m.quality_score),
            quality_flag            = m.quality_flag,
            notes                   = m.notes,
            created_at              = m.created_at,
        )
        for m in metrics
    ]

    flags = [b.quality_flag for b in band_items]
    if all(f == "PASS" for f in flags):
        overall = "PASS"
    elif any(f == "FAIL" for f in flags):
        overall = "FAIL"
    else:
        overall = "WARNING"

    return QualityResponse(scene_id=scene_id, bands=band_items, overall_quality=overall)


@router.get(
    "/summary/stats",
    summary="Quality summary statistics",
    description="Aggregated quality statistics across all processed scenes.",
)
async def quality_summary(
    db:        DatabaseClient = Depends(get_db),
    region_id: int | None     = Query(None),
    n_days:    int            = Query(30, ge=1, le=365),
) -> dict:
    """Returns count of PASS/FAIL/WARNING scenes and average quality score."""
    from datetime import datetime, timedelta, timezone
    from sqlalchemy import and_

    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=n_days)

    with db.session() as sess:
        stmt = select(
            QualityMetric.quality_flag,
            func.count(QualityMetric.metric_id).label("count"),
            func.avg(QualityMetric.quality_score).label("avg_score"),
        ).where(
            QualityMetric.assessed_at >= cutoff
        ).group_by(QualityMetric.quality_flag)

        rows = sess.execute(stmt).all()

    summary = {
        "period_days": n_days,
        "region_id":   region_id,
        "flags":       {r.quality_flag: {"count": r.count, "avg_score": round(float(r.avg_score or 0), 2)} for r in rows},
    }
    return summary


# Band yang diharapkan ada per scene untuk tiap source non-SAR. Dipakai
# menghitung coverage: berapa persen dari yang seharusnya benar-benar
# mendarat di disk dan tercatat di data_products.
_EXPECTED_BANDS: dict[str, tuple[str, ...]] = {
    "MODIS": ("FLOOD", "NDVI", "NDWI"),
    "GPM": ("RAIN_24H", "RAIN_72H", "RAIN_7D"),
}


@router.get(
    "/dataset/{dataset_id}/by-source",
    response_model=DatasetQualityBySourceResponse,
    summary="Kualitas per source untuk satu dataset",
    description=(
        "Kualitas dataset dipecah per sensor. SENTINEL1 melaporkan skor "
        "radiometrik sungguhan dari tabel quality_metrics (module6_analytics, "
        "atas band VV/VH). MODIS dan GPM tidak punya padanan radiometrik - "
        "speckle index dan backscatter tidak berarti untuk curah hujan atau "
        "indeks vegetasi - jadi yang dilaporkan adalah coverage: berapa persen "
        "band yang diharapkan benar-benar ada. Bedanya ditandai lewat field "
        "`kind` (RADIOMETRIC vs COVERAGE), jangan dibandingkan langsung."
    ),
)
async def get_dataset_quality_by_source(
    dataset_id: int,
    db: DatabaseClient = Depends(get_db),
) -> DatasetQualityBySourceResponse:
    from etl.dataset_manager import DatasetManager

    if DatasetManager(db).get_dataset(dataset_id) is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    sources: list[SourceQualityItem] = []

    with db.session() as sess:
        # --- Sentinel-1: skor radiometrik per band -------------------------
        rows = sess.execute(
            select(
                QualityMetric.band_name,
                func.avg(QualityMetric.quality_score),
                func.count(QualityMetric.metric_id),
            )
            .join(DataProduct, DataProduct.product_id == QualityMetric.product_id)
            .where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.source == "SENTINEL1",
            )
            .group_by(QualityMetric.band_name)
        ).all()

        if rows:
            bands = {band: round(float(avg), 2) for band, avg, _ in rows}
            overall = round(sum(bands.values()) / len(bands), 2)
            scene_count = sess.scalar(
                select(func.count(func.distinct(QualityMetric.scene_id)))
                .join(DataProduct, DataProduct.product_id == QualityMetric.product_id)
                .where(
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.source == "SENTINEL1",
                )
            ) or 0
            sources.append(SourceQualityItem(
                source="SENTINEL1",
                kind="RADIOMETRIC",
                product_count=sum(int(n) for _, _, n in rows),
                scene_count=int(scene_count),
                quality_score=overall,
                quality_flag=_flag_for(overall),
                bands=bands,
            ))

        # --- MODIS / GPM: coverage band yang benar-benar ada ---------------
        for source, expected in _EXPECTED_BANDS.items():
            band_rows = sess.execute(
                select(DataProduct.band_name, func.count(DataProduct.product_id))
                .where(
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.source == source,
                    DataProduct.product_tier == ProductTierEnum.GOLD,
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
                .group_by(DataProduct.band_name)
            ).all()
            if not band_rows:
                continue

            counts = {band: int(n) for band, n in band_rows}
            scene_count = max(counts.values())
            # Coverage per band: berapa hari band itu ada, dibanding hari
            # terbanyak yang dipunyai source ini. Band yang tidak muncul sama
            # sekali tetap dilaporkan sebagai 0.0, bukan dihilangkan diam-diam.
            bands = {
                band: round(counts.get(band, 0) / scene_count * 100, 2)
                for band in expected
            }
            overall = round(sum(bands.values()) / len(bands), 2)
            sources.append(SourceQualityItem(
                source=source,
                kind="COVERAGE",
                product_count=sum(counts.values()),
                scene_count=scene_count,
                quality_score=overall,
                quality_flag=_flag_for(overall),
                bands=bands,
            ))

    return DatasetQualityBySourceResponse(dataset_id=dataset_id, sources=sources)


def _flag_for(score: float) -> str:
    if score >= 80:
        return "GOOD"
    if score >= 60:
        return "ACCEPTABLE"
    return "POOR"
