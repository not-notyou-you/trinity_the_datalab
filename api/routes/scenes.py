# api/routes/scenes.py
"""
GET /api/scenes       — list scenes with filters
GET /api/scenes/{id}  — scene detail
GET /api/scenes/{id}/status — pipeline status per scene
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from api.schemas import (
    PipelineStatusResponse,
    SceneDetail,
    SceneListItem,
    SceneListResponse,
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

router  = APIRouter()
logger  = logging.getLogger(__name__)


@router.get(
    "",
    response_model=SceneListResponse,
    summary="List satellite scenes",
    description=(
        "Retrieve a paginated list of Sentinel-1 scenes with optional filters. "
        "Supports date range, region, orbit direction, and quality score filters."
    ),
)
async def list_scenes(
    db:              DatabaseClient = Depends(get_db),
    region_id:       int | None     = Query(None,  description="Filter by region_id"),
    orbit_direction: str | None     = Query(None,  description="ASCENDING or DESCENDING"),
    date_from:       datetime | None = Query(None, description="Acquisition from (UTC ISO)"),
    date_to:         datetime | None = Query(None, description="Acquisition to (UTC ISO)"),
    only_gold:       bool           = Query(False, description="Only scenes with GOLD product"),
    limit:           int            = Query(20,    ge=1, le=200, description="Results per page"),
    offset:          int            = Query(0,     ge=0,         description="Pagination offset"),
) -> SceneListResponse:
    """
    List scenes with filter support.

    Query patterns:
    - GET /api/scenes
    - GET /api/scenes?region_id=1&date_from=2024-01-01
    - GET /api/scenes?orbit_direction=ASCENDING&limit=50
    - GET /api/scenes?only_gold=true
    """
    with db.session() as sess:
        stmt = select(SatelliteScene).where(SatelliteScene.is_available == True)

        if region_id:
            stmt = stmt.where(SatelliteScene.region_id == region_id)
        if orbit_direction:
            stmt = stmt.where(SatelliteScene.orbit_direction == orbit_direction)
        if date_from:
            stmt = stmt.where(SatelliteScene.acquisition_datetime >= date_from)
        if date_to:
            stmt = stmt.where(SatelliteScene.acquisition_datetime <= date_to)
        if only_gold:
            gold_scene_ids = select(DataProduct.scene_id).where(
                DataProduct.product_tier == ProductTierEnum.GOLD,
                DataProduct.is_latest    == True,
                DataProduct.is_valid     == True,
            )
            stmt = stmt.where(SatelliteScene.scene_id.in_(gold_scene_ids))

        total = sess.scalar(select(func.count()).select_from(stmt.subquery()))
        scenes = sess.scalars(
            stmt.order_by(SatelliteScene.acquisition_datetime.desc())
                .limit(limit).offset(offset)
        ).all()

        items = [
            SceneListItem(
                scene_id             = s.scene_id,
                scene_uuid           = str(s.scene_uuid),
                product_identifier   = s.product_identifier,
                platform             = s.platform,
                instrument_mode      = s.instrument_mode,
                polarization_vv      = s.polarization_vv,
                polarization_vh      = s.polarization_vh,
                acquisition_datetime = s.acquisition_datetime,
                orbit_direction      = s.orbit_direction,
                orbit_number         = s.orbit_number,
                relative_orbit       = s.relative_orbit,
                cloud_cover_percent  = float(s.cloud_cover_percent) if s.cloud_cover_percent else None,
                resolution_m         = s.resolution_m,
                region_id            = s.region_id,
                is_available         = s.is_available,
                created_at           = s.created_at,
            )
            for s in scenes
        ]

    return SceneListResponse(total=total or 0, limit=limit, offset=offset, items=items)


@router.get(
    "/{scene_id}",
    response_model=SceneDetail,
    summary="Get scene detail",
    description="Retrieve full metadata for a single Sentinel-1 scene by scene_id.",
)
async def get_scene(
    scene_id: int,
    db: DatabaseClient = Depends(get_db),
) -> SceneDetail:
    meta = MetadataManager(db)
    scene = meta.get_scene_by_id(scene_id)
    if not scene:
        raise HTTPException(status_code=404, detail=f"Scene {scene_id} not found")

    with db.session() as sess:
        s = sess.get(SatelliteScene, scene_id)
        return SceneDetail(
            scene_id             = s.scene_id,
            scene_uuid           = str(s.scene_uuid),
            product_identifier   = s.product_identifier,
            platform             = s.platform,
            instrument_mode      = s.instrument_mode,
            polarization_vv      = s.polarization_vv,
            polarization_vh      = s.polarization_vh,
            acquisition_datetime = s.acquisition_datetime,
            orbit_direction      = s.orbit_direction,
            orbit_number         = s.orbit_number,
            relative_orbit       = s.relative_orbit,
            cloud_cover_percent  = float(s.cloud_cover_percent) if s.cloud_cover_percent else None,
            resolution_m         = s.resolution_m,
            region_id            = s.region_id,
            is_available         = s.is_available,
            created_at           = s.created_at,
            updated_at           = s.updated_at,
            raw_file_path        = s.raw_file_path,
            raw_file_size_mb     = float(s.raw_file_size_mb) if s.raw_file_size_mb else None,
            download_url         = s.download_url,
            checksum_md5         = s.checksum_md5,
            incidence_angle_near = float(s.incidence_angle_near) if s.incidence_angle_near else None,
            incidence_angle_far  = float(s.incidence_angle_far) if s.incidence_angle_far else None,
        )


@router.get(
    "/{scene_id}/status",
    response_model=PipelineStatusResponse,
    summary="Pipeline status",
    description="Get per-stage ETL job execution status for a scene.",
)
async def get_pipeline_status(
    scene_id: int,
    db: DatabaseClient = Depends(get_db),
) -> PipelineStatusResponse:
    meta   = MetadataManager(db)
    stages = meta.get_pipeline_status(scene_id)

    if not stages:
        overall = "NOT_STARTED"
    elif all(s["status"] == "SUCCESS" for s in stages):
        overall = "COMPLETE"
    elif any(s["status"] == "FAILED" for s in stages):
        overall = "FAILED"
    else:
        overall = "IN_PROGRESS"

    from api.schemas import JobStatusItem
    return PipelineStatusResponse(
        scene_id = scene_id,
        stages   = [JobStatusItem(**s) for s in stages],
        overall_status = overall,
    )
