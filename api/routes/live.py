# api/routes/live.py
from __future__ import annotations
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from api.schemas import (
    LiveBackfillRequest,
    LiveBackfillResponse,
    LiveClearResponse,
    LiveSceneItem,
    LiveSourceItem,
    LiveStatusResponse,
    LiveToggleRequest,
    LiveToggleResponse,
)
from api.deps import get_db
from etl.database_client import DatabaseClient, DataProduct, LiveDatasetSource
from etl.dataset_manager import DatasetManager

router = APIRouter()
logger = logging.getLogger(__name__)


def _mgr(db: DatabaseClient) -> DatasetManager:
    return DatasetManager(db)


@router.get("", response_model=LiveStatusResponse, summary="Status dataset live")
async def get_live_status(db: DatabaseClient = Depends(get_db)) -> LiveStatusResponse:
    live = _mgr(db).get_live_dataset()
    if live is None:
        raise HTTPException(404, "Dataset live belum ada")

    with db.session() as sess:
        sources = sess.scalars(select(LiveDatasetSource)).all()
        source_items = [
            LiveSourceItem(
                source_name=s.source_name,
                enabled=s.enabled,
                last_check=s.last_check,
                last_ingest=s.last_ingest,
                next_check=s.next_check,
            )
            for s in sources
        ]

    return LiveStatusResponse(
        dataset_id=live["dataset_id"],
        enabled=live["live_enabled"],
        status=live["status"],
        required_tiers=live["required_tiers"],
        bbox_wkt=live["bbox_wkt"],
        total_size_bytes=live["total_size_bytes"],
        last_checked_at=live["live_last_checked_at"],
        sources=source_items,
    )


@router.post("/toggle", response_model=LiveToggleResponse, summary="Nyalakan/matikan dataset live")
async def toggle_live(req: LiveToggleRequest, db: DatabaseClient = Depends(get_db)) -> LiveToggleResponse:
    try:
        result = _mgr(db).toggle_live(req.enabled)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return LiveToggleResponse(**result)


@router.post("/clear", response_model=LiveClearResponse, summary="Kosongkan dataset live")
async def clear_live(db: DatabaseClient = Depends(get_db)) -> LiveClearResponse:
    try:
        result = _mgr(db).clear_live_dataset()
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return LiveClearResponse(
        status="CLEARED",
        freed_bytes=result["freed_bytes"],
        deleted_count=result["deleted_count"],
    )


@router.post("/backfill", response_model=LiveBackfillResponse, summary="Backfill dataset live untuk rentang tanggal tertentu")
async def backfill_live(req: LiveBackfillRequest, db: DatabaseClient = Depends(get_db)) -> LiveBackfillResponse:
    try:
        result = _mgr(db).trigger_live_backfill(req.date_start, req.date_end)
    except ValueError as exc:
        raise HTTPException(404, str(exc))
    return LiveBackfillResponse(
        status=result["status"],
        job_id=result["job_id"],
        date_range=f"{req.date_start} to {req.date_end}",
    )


@router.get("/scenes", response_model=list[LiveSceneItem], summary="Scene terbaru di dataset live")
async def list_live_scenes(
    db: DatabaseClient = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
) -> list[LiveSceneItem]:
    live = _mgr(db).get_live_dataset()
    if live is None:
        raise HTTPException(404, "Dataset live belum ada")

    with db.session() as sess:
        products = sess.scalars(
            select(DataProduct)
            .where(DataProduct.dataset_id == live["dataset_id"], DataProduct.is_valid == True)
            .order_by(DataProduct.created_at.desc())
            .limit(limit)
        ).all()
        return [
            LiveSceneItem(
                product_id=p.product_id,
                scene_date=p.created_at,
                tier=p.product_tier.value,
                size_mb=float(p.file_size_mb),
            )
            for p in products
        ]
