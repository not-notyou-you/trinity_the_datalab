# api/routes/pipeline.py
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from api.deps import get_db
from etl.database_client import DatabaseClient, ProcessingJob, SatelliteScene
from etl.dataset_manager import DatasetManager
from etl.metadata_manager import MetadataManager

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/status/current",
    summary="Status pipeline saat ini",
    description="Status tahap pipeline untuk scene yang paling baru diproses, dipakai untuk progress rail di beranda.",
)
async def current_pipeline_status(db: DatabaseClient = Depends(get_db)) -> dict:
    with db.session() as sess:
        latest_job = sess.execute(
            select(ProcessingJob).order_by(ProcessingJob.created_at.desc()).limit(1)
        ).scalar_one_or_none()

        if not latest_job:
            return {"active": False, "scene_id": None, "stages": [], "overall_status": "NONE"}

        scene = sess.get(SatelliteScene, latest_job.scene_id)
        scene_id = latest_job.scene_id
        product_identifier = scene.product_identifier if scene else None

    meta = MetadataManager(db)
    stages = meta.get_pipeline_status(scene_id)

    if not stages:
        overall = "NOT_STARTED"
    elif all(s["status"] == "SUCCESS" for s in stages):
        overall = "COMPLETE"
    elif any(s["status"] == "FAILED" for s in stages):
        overall = "FAILED"
    else:
        overall = "IN_PROGRESS"

    return {
        "active": overall == "IN_PROGRESS",
        "scene_id": scene_id,
        "product_identifier": product_identifier,
        "stages": stages,
        "overall_status": overall,
    }


@router.post(
    "/trigger",
    summary="Retry job dataset yang gagal",
    description="Menjalankan ulang job pipeline terakhir untuk sebuah dataset, hanya jika job tersebut berstatus FAILED.",
)
async def trigger_pipeline(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> dict:
    mgr = DatasetManager(db)
    try:
        result = mgr.retry_dataset_job(dataset_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"started": True, "dataset_id": dataset_id, **result}