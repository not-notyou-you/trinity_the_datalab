# api/routes/live.py
from __future__ import annotations
import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from api.schemas import (
    LiveAreaCreateRequest,
    LiveAreaUpdateRequest,
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
from etl.live_monitor import LiveMonitor

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


# ---------------------------------------------------------------------------
# Live Monitoring: Daerah Live (LIVE_MONITORING.md)
#
# Endpoint di atas (dataset LIVE tunggal) dipertahankan untuk kompatibilitas;
# UI sekarang memakai /areas.
# ---------------------------------------------------------------------------

def _monitor(db: DatabaseClient) -> LiveMonitor:
    return LiveMonitor(db)


@router.get("/areas", summary="Daftar Daerah Live")
async def list_areas(db: DatabaseClient = Depends(get_db)) -> list[dict]:
    return _monitor(db).list_areas()


@router.post("/areas", status_code=201, summary="Tambah Daerah Live")
async def create_area(req: LiveAreaCreateRequest, db: DatabaseClient = Depends(get_db)) -> dict:
    try:
        return _monitor(db).create_area(req.region_id, req.name, req.retention)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.get("/areas/{area_id}", summary="Detail Daerah Live")
async def get_area(area_id: int, db: DatabaseClient = Depends(get_db)) -> dict:
    try:
        return _monitor(db).get_area(area_id)
    except LookupError as exc:
        raise HTTPException(404, str(exc))


@router.patch("/areas/{area_id}", summary="Ubah nama/retensi/status Daerah Live")
async def update_area(area_id: int, req: LiveAreaUpdateRequest,
                      db: DatabaseClient = Depends(get_db)) -> dict:
    try:
        return _monitor(db).update_area(area_id, retention=req.retention,
                                        name=req.name, enabled=req.enabled)
    except LookupError as exc:
        raise HTTPException(404, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@router.delete("/areas/{area_id}", summary="Hapus Daerah Live (berkas dihapus permanen, log tetap)")
async def delete_area(area_id: int, db: DatabaseClient = Depends(get_db)) -> dict:
    import asyncio
    try:
        # Menghapus berkas bisa lama; jangan blokir event loop.
        return await asyncio.to_thread(_monitor(db).delete_area, area_id)
    except LookupError as exc:
        raise HTTPException(404, str(exc))


@router.post("/areas/{area_id}/check", summary="Jalankan siklus sekarang")
async def run_area_check(area_id: int, db: DatabaseClient = Depends(get_db)) -> dict:
    mon = _monitor(db)
    try:
        mon.get_area(area_id)
    except LookupError as exc:
        raise HTTPException(404, str(exc))
    started = mon.start_cycle(area_id)
    return {"area_id": area_id, "started": started,
            "message": "Siklus dimulai" if started else "Siklus sedang berjalan"}


@router.get("/areas/{area_id}/card", summary="Isi kartu daerah (scene terpilih, tanggal, forecast)")
async def area_card(area_id: int, date: str | None = Query(None, description="YYYY-MM-DD"),
                    db: DatabaseClient = Depends(get_db)) -> dict:
    from datetime import date as date_cls
    try:
        d = date_cls.fromisoformat(date) if date else None
    except ValueError:
        raise HTTPException(400, "Format tanggal harus YYYY-MM-DD")
    try:
        return _monitor(db).get_card(area_id, d)
    except LookupError as exc:
        raise HTTPException(404, str(exc))


@router.get("/areas/{area_id}/preview/{scene_date}/{key}.png", summary="PNG preview satu scene")
async def area_preview(area_id: int, scene_date: str, key: str,
                       db: DatabaseClient = Depends(get_db)):
    from datetime import date as date_cls
    from fastapi.responses import FileResponse
    try:
        d = date_cls.fromisoformat(scene_date)
    except ValueError:
        raise HTTPException(400, "Format tanggal harus YYYY-MM-DD")
    path = _monitor(db).preview_path(area_id, d, key)
    if path is None:
        raise HTTPException(404, "Preview tidak ada")
    return FileResponse(path, media_type="image/png",
                        headers={"Cache-Control": "public, max-age=86400"})


@router.get("/areas/{area_id}/events", summary="Log langkah siklus")
async def area_events(area_id: int, limit: int = Query(100, ge=1, le=1000),
                      db: DatabaseClient = Depends(get_db)) -> list[dict]:
    return _monitor(db).events(area_id, limit)


@router.get("/areas/{area_id}/log", summary="Log scene, termasuk yang sudah dihapus")
async def area_scene_log(area_id: int, db: DatabaseClient = Depends(get_db)) -> list[dict]:
    return _monitor(db).scene_log(area_id)


@router.post("/areas/{area_id}/scenes/{scene_date}/retry", summary="Coba ulang MODIS/GPM satu scene")
async def retry_scene(area_id: int, scene_date: str, db: DatabaseClient = Depends(get_db)) -> dict:
    from datetime import date as date_cls
    try:
        d = date_cls.fromisoformat(scene_date)
        started = _monitor(db).retry_scene(area_id, d)
    except ValueError:
        raise HTTPException(400, "Format tanggal harus YYYY-MM-DD")
    except LookupError as exc:
        raise HTTPException(404, str(exc))
    return {"area_id": area_id, "scene_date": scene_date, "started": started,
            "message": "Coba ulang dimulai" if started else "Siklus daerah ini sedang berjalan"}
