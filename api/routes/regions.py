# api/routes/regions.py
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from geoalchemy2.shape import to_shape
from sqlalchemy import func, or_, select, text

from api.schemas import (
    GeocodeItem,
    GeocodeSearchResponse,
    OkResponse,
    RegionCreateRequest,
    RegionItem,
    RegionListResponse,
    RegionUpdateRequest,
)
from api.deps import get_db
from etl.database_client import DatabaseClient, RegionOfInterest
from etl.geo_utils import bbox_to_wkt, geocode_search

logger = logging.getLogger(__name__)
router = APIRouter()


def _to_item(r: RegionOfInterest) -> RegionItem:
    bounds = to_shape(r.bbox).bounds
    source = r.source or "SEEDER"
    return RegionItem(
        region_id=r.region_id,
        region_code=r.region_code,
        name=r.name,
        description=r.description,
        bbox=list(bounds),
        area_km2=float(r.area_km2) if r.area_km2 is not None else None,
        source=source,
        created_at=r.created_at,
        deletable=source != "SEEDER",
    )


def _derive_region_code(sess, name: str, explicit: str | None) -> str:
    """Bikin region_code unik. Pengguna cuma mengetik nama; kode dipakai internal."""
    if explicit:
        base = re.sub(r"[^A-Z0-9_]", "", explicit.strip().upper())[:20]
        if not base:
            raise HTTPException(400, "region_code hanya boleh huruf, angka, dan underscore")
        if sess.scalar(select(RegionOfInterest.region_id).where(RegionOfInterest.region_code == base)):
            raise HTTPException(409, f"Kode wilayah {base} sudah dipakai")
        return base

    # Sisakan ruang untuk sufiks angka supaya tetap muat di VARCHAR(20).
    base = re.sub(r"[^A-Z0-9]+", "_", name.strip().upper()).strip("_")[:16] or "LOC"
    candidate = base
    for suffix in range(1, 1000):
        taken = sess.scalar(
            select(RegionOfInterest.region_id).where(RegionOfInterest.region_code == candidate)
        )
        if not taken:
            return candidate
        candidate = f"{base}_{suffix}"
    raise HTTPException(409, "Tidak bisa membuat kode wilayah unik, ganti nama lokasi")


@router.get("", response_model=RegionListResponse, summary="Daftar lokasi")
async def list_regions(
    db: DatabaseClient = Depends(get_db),
    q: str | None = Query(None, description="Filter nama/kode lokasi (case-insensitive)"),
    include_deleted: bool = Query(False, description="Ikut sertakan lokasi yang sudah dihapus"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> RegionListResponse:
    stmt = select(RegionOfInterest)
    if not include_deleted:
        stmt = stmt.where(
            RegionOfInterest.is_active == True,
            RegionOfInterest.deleted_at.is_(None),
        )
    if q and q.strip():
        pattern = f"%{q.strip().lower()}%"
        stmt = stmt.where(or_(
            func.lower(RegionOfInterest.name).like(pattern),
            func.lower(RegionOfInterest.region_code).like(pattern),
        ))
    with db.session() as sess:
        total = sess.scalar(select(func.count()).select_from(stmt.subquery())) or 0
        rows = sess.scalars(
            stmt.order_by(RegionOfInterest.name).limit(limit).offset(offset)
        ).all()
        items = [_to_item(r) for r in rows]
    return RegionListResponse(items=items, total=total)


@router.get("/geocode", response_model=GeocodeSearchResponse, summary="Cari lokasi via OpenStreetMap")
async def search_geocode(
    q: str = Query(..., min_length=2, description="Nama lokasi yang dicari"),
    limit: int = Query(5, ge=1, le=20),
    country: str = Query("id", description="Kode negara ISO-2, kosongkan untuk global"),
) -> GeocodeSearchResponse:
    try:
        results = geocode_search(q, limit=limit, country_codes=country.strip())
    except RuntimeError as exc:
        raise HTTPException(502, str(exc))
    except Exception as exc:
        logger.exception("[REGIONS] geocoding gagal untuk q=%r", q)
        raise HTTPException(502, f"Layanan pencarian lokasi tidak bisa dihubungi: {exc}")
    return GeocodeSearchResponse(items=[GeocodeItem(**r) for r in results])


@router.post("", response_model=RegionItem, status_code=201, summary="Tambah lokasi")
async def create_region(
    req: RegionCreateRequest,
    db: DatabaseClient = Depends(get_db),
) -> RegionItem:
    # req sudah lolos validate_bbox lewat validator Pydantic.
    bbox_wkt = bbox_to_wkt(req.min_lon, req.min_lat, req.max_lon, req.max_lat)
    with db.session() as sess:
        duplicate = sess.scalar(
            select(RegionOfInterest).where(
                func.lower(RegionOfInterest.name) == req.name.lower(),
                RegionOfInterest.deleted_at.is_(None),
            )
        )
        if duplicate:
            raise HTTPException(409, f"Lokasi bernama {req.name} sudah ada")

        region_code = _derive_region_code(sess, req.name, req.region_code)
        # Luas dihitung PostGIS (geography = meter sungguhan), bukan aproksimasi derajat.
        area_km2 = sess.scalar(
            text("SELECT ST_Area(ST_GeomFromText(:wkt, 4326)::geography) / 1e6")
            .bindparams(wkt=bbox_wkt)
        )
        region = RegionOfInterest(
            region_code=region_code,
            name=req.name,
            description=(req.description or "").strip() or None,
            bbox=f"SRID=4326;{bbox_wkt}",
            area_km2=round(float(area_km2), 4) if area_km2 is not None else None,
            admin_level=3,
            country_code="ID",
            is_active=True,
            source="USER",
        )
        sess.add(region)
        sess.flush()
        sess.refresh(region)
        item = _to_item(region)
    logger.info("[REGIONS] lokasi baru id=%d code=%s name=%s", item.region_id, item.region_code, item.name)
    return item


@router.patch("/{region_id}", response_model=RegionItem, summary="Ubah lokasi")
async def update_region(
    region_id: int,
    req: RegionUpdateRequest,
    db: DatabaseClient = Depends(get_db),
) -> RegionItem:
    with db.session() as sess:
        region = sess.get(RegionOfInterest, region_id)
        if region is None or region.deleted_at is not None:
            raise HTTPException(404, f"Lokasi {region_id} tidak ditemukan")
        if (region.source or "SEEDER") == "SEEDER":
            raise HTTPException(403, "Lokasi bawaan sistem tidak bisa diubah")
        if req.name is not None and req.name.lower() != region.name.lower():
            clash = sess.scalar(
                select(RegionOfInterest.region_id).where(
                    func.lower(RegionOfInterest.name) == req.name.lower(),
                    RegionOfInterest.deleted_at.is_(None),
                    RegionOfInterest.region_id != region_id,
                )
            )
            if clash:
                raise HTTPException(409, f"Lokasi bernama {req.name} sudah ada")
        if req.name is not None:
            region.name = req.name
        if req.description is not None:
            region.description = req.description.strip() or None
        sess.flush()
        sess.refresh(region)
        item = _to_item(region)
    return item


@router.delete("/{region_id}", response_model=OkResponse, summary="Hapus lokasi (soft-delete)")
async def delete_region(region_id: int, db: DatabaseClient = Depends(get_db)) -> OkResponse:
    with db.session() as sess:
        region = sess.get(RegionOfInterest, region_id)
        if region is None:
            raise HTTPException(404, f"Lokasi {region_id} tidak ditemukan")
        if region.deleted_at is not None:
            return OkResponse(message=f"Lokasi {region.name} memang sudah dihapus")
        if (region.source or "SEEDER") == "SEEDER":
            raise HTTPException(403, "Lokasi bawaan sistem tidak bisa dihapus")
        # Soft-delete: baris tetap ada supaya dataset/scene lama yang menunjuk
        # region_id ini tetap bisa dibuka (FK-nya ON DELETE RESTRICT).
        region.is_active = False
        region.deleted_at = datetime.now(timezone.utc)
        name = region.name
    logger.info("[REGIONS] soft-delete lokasi id=%d name=%s", region_id, name)
    return OkResponse(message=f"Lokasi {name} dihapus")


@router.post("/{region_id}/restore", response_model=RegionItem, summary="Pulihkan lokasi terhapus")
async def restore_region(region_id: int, db: DatabaseClient = Depends(get_db)) -> RegionItem:
    with db.session() as sess:
        region = sess.get(RegionOfInterest, region_id)
        if region is None:
            raise HTTPException(404, f"Lokasi {region_id} tidak ditemukan")
        region.is_active = True
        region.deleted_at = None
        sess.flush()
        sess.refresh(region)
        item = _to_item(region)
    return item
