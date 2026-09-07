# etl/location_resolver.py
from __future__ import annotations
import hashlib
import logging
from geoalchemy2.shape import to_shape
from sqlalchemy import select
from etl.database_client import DatabaseClient, RegionOfInterest
from etl.geo_utils import geocode_search

logger = logging.getLogger(__name__)


def resolve_region_id(db: DatabaseClient, region_id: int) -> tuple[str, int, str]:
    """Ambil bbox/label dari lokasi yang dipilih pengguna di UI.

    Jalur utama sejak lokasi dikelola di tabel: UI mengirim region_id, jadi tidak
    ada lagi pencocokan nama yang ambigu kalau ada dua lokasi bernama mirip.
    """
    with db.session() as sess:
        region = sess.get(RegionOfInterest, region_id)
        if region is None or not region.is_active or region.deleted_at is not None:
            raise ValueError(f"Lokasi dengan id {region_id} tidak ditemukan atau sudah dihapus")
        return to_shape(region.bbox).wkt, region.region_id, region.name


def _match_known_region(db: DatabaseClient, location: str) -> tuple[str, int, str] | None:
    normalized = location.strip().lower()
    with db.session() as sess:
        rows = sess.scalars(
            select(RegionOfInterest).where(
                RegionOfInterest.is_active == True,
                RegionOfInterest.deleted_at.is_(None),
            )
        ).all()
        for r in rows:
            if r.name.strip().lower() == normalized or r.region_code.strip().lower() == normalized:
                bbox_wkt = to_shape(r.bbox).wkt
                return bbox_wkt, r.region_id, r.name
    return None


def _geocode_nominatim(location: str) -> tuple[str, str]:
    results = geocode_search(location, limit=1)
    if not results:
        raise ValueError(f"Lokasi tidak ditemukan: {location}")
    return results[0]["bbox_wkt"], results[0]["display_name"]


def _create_region_from_geocode(db: DatabaseClient, bbox_wkt: str, label: str, location: str) -> int:
    code = "AUTO" + hashlib.md5(bbox_wkt.encode()).hexdigest()[:12].upper()
    with db.session() as sess:
        existing = sess.scalar(
            select(RegionOfInterest).where(RegionOfInterest.region_code == code)
        )
        if existing:
            # Lokasi hasil geocoding yang pernah di-soft-delete dihidupkan kembali,
            # supaya tidak bentrok dengan UNIQUE(region_code) saat dipakai lagi.
            if existing.deleted_at is not None or not existing.is_active:
                existing.is_active = True
                existing.deleted_at = None
            return existing.region_id
        region = RegionOfInterest(
            region_code=code,
            name=label[:100],
            description=f"Auto-created dari geocoding lokasi: {location}",
            bbox=f"SRID=4326;{bbox_wkt}",
            admin_level=3,
            country_code="ID",
            is_active=True,
            source="GEOCODE",
        )
        sess.add(region)
        sess.flush()
        region_id = region.region_id
    return region_id


def resolve_location(db: DatabaseClient, location: str) -> tuple[str, int, str]:
    known = _match_known_region(db, location)
    if known:
        bbox_wkt, region_id, label = known
        logger.info("[LOCATION] '%s' matched known region_id=%d", location, region_id)
        return bbox_wkt, region_id, label
    bbox_wkt, label = _geocode_nominatim(location)
    region_id = _create_region_from_geocode(db, bbox_wkt, label, location)
    logger.info("[LOCATION] '%s' geocoded via Nominatim -> %s (region_id=%d)", location, label, region_id)
    return bbox_wkt, region_id, label
