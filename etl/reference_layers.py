# etl/reference_layers.py
"""
Satu pintu untuk layer referensi per dataset: darat/laut dan air permanen.

Menyatukan etl/land_mask.py dan etl/water_occurrence.py supaya orchestrator
cukup memanggil satu fungsi, dan supaya kedua layer dijamin lahir di grid yang
sama dengan stack fusion dataset itu.

IDEMPOTEN, dan itu syarat utamanya. Fungsi ini dipanggil dari _finalize_date,
yang jalan SEKALI PER TANGGAL, sementara layernya properti dataset — sama untuk
semua tanggal. Tanpa penjagaan, dataset 90 tanggal akan membangun ulang layer
yang identik 90 kali. Karena itu kerja sungguhan hanya terjadi kalau berkasnya
belum ada.

Kegagalan di sini TIDAK PERNAH menggagalkan job. Layer ini informasi tambahan,
bukan mata rantai lineage: dataset tanpa layer referensi tetap dataset yang
sah dan lengkap. Tabel garis pantai yang belum dimuat, atau tile JRC yang belum
diunduh, tidak boleh menjatuhkan pipeline yang sudah berjam-jam berjalan.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

MODULE = "REFERENCE_LAYERS"


def _database_url() -> str | None:
    return os.environ.get("DATABASE_URL")


def _grid_from_pinned(db, dataset_id: int):
    """Grid dataset dari kolom datasets.fusion_grid yang sudah dipaku.

    Dibaca dari sana, bukan dari berkas H5 mana pun, supaya layer referensi
    memakai grid yang SAMA dengan yang dipakai module9 — termasuk untuk dataset
    yang stack lamanya terlanjur lahir di grid lain.
    """
    from rasterio.crs import CRS
    from rasterio.transform import Affine
    from sqlalchemy import select

    from etl.database_client import Dataset

    with db.session() as sess:
        raw = sess.scalar(
            select(Dataset.fusion_grid).where(Dataset.dataset_id == dataset_id)
        )
    if not raw:
        return None
    try:
        t = [float(v) for v in raw["transform"]]
        return (
            Affine(t[0], t[1], t[2], t[3], t[4], t[5]),
            CRS.from_string(str(raw["crs"])),
            (int(raw["height"]), int(raw["width"])),
        )
    except (KeyError, TypeError, ValueError):
        return None


def ensure_reference_layers(
    db,
    dataset_id: int,
    dataset_root: Path,
    bbox: tuple[float, float, float, float],
    database_url: str | None = None,
    force: bool = False,
) -> dict[str, str]:
    """Pastikan masks/ dataset ini lengkap. Aman dipanggil berulang kali.

    Mengembalikan peta {layer: status} untuk dicatat log — "written", "exists",
    atau "skipped: <alasan>". Tidak pernah melempar.
    """
    from etl import land_mask as lm

    out: dict[str, str] = {}
    dataset_root = Path(dataset_root)
    masks_dir = lm.get_masks_dir(dataset_root)

    grid = _grid_from_pinned(db, dataset_id)
    if grid is None:
        logger.info(
            "[%s] dataset %s belum punya fusion_grid terpaku, layer referensi "
            "ditunda sampai fusion pertama", MODULE, dataset_id,
        )
        return {"land_distance": "skipped: no pinned grid",
                "water_occurrence": "skipped: no pinned grid"}
    transform, _crs, shape = grid

    url = database_url or _database_url()
    out["land_distance"] = _ensure_land(
        masks_dir, bbox, transform, shape, url, force
    )
    out["water_occurrence"] = _ensure_occurrence(
        masks_dir, bbox, transform, shape, force
    )
    return out


def _ensure_land(masks_dir, bbox, transform, shape, url, force) -> str:
    from etl import land_mask as lm

    target = masks_dir / f"{lm.LAND_DISTANCE_STEM}.tif"
    if target.exists() and not force:
        return "exists"
    if not url:
        return "skipped: DATABASE_URL tidak diset"
    try:
        geoms = lm.fetch_land_geometries(bbox, url)
        land = lm.rasterize_land(geoms, transform, shape)
        arr, note = lm.build_land_distance(land, transform, bbox)
        lm.write_cog(
            arr, target, transform,
            tags={
                **note,
                "source": "OSM land polygons (osmdata.openstreetmap.de)",
                "source_license": "ODbL, OpenStreetMap contributors",
                "generated_by": "etl/land_mask.py",
                "semantics": (
                    "signed distance to coastline in metres; >0 land, <0 sea"
                ),
                "note": "rivers and lakes are NOT excluded; only the sea is negative",
            },
        )
        lm.render_preview(arr, masks_dir / f"{lm.LAND_DISTANCE_STEM}.png")
        lm.write_manifest(masks_dir / "manifest.json", note)
        logger.info("[%s] land_distance ditulis: %s (%.1f%% laut)",
                    MODULE, target, note.get("pct_sea", float("nan")))
        return "written"
    except Exception as exc:  # noqa: BLE001 — lihat catatan modul
        logger.warning("[%s] land_distance gagal, dilewati: %s", MODULE, exc)
        return f"skipped: {exc}"


def _ensure_occurrence(masks_dir, bbox, transform, shape, force) -> str:
    from etl import water_occurrence as wo

    target = masks_dir / f"{wo.WATER_OCCURRENCE_STEM}.tif"
    if target.exists() and not force:
        return "exists"
    try:
        arr, note = wo.build_occurrence(bbox, transform, shape)
        if note.get("tiles_missing"):
            # Tile JRC tidak diunduh otomatis: ukurannya puluhan megabita per
            # tile dan pipeline tidak boleh diam-diam menarik data eksternal di
            # tengah job. Yang hilang dicatat supaya bisa diambil belakangan.
            logger.info(
                "[%s] tile JRC belum ada di %s: %s — unduh dari %s",
                MODULE, wo.REFERENCE_DIR, note["tiles_missing"],
                wo.SOURCE_URL_TEMPLATE,
            )
        if not note.get("tiles_used"):
            return f"skipped: tile JRC belum diunduh {note.get('tiles_missing')}"
        wo.write_occurrence(arr, target, transform, note)
        wo.render_preview(arr, masks_dir / f"{wo.WATER_OCCURRENCE_STEM}.png")
        wo.write_manifest(masks_dir / "manifest_water_occurrence.json", note)
        logger.info("[%s] water_occurrence ditulis: %s", MODULE, target)
        return "written"
    except Exception as exc:  # noqa: BLE001 — lihat catatan modul
        logger.warning("[%s] water_occurrence gagal, dilewati: %s", MODULE, exc)
        return f"skipped: {exc}"
