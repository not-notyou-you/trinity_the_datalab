# api/routes/storage.py
"""
Storage management API: informasi penggunaan disk dan cleanup per tier.

GET  /api/storage/summary        — ringkasan penggunaan per tier
GET  /api/storage/files/{tier}   — list file di tier tertentu
POST /api/storage/cleanup        — hapus file berdasarkan tier atau semua
POST /api/storage/cleanup/partial — hapus file .part yang tidak lengkap

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from etl import folder_manager as fm

router = APIRouter()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

StorageTier = Literal["raw", "bronze", "silver", "gold", "preview", "fusion", "partial", "all"]

# Tier yang dianggap turunan dan aman dihapus massal lewat tier="all".
# gold + fusion adalah deliverable akhir, jadi tidak ikut. preview juga tidak
# ikut walau isinya turunan: PNG-nya cuma bisa dibangun ulang selama gold/
# masih ada, dan pada dataset yang cuma meminta FUSION gold/ sudah dihapus --
# jadi menghapus preview di sana berarti menghilangkannya untuk selamanya.
# Hapus tier ini secara eksplisit dengan tier="preview" kalau memang diinginkan.
_DERIVED_TIERS: tuple[str, ...] = ("raw", "bronze", "silver")


def _dataset_roots() -> list[Path]:
    """Semua folder dataset di data/datasets/. Endpoint di router ini
    lintas-dataset (ringkasan disk mesin), sementara
    /api/datasets/{id}/storage/summary adalah versi satu dataset."""
    if not fm.DATA_ROOT.exists():
        return []
    return sorted(d for d in fm.DATA_ROOT.iterdir() if d.is_dir())


def _get_tier_paths() -> dict[str, list[Path]]:
    """Folder per tier di layout tier-source-scene sekarang, dikumpulkan dari
    seluruh dataset: data/datasets/{id}_{slug}/{tier}/.

    Sebelumnya fungsi ini menunjuk `processed/{bronze,silver,gold}` dan
    `recovered_temp/` -- layout sebelum refactor tier/source, yang sudah
    tidak pernah ditulis lagi. Akibatnya seluruh router ini melaporkan 0 byte
    untuk semua tier dan cleanup-nya tidak pernah menghapus apa pun."""
    roots = _dataset_roots()
    paths = {tier: [r / tier for r in roots] for tier in fm.TIERS}
    paths["partial"] = [r / tier for r in roots for tier in fm.TIERS]
    paths["all"] = [r / tier for r in roots for tier in _DERIVED_TIERS]
    return paths


def _dir_info(path: Path, ext_filter: str | None = None) -> dict:
    """Hitung jumlah file dan total ukuran di sebuah direktori."""
    if not path.exists():
        return {"path": str(path), "exists": False, "file_count": 0, "size_mb": 0.0, "files": []}

    files = []
    total_bytes = 0
    pattern = f"*{ext_filter}" if ext_filter else "*"

    for f in sorted(path.rglob("*")):
        if f.is_file():
            if ext_filter and not f.name.endswith(ext_filter):
                continue
            size_mb = f.stat().st_size / (1024 ** 2)
            total_bytes += f.stat().st_size
            files.append({
                "name":    f.name,
                "path":    str(f),
                "size_mb": round(size_mb, 2),
            })

    return {
        "path":       str(path),
        "exists":     True,
        "file_count": len(files),
        "size_mb":    round(total_bytes / (1024 ** 2), 2),
        "files":      files,
    }


def _human(mb: float) -> str:
    return f"{mb / 1024:.2f} GB" if mb >= 1024 else f"{mb:.1f} MB"


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class CleanupRequest(BaseModel):
    tier:    StorageTier
    dry_run: bool = False  # True = hitung saja tanpa hapus


class CleanupResponse(BaseModel):
    tier:          str
    dry_run:       bool
    files_deleted: int
    size_freed_mb: float
    size_freed_human: str
    errors:        list[str]
    message:       str


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------

@router.get(
    "/summary",
    summary="Ringkasan storage",
    description="Menampilkan penggunaan disk per tier lengkap dengan jumlah file.",
)
async def storage_summary() -> JSONResponse:
    tier_paths = _get_tier_paths()
    tiers = {}
    by_source: dict[str, dict] = {}
    total_mb = 0.0

    for tier_name in fm.TIERS:
        size_mb = 0.0
        count   = 0
        sources: dict[str, dict] = {}

        for tier_dir in tier_paths[tier_name]:
            info = _dir_info(tier_dir)
            size_mb += info["size_mb"]
            count   += info["file_count"]
            # Pecahan per source dibaca dari subfolder source di dalam tier.
            # Tier fusion tidak punya level itu (isinya gabungan semua
            # source), jadi dilewati -- sama seperti folder_manager.
            for src in fm.sources_for_tier(tier_name):
                sub = _dir_info(tier_dir / src)
                if sub["file_count"] == 0:
                    continue
                agg = sources.setdefault(src, {"size_mb": 0.0, "file_count": 0})
                agg["size_mb"] += sub["size_mb"]
                agg["file_count"] += sub["file_count"]
                tot = by_source.setdefault(src, {"size_mb": 0.0, "file_count": 0})
                tot["size_mb"] += sub["size_mb"]
                tot["file_count"] += sub["file_count"]

        total_mb += size_mb
        tiers[tier_name] = {
            "size_mb":    round(size_mb, 2),
            "size_human": _human(size_mb),
            "file_count": count,
            "sources": {
                src: {
                    "size_mb": round(v["size_mb"], 2),
                    "size_human": _human(v["size_mb"]),
                    "file_count": v["file_count"],
                }
                for src, v in sources.items()
            },
        }

    # Hitung file .part (download tidak selesai). Sisa download terputus bisa
    # muncul di tier mana pun yang menulis lewat file .part, bukan cuma raw.
    partial_mb    = 0.0
    partial_count = 0
    seen_partial: set[Path] = set()
    for p in tier_paths["partial"]:
        if not p.exists():
            continue
        for f in p.rglob("*.part"):
            if f in seen_partial:
                continue
            seen_partial.add(f)
            partial_mb    += f.stat().st_size / (1024 ** 2)
            partial_count += 1

    return JSONResponse(content={
        "tiers": {
            "raw": {
                **tiers["raw"],
                "description": "File ZIP download asli + TIF hasil ekstrak",
                "note":        "Hapus ini setelah pipeline selesai (keep_raw=false)",
            },
            "bronze": {
                **tiers["bronze"],
                "description": "Setelah dipotong ke area AOI (Module 2)",
                "note":        "±50 MB per scene per band",
            },
            "silver": {
                **tiers["silver"],
                "description": "Setelah Lee Filter noise (Module 3)",
                "note":        "±45 MB per scene per band",
            },
            "gold": {
                **tiers["gold"],
                "description": "COG analysis-ready per source (Module 4) — ini yang terpenting",
                "note":        "JANGAN hapus ini kecuali scene sudah tidak diperlukan",
            },
            "fusion": {
                **tiers["fusion"],
                "description": "HDF5 multi-modal gabungan semua source (Module 9)",
                "note":        "Deliverable akhir — tidak ikut terhapus oleh tier 'all'",
            },
        },
        "by_source": {
            src: {
                "size_mb": round(v["size_mb"], 2),
                "size_human": _human(v["size_mb"]),
                "file_count": v["file_count"],
            }
            for src, v in by_source.items()
        },
        "partial_downloads": {
            "size_mb":    round(partial_mb, 2),
            "size_human": _human(partial_mb),
            "file_count": partial_count,
            "note":       "File .part = download terputus di tengah jalan, bisa dilanjutkan otomatis",
        },
        "total": {
            "size_mb":    round(total_mb, 2),
            "size_human": _human(total_mb),
        },
    })


@router.get(
    "/files/{tier}",
    summary="List file per tier",
    description="Tampilkan daftar file lengkap di tier tertentu (raw/bronze/silver/gold).",
)
async def list_files(tier: StorageTier) -> JSONResponse:
    if tier == "all":
        raise HTTPException(400, "Tier 'all' hanya untuk cleanup, bukan listing")

    tier_paths = _get_tier_paths()
    paths = tier_paths.get(tier, [])

    result = []
    for p in paths:
        ext = ".part" if tier == "partial" else None
        info = _dir_info(p, ext_filter=ext)
        result.append(info)

    return JSONResponse(content={"tier": tier, "directories": result})


@router.post(
    "/cleanup",
    response_model=CleanupResponse,
    summary="Hapus file per tier",
    description=(
        "Hapus semua file di tier tertentu untuk membebaskan storage.\n\n"
        "- **raw**: hapus ZIP dan TIF mentah (hemat ±800 MB per scene)\n"
        "- **bronze**: hapus hasil crop (hemat ±50 MB per scene per band)\n"
        "- **silver**: hapus hasil Lee filter (hemat ±45 MB per scene per band)\n"
        "- **gold**: ⚠️ hapus COG production-ready (data utama!)\n"
        "- **partial**: hapus file .part (download terputus)\n"
        "- **all**: hapus semua kecuali gold\n\n"
        "Gunakan `dry_run=true` untuk melihat apa yang akan dihapus tanpa benar-benar menghapus."
    ),
)
async def cleanup_storage(req: CleanupRequest) -> CleanupResponse:
    tier_paths = _get_tier_paths()
    errors     = []
    deleted    = 0
    freed_mb   = 0.0

    # Tentukan paths yang akan dibersihkan
    if req.tier == "all":
        # Hapus tier turunan saja; gold + fusion adalah deliverable akhir dan
        # terlalu berbahaya dihapus tanpa konfirmasi eksplisit per tier.
        paths_to_clean = tier_paths["all"]
        logger.warning(
            "[CLEANUP] Cleanup ALL (%s) dry_run=%s",
            "+".join(_DERIVED_TIERS), req.dry_run,
        )
    elif req.tier == "partial":
        paths_to_clean = tier_paths["partial"]
    else:
        paths_to_clean = tier_paths.get(req.tier, [])

    for base_path in paths_to_clean:
        if not base_path.exists():
            continue

        if req.tier == "partial":
            # Hanya hapus .part files
            pattern_iter = list(base_path.rglob("*.part"))
        else:
            # Hapus semua file TIF dan ZIP di folder ini
            pattern_iter = [f for f in base_path.rglob("*") if f.is_file()]

        for f in pattern_iter:
            try:
                size_mb = f.stat().st_size / (1024 ** 2)
                if req.dry_run:
                    logger.info("[CLEANUP DRY] Would delete: %s (%.1f MB)", f.name, size_mb)
                else:
                    f.unlink()
                    logger.info("[CLEANUP] Deleted: %s (%.1f MB)", f.name, size_mb)
                deleted  += 1
                freed_mb += size_mb
            except Exception as exc:
                errors.append(f"{f.name}: {exc}")
                logger.error("[CLEANUP] Error deleting %s: %s", f, exc)

        # Hapus folder kosong (bukan base path itu sendiri)
        if not req.dry_run and req.tier != "partial":
            for sub in sorted(base_path.rglob("*"), reverse=True):
                if sub.is_dir() and sub != base_path:
                    try:
                        sub.rmdir()  # hanya hapus jika benar-benar kosong
                    except OSError:
                        pass  # tidak kosong, skip

    verb = "Akan dihapus" if req.dry_run else "Dihapus"
    msg  = (
        f"{verb}: {deleted} file ({_human(freed_mb)}) "
        f"dari tier '{req.tier}'"
        + (" [DRY RUN - tidak ada yang dihapus]" if req.dry_run else "")
        + (f" — {len(errors)} error" if errors else "")
    )

    logger.info("[CLEANUP] %s", msg)

    return CleanupResponse(
        tier             = req.tier,
        dry_run          = req.dry_run,
        files_deleted    = deleted,
        size_freed_mb    = round(freed_mb, 2),
        size_freed_human = _human(freed_mb),
        errors           = errors,
        message          = msg,
    )


@router.post(
    "/cleanup/partial",
    summary="Hapus file download terputus (.part)",
    description=(
        "Hapus semua file `.part` yang merupakan sisa download yang terputus. "
        "File ini tidak bisa dipakai tapi bisa makan storage. "
        "**Catatan:** download yang sedang berjalan juga punya file .part — "
        "jangan jalankan ini saat pipeline sedang aktif download."
    ),
)
async def cleanup_partial(dry_run: bool = False) -> CleanupResponse:
    return await cleanup_storage(CleanupRequest(tier="partial", dry_run=dry_run))