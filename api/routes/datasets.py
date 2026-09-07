# api/routes/datasets.py
from __future__ import annotations
import json
import logging
import os
import tempfile
import zipfile
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask
from api.schemas import (
    DatasetCancelRequest,
    DatasetCancelResponse,
    DatasetCreateRequest,
    DatasetCreateResponse,
    DatasetDeleteResponse,
    DatasetDetail,
    DatasetListResponse,
    DatasetLogsResponse,
    DatasetPauseRequest,
    DatasetPauseResponse,
    DatasetFileItem,
    DatasetProgressResponse,
    DatasetResumeResponse,
    DatasetSceneFiles,
    DatasetStorageSummary,
    DatasetTierFilesResponse,
    DeletionProgressResponse,
    SourceStorageItem,
    TierStorageItem,
)
from etl import folder_manager as fm
from api.deps import get_db
from etl.database_client import DatabaseClient
from etl.dataset_manager import DatasetManager
from etl.pipeline_logger import PipelineLogManager

router = APIRouter()
logger = logging.getLogger(__name__)


def _mgr(db: DatabaseClient) -> DatasetManager:
    return DatasetManager(db)


def _slugify(name: str) -> str:
    return fm.slugify(name)


@router.post("", response_model=DatasetCreateResponse, summary="Buat dataset baru")
async def create_dataset(
    req: DatasetCreateRequest,
    db: DatabaseClient = Depends(get_db),
) -> DatasetCreateResponse:
    try:
        result = _mgr(db).create_dataset(
            region_id=req.region_id,
            location=req.location,
            date_start=req.date_start,
            date_end=req.date_end,
            tiers=req.tiers,
            name=req.name,
            description=req.description,
            quality_settings=req.quality_settings.model_dump() if req.quality_settings else None,
            generate_preview=req.generate_preview,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc))
    return DatasetCreateResponse(**result)


@router.get("", response_model=DatasetListResponse, summary="List dataset")
async def list_datasets(
    db: DatabaseClient = Depends(get_db),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> DatasetListResponse:
    result = _mgr(db).list_datasets(limit=limit, offset=offset, dataset_kind="STANDARD")
    return DatasetListResponse(**result)


@router.get("/{dataset_id}", response_model=DatasetDetail, summary="Detail dataset")
async def get_dataset(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> DatasetDetail:
    result = _mgr(db).get_dataset(dataset_id)
    if result is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")
    return DatasetDetail(**result)


@router.get("/{dataset_id}/status", response_model=DatasetProgressResponse, summary="Progres pipeline dataset")
async def get_dataset_status(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> DatasetProgressResponse:
    result = _mgr(db).get_progress(dataset_id)
    if result is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")
    return DatasetProgressResponse(**result)


@router.post("/{dataset_id}/pause", response_model=DatasetPauseResponse, summary="Pause dataset")
async def pause_dataset(
    dataset_id: int,
    req: DatasetPauseRequest = DatasetPauseRequest(),
    db: DatabaseClient = Depends(get_db),
) -> DatasetPauseResponse:
    try:
        result = _mgr(db).pause_dataset(dataset_id, reason=req.reason)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetPauseResponse(**result)


@router.post("/{dataset_id}/resume", response_model=DatasetResumeResponse, summary="Resume dataset")
async def resume_dataset(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> DatasetResumeResponse:
    try:
        result = _mgr(db).resume_dataset(dataset_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetResumeResponse(**result)


@router.post("/{dataset_id}/cancel", response_model=DatasetCancelResponse, summary="Batalkan dataset yang sedang berjalan")
async def cancel_dataset(
    dataset_id: int,
    req: DatasetCancelRequest = DatasetCancelRequest(),
    db: DatabaseClient = Depends(get_db),
) -> DatasetCancelResponse:
    try:
        result = _mgr(db).cancel_dataset(dataset_id, cascade_delete=req.cascade_delete)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetCancelResponse(**result)


@router.get("/{dataset_id}/logs", response_model=DatasetLogsResponse, summary="Log pipeline terstruktur per stage")
async def get_dataset_logs(
    dataset_id: int,
    stage: str | None = Query(None, description="Filter stage, mis. DOWNLOAD, CROP, FUSION"),
    status: str | None = Query(None, description="Filter status: STARTED, RUNNING, COMPLETED, FAILED"),
    scene_id: str | None = Query(None, description="Filter product_identifier scene"),
    limit: int = Query(50, ge=1, le=1000),
    order: str = Query("desc", pattern="^(asc|desc)$"),
    db: DatabaseClient = Depends(get_db),
) -> DatasetLogsResponse:
    if _mgr(db).get_dataset(dataset_id) is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")
    logs, total = PipelineLogManager(db).query_logs(
        dataset_id, stage=stage, status=status, scene_id=scene_id, limit=limit, order=order,
    )
    return DatasetLogsResponse(total=total, limit=limit, logs=logs)


@router.delete("/{dataset_id}", response_model=DatasetDeleteResponse, summary="Hapus dataset")
async def delete_dataset(
    dataset_id: int,
    force: bool = Query(False, description="Paksa hentikan proses yang sedang berjalan lalu hapus"),
    db: DatabaseClient = Depends(get_db),
) -> DatasetDeleteResponse:
    try:
        result = _mgr(db).delete_dataset(dataset_id, force=force)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return DatasetDeleteResponse(**result)


@router.get("/{dataset_id}/deletion-progress", response_model=DeletionProgressResponse, summary="Progres penghapusan")
async def get_deletion_progress(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> DeletionProgressResponse:
    result = _mgr(db).get_deletion_progress(dataset_id)
    if result is None:
        raise HTTPException(404, "Tidak ada proses penghapusan untuk dataset ini")
    return DeletionProgressResponse(**result)


def _mb(size_bytes: int) -> float:
    return round(size_bytes / (1024 ** 2), 3)


def _resolve_tier_source(tier: str, source: str | None) -> tuple[str, str | None]:
    """Validasi pasangan tier/source dari query string jadi bentuk yang
    dipakai folder_manager. Melempar HTTPException 400 alih-alih membiarkan
    ValueError folder_manager keluar sebagai 500."""
    try:
        tier_l = fm.normalize_tier(tier)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    if source is None:
        return tier_l, None
    try:
        source_l = fm.normalize_source(source)
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    allowed = fm.sources_for_tier(tier_l)
    if not allowed:
        raise HTTPException(
            400,
            f"Tier {tier_l} tidak punya level source (dia gabungan semua "
            f"source) - hilangkan parameter source",
        )
    if source_l not in allowed:
        raise HTTPException(
            400, f"Source {source_l} tidak dipakai di tier {tier_l}. Valid: {list(allowed)}"
        )
    return tier_l, source_l


@router.get("/{dataset_id}/download", summary="Unduh dataset (ZIP)")
async def download_dataset(
    dataset_id: int,
    tier: str | None = Query(None, description="Batasi ke satu tier, mis. gold"),
    source: str | None = Query(None, description="Batasi ke satu source, mis. modis"),
    db: DatabaseClient = Depends(get_db),
) -> FileResponse:
    """ZIP isi dataset. Tanpa filter: seluruh dataset. Dengan `tier` dan/atau
    `source`: cuma bagian itu - supaya bisa mengunduh mis. hanya GOLD MODIS
    tanpa ikut menarik puluhan GB tier RAW."""
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    if source is not None and tier is None:
        raise HTTPException(400, "Parameter source hanya bisa dipakai bersama tier")

    base_dir = fm.get_dataset_root(dataset_id, info["name"])
    if tier is None:
        root = base_dir
    else:
        tier_l, source_l = _resolve_tier_source(tier, source)
        root = (
            fm.get_source_dir(dataset_id, info["name"], tier_l, source_l)
            if source_l else fm.get_tier_dir(dataset_id, info["name"], tier_l)
        )

    files = [f for f in root.rglob("*") if f.is_file()] if root.exists() else []
    if not files:
        raise HTTPException(404, "Tidak ada file untuk diunduh dengan filter ini")

    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    with zipfile.ZipFile(tmp.name, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files:
            # arcname tetap relatif ke root dataset walau di-filter, supaya ZIP
            # parsial dan ZIP penuh punya struktur folder yang sama.
            zf.write(f, arcname=str(f.relative_to(base_dir)))

    suffix = "".join(f"_{part}" for part in (tier, source) if part)
    filename = f"{_slugify(info['name'])}{suffix}.zip"
    return FileResponse(
        tmp.name,
        filename=filename,
        media_type="application/zip",
        background=BackgroundTask(os.remove, tmp.name),
    )


@router.get("/{dataset_id}/metadata", summary="metadata.json level-dataset")
async def get_dataset_metadata(dataset_id: int, db: DatabaseClient = Depends(get_db)) -> dict:
    """Isi data/datasets/{id}_{slug}/metadata.json apa adanya.

    Ini ringkasan yang ditulis orchestrator tiap job selesai, bukan sumber
    kebenaran - kalau berbeda dari endpoint lain, database yang benar. Berguna
    untuk melihat kondisi dataset persis seperti yang terekam di disk."""
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    metadata = fm.read_dataset_metadata(dataset_id, info["name"])
    if metadata is None:
        raise HTTPException(
            404,
            "metadata.json belum ada untuk dataset ini - file ini baru ditulis "
            "saat job pertama selesai (COMPLETED/CANCELLED/PAUSED).",
        )
    return metadata


# ---------------------------------------------------------------------------
# Tier PREVIEW
# ---------------------------------------------------------------------------
# Isi tier ini dibaca langsung dari disk, bukan dari data_products. PNG preview
# sengaja tidak didaftarkan sebagai produk data (lihat migrasi 015): dia
# turunan murni yang bisa dibangun ulang dari gold/, dan sidecar JSON yang
# ditulis module10 sudah memuat seluruh keterangan yang dibutuhkan UI. Menaruh
# 8+ baris per scene di data_products cuma untuk itu akan menambah beban tulis
# tanpa ada yang membacanya.


def _preview_scene_payload(dataset_id: int, name: str, scene: str) -> dict:
    """Rakit satu entri scene preview: isi preview_metadata.json ditambah URL
    gambar yang siap dipakai <img src>."""
    scene_dir = fm.get_preview_dir(dataset_id, name, scene)
    base_url = f"/api/datasets/{dataset_id}/preview/{scene}"

    def _read(path: Path) -> dict | None:
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            # Sidecar hilang/rusak tidak boleh menjatuhkan seluruh listing:
            # PNG-nya sendiri masih ada dan masih berguna ditampilkan.
            return None

    metadata = _read(scene_dir / "preview_metadata.json") or {}
    kinds: dict[str, dict] = {}
    for kind in fm.PREVIEW_KINDS:
        kind_dir = fm.get_preview_kind_dir(dataset_id, name, scene, kind)
        if not kind_dir.is_dir():
            continue
        info = _read(kind_dir / f"{kind}_info.json") or {}
        images = []
        for entry in info.get("images", []):
            filename = entry.get("file")
            if not filename or not (kind_dir / filename).exists():
                continue
            images.append({**entry, "url": f"{base_url}/{kind}/{filename}"})
        # Cadangan kalau sidecar tidak terbaca: listing PNG apa adanya, supaya
        # galeri tetap terisi walau tanpa keterangan.
        if not images:
            images = [
                {"key": f.stem, "file": f.name, "label": f.stem,
                 "url": f"{base_url}/{kind}/{f.name}",
                 "size_bytes": f.stat().st_size}
                for f in sorted(kind_dir.glob("*.png"))
            ]
        kinds[kind] = {
            "count": len(images),
            "info": {k: v for k, v in info.items() if k != "images"},
            "images": images,
        }

    files = fm.get_preview_scene_files(dataset_id, name, scene)
    return {
        "scene": scene,
        "acquisition_date": metadata.get("acquisition_date", scene),
        "s1_scene_key": metadata.get("s1_scene_key"),
        "generated_at": metadata.get("generated_at"),
        "sources_present": metadata.get("sources_present", []),
        "skipped": metadata.get("skipped", []),
        "usage": metadata.get("usage", {}),
        "size_bytes": sum(f.stat().st_size for f in files),
        "kinds": kinds,
    }


@router.get(
    "/{dataset_id}/preview",
    summary="Galeri preview dataset (grayscale + colored per tanggal)",
)
async def list_dataset_previews(
    dataset_id: int,
    scene: str | None = Query(None, description="Batasi ke satu tanggal (YYYYMMDD)"),
    db: DatabaseClient = Depends(get_db),
) -> dict:
    """
    Daftar PNG preview yang ada di disk untuk dataset ini, dikelompokkan per
    tanggal akuisisi lalu per jenis (grayscale / colored), lengkap dengan
    keterangan colormap dan interpretasinya dari sidecar JSON.

    Selalu 200 walau tier preview kosong: dataset lama (dan dataset yang
    berhenti sebelum GOLD) memang tidak punya preview, dan itu kondisi normal
    yang perlu dibedakan UI dari error.
    """
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    name = info["name"]
    available = fm.list_preview_scenes(dataset_id, name)
    if scene is not None:
        if scene not in available:
            raise HTTPException(404, f"Tidak ada preview untuk tanggal {scene}")
        available = [scene]

    scenes = [_preview_scene_payload(dataset_id, name, sc) for sc in available]
    return {
        "dataset_id": dataset_id,
        "tier": "preview",
        "kinds": list(fm.PREVIEW_KINDS),
        "scene_count": len(scenes),
        "total_size_bytes": sum(sc["size_bytes"] for sc in scenes),
        "scenes": scenes,
    }


@router.get(
    "/{dataset_id}/preview/{scene}/{kind}/{filename}",
    response_class=FileResponse,
    summary="Satu berkas PNG preview",
)
async def get_preview_image(
    dataset_id: int,
    scene: str,
    kind: str,
    filename: str,
    db: DatabaseClient = Depends(get_db),
) -> FileResponse:
    """Kirim satu PNG dari preview/{scene}/{kind}/.

    Ketiga komponen path divalidasi ketat lalu hasilnya dicek harus benar-benar
    berada di dalam folder kind: `filename` datang dari URL, jadi tanpa
    pemeriksaan itu ".." di dalamnya bisa membaca berkas mana pun yang bisa
    dijangkau proses ini.
    """
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    if kind not in fm.PREVIEW_KINDS:
        raise HTTPException(400, f"Jenis preview tidak valid: {kind}. Valid: {list(fm.PREVIEW_KINDS)}")
    if not filename.endswith(".png") or Path(filename).name != filename:
        raise HTTPException(400, "Nama berkas preview harus satu nama .png tanpa path")

    kind_dir = fm.get_preview_kind_dir(dataset_id, info["name"], scene, kind).resolve()
    path = (kind_dir / filename).resolve()
    if not path.is_relative_to(kind_dir) or not path.is_file():
        raise HTTPException(404, f"Preview tidak ditemukan: {scene}/{kind}/{filename}")

    return FileResponse(
        path,
        media_type="image/png",
        # Preview di-render ulang tiap job jalan lagi, tapi selalu untuk
        # tanggal yang isinya sudah final -- aman di-cache lama di browser.
        headers={"Cache-Control": "public, max-age=86400"},
    )


@router.get(
    "/{dataset_id}/storage/summary",
    response_model=DatasetStorageSummary,
    summary="Ringkasan storage per tier dan per source untuk dataset ini",
)
async def get_dataset_storage_summary(
    dataset_id: int, db: DatabaseClient = Depends(get_db)
) -> DatasetStorageSummary:
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    breakdown = fm.storage_breakdown(dataset_id, info["name"])
    return DatasetStorageSummary(
        dataset_id=dataset_id,
        tiers={
            tier: TierStorageItem(
                size_bytes=t["size_bytes"],
                size_mb=_mb(t["size_bytes"]),
                file_count=t["file_count"],
                scene_count=t["scene_count"],
                sources={
                    src: SourceStorageItem(
                        size_bytes=v["size_bytes"],
                        size_mb=_mb(v["size_bytes"]),
                        file_count=v["file_count"],
                        scene_count=v["scene_count"],
                    )
                    for src, v in t["sources"].items()
                },
            )
            for tier, t in breakdown["tiers"].items()
        },
        sources={
            src: SourceStorageItem(
                size_bytes=v["size_bytes"],
                size_mb=_mb(v["size_bytes"]),
                file_count=v["file_count"],
            )
            for src, v in breakdown["sources"].items()
        },
        total_size_bytes=breakdown["total_size_bytes"],
        total_size_mb=_mb(breakdown["total_size_bytes"]),
    )


@router.get(
    "/{dataset_id}/storage/files/{tier}",
    response_model=DatasetTierFilesResponse,
    summary="List file dataset ini per tier, dikelompokkan per source dan scene",
)
async def list_dataset_tier_files(
    dataset_id: int,
    tier: str,
    source: str | None = Query(None, description="Filter satu source: sentinel1 | modis | gpm"),
    scene: str | None = Query(None, description="Filter satu scene (product_identifier atau YYYYMMDD)"),
    db: DatabaseClient = Depends(get_db),
) -> DatasetTierFilesResponse:
    info = _mgr(db).get_dataset(dataset_id)
    if info is None:
        raise HTTPException(404, f"Dataset {dataset_id} tidak ditemukan")

    tier_l, source_l = _resolve_tier_source(tier, source)
    name = info["name"]
    result: list[DatasetSceneFiles] = []

    def _entry(scene_key: str, src: str | None, files: list) -> DatasetSceneFiles:
        return DatasetSceneFiles(
            scene=scene_key,
            source=src,
            files=[
                DatasetFileItem(name=f.name, path=str(f), size_mb=_mb(f.stat().st_size))
                for f in files
            ],
        )

    if not fm.sources_for_tier(tier_l):
        # Tier fusion/preview: langsung scene, tanpa level source.
        scenes = [scene] if scene else fm.list_sourceless_scenes(dataset_id, name, tier_l)
        for sc in scenes:
            result.append(
                _entry(sc, None, fm.get_sourceless_scene_files(dataset_id, name, tier_l, sc))
            )
    else:
        sources = [source_l] if source_l else fm.list_sources(dataset_id, name, tier_l)
        for src in sources:
            scenes = [scene] if scene else fm.list_scenes(dataset_id, name, tier_l, src)
            for sc in scenes:
                result.append(
                    _entry(sc, src, fm.get_scene_files(dataset_id, name, tier_l, src, sc))
                )
            # Cache granule mentah MODIS/GPM duduk langsung di raw/{source}/
            # tanpa folder scene, jadi list_scenes() di atas melewatinya.
            # Tanpa cabang ini listing berkas melaporkan tier RAW kosong
            # untuk MODIS/GPM padahal storage/summary menghitung granulenya.
            if scene is None:
                loose = fm.list_loose_files(dataset_id, name, tier_l, src)
                if loose:
                    result.append(_entry("(granule cache)", src, loose))

    return DatasetTierFilesResponse(
        dataset_id=dataset_id, tier=tier_l, source=source_l, scenes=result
    )
