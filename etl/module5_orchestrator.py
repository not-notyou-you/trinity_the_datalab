# etl/module5_orchestrator.py
from __future__ import annotations
import json
import logging
import os
import shutil
import threading
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue
import rasterio
from shapely import wkt as shapely_wkt
from etl.database_client import DatabaseClient, DatasetJob
from etl.dataset_manager import (
    DatasetManager,
    compute_skip_stages,
    compute_tiers_to_delete,
    get_cancel_event,
    get_pause_event,
)
from etl import folder_manager as fm
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager
from etl.module1_download import discover_scenes, download_scene
from etl.module1b_calibrate import run as calibrate_run
from etl.module2_crop import run as crop_run
from etl.module3_lee_filter import run as lee_run
from etl.module4_gold_export import export_scene_to_gold, gold_product_type
from etl.module6_analytics import compute_band_metrics
from etl.module9_fusion import FUSION_LAYERS, create_fusion_stack, ensure_aux_inputs_for_date
from etl.module10_generate_preview import generate_previews
from etl.pipeline_logger import (
    PipelineLogger,
    adopt_dataset_log_scope,
    dataset_log_file,
)

logger = logging.getLogger(__name__)

_MAX_CONCURRENT_SCENE_PIPELINES = int(os.getenv("PIPELINE_MAX_CONCURRENT_SCENES", "2"))
_pipeline_semaphore = threading.Semaphore(_MAX_CONCURRENT_SCENE_PIPELINES)


@dataclass
class _JobContext:
    db: DatabaseClient
    dsmgr: DatasetManager
    meta: MetadataManager
    lineage: LineageTracker
    plog: PipelineLogger
    job_id: int
    dataset_id: int
    dataset_name: str
    region_id: int
    bbox_wkt: str
    bbox_tuple: tuple[float, float, float, float]
    required_tiers: list[str]
    skip_stages: set[str]
    min_quality_score: float
    base_dir: Path
    pause_event: threading.Event
    cancel_event: threading.Event
    # Set once run_dataset_job enters its dataset_log_file(...) block; worker
    # threads enrol themselves with it so their records reach the .txt file.
    log_path: Path | None = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _bbox_tuple_from_wkt(wkt_str: str) -> tuple[float, float, float, float]:
    return shapely_wkt.loads(wkt_str).bounds


def _file_size_mb(path: str) -> float:
    return round(Path(path).stat().st_size / (1024 ** 2), 3)


def _raster_dims(path: str) -> tuple[int | None, int | None]:
    try:
        with rasterio.open(path) as src:
            return src.height, src.width
    except Exception:
        return None, None


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _write_dataset_metadata(
    dsmgr: DatasetManager, dataset_id: int, total_size_bytes: int | None = None
) -> None:
    """Delegasi ke DatasetManager.write_metadata_file — definisinya pindah ke
    sana supaya create_dataset() bisa menulis metadata.json sejak dataset
    dibuat, bukan cuma orchestrator setelah job selesai."""
    try:
        dsmgr.write_metadata_file(dataset_id, total_size_bytes=total_size_bytes)
    except OSError:
        logger.warning("[ORCH] gagal tulis metadata.json dataset_id=%d", dataset_id, exc_info=True)


def _process_scene(
    jc: _JobContext, scene_meta: dict, dl_result
) -> tuple[int, list[str], dict[str, list[str]]]:
    pid = scene_meta["product_identifier"]
    acq_date = dl_result.acquisition_datetime
    produced_tiers: list[str] = []
    produced_files: dict[str, list[str]] = {}

    scene_id = jc.meta.insert_satellite_scene(
        product_identifier=dl_result.product_identifier,
        acquisition_datetime=dl_result.acquisition_datetime,
        region_id=jc.region_id,
        bbox_wkt=jc.bbox_wkt,
        orbit_direction=dl_result.orbit_direction,
        orbit_number=dl_result.orbit_number,
        relative_orbit=dl_result.relative_orbit,
        cloud_cover_percent=dl_result.cloud_cover,
        raw_file_path=dl_result.zip_path or None,
        raw_file_size_mb=dl_result.file_size_mb,
        download_url=dl_result.download_url,
        checksum_md5=dl_result.checksum_md5,
    )
    jc.dsmgr.upsert_scene_job_state(
        jc.job_id, pid, scene_id=scene_id, current_stage="DOWNLOAD", stage_status="COMPLETED"
    )

    dl_job_id = jc.meta.insert_processing_job(scene_id, "DOWNLOAD", parameters={"dataset_id": jc.dataset_id})
    jc.meta.start_job(dl_job_id)
    raw_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", source="SENTINEL1", product_type="RAW_EXTRACTED_TIFF", band_name="VV",
        file_path=dl_result.vv_tif_path, file_name=Path(dl_result.vv_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vv_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vv_tif_path),
    )
    raw_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", source="SENTINEL1", product_type="RAW_EXTRACTED_TIFF", band_name="VH",
        file_path=dl_result.vh_tif_path, file_name=Path(dl_result.vh_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vh_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vh_tif_path),
    )
    jc.meta.complete_job(dl_job_id, output_size_mb=dl_result.file_size_mb)
    produced_tiers.append("RAW")
    # .SAFE.zip ikut didaftarkan sebagai file tier RAW. _download_worker selalu
    # memanggil download_scene(keep_raw=True) karena module1b_calibrate membaca
    # LUT kalibrasi dari dalam ZIP saat tahap CROP; kalau ZIP-nya tidak
    # tercatat di sini, _cleanup_scene_tiers cuma menghapus dua TIFF hasil
    # ekstrak dan meninggalkan ~2 GB ZIP per scene di disk selamanya untuk
    # dataset yang tidak meminta tier RAW. Cleanup baru jalan setelah seluruh
    # pipeline scene selesai (lewat cleanup_queue), jadi kalibrasi sudah lewat.
    produced_files["RAW"] = [dl_result.vv_tif_path, dl_result.vh_tif_path]
    if dl_result.zip_path:
        produced_files["RAW"].append(dl_result.zip_path)

    if "CROP" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="CROP", stage_status="RUNNING")
    crop_job_id = jc.meta.insert_processing_job(
        scene_id, "CROP", parameters={"bbox": list(jc.bbox_tuple), "dataset_id": jc.dataset_id}
    )
    jc.meta.start_job(crop_job_id)

    calib_dir = fm.get_scratch_dir(jc.dataset_id, jc.dataset_name, pid)
    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE1B_CALIBRATE", stage="CALIBRATE",
        message="Applying radiometric calibration",
        input_vv=dl_result.vv_tif_path, input_vh=dl_result.vh_tif_path,
    ) as st:
        calib_dir.mkdir(parents=True, exist_ok=True)
        calib_vv, calib_vh = calibrate_run(dl_result.zip_path, dl_result.vv_tif_path, dl_result.vh_tif_path, str(calib_dir))
        st.output(output_vv=calib_vv, output_vh=calib_vh)

    bronze_dir = fm.get_scene_dir(jc.dataset_id, jc.dataset_name, "bronze", "sentinel1", pid)
    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE2_CROP", stage="CROP",
        message="Cropping to region boundaries", bbox=list(jc.bbox_tuple),
    ) as st:
        bronze_dir.mkdir(parents=True, exist_ok=True)
        crop_vv, crop_vh = crop_run(calib_vv, calib_vh, str(bronze_dir), bbox=jc.bbox_tuple)
        st.output(
            output_vv=crop_vv, output_vh=crop_vh,
            file_size_mb=round(_file_size_mb(crop_vv) + _file_size_mb(crop_vh), 3),
        )

    shutil.rmtree(calib_dir, ignore_errors=True)

    vv_rows, vv_cols = _raster_dims(crop_vv)
    bronze_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id, dataset_id=jc.dataset_id,
        product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VV",
        file_path=crop_vv, file_name=Path(crop_vv).name,
        file_size_mb=_file_size_mb(crop_vv), data_hash_sha256=jc.lineage.compute_sha256(crop_vv),
        rows=vv_rows, cols=vv_cols,
    )
    vh_rows, vh_cols = _raster_dims(crop_vh)
    bronze_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id, dataset_id=jc.dataset_id,
        product_tier="BRONZE", source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VH",
        file_path=crop_vh, file_name=Path(crop_vh).name,
        file_size_mb=_file_size_mb(crop_vh), data_hash_sha256=jc.lineage.compute_sha256(crop_vh),
        rows=vh_rows, cols=vh_cols,
    )
    jc.lineage.record_transformation(raw_vv_id, bronze_vv_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.lineage.record_transformation(raw_vh_id, bronze_vh_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.meta.complete_job(crop_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb)
    produced_tiers.append("BRONZE")
    produced_files["BRONZE"] = [crop_vv, crop_vh]
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="CROP", stage_status="COMPLETED")

    if "LEE_FILTER" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="RUNNING")
    lee_job_id = jc.meta.insert_processing_job(scene_id, "LEE_FILTER", parameters={"window_size": 7, "looks": 1})
    jc.meta.start_job(lee_job_id)

    silver_dir = fm.get_scene_dir(jc.dataset_id, jc.dataset_name, "silver", "sentinel1", pid)
    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE3_LEE_FILTER", stage="LEE_FILTER",
        message="Applying Lee filter to reduce speckle", window_size=7, looks=1,
    ) as st:
        silver_dir.mkdir(parents=True, exist_ok=True)
        lee_vv, lee_vh = lee_run(crop_vv, crop_vh, str(silver_dir), window_size=7, looks=1)
        st.output(
            output_vv=lee_vv, output_vh=lee_vh,
            file_size_mb=round(_file_size_mb(lee_vv) + _file_size_mb(lee_vh), 3),
        )

    silver_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id, dataset_id=jc.dataset_id,
        product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VV",
        file_path=lee_vv, file_name=Path(lee_vv).name,
        file_size_mb=_file_size_mb(lee_vv), data_hash_sha256=jc.lineage.compute_sha256(lee_vv),
    )
    silver_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id, dataset_id=jc.dataset_id,
        product_tier="SILVER", source="SENTINEL1", product_type="LEE_FILTERED", band_name="VH",
        file_path=lee_vh, file_name=Path(lee_vh).name,
        file_size_mb=_file_size_mb(lee_vh), data_hash_sha256=jc.lineage.compute_sha256(lee_vh),
    )
    jc.lineage.record_transformation(bronze_vv_id, silver_vv_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.lineage.record_transformation(bronze_vh_id, silver_vh_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.meta.complete_job(lee_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb)
    produced_tiers.append("SILVER")
    produced_files["SILVER"] = [lee_vv, lee_vh]
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="COMPLETED")

    if "QUALITY_ANALYTICS" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="QUALITY_ANALYTICS", stage_status="RUNNING")
    qa_job_id = jc.meta.insert_processing_job(scene_id, "QUALITY_ANALYTICS", parameters={})
    jc.meta.start_job(qa_job_id)

    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE6_ANALYTICS", stage="QUALITY_ANALYTICS",
        message="Running quality assurance checks",
    ) as st:
        band_metrics: dict[str, dict] = {}
        for band, path, product_id in (("VV", lee_vv, silver_vv_id), ("VH", lee_vh, silver_vh_id)):
            m = compute_band_metrics(path, band, min_quality_score=jc.min_quality_score)
            band_metrics[band] = asdict(m)
            jc.meta.insert_quality_metrics(
                scene_id=scene_id, product_id=product_id, band_name=band,
                total_pixels=m.total_pixels, valid_pixels=m.valid_pixels, nodata_pixels=m.nodata_pixels,
                quality_score=m.quality_score, backscatter_mean_db=m.backscatter_mean_db,
                backscatter_std_db=m.backscatter_std_db, backscatter_min_db=m.backscatter_min_db,
                backscatter_max_db=m.backscatter_max_db, radiometric_consistency=m.radiometric_consistency,
                speckle_index=m.speckle_index, quality_flag=m.quality_flag,
            )
        jc.meta.complete_job(qa_job_id)

        qa_path = silver_dir / "metadata_qa.json"
        with open(qa_path, "w") as f:
            json.dump(
                {
                    "scene_id": scene_id,
                    "product_identifier": pid,
                    "acquisition_date": acq_date.isoformat() if hasattr(acq_date, "isoformat") else str(acq_date),
                    "bands": band_metrics,
                },
                f, indent=2, default=str,
            )

        avg_quality_score = round(sum(m["quality_score"] for m in band_metrics.values()) / len(band_metrics), 2) if band_metrics else None
        st.output(quality_score=avg_quality_score, bands=band_metrics)

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="QUALITY_ANALYTICS", stage_status="COMPLETED")

    if "GOLD_EXPORT" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="GOLD_EXPORT", stage_status="RUNNING")
    gold_job_id = jc.meta.insert_processing_job(
        scene_id, "GOLD_EXPORT",
        parameters={"dataset_id": jc.dataset_id, "source": "SENTINEL1"},
    )
    jc.meta.start_job(gold_job_id)

    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE4_GOLD_EXPORT", stage="GOLD_EXPORT",
        message="Exporting Sentinel-1 bands to analysis-ready COG",
    ) as st:
        gold_files = export_scene_to_gold(
            jc.dataset_id, jc.dataset_name, "sentinel1", pid,
            {"VV": lee_vv, "VH": lee_vh},
        )
        st.output(
            output_vv=gold_files.get("VV"), output_vh=gold_files.get("VH"),
            file_size_mb=round(
                sum(_file_size_mb(f) for f in gold_files.values() if Path(f).exists()), 3
            ),
        )

    gold_product_ids: dict[str, int] = {}
    for band, silver_product_id in (("VV", silver_vv_id), ("VH", silver_vh_id)):
        gold_path = gold_files.get(band)
        if not gold_path:
            continue
        g_rows, g_cols = _raster_dims(gold_path)
        gold_product_ids[band] = jc.meta.insert_data_product(
            scene_id=scene_id, job_id=gold_job_id, dataset_id=jc.dataset_id,
            product_tier="GOLD", source="SENTINEL1",
            product_type=gold_product_type("sentinel1"), band_name=band,
            file_path=gold_path, file_name=Path(gold_path).name,
            file_size_mb=_file_size_mb(gold_path),
            data_hash_sha256=jc.lineage.compute_sha256(gold_path),
            file_format="COG", rows=g_rows, cols=g_cols,
        )
        jc.lineage.record_transformation(
            silver_product_id, gold_product_ids[band], "GOLD_EXPORT", gold_job_id,
            {"source": "sentinel1"},
        )
    jc.meta.complete_job(
        gold_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb
    )
    produced_tiers.append("GOLD")
    produced_files["GOLD"] = list(gold_files.values())
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="GOLD_EXPORT", stage_status="COMPLETED")

    if "FUSION" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files

    s1_date = acq_date.date()
    date_key = s1_date.strftime("%Y%m%d")

    # Input aux (MODIS + GPM: unduh -> SILVER -> COG GOLD) disiapkan di sini,
    # bukan lagi di dalam blok FUSION. PREVIEW me-render dari tier GOLD, jadi
    # kalau MODIS/GPM baru dimaterialisasi saat fusion berjalan, preview akan
    # selalu kehabisan lima dari tujuh lapisannya. Pemanggilannya idempotent
    # dan tetap cuma sekali per scene.
    aux_produced = ensure_aux_inputs_for_date(
        jc.db, jc.dataset_id, jc.dataset_name, jc.region_id, jc.bbox_tuple, s1_date, plog=jc.plog
    )
    # File MODIS/GPM yang baru ditulis ikut dicatat di tier-nya masing-masing,
    # supaya _cleanup_scene_tiers bisa menghapusnya juga kalau tier itu tidak
    # diminta dataset ini.
    for aux_tier, aux_paths in aux_produced.items():
        produced_files.setdefault(aux_tier, []).extend(aux_paths)
        if aux_paths and aux_tier not in produced_tiers:
            produced_tiers.append(aux_tier)

    # --- PREVIEW ---------------------------------------------------------
    # Dijalankan sebelum FUSION dan sebelum _cleanup_scene_tiers: dataset yang
    # cuma meminta tier FUSION akan menghapus gold/ setelah scene selesai,
    # jadi ini satu-satunya jendela waktu ketika seluruh raster GOLD satu
    # tanggal masih ada di disk untuk dirender.
    #
    # Kegagalannya sengaja tidak menjatuhkan scene: preview adalah artefak
    # turunan, dan HDF5 fusion -- deliverable yang sebenarnya -- tidak
    # bergantung padanya. plog.stage sudah mencatat baris FAILED lengkap
    # dengan traceback, lalu pipeline lanjut ke FUSION.
    if "PREVIEW" not in jc.skip_stages:
        jc.dsmgr.upsert_scene_job_state(
            jc.job_id, pid, current_stage="PREVIEW", stage_status="RUNNING"
        )
        try:
            with jc.plog.stage(
                jc.dataset_id, pid, module="MODULE10_PREVIEW", stage="PREVIEW",
                message="Rendering PNG preview (grayscale + colored) dari tier GOLD",
                acquisition_date=date_key,
            ) as st:
                preview_result = generate_previews(
                    jc.dataset_id, jc.dataset_name, s1_date,
                    s1_scene_key=pid, s1_gold_files=gold_files,
                )
                st.output(
                    output_dir=str(fm.get_preview_dir(jc.dataset_id, jc.dataset_name, date_key)),
                    grayscale_count=preview_result["counts"]["grayscale"],
                    colored_count=preview_result["counts"]["colored"],
                    skipped_count=preview_result["counts"]["skipped"],
                    file_size_mb=preview_result["total_size_mb"],
                )
            produced_files["PREVIEW"] = preview_result["files"]
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, current_stage="PREVIEW", stage_status="COMPLETED"
            )
        except Exception:
            # State scene sengaja tidak ditandai FAILED: scene-nya sendiri
            # tidak gagal, dan menandainya begitu akan membuatnya terhitung
            # di failed_count padahal FUSION masih akan berhasil.
            logger.exception("[ORCH] PREVIEW gagal pid=%s, lanjut ke FUSION", pid)

    # PREVIEW sengaja TIDAK masuk produced_tiers: dia bukan mata rantai
    # lineage RAW->FUSION, jadi compute_tiers_to_delete tidak boleh
    # menghapusnya cuma karena tidak disebut di required_tiers dataset.

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="FUSION", stage_status="RUNNING")

    fusion_dir = fm.get_fusion_dir(jc.dataset_id, jc.dataset_name, date_key)
    h5_path = fusion_dir / f"fusion_{date_key}.h5"

    with jc.plog.stage(
        jc.dataset_id, pid, module="MODULE9_FUSION", stage="FUSION",
        message="Fusing multi-modal data into H5", layers=FUSION_LAYERS,
    ) as st:
        def _fusion_progress(layer_name: str, done: int, total: int) -> None:
            jc.plog.log_event(
                jc.dataset_id, pid, "MODULE9_FUSION", "FUSION", "RUNNING",
                f"Fusing layer {layer_name} ({done}/{total})",
                {"progress_percent": round(done / total * 100, 1), "layer": layer_name},
            )

        create_fusion_stack(
            jc.dataset_id, jc.dataset_name, s1_date, jc.bbox_tuple, scene_id,
            db=jc.db, progress_cb=_fusion_progress,
        )
        st.output(
            output_path=str(h5_path),
            file_size_mb=_file_size_mb(str(h5_path)) if h5_path.exists() else None,
        )

    produced_tiers.append("FUSION")
    produced_files["FUSION"] = [
        str(h5_path),
        str(fusion_dir / "fusion_metadata.json"),
    ]
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="FUSION", stage_status="COMPLETED")

    return scene_id, produced_tiers, produced_files


def _cleanup_scene_tiers(
    jc: _JobContext,
    pid: str,
    scene_id: int,
    produced_tiers: list[str],
    produced_files: dict[str, list[str]],
) -> None:
    tiers_to_delete = compute_tiers_to_delete(produced_tiers, jc.required_tiers)
    if not tiers_to_delete:
        return
    for tier in tiers_to_delete:
        # Satu tier sekarang bisa berisi beberapa folder scene sekaligus
        # (silver/sentinel1/{pid}, silver/modis/{date}, silver/gpm/{date}),
        # jadi folder induknya dikumpulkan semua — bukan cuma yang terakhir.
        scene_dirs: set[Path] = set()
        deleted_paths: list[str] = []
        for file_path in produced_files.get(tier, []):
            p = Path(file_path)
            scene_dirs.add(p.parent)
            if p.exists():
                try:
                    p.unlink()
                    deleted_paths.append(file_path)
                except OSError as exc:
                    logger.error("[ORCH] gagal hapus %s: %s", p, exc)
            else:
                deleted_paths.append(file_path)
        for d in scene_dirs:
            try:
                d.rmdir()
            except OSError:
                pass  # masih ada file scene lain di tanggal yang sama
        # Produk Sentinel-1 dicocokkan lewat scene_id (menangkap juga file
        # yang ditulis run sebelumnya), produk aux MODIS/GPM lewat path
        # karena barisnya menempel ke scene placeholder, bukan scene ini.
        jc.meta.mark_products_invalid(scene_id=scene_id, dataset_id=jc.dataset_id, tier=tier)
        jc.meta.mark_products_invalid_by_paths(jc.dataset_id, deleted_paths)
    logger.info("[ORCH] cleanup pid=%s tiers dihapus=%s", pid, sorted(tiers_to_delete))


def _record_worker_failure(
    jc: _JobContext, pid: str, stage: str, exc: BaseException
) -> None:
    """Persist a worker-level failure to processing_logs.

    Exceptions raised outside a `plog.stage(...)` block — DB registration,
    lineage recording, cleanup — used to be visible only as
    dataset_scene_jobs.last_error, so neither the log file nor /logs ever
    showed why a stage failed. Never raises: logging must not mask the
    original error."""
    try:
        jc.plog.log_event(
            jc.dataset_id, pid, "ORCHESTRATOR", stage, "FAILED",
            f"{stage} failed: {exc}",
            {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": traceback.format_exc()[-4000:],
            },
        )
    except Exception:
        logger.exception("[ORCH] gagal mencatat kegagalan pid=%s stage=%s", pid, stage)


class _JobCancelled(Exception):
    """Raised from a progress callback to abort work already in flight."""


def _download_worker(jc: _JobContext, scenes: list[dict], download_queue: Queue) -> None:
    if jc.log_path:
        adopt_dataset_log_scope(jc.log_path)
    for scene_meta in scenes:
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        pid = scene_meta["product_identifier"]
        state = jc.dsmgr.get_scene_job_state(jc.job_id, pid)
        if state and state["stage_status"] == "COMPLETED":
            continue
        try:
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, current_stage="DOWNLOAD", stage_status="RUNNING", started_at=_now()
            )
            raw_dir = fm.ensure_scene_dir(
                jc.dataset_id, jc.dataset_name, "raw", "sentinel1", pid
            )

            def _download_progress(pct: float, detail: str) -> None:
                # Cancel was previously only checked between scenes, so a
                # cancel (or force-delete) landing mid-download left the
                # worker streaming a ~2 GB scene for minutes against a job --
                # and, after a force-delete, a dataset -- that no longer
                # exists. Progress callbacks are the only hook into the
                # transfer, so the abort is raised from here.
                if jc.cancel_event.is_set():
                    raise _JobCancelled(f"job_id={jc.job_id} dibatalkan")
                jc.plog.log_event(
                    jc.dataset_id, pid, "MODULE1_DOWNLOAD", "DOWNLOAD", "RUNNING",
                    f"Downloading: {detail}", {"progress_percent": round(pct, 1)},
                )

            with jc.plog.stage(
                jc.dataset_id, pid, module="MODULE1_DOWNLOAD", stage="DOWNLOAD",
                message="Downloading scene from ESA server",
                expected_size_mb=scene_meta.get("size_mb"),
            ) as st:
                result = download_scene(
                    scene_meta, output_dir=str(raw_dir), keep_raw=True, progress_cb=_download_progress,
                )
                st.output(
                    output_vv=result.vv_tif_path, output_vh=result.vh_tif_path,
                    file_size_mb=result.file_size_mb, checksum_md5=result.checksum_md5,
                    message="Scene downloaded successfully",
                )
            jc.dsmgr.increment_job_counters(jc.job_id, downloaded=1)
            download_queue.put((scene_meta, result))
        except _JobCancelled:
            logger.info("[ORCH] download dibatalkan pid=%s job_id=%d", pid, jc.job_id)
            break
        except Exception as exc:
            if jc.cancel_event.is_set():
                logger.info("[ORCH] download dibatalkan pid=%s job_id=%d", pid, jc.job_id)
                break
            logger.exception("[ORCH] download gagal pid=%s job_id=%d", pid, jc.job_id)
            _record_worker_failure(jc, pid, "DOWNLOAD", exc)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
    download_queue.put(None)


def _pipeline_worker(jc: _JobContext, download_queue: Queue, cleanup_queue: Queue) -> None:
    if jc.log_path:
        adopt_dataset_log_scope(jc.log_path)
    while True:
        item = download_queue.get()
        if item is None:
            break
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        scene_meta, dl_result = item
        pid = scene_meta["product_identifier"]
        _pipeline_semaphore.acquire()
        t0 = time.monotonic()
        try:
            scene_id, produced_tiers, produced_files = _process_scene(jc, scene_meta, dl_result)
            jc.dsmgr.increment_job_counters(jc.job_id, processed=1)
            storage_breakdown = {
                tier: round(sum(_file_size_mb(p) for p in paths if Path(p).exists()), 3)
                for tier, paths in produced_files.items()
            }
            jc.plog.log_event(
                jc.dataset_id, pid, "ORCHESTRATOR", "SCENE_PIPELINE", "COMPLETED",
                f"Scene processed successfully (tiers: {', '.join(produced_tiers)})",
                {
                    "duration_seconds": round(time.monotonic() - t0, 3),
                    "produced_tiers": produced_tiers,
                    "storage_breakdown_mb": storage_breakdown,
                    "total_size_mb": round(sum(storage_breakdown.values()), 3),
                },
            )
            cleanup_queue.put((pid, scene_id, produced_tiers, produced_files))
        except Exception as exc:
            logger.exception("[ORCH] pipeline gagal pid=%s job_id=%d", pid, jc.job_id)
            _record_worker_failure(jc, pid, "SCENE_PIPELINE", exc)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
        finally:
            _pipeline_semaphore.release()
    cleanup_queue.put(None)


def _cleanup_worker(jc: _JobContext, cleanup_queue: Queue) -> None:
    if jc.log_path:
        adopt_dataset_log_scope(jc.log_path)
    while True:
        item = cleanup_queue.get()
        if item is None:
            break
        pid, scene_id, produced_tiers, produced_files = item
        try:
            _cleanup_scene_tiers(jc, pid, scene_id, produced_tiers, produced_files)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, current_stage="CLEANUP", stage_status="COMPLETED", completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, cleaned=1)
        except Exception as exc:
            logger.exception("[ORCH] cleanup gagal pid=%s job_id=%d", pid, jc.job_id)
            _record_worker_failure(jc, pid, "CLEANUP", exc)


def run_dataset_job(db: DatabaseClient, job_id: int) -> None:
    with db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        if job is None:
            logger.error("[ORCH] job_id=%d tidak ditemukan", job_id)
            return
        dataset_id = job.dataset_id
        date_range_start = job.date_range_start
        date_range_end = job.date_range_end

    dsmgr = DatasetManager(db)
    meta = MetadataManager(db)
    lineage = LineageTracker(db)
    plog = PipelineLogger(db)

    dataset = dsmgr.get_dataset(dataset_id)
    if dataset is None:
        logger.error("[ORCH] dataset_id=%d tidak ditemukan", dataset_id)
        dsmgr.set_job_status(job_id, "FAILED", completed_at=_now())
        return

    dataset_name = dataset["name"]
    required_tiers = dataset["required_tiers"]
    region_id = dataset["region_id"]
    bbox_wkt = dataset["bbox_wkt"]
    quality_settings = dataset["quality_settings"] or {}
    min_quality_score = float(quality_settings.get("min_quality_score") or 60.0)
    min_cloud_cover = quality_settings.get("min_cloud_cover")
    skip_stages = compute_skip_stages(required_tiers)

    # PREVIEW punya dua alasan bisa dilewati, dan keduanya dilipat jadi satu di
    # sini supaya _process_scene cukup memeriksa skip_stages seperti tahap lain:
    #   1. tier tertinggi dataset di bawah GOLD -> sudah ditangani
    #      compute_skip_stages (tidak ada raster GOLD untuk dirender);
    #   2. user mematikan checkbox "Buat Preview" -> kolom generate_preview.
    # Default kolomnya TRUE, jadi dataset lama berperilaku persis seperti dulu.
    if not dataset.get("generate_preview", True):
        skip_stages.add("PREVIEW")
        logger.info(
            "[ORCH] job_id=%d PREVIEW dilewati: dimatikan di konfigurasi dataset "
            "(generate_preview=false)", job_id,
        )

    bbox_tuple = _bbox_tuple_from_wkt(bbox_wkt)
    base_dir = fm.get_dataset_root(dataset_id, dataset_name)
    base_dir.mkdir(parents=True, exist_ok=True)

    pause_event = get_pause_event(job_id)
    cancel_event = get_cancel_event(job_id)

    jc = _JobContext(
        db=db, dsmgr=dsmgr, meta=meta, lineage=lineage, plog=plog,
        job_id=job_id, dataset_id=dataset_id, dataset_name=dataset_name, region_id=region_id,
        bbox_wkt=bbox_wkt, bbox_tuple=bbox_tuple,
        required_tiers=required_tiers, skip_stages=skip_stages,
        min_quality_score=min_quality_score,
        base_dir=base_dir, pause_event=pause_event, cancel_event=cancel_event,
    )

    dsmgr.set_job_status(job_id, "PREPARING", started_at=_now())

    date_from = (
        datetime.combine(date_range_start, datetime.min.time(), tzinfo=timezone.utc)
        if date_range_start else _now() - timedelta(days=30)
    )
    date_to = (
        datetime.combine(date_range_end, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
        if date_range_end else _now()
    )

    try:
        scenes = discover_scenes(bbox_wkt=bbox_wkt, date_from=date_from, date_to=date_to, max_results=200)
    except Exception:
        logger.exception("[ORCH] discovery gagal job_id=%d", job_id)
        dsmgr.set_job_status(job_id, "FAILED", completed_at=_now())
        _write_dataset_metadata(dsmgr, dataset_id)
        return

    if not scenes:
        logger.info("[ORCH] tidak ada scene ditemukan job_id=%d", job_id)
        dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
        _write_dataset_metadata(dsmgr, dataset_id)
        return

    if min_cloud_cover is not None:
        before = len(scenes)
        scenes = [s for s in scenes if (s.get("cloud_cover") or 0) <= min_cloud_cover]
        logger.info("[ORCH] filter cloud_cover<=%.1f job_id=%d: %d -> %d scene",
                    min_cloud_cover, job_id, before, len(scenes))
        if not scenes:
            logger.info("[ORCH] semua scene tersaring cloud_cover job_id=%d", job_id)
            dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
            _write_dataset_metadata(dsmgr, dataset_id)
            return

    dsmgr.create_scene_job_states(job_id, [s["product_identifier"] for s in scenes])
    dsmgr.set_job_status(job_id, "DOWNLOADING")

    download_queue: Queue = Queue(maxsize=3)
    cleanup_queue: Queue = Queue()

    with dataset_log_file(dataset["name"]) as run_log_path:
        # Worker threads read this to enrol in the run's log scope.
        jc.log_path = run_log_path
        logger.info(
            "[ORCH] job_id=%d start dataset=%r scenes=%d tiers=%s log=%s",
            job_id, dataset["name"], len(scenes), jc.required_tiers, run_log_path,
        )
        t_download = threading.Thread(target=_download_worker, args=(jc, scenes, download_queue), daemon=False)
        t_pipeline = threading.Thread(target=_pipeline_worker, args=(jc, download_queue, cleanup_queue), daemon=False)
        t_cleanup = threading.Thread(target=_cleanup_worker, args=(jc, cleanup_queue), daemon=False)

        t_download.start()
        t_pipeline.start()
        t_cleanup.start()
        t_download.join()
        t_pipeline.join()
        t_cleanup.join()

    total_size = _dir_size_bytes(base_dir)
    dsmgr.set_dataset_size(dataset_id, total_size)
    _write_dataset_metadata(dsmgr, dataset_id, total_size)

    if cancel_event.is_set():
        dsmgr.set_job_status(job_id, "CANCELLED", completed_at=_now())
        logger.info("[ORCH] job_id=%d dibatalkan", job_id)
        return

    with db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        still_paused = job.status == "PAUSED" if job else False
        failed_count = job.failed_count if job else 0

    if still_paused:
        logger.info("[ORCH] job_id=%d berhenti dalam status PAUSED", job_id)
        return

    # failed_count is incremented per-scene on stage failure (see _download_worker /
    # _pipeline_worker above) but was never consulted here, so a job with failed
    # scenes still ended up "COMPLETED" — hiding the FAILED-only retry button/endpoint
    # (web/index.html canRetry, api/routes/pipeline.py trigger) from the scenes that
    # actually need a retry.
    final_status = "FAILED" if failed_count > 0 else "COMPLETED"
    dsmgr.set_job_status(job_id, final_status, completed_at=_now())
    logger.info("[ORCH] job_id=%d selesai status=%s failed_count=%d", job_id, final_status, failed_count)
