# etl/module5_orchestrator.py
"""
Orchestrator dataset: menyusun DAG pemrosesan dari konfigurasi per-satelit.

Sejak model per-satelit (DOCS/ARCHITECTURE.md: dataset_source_config), pipeline
sebuah dataset bukan lagi satu rantai tetap. Setiap sumber punya cabangnya
sendiri, dan level (RAW/PROCESSED) sumber itulah yang menentukan sampai mana
cabangnya jalan:

    SENTINEL1 RAW        DOWNLOAD -> CALIBRATE -> CROP                (BRONZE)
    SENTINEL1 PROCESSED  ... -> LEE_FILTER -> QUALITY_ANALYTICS
                             -> GOLD_EXPORT                   (SILVER, GOLD)
    MODIS     RAW        peta banjir saja                             (BRONZE)
    MODIS     PROCESSED  + NDVI + NDWI                        (SILVER, GOLD)
    GPM       RAW        curah hujan harian                           (BRONZE)
    GPM       PROCESSED  + window akumulasi 24h/72h/7d        (SILVER, GOLD)

    FUSION jalan hanya kalau >1 sumber dikonfigurasi DAN dataset punya
    fusion_strategy.

Terjemahan konfigurasi -> keputusan ada di etl/processing_plan.py; modul ini
cuma menjalankan keputusannya dan menandai setiap baris data_products dengan
processing_level yang menghasilkannya.

Sentinel-1 tetap jadi jangkar tanggal ketika dikonfigurasi (MODIS/GPM diambil
untuk tanggal akuisisi tiap scene S1). Dataset tanpa Sentinel-1 tidak punya
jangkar itu, jadi sumber aux-nya diproses per hari sepanjang rentang tanggal
dataset — lihat _run_aux_only().
"""
from __future__ import annotations
import json
import logging
import os
import re
import shutil
import threading
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
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
from etl.fusion_strategies import DEFAULT_S1_MATCH_TOLERANCE_DAYS, FusionPlan, plan_fusion
from etl.module1_download import (
    MIN_S1_AOI_COVERAGE,
    aoi_coverage,
    discover_scenes,
    download_scene,
)
from etl.module1b_calibrate import run as calibrate_run
from etl.module2_crop import run as crop_run
from etl.module3_lee_filter import run as lee_run
from etl.module4_gold_export import export_scene_to_gold, gold_product_type
from etl.module6_analytics import compute_band_metrics
from etl.module9_fusion import (
    create_fusion_stack,
    ensure_aux_inputs_for_date,
    fusion_layers_for,
)
from etl.module10_generate_preview import (
    generate_previews,
    tier_for_level as preview_tier_for_level,
)
from etl.processing_plan import (
    PROCESSED,
    RAW,
    ProcessingPlan,
    SourcePlan,
    load_processing_plan,
)
from etl.processing_plan import SENTINEL1 as S1_SOURCE_NAME
from etl.s1_mosaic import mosaic_frames
from etl.pipeline_logger import (
    PipelineLogger,
    adopt_dataset_log_scope,
    dataset_log_file,
)

from etl import tier_names as tn

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
    # Rencana per-satelit dataset ini (etl/processing_plan.py). Sumber yang
    # tidak ada di sini tidak diproses sama sekali, dan level tiap sumber
    # menentukan tahap mana yang dilewati + nilai data_products.processing_level.
    plan: ProcessingPlan
    fusion_strategy: str | None
    # Rencana temporal strategi fusi (etl/fusion_strategies). None kalau dataset
    # tidak memfusikan apa pun — dataset satu sumber, atau tanpa strategi.
    # Menyimpan rencananya di konteks, bukan menghitung ulang per scene, supaya
    # daftar tanggal yang dipakai sumbu unduh dan sumbu rakit dijamin sama.
    fusion_plan: "FusionPlan | None"
    # datasets.preview_options — varian PNG yang diminta user. None berarti
    # kolomnya tidak dinyatakan; module10 me-render ketiganya (perilaku lama).
    preview_options: list[str] | None
    pause_event: threading.Event
    cancel_event: threading.Event
    # {YYYYMMDD: {product_identifier, ...}} seluruh scene S1 yang ditemukan
    # discover_scenes untuk tanggal itu. Dipakai _pipeline_worker untuk tahu
    # kapan sebuah tanggal sudah lengkap dan boleh masuk PREVIEW/FUSION --
    # tanpa itu, tanggal yang tertutup dua frame akan difinalisasi dua kali
    # dan yang kedua menimpa yang pertama.
    expected_pids_by_date: dict[str, set[str]] = field(default_factory=dict)
    # product_identifier scene yang gagal di run ini (diisi _record_worker_failure,
    # dibaca _sweep_scratch): scratch scene ini TIDAK disapu di akhir run supaya
    # .part yang sudah terunduh sebagian bisa di-resume oleh retry berikutnya,
    # bukan diunduh ulang dari nol.
    failed_scene_keys: set[str] = field(default_factory=set)
    _failed_scene_keys_lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def s1_plan(self) -> SourcePlan:
        """Konfigurasi Sentinel-1. Hanya dipanggil dari jalur yang sudah
        memastikan S1 dikonfigurasi (_process_scene dan pemanggilnya)."""
        plan = self.plan.get(S1_SOURCE_NAME)
        if plan is None:  # pragma: no cover - dijaga run_dataset_job
            raise RuntimeError("jalur Sentinel-1 dipanggil tanpa konfigurasi S1")
        return plan
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


def _run_s1_chain(
    jc: _JobContext, scene_meta: dict, dl_result
) -> tuple[int, list[str], dict[str, list[str]], dict[str, dict[str, str]]]:
    """Cabang Sentinel-1 untuk satu scene: DOWNLOAD -> CALIBRATE -> CROP, lalu
    (hanya untuk level PROCESSED) LEE_FILTER -> QUALITY_ANALYTICS -> GOLD_EXPORT.

    Berhenti lebih awal di tahap mana pun yang ada di jc.skip_stages — untuk
    S1 RAW-only itu berarti berhenti tepat setelah CROP, dengan BRONZE sebagai
    artefaknya.

    Returns:
        (scene_id, produced_tiers, produced_files, s1_files_by_level).
        `s1_files_by_level` memetakan level pemrosesan ke {band: path} raster
        S1 terakhir pada level itu — RAW ke hasil crop, PROCESSED ke COG.
        Kosong kalau scene berhenti sebelum CROP. Pemanggil memakainya untuk
        PREVIEW dan untuk memosaikkan frame satu tanggal.
    """
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
    # Level yang menandai artefak tiap tier untuk sumber ini. RAW/BRONZE
    # ditandai 'RAW' hanya kalau user memang meminta level RAW; kalau S1
    # dikonfigurasi PROCESSED saja, keduanya cuma langkah antara jalur penuh.
    s1_plan = jc.s1_plan
    raw_level = s1_plan.level_for_tier("RAW")
    bronze_level = s1_plan.level_for_tier(tn.ALIGNED)
    raw_vv_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", source="SENTINEL1", product_type="RAW_EXTRACTED_TIFF", band_name="VV",
        file_path=dl_result.vv_tif_path, file_name=Path(dl_result.vv_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vv_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vv_tif_path),
        processing_level=raw_level,
    )
    raw_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=dl_job_id, dataset_id=jc.dataset_id,
        product_tier="RAW", source="SENTINEL1", product_type="RAW_EXTRACTED_TIFF", band_name="VH",
        file_path=dl_result.vh_tif_path, file_name=Path(dl_result.vh_tif_path).name,
        file_size_mb=_file_size_mb(dl_result.vh_tif_path),
        data_hash_sha256=jc.lineage.compute_sha256(dl_result.vh_tif_path),
        processing_level=raw_level,
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
        return scene_id, produced_tiers, produced_files, {}
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files, {}

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

    bronze_dir = fm.get_scene_dir(jc.dataset_id, jc.dataset_name, "aligned", "sentinel1", pid)
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
        product_tier=tn.ALIGNED, source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VV",
        file_path=crop_vv, file_name=Path(crop_vv).name,
        file_size_mb=_file_size_mb(crop_vv), data_hash_sha256=jc.lineage.compute_sha256(crop_vv),
        rows=vv_rows, cols=vv_cols, processing_level=bronze_level,
    )
    vh_rows, vh_cols = _raster_dims(crop_vh)
    bronze_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=crop_job_id, dataset_id=jc.dataset_id,
        product_tier=tn.ALIGNED, source="SENTINEL1", product_type="CROPPED_TIFF", band_name="VH",
        file_path=crop_vh, file_name=Path(crop_vh).name,
        file_size_mb=_file_size_mb(crop_vh), data_hash_sha256=jc.lineage.compute_sha256(crop_vh),
        rows=vh_rows, cols=vh_cols, processing_level=bronze_level,
    )
    jc.lineage.record_transformation(raw_vv_id, bronze_vv_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.lineage.record_transformation(raw_vh_id, bronze_vh_id, "CROP", crop_job_id, {"bbox": list(jc.bbox_tuple)})
    jc.meta.complete_job(crop_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb)
    produced_tiers.append(tn.ALIGNED)
    produced_files[tn.ALIGNED] = [crop_vv, crop_vh]
    # Dipetakan per band di sini, bukan disimpulkan dari urutan daftar di
    # atas: pemanggil memakainya untuk memosaikkan band yang sama dari
    # beberapa frame, dan "elemen pertama pasti VV" adalah asumsi yang diam
    # begitu daftarnya diubah.
    s1_files_by_level: dict[str, dict[str, str]] = {RAW: {"VV": crop_vv, "VH": crop_vh}}
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="CROP", stage_status="COMPLETED")

    # Sentinel-1 RAW berhenti di sini: BRONZE (terkalibrasi, ter-crop, tanpa
    # Lee filter dan tanpa QA) ADALAH artefak RAW-nya (DOCS/PIPELINE.md, "What RAW
    # means for Sentinel-1"). jc.skip_stages sudah memuat LEE_FILTER/
    # QUALITY_ANALYTICS/GOLD_EXPORT dari SourcePlan.s1_skip_stages(); cabang
    # ini cuma membuat alasannya terbaca di log.
    if "LEE_FILTER" in jc.skip_stages:
        if s1_plan.raw_only:
            logger.info(
                "[ORCH] pid=%s berhenti di BRONZE: SENTINEL1 dikonfigurasi RAW-only", pid
            )
        return scene_id, produced_tiers, produced_files, s1_files_by_level
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files, s1_files_by_level

    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="RUNNING")
    lee_job_id = jc.meta.insert_processing_job(scene_id, "LEE_FILTER", parameters={"window_size": 7, "looks": 1})
    jc.meta.start_job(lee_job_id)

    silver_dir = fm.get_scene_dir(jc.dataset_id, jc.dataset_name, "despeckled", "sentinel1", pid)
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
        product_tier=tn.DESPECKLED, source="SENTINEL1", product_type="LEE_FILTERED", band_name="VV",
        file_path=lee_vv, file_name=Path(lee_vv).name,
        file_size_mb=_file_size_mb(lee_vv), data_hash_sha256=jc.lineage.compute_sha256(lee_vv),
        processing_level=PROCESSED,
    )
    silver_vh_id = jc.meta.insert_data_product(
        scene_id=scene_id, job_id=lee_job_id, dataset_id=jc.dataset_id,
        product_tier=tn.DESPECKLED, source="SENTINEL1", product_type="LEE_FILTERED", band_name="VH",
        file_path=lee_vh, file_name=Path(lee_vh).name,
        file_size_mb=_file_size_mb(lee_vh), data_hash_sha256=jc.lineage.compute_sha256(lee_vh),
        processing_level=PROCESSED,
    )
    jc.lineage.record_transformation(bronze_vv_id, silver_vv_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.lineage.record_transformation(bronze_vh_id, silver_vh_id, "LEE_FILTER", lee_job_id, {"window_size": 7, "looks": 1})
    jc.meta.complete_job(lee_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb)
    produced_tiers.append(tn.DESPECKLED)
    produced_files[tn.DESPECKLED] = [lee_vv, lee_vh]
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="LEE_FILTER", stage_status="COMPLETED")

    if "QUALITY_ANALYTICS" in jc.skip_stages:
        return scene_id, produced_tiers, produced_files, s1_files_by_level
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files, s1_files_by_level

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
        return scene_id, produced_tiers, produced_files, s1_files_by_level
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return scene_id, produced_tiers, produced_files, s1_files_by_level

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
            product_tier=tn.COG, source="SENTINEL1",
            product_type=gold_product_type("sentinel1"), band_name=band,
            file_path=gold_path, file_name=Path(gold_path).name,
            file_size_mb=_file_size_mb(gold_path),
            data_hash_sha256=jc.lineage.compute_sha256(gold_path),
            file_format="COG", rows=g_rows, cols=g_cols,
            # SILVER dan GOLD hanya pernah lahir dari jalur PROCESSED.
            processing_level=PROCESSED,
        )
        jc.lineage.record_transformation(
            silver_product_id, gold_product_ids[band], "GOLD_EXPORT", gold_job_id,
            {"source": "sentinel1"},
        )
    jc.meta.complete_job(
        gold_job_id, cpu_usage_percent=st.cpu_peak_percent, memory_usage_mb=st.memory_peak_mb
    )
    produced_tiers.append(tn.COG)
    produced_files[tn.COG] = list(gold_files.values())
    s1_files_by_level[PROCESSED] = dict(gold_files)
    jc.dsmgr.upsert_scene_job_state(jc.job_id, pid, current_stage="GOLD_EXPORT", stage_status="COMPLETED")

    return scene_id, produced_tiers, produced_files, s1_files_by_level


@dataclass
class _SceneResult:
    """Hasil satu scene sampai SEBELUM PREVIEW/FUSION.

    PREVIEW dan FUSION tidak lagi jalan di dalam pipeline scene: keduanya
    menulis berkas yang namanya cuma memuat TANGGAL (`20250123_s1_vv.png`,
    `fusion_20250123_hybrid_processed.h5`), sementara satu tanggal bisa punya
    beberapa scene. Dijalankan per scene, scene yang selesai belakangan
    menimpa yang duluan -- di dataset 22_try6 frame dengan cakupan 68,7%
    ditimpa frame 51,7% dan separuh AOI hilang dari deliverable.

    Jadi scene berhenti di sini, dan _finalize_date menjalankan keduanya
    SEKALI per tanggal di atas mosaik seluruh frame tanggal itu."""

    pid: str
    scene_id: int
    acquisition_date: date
    produced_tiers: list[str]
    produced_files: dict[str, list[str]]
    s1_files_by_level: dict[str, dict[str, str]]
    duration_seconds: float = 0.0


def _process_scene(jc: _JobContext, scene_meta: dict, dl_result) -> _SceneResult:
    """Pipeline satu scene Sentinel-1 sampai input aux tanggalnya siap.

    Dua bagian yang sengaja dipisah: cabang S1 (_run_s1_chain) berhenti sesuai
    level yang dikonfigurasi untuk SENTINEL1, sementara tahap lintas-sumber di
    bawah (input MODIS/GPM) tetap jalan setelahnya.

    Pemisahan itu bukan kosmetik: sebelumnya tahap aux menempel di ujung
    rantai S1, jadi dataset dengan sentinel1[RAW] + modis[PROCESSED] berhenti
    di CROP dan TIDAK PERNAH mengunduh MODIS-nya. Level satu sumber tidak
    boleh memutus sumber lain.
    """
    pid = scene_meta["product_identifier"]
    acq_date = dl_result.acquisition_datetime

    scene_id, produced_tiers, produced_files, s1_files_by_level = _run_s1_chain(
        jc, scene_meta, dl_result
    )
    s1_date = acq_date.date()

    def _result() -> _SceneResult:
        return _SceneResult(
            pid=pid, scene_id=scene_id, acquisition_date=s1_date,
            produced_tiers=produced_tiers, produced_files=produced_files,
            s1_files_by_level=s1_files_by_level,
        )

    if jc.cancel_event.is_set():
        return _result()
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return _result()

    # Input aux (MODIS + GPM) disiapkan di sini, bukan lagi di dalam blok
    # FUSION. PREVIEW me-render dari tier GOLD, jadi kalau MODIS/GPM baru
    # dimaterialisasi saat fusion berjalan, preview akan selalu kehabisan lima
    # dari tujuh lapisannya. Pemanggilannya idempotent dan tetap cuma sekali
    # per scene.
    #
    # Tidak lagi digantung pada "FUSION" in skip_stages: MODIS dan GPM sekarang
    # sumber yang berdiri sendiri di dataset_source_config, bukan sekadar bahan
    # fusi. Dataset yang meminta modis[PROCESSED] tanpa fusi tetap harus dapat
    # NDVI/NDWI-nya.
    #
    # plan diteruskan supaya sumber yang tidak dikonfigurasi tidak diunduh
    # sama sekali, dan sumber RAW-only berhenti di BRONZE.
    aux_produced, _ = ensure_aux_inputs_for_date(
        jc.db, jc.dataset_id, jc.dataset_name, jc.region_id, jc.bbox_tuple, s1_date,
        plog=jc.plog, plan=jc.plan,
    )
    # File MODIS/GPM yang baru ditulis ikut dicatat di tier-nya masing-masing,
    # supaya _cleanup_scene_tiers bisa menghapusnya juga kalau tier itu tidak
    # diminta dataset ini.
    for aux_tier, aux_paths in aux_produced.items():
        produced_files.setdefault(aux_tier, []).extend(aux_paths)
        if aux_paths and aux_tier not in produced_tiers:
            produced_tiers.append(aux_tier)

    return _result()


def _mosaic_s1_by_level(
    jc: _JobContext, date_key: str, members: list[_SceneResult]
) -> dict[str, dict[str, str]]:
    """{level: {band: path}} untuk satu tanggal, gabungan SEMUA frame-nya.

    Satu frame -> path aslinya dipakai langsung (lihat etl/s1_mosaic.py).
    Level yang tidak punya satu pun raster tidak muncul di hasil, jadi
    pemanggil bisa membedakan "tidak ada S1" dari "ada tapi kosong"."""
    scratch = fm.get_scratch_dir(jc.dataset_id, jc.dataset_name, date_key)
    mosaics: dict[str, dict[str, str]] = {}
    for level in jc.plan.output_levels():
        frames = [m.s1_files_by_level.get(level, {}) for m in members]
        frames = [f for f in frames if f]
        if not frames:
            continue
        files = mosaic_frames(frames, scratch, date_key=date_key, level=level)
        if files:
            mosaics[level] = files
    return mosaics


def _run_preview_for_date(
    jc: _JobContext, date_key: str, s1_date: date, members: list[_SceneResult],
    primary: _SceneResult, mosaics: dict[str, dict[str, str]],
) -> list[str]:
    """PREVIEW satu tanggal. Mengembalikan daftar berkas yang ditulis.

    Kegagalannya sengaja tidak menjatuhkan scene: preview adalah artefak
    turunan, dan HDF5 fusion -- deliverable yang sebenarnya -- tidak
    bergantung padanya. plog.stage sudah mencatat baris FAILED lengkap
    dengan traceback, lalu pipeline lanjut ke FUSION."""
    for member in members:
        jc.dsmgr.upsert_scene_job_state(
            jc.job_id, member.pid, current_stage="PREVIEW", stage_status="RUNNING"
        )
    preview_files: list[str] = []
    try:
        # Satu render per level yang dihasilkan dataset ini -- sama dengan
        # jumlah stack fusion (ProcessingPlan.output_levels). Dataset yang
        # meminta sebuah sumber di RAW dan PROCESSED sekaligus mendapat dua
        # set PNG: satu dari bronze/, satu dari gold/, di folder terpisah.
        for level in jc.plan.output_levels():
            with jc.plog.stage(
                jc.dataset_id, primary.pid, module="MODULE10_PREVIEW", stage="PREVIEW",
                message=f"Rendering PNG preview level {level} dari tier "
                        f"{preview_tier_for_level(level).upper()}",
                acquisition_date=date_key, processing_level=level,
                s1_frames=len(members),
            ) as st:
                preview_result = generate_previews(
                    jc.dataset_id, jc.dataset_name, s1_date,
                    s1_scene_key=primary.pid,
                    # Raster S1 dioper eksplisit -- mosaik tanggal ini kalau
                    # frame-nya lebih dari satu, berkas frame tunggal kalau
                    # tidak. Tanpa ini module10 mencari sendiri lewat glob dan
                    # menemukan satu frame saja.
                    s1_files=mosaics.get(level),
                    processing_level=level,
                    options=jc.preview_options,
                )
                st.output(
                    output_dir=str(fm.get_preview_level_dir(
                        jc.dataset_id, jc.dataset_name, date_key, level
                    )),
                    grayscale_count=preview_result["counts"]["grayscale"],
                    colored_count=preview_result["counts"]["colored"],
                    composite_count=preview_result["counts"]["composite"],
                    skipped_count=preview_result["counts"]["skipped"],
                    file_size_mb=preview_result["total_size_mb"],
                )
            # Dicatat per level, bukan sekali setelah loop selesai: dataset dua
            # level yang gagal di level kedua sudah terlanjur menulis PNG level
            # pertama ke disk, dan pencatatan di ujung loop membuat berkas itu
            # hilang dari hitungan job.
            preview_files.extend(preview_result["files"])
        for member in members:
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, member.pid, current_stage="PREVIEW", stage_status="COMPLETED"
            )
    except Exception:
        # State scene sengaja tidak ditandai FAILED: scene-nya sendiri tidak
        # gagal, dan menandainya begitu akan membuatnya terhitung di
        # failed_count padahal FUSION masih akan berhasil.
        logger.exception(
            "[ORCH] PREVIEW gagal tanggal=%s pid=%s, lanjut ke FUSION",
            date_key, primary.pid,
        )
    return preview_files


def _run_fusion_for_date(
    jc: _JobContext, s1_date: date, members: list[_SceneResult],
    primary: _SceneResult, mosaics: dict[str, dict[str, str]],
) -> list[str]:
    """FUSION satu tanggal. Mengembalikan daftar berkas HDF5+JSON yang ditulis
    (kosong kalau fusi memang dilewati dataset ini)."""
    # Fusi butuh lebih dari satu sumber DAN sebuah strategi (DOCS/PIPELINE.md,
    # "Fusion Stage"). Dataset satu-sumber tidak punya apa-apa untuk
    # dipasangkan; menjalankannya cuma menghasilkan HDF5 berisi satu grup dan
    # enam lapisan NaN. required_tiers biasanya sudah menutup kasus ini lewat
    # skip_stages, tapi kondisinya diperiksa eksplisit di sini karena
    # required_tiers dataset bersifat global sementara jumlah sumber tidak.
    if "FUSION" in jc.skip_stages or not jc.plan.fusion_eligible(jc.fusion_strategy):
        logger.info(
            "[ORCH] pid=%s FUSION dilewati: sumber=%d strategi=%r skipped=%s",
            primary.pid, jc.plan.source_count, jc.fusion_strategy,
            "FUSION" in jc.skip_stages,
        )
        return []

    for member in members:
        jc.dsmgr.upsert_scene_job_state(
            jc.job_id, member.pid, current_stage="FUSION", stage_status="RUNNING"
        )

    # Lapisan yang akan ditulis ikut konfigurasi sumber, bukan konstanta
    # FUSION_LAYERS: dataset selektif menghasilkan HDF5 dengan group yang lebih
    # sedikit, dan mencatat daftar penuh di log membuat progress terlihat macet
    # di lapisan yang memang tidak pernah dibuat.
    planned_layers = sorted({
        layer
        for level in jc.plan.output_levels()
        for layer in fusion_layers_for(jc.plan.source_levels_for_run(level))
    })

    with jc.plog.stage(
        jc.dataset_id, primary.pid, module="MODULE9_FUSION", stage="FUSION",
        message="Fusing multi-modal data into H5", layers=planned_layers,
        processing_levels=list(jc.plan.output_levels()),
        s1_frames=len(members),
    ) as st:
        def _fusion_progress(layer_name: str, done: int, total: int) -> None:
            jc.plog.log_event(
                jc.dataset_id, primary.pid, "MODULE9_FUSION", "FUSION", "RUNNING",
                f"Fusing layer {layer_name} ({done}/{total})",
                {"progress_percent": round(done / total * 100, 1), "layer": layer_name},
            )

        runs = create_fusion_stack(
            jc.dataset_id, jc.dataset_name, s1_date, jc.bbox_tuple, primary.scene_id,
            db=jc.db, progress_cb=_fusion_progress,
            plan=jc.plan, fusion_strategy=jc.fusion_strategy,
            region_id=jc.region_id,
            # Raster S1 tanggal ini, sudah termasuk frame tetangga. scene_id di
            # atas tetap scene utama -- itu yang memegang baris DB-nya -- dan
            # member di bawah membuat lineage menyebut semua frame yang datanya
            # benar-benar masuk ke stack.
            s1_files_by_level=mosaics,
            s1_member_scene_ids=tuple(m.scene_id for m in members),
        )
        st.output(
            output_paths=[str(run.h5_path) for run in runs],
            processing_levels=[run.processing_level for run in runs],
            file_size_mb=round(
                sum(_file_size_mb(str(run.h5_path)) for run in runs
                    if run.h5_path.exists()), 3,
            ),
        )

    for member in members:
        jc.dsmgr.upsert_scene_job_state(
            jc.job_id, member.pid, current_stage="FUSION", stage_status="COMPLETED"
        )
    # Path diambil dari hasil create_fusion_stack, bukan disusun ulang di sini:
    # jumlah berkasnya (satu atau dua) dan nama berkasnya ditentukan level yang
    # dijalankan, dan menebaknya di dua tempat adalah cara kedua tempat itu
    # berbeda pendapat begitu salah satunya diubah.
    return [str(path) for run in runs for path in (run.h5_path, run.json_path)]


def _finalize_date(jc: _JobContext, members: list[_SceneResult]) -> None:
    """PREVIEW + FUSION untuk satu tanggal, sekali, di atas seluruh frame-nya.

    Dijalankan setelah SEMUA scene tanggal ini selesai diproses, dan tetap di
    thread pipeline yang sama supaya urutannya deterministik tanpa kunci.

    Berkas yang dihasilkan ditempelkan ke scene utama: cleanup bekerja per
    scene, dan berkas tanggal ini memang cuma boleh dihitung sekali.
    """
    if not members:
        return
    # Urut pid supaya "scene utama" tidak bergantung pada urutan selesainya
    # thread -- itu justru sumber penyakit yang sedang diperbaiki di sini.
    members = sorted(members, key=lambda m: m.pid)
    primary = members[0]
    s1_date = primary.acquisition_date
    date_key = s1_date.strftime("%Y%m%d")

    if jc.cancel_event.is_set():
        return
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return

    if len(members) > 1:
        logger.info(
            "[ORCH] tanggal=%s: %d frame Sentinel-1 disatukan sebelum PREVIEW/FUSION (%s)",
            date_key, len(members), ", ".join(m.pid for m in members),
        )

    mosaics = _mosaic_s1_by_level(jc, date_key, members)

    # --- PREVIEW ---------------------------------------------------------
    # Dijalankan sebelum FUSION dan sebelum _cleanup_scene_tiers: dataset yang
    # cuma meminta tier FUSION akan menghapus gold/ setelah scene selesai, jadi
    # ini satu-satunya jendela waktu ketika seluruh raster GOLD satu tanggal
    # masih ada di disk untuk dirender.
    if "PREVIEW" not in jc.skip_stages:
        preview_files = _run_preview_for_date(
            jc, date_key, s1_date, members, primary, mosaics
        )
        if preview_files:
            primary.produced_files["PREVIEW"] = preview_files
    # PREVIEW sengaja TIDAK masuk produced_tiers: dia bukan mata rantai lineage
    # RAW->FUSION, jadi compute_tiers_to_delete tidak boleh menghapusnya cuma
    # karena tidak disebut di required_tiers dataset.

    if jc.cancel_event.is_set():
        return
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return

    fused_files = _run_fusion_for_date(jc, s1_date, members, primary, mosaics)
    if fused_files:
        primary.produced_files[tn.FUSED] = fused_files
        if tn.FUSED not in primary.produced_tiers:
            primary.produced_tiers.append(tn.FUSED)
        # Layer referensi darat/laut dan air permanen. Ditaruh SESUDAH fusion
        # karena baru di situ grid dataset dipaku, dan keduanya harus lahir di
        # grid yang sama persis dengan stack-nya. Idempoten: tanggal kedua dan
        # seterusnya cuma memeriksa berkasnya sudah ada.
        _ensure_reference_layers(jc)


def _ensure_reference_layers(jc: _JobContext) -> None:
    """Layer referensi masks/ dataset ini, sekali saja, tanpa pernah menggagalkan job.

    Bukan mata rantai lineage RAW->FUSION: dataset tanpa layer ini tetap sah
    dan lengkap. Jadi kegagalannya dicatat dan ditelan, tidak dinaikkan —
    tabel garis pantai yang belum dimuat tidak boleh menjatuhkan job yang
    sudah berjam-jam berjalan.
    """
    try:
        from etl import folder_manager as _fm
        from etl.reference_layers import ensure_reference_layers

        root = _fm.get_dataset_root(jc.dataset_id, jc.dataset_name)
        status = ensure_reference_layers(
            jc.db, jc.dataset_id, root, jc.bbox_tuple,
        )
        if any(v == "written" for v in status.values()):
            logger.info("[ORCH] layer referensi dataset=%s: %s",
                        jc.dataset_id, status)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[ORCH] layer referensi dilewati: %s", exc)


def _fuse_days_without_s1(
    jc: _JobContext, s1_dates: set[date], scenes: list[dict] | None = None
) -> int:
    """Rakit HDF5 untuk tanggal fusi yang tidak punya scene Sentinel-1.

    Ini sumbu RAKIT dari etl/fusion_strategies untuk FULL_COVERAGE. Tanggal
    yang PUNYA scene S1 sudah difusikan di dalam _process_scene sebagai bagian
    dari pipeline scene; yang tersisa di sini hanyalah hari-hari yang tidak
    pernah masuk ke pipeline itu karena memang tidak ada scene-nya.

    Tidak dijalankan untuk CO_OCCURRENCE dan HYBRID: keduanya berjangkar pada
    S1, jadi himpunan tanggal fusinya memang persis tanggal scene dan daftar
    di bawah ini akan selalu kosong.

    Returns jumlah tanggal yang berhasil ditulis.
    """
    plan = jc.fusion_plan
    if plan is None or plan.requires_s1:
        return 0

    pending = [d for d in plan.fusion_dates if d not in s1_dates]
    if not pending:
        return 0

    logger.info(
        "[ORCH] job_id=%d strategi=%s: merakit %d tanggal fusi tanpa scene S1",
        jc.job_id, plan.strategy, len(pending),
    )

    # Tanggal scene -> product_identifier, untuk meminjam scene S1 dari hari
    # terdekat dalam toleransi. Tanpa ini, hari yang SEBENARNYA punya pasangan
    # S1 dalam jangkauan akan ditulis dengan group sentinel1/ berisi NaN --
    # persis kebalikan dari yang dijanjikan toleransi itu.
    pid_by_date: dict[date, str] = {}
    for meta_row in scenes or []:
        d = _scene_date(meta_row)
        if d is not None:
            pid_by_date.setdefault(d, meta_row["product_identifier"])

    written = 0
    for day in pending:
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        date_key = day.strftime("%Y%m%d")
        anchor = plan.anchor(day)
        anchor_pid = pid_by_date.get(anchor) if anchor else None
        anchor_scene = (
            jc.meta.get_scene_by_pid(anchor_pid) if anchor_pid else None
        )
        try:
            runs = create_fusion_stack(
                jc.dataset_id, jc.dataset_name, day, jc.bbox_tuple,
                # scene_id milik tanggal JANGKAR, bukan tanggal fusi: inilah
                # peminjaman S1 dalam toleransi. None kalau memang tidak ada
                # pasangan -- stack tetap ditulis dengan sentinel1/ NaN.
                anchor_scene["scene_id"] if anchor_scene else None,
                db=jc.db, plan=jc.plan, fusion_strategy=jc.fusion_strategy,
                region_id=jc.region_id, require_s1=False,
                s1_offset_days=plan.offset_days(day) if anchor_scene else None,
            )
            written += 1
            jc.plog.log_event(
                jc.dataset_id, date_key, "MODULE9_FUSION", "FUSION", "COMPLETED",
                f"Fusi {date_key} tanpa scene S1: {len(runs)} stack",
                {"date": day.isoformat(), "s1_offset_days": plan.offset_days(day),
                 "output_paths": [str(r.h5_path) for r in runs]},
            )
        except Exception as exc:
            logger.exception(
                "[ORCH] fusi tanpa-S1 gagal tanggal=%s job_id=%d", day, jc.job_id
            )
            _record_worker_failure(jc, date_key, "FUSION", exc)
            jc.meta.fail_open_jobs(exc)
            # Stack fusi adalah deliverable, bukan input opsional. Dulu
            # kegagalan di sini cuma dicatat ke log, sehingga try2 selesai
            # COMPLETED dengan 11 dari 16 hari gagal dan tanpa tombol retry.
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)

    return written


# Tier yang dihapus saat fusion_output_only aktif. FUSION dan PREVIEW tidak
# ikut: yang pertama adalah deliverable-nya, yang kedua tidak bisa dibangun
# ulang setelah tier sumbernya hilang.
_FUSION_ONLY_DISPOSABLE: tuple[str, ...] = (
    "raw", "aligned", "despeckled", "indices", "accumulated", "cog",
    # Nama lama ikut supaya dataset pra-migrasi tetap ikut dibersihkan.
    "bronze", "silver", "gold",
)


def _sweep_scratch(jc: _JobContext) -> int:
    """Buang _work/ di akhir job -- KECUALI folder scene yang gagal run ini.

    Sejak relayout, _work/ bukan cuma scratch kalibrasi: tier antara yang
    tidak punya laci (RAW = ZIP SAFE, SILVER = Lee pre-COG) juga mendarat di
    sana. Tanpa sapuan ini, "artefak antara dibuang setelah selesai" -- dasar
    keputusan menghilangkan laci ketiga -- tidak pernah benar-benar terjadi,
    dan ZIP SAFE ~1,6 GB per scene menumpuk diam-diam.

    Scene yang gagal (jc.failed_scene_keys, diisi _record_worker_failure)
    DIKECUALIKAN dari sapuan: folder scratch-nya menyimpan .zip.part yang
    sudah separuh terunduh, dan menghapusnya membuat retry berikutnya mulai
    dari nol walau resume sebenarnya mungkin (M1 mengecek ukuran .part yang
    ada). Sebelum pengecualian ini, retry SELALU mengunduh ulang dari awal
    karena sapuan ini sudah menghapus .part-nya duluan di akhir run yang
    gagal -- dampaknya sama seperti server menolak resume, tapi penyebabnya
    kode kita sendiri.

    Scene yang akhirnya berhasil di run manapun otomatis tersapu di run
    berikutnya (tidak lagi masuk failed_scene_keys begitu sukses), jadi tidak
    ada residu permanen: begitu dataset selesai tanpa scene gagal, sapuan
    run terakhir itu penuh seperti sebelumnya -- tidak ada pengecualian.

    Dijalankan untuk SEMUA job, bukan hanya fusion_output_only: isinya memang
    scratch menurut definisinya sendiri.

    Returns jumlah berkas yang dihapus.
    """
    scratch_root = jc.base_dir / fm.SCRATCH_DIRNAME
    if not scratch_root.is_dir():
        return 0

    with jc._failed_scene_keys_lock:
        protected_slugs = {fm.scratch_slug(key) for key in jc.failed_scene_keys}

    # Lewat prefix extended-length: berkas di _work/ bisa melewati MAX_PATH
    # Windows, dan rmtree biasa gagal WinError 3 lalu meninggalkan ZIP ~1,6 GB.
    long_root = fm.long_path(scratch_root)
    deleted = 0
    kept_dirs: list[str] = []
    for entry in long_root.iterdir():
        if entry.name in protected_slugs:
            kept_dirs.append(entry.name)
            continue
        entry_files = [entry] if entry.is_file() else [p for p in entry.rglob("*") if p.is_file()]
        try:
            if entry.is_dir():
                shutil.rmtree(entry)
            else:
                entry.unlink()
        except OSError as exc:
            logger.error("[ORCH] gagal menyapu %s: %s", entry, exc)
            continue
        deleted += len(entry_files)

    if not kept_dirs:
        # Tidak ada yang dipertahankan: _work/ itu sendiri ikut dibuang
        # persis seperti sapuan penuh yang lama, bukan cuma dikosongkan.
        try:
            long_root.rmdir()
        except OSError as exc:
            logger.error("[ORCH] gagal membuang %s: %s", scratch_root, exc)

    if deleted:
        logger.info(
            "[ORCH] job_id=%d _work/ disapu: %d berkas antara dihapus", jc.job_id, deleted,
        )
    if kept_dirs:
        logger.info(
            "[ORCH] job_id=%d _work/: %d folder scene gagal dipertahankan untuk resume: %s",
            jc.job_id, len(kept_dirs), sorted(kept_dirs),
        )
    return deleted


def _apply_fusion_output_only(jc: _JobContext, fusion_written: bool) -> int:
    """Hapus artefak per-satelit setelah stack fusi selesai ditulis.

    Ini BUKAN "lewati pemrosesan": fusi membaca raster GOLD/BRONZE tiap
    sumber, jadi bahannya harus dibangun lebih dulu. Yang dihemat adalah disk
    setelah bahan itu tidak diperlukan lagi.

    Dijalankan sebagai pass AKHIR, bukan per-scene lewat _cleanup_scene_tiers,
    karena FULL_COVERAGE merakit tanggal tanpa-S1 setelah seluruh pipeline
    scene selesai — menghapus per-scene akan mencabut bahannya sebelum
    perakitan itu sempat jalan.

    `fusion_written=False` membatalkan penghapusan sepenuhnya. Tanpa penjaga
    ini, job yang gagal di tahap FUSION akan menghapus satu-satunya output
    yang berhasil dibuatnya dan menyisakan dataset kosong.

    Returns jumlah berkas yang dihapus.
    """
    if not fusion_written:
        logger.warning(
            "[ORCH] job_id=%d fusion_output_only diminta tapi tidak ada stack "
            "fusi yang berhasil ditulis — artefak per-satelit DIPERTAHANKAN",
            jc.job_id,
        )
        return 0

    deleted = 0
    for tier in _FUSION_ONLY_DISPOSABLE:
        for tier_dir in fm.tier_dirs_under(jc.base_dir, tier):
            for path in sorted(tier_dir.rglob("*"), reverse=True):
                try:
                    if path.is_file():
                        path.unlink()
                        deleted += 1
                    elif path.is_dir():
                        path.rmdir()
                except OSError as exc:
                    logger.error("[ORCH] gagal hapus %s: %s", path, exc)
            try:
                tier_dir.rmdir()
            except OSError:
                pass

    logger.info(
        "[ORCH] job_id=%d fusion_output_only: %d berkas per-satelit dihapus",
        jc.job_id, deleted,
    )
    return deleted


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
        # ({date}/silver/sentinel1/{pid}, {date}/silver/modis, {date}/silver/gpm),
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
    with jc._failed_scene_keys_lock:
        jc.failed_scene_keys.add(pid)
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


# Jumlah scene S1 yang diunduh bersamaan. CDSE membatasi ~5 MB/s per koneksi
# (26_JAWA: 1,6 GB = 5,5 menit per scene walau jalur 84 Mbps), dan
# mengizinkan maksimal 4 unduhan paralel per akun. 3 menyisakan satu slot
# untuk sesi lain (browser, job kedua). 1 = perilaku lama (berurutan).
S1_PARALLEL_DOWNLOADS = max(1, min(4, int(os.getenv("S1_PARALLEL_DOWNLOADS", "2"))))


def _download_worker(jc: _JobContext, scenes: list[dict], download_queue: Queue) -> None:
    if jc.log_path:
        adopt_dataset_log_scope(jc.log_path)
    stop = threading.Event()  # cancel: scene yang belum mulai tidak diambil

    def _pool_init() -> None:
        if jc.log_path:
            adopt_dataset_log_scope(jc.log_path)

    def _task(scene_meta: dict) -> None:
        if stop.is_set():
            return
        try:
            if not _download_one(jc, scene_meta, download_queue):
                stop.set()
        except Exception:
            # _download_one sudah menangani kegagalan per scene; ini hanya
            # jaring terakhir supaya satu scene tidak menggugurkan scene lain.
            logger.exception(
                "[ORCH] download worker error pid=%s job_id=%d",
                scene_meta.get("product_identifier"), jc.job_id,
            )

    with ThreadPoolExecutor(
        max_workers=S1_PARALLEL_DOWNLOADS,
        thread_name_prefix="_download_worker",
        initializer=_pool_init,
    ) as pool:
        # Diserahkan menurut urutan `scenes`; worker mengambil berikutnya
        # begitu satu selesai. Pipeline menunggu per TANGGAL (lihat
        # expected_pids_by_date), jadi urutan selesai tidak memengaruhi hasil.
        for _ in pool.map(_task, scenes):
            pass
    download_queue.put(None)


def _download_one(jc: _JobContext, scene_meta: dict, download_queue: Queue) -> bool:
    """Unduh satu scene dan antrekan hasilnya. False = job dibatalkan, jangan
    mulai scene lain."""
    jc.pause_event.wait()
    if jc.cancel_event.is_set():
        return False
    pid = scene_meta["product_identifier"]
    state = jc.dsmgr.get_scene_job_state(jc.job_id, pid)
    if state and DatasetManager.scene_is_done(state.get("current_stage"), state.get("stage_status")):
        return True
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
                reuse_root=fm.DATA_ROOT,
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
        return False
    except Exception as exc:
        if jc.cancel_event.is_set():
            logger.info("[ORCH] download dibatalkan pid=%s job_id=%d", pid, jc.job_id)
            return False
        logger.exception("[ORCH] download gagal pid=%s job_id=%d", pid, jc.job_id)
        _record_worker_failure(jc, pid, "DOWNLOAD", exc)
        jc.dsmgr.upsert_scene_job_state(
            jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
        )
        jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
    return True


def _flush_date(
    jc: _JobContext, date_key: str, members: list[_SceneResult], cleanup_queue: Queue
) -> None:
    """Selesaikan satu tanggal: PREVIEW + FUSION sekali, lalu antre cleanup.

    Cleanup baru diantre SETELAH finalisasi. Urutan itu wajib: untuk dataset
    fusion_output_only, _cleanup_scene_tiers menghapus gold/, dan mengantrenya
    lebih dulu berarti raster yang mau dirender/difusikan bisa lenyap di
    tengah jalan.
    """
    try:
        _finalize_date(jc, members)
    except Exception as exc:
        # Tanggal gagal difinalisasi tidak boleh menahan cleanup scene-nya:
        # berkas tier sumbernya sudah ada di disk dan tetap harus dibereskan
        # menurut required_tiers.
        logger.exception(
            "[ORCH] finalisasi tanggal=%s gagal job_id=%d", date_key, jc.job_id
        )
        _record_worker_failure(jc, date_key, "SCENE_PIPELINE", exc)
        jc.meta.fail_open_jobs(exc)
        jc.dsmgr.increment_job_counters(jc.job_id, failed=1)

    for member in sorted(members, key=lambda m: m.pid):
        storage_breakdown = {
            tier: round(sum(_file_size_mb(p) for p in paths if Path(p).exists()), 3)
            for tier, paths in member.produced_files.items()
        }
        jc.plog.log_event(
            jc.dataset_id, member.pid, "ORCHESTRATOR", "SCENE_PIPELINE", "COMPLETED",
            f"Scene processed successfully (tiers: {', '.join(member.produced_tiers)})",
            {
                "duration_seconds": round(member.duration_seconds, 3),
                "produced_tiers": member.produced_tiers,
                "storage_breakdown_mb": storage_breakdown,
                "total_size_mb": round(sum(storage_breakdown.values()), 3),
            },
        )
        cleanup_queue.put(
            (member.pid, member.scene_id, member.produced_tiers, member.produced_files)
        )


def _pipeline_worker(jc: _JobContext, download_queue: Queue, cleanup_queue: Queue) -> None:
    if jc.log_path:
        adopt_dataset_log_scope(jc.log_path)
    # Scene yang sudah diproses tapi tanggalnya belum lengkap. Semuanya hidup
    # di thread ini saja, jadi tidak perlu kunci: satu-satunya thread yang
    # memfinalisasi tanggal adalah thread yang memprosesnya.
    pending: dict[str, list[_SceneResult]] = {}
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
            result = _process_scene(jc, scene_meta, dl_result)
            result.duration_seconds = time.monotonic() - t0
            jc.dsmgr.increment_job_counters(jc.job_id, processed=1)
            date_key = result.acquisition_date.strftime("%Y%m%d")
            pending.setdefault(date_key, []).append(result)
        except Exception as exc:
            logger.exception("[ORCH] pipeline gagal pid=%s job_id=%d", pid, jc.job_id)
            _record_worker_failure(jc, pid, "SCENE_PIPELINE", exc)
            # Tahap yang sudah start_job tapi melempar sebelum complete_job.
            jc.meta.fail_open_jobs(exc)
            jc.dsmgr.upsert_scene_job_state(
                jc.job_id, pid, stage_status="FAILED", last_error=str(exc)[:2000], completed_at=_now()
            )
            jc.dsmgr.increment_job_counters(jc.job_id, failed=1)
        finally:
            _pipeline_semaphore.release()

        # Tanggal yang seluruh scene-nya sudah lewat sini difinalisasi
        # sekarang, supaya PREVIEW/FUSION tidak menunggu tanggal lain selesai
        # diunduh. Sisanya disapu setelah antrean habis -- itu yang menangani
        # scene yang gagal sebelum sampai ke thread ini (unduhan gagal), yang
        # membuat tanggalnya tidak akan pernah "lengkap".
        for ready in [
            d for d, members in pending.items()
            if jc.expected_pids_by_date.get(d)
            and {m.pid for m in members} >= jc.expected_pids_by_date[d]
        ]:
            _flush_date(jc, ready, pending.pop(ready), cleanup_queue)

    if jc.cancel_event.is_set():
        # Job dibatalkan: yang tersisa sengaja tidak difinalisasi. Menulis
        # preview dan HDF5 untuk tanggal yang scene-nya baru separuh diproses
        # justru menghasilkan deliverable yang salah diam-diam.
        if pending:
            logger.info(
                "[ORCH] job_id=%d dibatalkan: %d tanggal tidak difinalisasi",
                jc.job_id, len(pending),
            )
    else:
        for date_key in sorted(pending):
            _flush_date(jc, date_key, pending[date_key], cleanup_queue)
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


def _drop_dates_barely_covering_aoi(
    scenes: list[dict], bbox_wkt: str, job_id: int,
    min_fraction: float = MIN_S1_AOI_COVERAGE,
) -> list[dict]:
    """Buang tanggal S1 yang gabungan footprint-nya menutup kurang dari
    `min_fraction` AOI.

    Diputuskan per TANGGAL, bukan per scene: satu pass bisa terpotong jadi
    dua frame (24_try8, 11 Jan: 111502 + 111532) yang masing-masing cuma
    menutup sebagian AOI tapi bersama-sama menutup hampir semuanya -- frame
    seperti itu tidak boleh terbuang. Scene tanpa footprint yang bisa dibaca
    tidak pernah dibuang: tanpa bukti, lebih aman mengunduh daripada diam-diam
    kehilangan tanggal."""
    by_date: dict[date | None, list[dict]] = {}
    for scene in scenes:
        by_date.setdefault(_scene_date(scene), []).append(scene)

    kept: list[dict] = []
    for day, members in by_date.items():
        footprints = [m.get("footprint_wkt") for m in members]
        coverage = (
            aoi_coverage([fp for fp in footprints if fp], bbox_wkt)
            if day is not None and all(footprints) else None
        )
        if coverage is not None and coverage < min_fraction:
            logger.warning(
                "[ORCH] job_id=%d tanggal %s dilewati: %d frame S1 cuma menutup "
                "%.1f%% AOI (< %.0f%%): %s",
                job_id, day, len(members), coverage * 100, min_fraction * 100,
                [m["product_identifier"] for m in members],
            )
            continue
        kept.extend(members)
    return kept


def _scene_date(scene_meta: dict) -> date | None:
    """Tanggal akuisisi satu scene S1 hasil discover_scenes, atau None.

    Dipakai menyusun kedua sumbu strategi fusi: yang dihitung adalah
    tanggalnya, bukan jamnya — satu tanggal bisa punya dua scene (orbit
    menaik + menurun) yang berbagi aux MODIS/GPM yang sama.

    Kalau `acquisition_datetime` tidak ada, tanggalnya diambil dari
    product_identifier, yang untuk Sentinel-1 selalu memuat stempel waktu
    akuisisi. Fallback ini bukan kemewahan: himpunan tanggal S1 yang kosong
    membuat plan_fusion mengira dataset tidak punya scene sama sekali,
    sehingga SETIAP tanggal diperlakukan sebagai tanggal tanpa-S1 dan
    difusikan ulang — menimpa stack same-day yang sudah benar dengan stack
    berisi NaN. Gagal diam yang mahal, jadi lebih baik ditebak dari PID.
    """
    acq = scene_meta.get("acquisition_datetime")
    if acq is not None and hasattr(acq, "date"):
        return acq.date()

    m = re.search(r"(\d{8})", str(scene_meta.get("product_identifier") or ""))
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            pass
    logger.warning(
        "[ORCH] scene tanpa tanggal yang bisa dibaca: %r",
        scene_meta.get("product_identifier"),
    )
    return None


def _ingest_aux_days(
    jc: _JobContext, days: list[date], count_as_scenes: bool = True
) -> tuple[int, int]:
    """Unduh + proses MODIS/GPM untuk sekumpulan tanggal. Returns (ok, gagal).

    Ini sumbu UNDUH dari etl/fusion_strategies: dipakai dua pemanggil dengan
    daftar tanggal yang berbeda — `_run_aux_only` (dataset tanpa S1, tiap hari
    dalam rentang) dan jalur S1 (tanggal tambahan yang diminta FULL_COVERAGE /
    HYBRID di luar tanggal scene S1).

    Tiap hari berdiri sendiri: satu hari yang gagal (granule NASA belum terbit)
    dicatat lalu dilewati, tidak menjatuhkan hari lain. Pemanggil bertanggung
    jawab membuka `dataset_log_file` lebih dulu.

    Idempotent lewat ensure_aux_inputs_for_date: module7/module8 melewati file
    yang sudah ada. Tapi yang dilewati cuma UNDUHANNYA — reproject/crop/COG
    tetap dijalankan ulang (~4 detik per granule), jadi memanggil ulang satu
    rentang penuh TIDAK murah. Karena itu tanggal yang sudah pernah selesai
    dilewati lebih awal lewat plog.completed_aux_dates.

    `count_as_scenes=False` untuk jalur S1: di sana unit counter job/dataset
    adalah SCENE S1 (total_scenes = jumlah scene), jadi menghitung hari aux
    tambahan di counter yang sama menghasilkan completed_scenes=16 untuk
    total_scenes=1 (try1/try2), dan satu hari aux yang gagal akan membuat job
    FAILED padahal fusi mentoleransi aux yang hilang dengan NaN.
    """
    ok_days = 0
    failed_days = 0

    def _count(**kwargs) -> None:
        if count_as_scenes:
            jc.dsmgr.increment_job_counters(jc.job_id, **kwargs)

    already_done = jc.plog.completed_aux_dates(
        jc.dataset_id, [d.strftime("%Y%m%d") for d in days]
    )
    if already_done:
        logger.info(
            "[ORCH] job_id=%d %d/%d tanggal aux sudah selesai di run sebelumnya, dilewati",
            jc.job_id, len(already_done), len(days),
        )

    for day in days:
        jc.pause_event.wait()
        if jc.cancel_event.is_set():
            break
        date_key = day.strftime("%Y%m%d")
        if date_key in already_done:
            # Dihitung ok persis seperti kalau diproses ulang dan berhasil,
            # supaya akuntansi job tidak berubah oleh optimasi ini.
            ok_days += 1
            _count(processed=1)
            continue
        try:
            produced, missing_sources = ensure_aux_inputs_for_date(
                jc.db, jc.dataset_id, jc.dataset_name, jc.region_id,
                jc.bbox_tuple, day, plog=jc.plog, plan=jc.plan,
            )
            written = sum(len(paths) for paths in produced.values())
            # `written > 0` saja tidak berarti tanggalnya tuntas: satu sumber
            # bisa berhasil sementara yang lain tidak menghasilkan apa pun.
            # Hanya tanggal yang SEMUA sumbernya berhasil yang boleh dilewati
            # run berikutnya -- kalau tidak, kekurangannya permanen.
            aux_complete = bool(written) and not missing_sources
            if written:
                ok_days += 1
                _count(processed=1)
            else:
                failed_days += 1
                _count(failed=1)
            jc.plog.log_event(
                jc.dataset_id, date_key, "ORCHESTRATOR", "SCENE_PIPELINE",
                "COMPLETED" if written else "FAILED",
                f"Aux {date_key}: {written} berkas ditulis",
                {"date": day.isoformat(), "files_written": written,
                 "aux_complete": aux_complete,
                 "missing_sources": sorted(missing_sources),
                 "tiers": {tier: len(paths) for tier, paths in produced.items()}},
            )
        except Exception as exc:
            failed_days += 1
            logger.exception("[ORCH] aux gagal tanggal=%s job_id=%d", day, jc.job_id)
            _record_worker_failure(jc, date_key, "SCENE_PIPELINE", exc)
            jc.meta.fail_open_jobs(exc)
            _count(failed=1)
    return ok_days, failed_days


def _run_aux_only(jc: _JobContext, date_from: date, date_to: date) -> None:
    """Jalankan dataset yang TIDAK mengkonfigurasi Sentinel-1.

    Tanpa S1 tidak ada scene yang bisa ditemukan CDSE dan tidak ada jangkar
    tanggal, jadi MODIS/GPM diproses satu hari per hari sepanjang rentang
    dataset. Tiap hari berdiri sendiri: satu hari yang gagal (granule NASA
    belum terbit) dicatat lalu dilewati, tidak menjatuhkan hari lain.

    FUSION tidak dijalankan di jalur ini: create_fusion_stack memakai raster
    GOLD Sentinel-1 sebagai grid referensi dan melempar tanpa itu. Fusi
    tanpa-S1 (reproyeksi ke sumber ber-extent terbesar, DOCS/PIPELINE.md) belum
    diimplementasikan.
    """
    total_days = (date_to - date_from).days + 1
    jc.dsmgr.set_job_status(jc.job_id, "DOWNLOADING")
    logger.info(
        "[ORCH] job_id=%d mode aux-only (tanpa Sentinel-1) sumber=%s hari=%d",
        jc.job_id, list(jc.plan.aux_sources()), total_days,
    )

    days = [date_from + timedelta(days=i) for i in range(total_days)]
    with dataset_log_file(jc.dataset_id, jc.dataset_name) as run_log_path:
        jc.log_path = run_log_path
        ok_days, failed_days = _ingest_aux_days(jc, days)

    total_size = _dir_size_bytes(jc.base_dir)
    jc.dsmgr.set_dataset_size(jc.dataset_id, total_size)

    if jc.cancel_event.is_set():
        jc.dsmgr.set_job_status(jc.job_id, "CANCELLED", completed_at=_now())
        _write_dataset_metadata(jc.dsmgr, jc.dataset_id, total_size)
        logger.info("[ORCH] job_id=%d dibatalkan", jc.job_id)
        return

    final_status = "FAILED" if ok_days == 0 else "COMPLETED"
    jc.dsmgr.set_job_status(jc.job_id, final_status, completed_at=_now())
    _write_dataset_metadata(jc.dsmgr, jc.dataset_id, total_size)
    logger.info(
        "[ORCH] job_id=%d aux-only selesai status=%s hari_ok=%d hari_gagal=%d",
        jc.job_id, final_status, ok_days, failed_days,
    )


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
    fusion_strategy = dataset.get("fusion_strategy")

    # --- rencana per-satelit ------------------------------------------------
    plan = load_processing_plan(db, dataset_id)
    if not plan.sources:
        logger.error(
            "[ORCH] job_id=%d dataset_id=%d tidak punya sumber terkonfigurasi",
            job_id, dataset_id,
        )
        dsmgr.set_job_status(job_id, "FAILED", completed_at=_now())
        _write_dataset_metadata(dsmgr, dataset_id)
        return

    # Dua sumber pembatas tahap, di-union:
    #   1. required_tiers dataset (retensi tier yang diminta user);
    #   2. level Sentinel-1 di dataset_source_config (RAW berhenti di CROP).
    # Keduanya perlu: yang pertama bisa memangkas lebih dalam dari yang kedua
    # (dataset yang cuma menyimpan BRONZE), yang kedua memangkas walau
    # required_tiers memuat GOLD karena sumber LAIN yang PROCESSED.
    skip_stages = compute_skip_stages(required_tiers)
    s1_plan = plan.get(S1_SOURCE_NAME)
    if s1_plan is not None:
        skip_stages |= s1_plan.s1_skip_stages()
    logger.info(
        "[ORCH] job_id=%d rencana sumber=%s strategi_fusi=%r skip=%s",
        job_id, plan.summary(), fusion_strategy, sorted(skip_stages),
    )

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
    #   3. user mencentang "Buat Preview" tapi tidak memilih satu varian pun.
    # preview_options KOSONG berarti persis itu (DOCS/ARCHITECTURE.md); dibedakan
    # dari NULL, yang berarti "tidak dinyatakan" dan tetap merender ketiganya.
    # Migrasi 018 mem-backfill NULL jadi ketiga varian, jadi kolomnya sekarang
    # selalu menyatakan pilihan yang sebenarnya.
    elif dataset.get("preview_options") == []:
        skip_stages.add("PREVIEW")
        logger.info(
            "[ORCH] job_id=%d PREVIEW dilewati: preview_options kosong", job_id,
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
        plan=plan, fusion_strategy=fusion_strategy,
        # Diisi setelah discovery: rencananya butuh tanggal scene S1 yang nyata.
        fusion_plan=None,
        preview_options=dataset.get("preview_options"),
    )

    dsmgr.set_job_status(job_id, "PREPARING", started_at=_now())
    # Counter & last_error per eksekusi, bukan akumulasi semua retry/resume --
    # status akhir di bawah dibaca dari failed_count.
    dsmgr.begin_job_run(job_id)

    date_from = (
        datetime.combine(date_range_start, datetime.min.time(), tzinfo=timezone.utc)
        if date_range_start else _now() - timedelta(days=30)
    )
    date_to = (
        datetime.combine(date_range_end, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
        if date_range_end else _now()
    )

    # Dataset tanpa Sentinel-1 tidak punya scene untuk ditemukan: seluruh
    # jalur discovery/download/pipeline di bawah ini berputar di sekitar scene
    # S1. Sumber aux-nya diproses per hari sepanjang rentang tanggal.
    if s1_plan is None:
        _run_aux_only(jc, date_from.date(), (date_to - timedelta(days=1)).date())
        return

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

    scenes = _drop_dates_barely_covering_aoi(scenes, bbox_wkt, job_id)
    if not scenes:
        logger.info("[ORCH] semua tanggal S1 cuma menyerempet AOI job_id=%d", job_id)
        dsmgr.set_job_status(job_id, "COMPLETED", completed_at=_now())
        _write_dataset_metadata(dsmgr, dataset_id)
        return

    dsmgr.create_scene_job_states(job_id, [s["product_identifier"] for s in scenes])
    dsmgr.set_job_status(job_id, "DOWNLOADING")

    # --- sumbu unduh strategi fusi -----------------------------------------
    # Sampai sini pipeline selalu berjangkar pada scene S1: MODIS/GPM cuma
    # diambil untuk tanggal yang punya scene, yang secara efektif memaksa
    # CO_OCCURRENCE apa pun strategi yang dipilih user. FULL_COVERAGE dan
    # HYBRID butuh aux harian, jadi tanggal di luar tanggal scene diunduh di
    # sini sebagai pra-lintasan sebelum pipeline scene berjalan.
    #
    # Pra-lintasan, bukan disisipkan ke dalam worker: ensure_aux_inputs_for_date
    # idempotent dan melewati file yang sudah ada, jadi panggilan per-scene di
    # _process_scene nanti otomatis jadi no-op untuk tanggal yang sudah terisi
    # di sini. Dengan begitu jalur scene tidak perlu tahu apa-apa soal strategi.
    s1_dates = {d for d in (_scene_date(s) for s in scenes) if d is not None}
    # Peta tanggal -> scene, dipakai _pipeline_worker untuk tahu kapan sebuah
    # tanggal sudah lengkap. Disusun dari hasil discover_scenes, bukan dari
    # scene yang sudah selesai, karena yang perlu diketahui justru berapa yang
    # masih ditunggu.
    for scene_meta in scenes:
        scene_day = _scene_date(scene_meta)
        if scene_day is None:
            continue
        jc.expected_pids_by_date.setdefault(
            scene_day.strftime("%Y%m%d"), set()
        ).add(scene_meta["product_identifier"])
    # Syaratnya sama persis dengan gerbang FUSION di _process_scene. Diperiksa
    # di sini juga supaya dataset yang tidak memfusikan apa pun tidak ikut
    # membayar unduhan aux harian: sumbu unduh cuma ada untuk melayani fusi.
    if "FUSION" not in skip_stages and jc.plan.fusion_eligible(jc.fusion_strategy):
        jc.fusion_plan = plan_fusion(
            jc.fusion_strategy, s1_dates,
            date_from.date(), (date_to - timedelta(days=1)).date(),
            tolerance_days=dataset.get("s1_match_tolerance_days")
            or DEFAULT_S1_MATCH_TOLERANCE_DAYS,
        )

    if jc.fusion_plan is not None:
        extra_days = [d for d in jc.fusion_plan.aux_dates if d not in s1_dates]
        if extra_days:
            logger.info(
                "[ORCH] job_id=%d strategi=%s: %d tanggal aux tambahan di luar "
                "%d tanggal scene S1",
                job_id, jc.fusion_plan.strategy, len(extra_days), len(s1_dates),
            )
            with dataset_log_file(dataset_id, dataset["name"]) as aux_log_path:
                jc.log_path = aux_log_path
                _ingest_aux_days(jc, extra_days, count_as_scenes=False)

    download_queue: Queue = Queue(maxsize=3)
    cleanup_queue: Queue = Queue()

    with dataset_log_file(dataset_id, dataset["name"]) as run_log_path:
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

        # Sumbu rakit dijalankan SETELAH pipeline scene: hari tanpa S1 tetap
        # membaca MODIS/GPM dari disk, dan pra-lintasan unduh di atas sudah
        # memastikan berkasnya ada. Sekuensial, bukan di dalam worker, karena
        # tidak ada scene yang jadi unit kerjanya.
        extra_fused = _fuse_days_without_s1(jc, s1_dates, scenes)

        # Artefak antara dibuang lebih dulu, sebelum keputusan penghematan
        # lain: isinya scratch menurut definisinya sendiri, jadi tidak
        # bergantung pada konfigurasi apa pun.
        _sweep_scratch(jc)

        # Penghematan disk dijalankan paling akhir, setelah SEMUA stack fusi
        # (baik dari jalur scene maupun jalur tanpa-S1) selesai ditulis.
        if dataset.get("fusion_output_only"):
            _apply_fusion_output_only(
                jc,
                fusion_written=bool(
                    extra_fused
                    or list(jc.base_dir.glob("**/fusion/**/*.h5"))
                ),
            )

    total_size = _dir_size_bytes(base_dir)
    dsmgr.set_dataset_size(dataset_id, total_size)

    # metadata.json ditulis SETELAH status akhir diset di setiap jalur keluar.
    # Dulu ditulis sebelumnya, sehingga ringkasan di disk selalu membawa status
    # "DOWNLOADING" walau database sudah COMPLETED (try1/try2/try3).
    if cancel_event.is_set():
        dsmgr.set_job_status(job_id, "CANCELLED", completed_at=_now())
        _write_dataset_metadata(dsmgr, dataset_id, total_size)
        logger.info("[ORCH] job_id=%d dibatalkan", job_id)
        return

    with db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        still_paused = job.status == "PAUSED" if job else False
        failed_count = job.failed_count if job else 0

    if still_paused:
        _write_dataset_metadata(dsmgr, dataset_id, total_size)
        logger.info("[ORCH] job_id=%d berhenti dalam status PAUSED", job_id)
        return

    # failed_count is incremented per-scene on stage failure (see _download_worker /
    # _pipeline_worker above) but was never consulted here, so a job with failed
    # scenes still ended up "COMPLETED" — hiding the FAILED-only retry button/endpoint
    # (web/index.html canRetry, api/routes/pipeline.py trigger) from the scenes that
    # actually need a retry.
    final_status = "FAILED" if failed_count > 0 else "COMPLETED"
    dsmgr.set_job_status(job_id, final_status, completed_at=_now())
    _write_dataset_metadata(dsmgr, dataset_id, total_size)
    logger.info("[ORCH] job_id=%d selesai status=%s failed_count=%d", job_id, final_status, failed_count)
