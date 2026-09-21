# etl/refusion.py
"""
Rakit ulang PREVIEW + FUSION satu tanggal dari produk yang SUDAH ada di disk.

MASALAH YANG DISELESAIKAN
Stack fusion bisa jadi usang tanpa scene-nya ikut usang. Kasus yang melahirkan
modul ini: keempat strip Jawa dipakukan ke satu grid bersama (D19) setelah
sebagian stack-nya terlanjur dirakit, sehingga tiga stack tertinggal di grid
lama dan tidak bisa ditempel dengan tetangganya.

Jalur yang ada tidak bisa memperbaikinya. `run_dataset_job` melewati scene yang
`scene_is_done` -- CLEANUP/COMPLETED -- sehingga job yang diulang selesai dalam
sepersekian detik tanpa menyentuh fusion. Mereset status scene memang memaksa
pipeline mengulang, tapi mengulang dari DOWNLOAD: sejak D15 ZIP SAFE tidak lagi
disimpan, jadi harganya ~1,7 GB unduhan per scene untuk pekerjaan yang
sebenarnya tidak butuh satu byte pun dari jaringan. Raster S1 yang dibutuhkan
fusi sudah ada di `sentinel-1/PROCESSED/`.

KENAPA MEMAKAI ULANG `_finalize_date`, BUKAN MEMANGGIL `create_fusion_stack`
Dua dari tiga tanggal yang perlu diperbaiki tertutup LEBIH DARI SATU frame S1
(JAWA_B 4 Desember: tiga frame). Memanggil `create_fusion_stack` langsung
dengan satu scene_id akan menghasilkan stack yang cuma memuat satu frame --
persis penyakit yang `etl/s1_mosaic.py` dibuat untuk menyembuhkan. Urutan
mosaik -> preview -> fusi -> layer referensi hidup di `_finalize_date`, dan
menirunya di sini berarti menciptakan salinan kedua yang bisa menyimpang diam-
diam begitu salah satunya diubah. Jadi modul ini menyusun konteks yang sama dan
memanggil fungsi yang sama.

BATASAN
Hanya untuk tanggal yang scene S1-nya sudah selesai diproses dan raster
PROCESSED-nya masih ada. Tanggal yang rasternya sudah tersapu memang harus
lewat pipeline penuh, dan modul ini menolaknya alih-alih menghasilkan stack
separuh.
"""
from __future__ import annotations

import logging
import threading
from collections import defaultdict
from datetime import date as date_type
from pathlib import Path

logger = logging.getLogger(__name__)

MODULE = "REFUSION"

# Band yang dikenali dari nama berkas COG S1 (`..._calibrated_VV_lee.tif`).
_BANDS = ("VV", "VH")


def _s1_cogs_by_pid(dataset_id: int, dataset_name: str) -> dict[str, dict[str, str]]:
    """Petakan {product_identifier_prefix: {band: path}} dari raster di disk.

    Nama berkas COG memuat potongan pid, bukan pid utuh
    (`S1A_IW_GRDH_1SDV_20251204T222544_20_calibrated_VV_lee.tif`), jadi
    pencocokannya lewat awalan dan bukan kesamaan persis.
    """
    from etl import folder_manager as fm

    root = fm.get_dataset_root(dataset_id, dataset_name)
    proc = root / "sentinel-1" / "PROCESSED"
    out: dict[str, dict[str, str]] = defaultdict(dict)
    if not proc.is_dir():
        return out
    for path in sorted(proc.glob("*.tif")):
        for band in _BANDS:
            if f"_{band}_" in path.name:
                stem = path.name.split("_calibrated_")[0]
                out[stem][band] = str(path)
                break
    return out


def _match_cogs(pid: str, cogs: dict[str, dict[str, str]]) -> dict[str, str]:
    for stem, bands in cogs.items():
        if pid.startswith(stem):
            return bands
    return {}


def build_job_context(db, job_id: int):
    """Susun `_JobContext` seperti `run_dataset_job`, tanpa menjalankan job.

    Event pause/cancel dibuat lokal dan sudah "hijau": tidak ada job sungguhan
    yang bisa mem-pause perbaikan ini, dan konteks yang pause_event-nya belum
    di-set akan menggantung di `pause_event.wait()` pertama.
    """
    from sqlalchemy import select

    from etl.database_client import DatasetJob
    from etl.dataset_manager import DatasetManager
    from etl.lineage_tracker import LineageTracker
    from etl.metadata_manager import MetadataManager
    from etl.module5_orchestrator import (
        S1_SOURCE_NAME,
        _JobContext,
        compute_skip_stages,
    )
    from etl.pipeline_logger import PipelineLogger
    from etl.processing_plan import load_processing_plan
    from etl import folder_manager as fm

    with db.session() as sess:
        job = sess.scalar(select(DatasetJob).where(DatasetJob.job_id == job_id))
        if job is None:
            raise ValueError(f"job_id={job_id} tidak ditemukan")
        dataset_id = job.dataset_id

    dsmgr = DatasetManager(db)
    dataset = dsmgr.get_dataset(dataset_id)
    if dataset is None:
        raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")

    plan = load_processing_plan(db, dataset_id)
    required_tiers = dataset["required_tiers"]
    skip_stages = compute_skip_stages(required_tiers)
    s1_plan = plan.get(S1_SOURCE_NAME)
    if s1_plan is not None:
        skip_stages |= s1_plan.s1_skip_stages()

    from shapely import wkt as shapely_wkt

    bbox_tuple = shapely_wkt.loads(dataset["bbox_wkt"]).bounds

    pause = threading.Event()
    pause.set()

    return _JobContext(
        db=db,
        dsmgr=dsmgr,
        meta=MetadataManager(db),
        lineage=LineageTracker(db),
        plog=PipelineLogger(db),
        job_id=job_id,
        dataset_id=dataset_id,
        dataset_name=dataset["name"],
        region_id=dataset["region_id"],
        bbox_wkt=dataset["bbox_wkt"],
        bbox_tuple=bbox_tuple,
        required_tiers=required_tiers,
        skip_stages=skip_stages,
        min_quality_score=float(
            (dataset["quality_settings"] or {}).get("min_quality_score") or 60.0
        ),
        base_dir=fm.get_dataset_root(dataset_id, dataset["name"]),
        plan=plan,
        fusion_strategy=dataset.get("fusion_strategy"),
        fusion_plan=None,
        preview_options=dataset.get("preview_options"),
        pause_event=pause,
        cancel_event=threading.Event(),
    )


def scene_results_for_date(db, job_id: int, jc, date_key: str) -> list:
    """`_SceneResult` tiap frame S1 tanggal itu, dirakit dari disk + database.

    `produced_tiers`/`produced_files` sengaja dikosongkan: keduanya dipakai
    pipeline untuk memutuskan tier mana yang boleh dihapus saat cleanup, dan
    perbaikan ini tidak boleh menghapus apa pun.
    """
    from sqlalchemy import select

    from etl.database_client import SceneJobState
    from etl.module5_orchestrator import _SceneResult

    cogs = _s1_cogs_by_pid(jc.dataset_id, jc.dataset_name)

    with db.session() as sess:
        rows = sess.execute(
            select(SceneJobState.product_identifier, SceneJobState.scene_id)
            .where(SceneJobState.job_id == job_id)
        ).all()

    out = []
    for pid, scene_id in rows:
        if date_key not in pid:
            continue
        bands = _match_cogs(pid, cogs)
        if not bands:
            logger.warning(
                "[%s] %s: raster PROCESSED tidak ditemukan, frame dilewati",
                MODULE, pid,
            )
            continue
        out.append(
            _SceneResult(
                pid=pid,
                scene_id=scene_id,
                acquisition_date=date_type(
                    int(date_key[:4]), int(date_key[4:6]), int(date_key[6:8])
                ),
                produced_tiers=[],
                produced_files={},
                s1_files_by_level={"PROCESSED": bands},
            )
        )
    return sorted(out, key=lambda m: m.pid)


def refuse_date(db, job_id: int, date_key: str) -> bool:
    """Rakit ulang satu tanggal. True kalau dikerjakan.

    Tidak menghapus stack lama lebih dulu: `_finalize_date` menulis ke nama
    berkas yang sama, jadi stack barulah yang menimpa. Kalau perakitannya
    gagal di tengah, yang tertinggal berkas separuh -- karena itu pemanggil
    disarankan menyingkirkan stack lama ke nama lain dulu, bukan menghapusnya,
    sampai hasil barunya terverifikasi.
    """
    from etl.module5_orchestrator import _finalize_date

    members = scene_results_for_date(db, job_id, jc := build_job_context(db, job_id),
                                     date_key)
    if not members:
        logger.error(
            "[%s] job %s tanggal %s: tidak ada frame yang bisa dipakai",
            MODULE, job_id, date_key,
        )
        return False

    logger.info(
        "[%s] job %s tanggal %s: merakit ulang dari %d frame yang sudah ada",
        MODULE, job_id, date_key, len(members),
    )
    _finalize_date(jc, members)
    return True
