# etl/live_scheduler.py
from __future__ import annotations
import logging
from datetime import date, datetime, timedelta, timezone
from sqlalchemy import select
from etl.constants import (
    GPM_PRODUCT_SHORT_NAME,
    GPM_SOURCE,
    GPM_TILE_ID,
)
from etl.database_client import DatabaseClient, Dataset, DatasetJob, LiveDatasetSource
from etl.dataset_manager import DatasetManager
from etl.metadata_manager import MetadataManager
from etl.module1_download import discover_scenes
from etl.module5_orchestrator import run_dataset_job
from etl.module9_fusion import (
    ensure_gpm_inputs_for_date,
    ensure_modis_inputs_for_date,
)
from etl.pipeline_logger import PipelineLogger, dataset_log_file

logger = logging.getLogger(__name__)

_CHECK_HOUR = 2
_CHECK_MINUTE = 0
_TIMEZONE = "Asia/Jakarta"
_AREA_CHECK_HOURS = "1,7,13,19"
_DEFAULT_LOOKBACK_DAYS = 7


def _now() -> datetime:
    return datetime.now(timezone.utc)


class LiveScheduler:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db
        self._dsmgr = DatasetManager(db)
        self._plog = PipelineLogger(db)
        self._scheduler = None

    def start(self) -> None:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger

        self._scheduler = BackgroundScheduler(timezone=_TIMEZONE)
        # Live Monitoring (LIVE_MONITORING.md): siklus per Daerah Live. Cek
        # beberapa kali sehari karena scene S1 terbit di katalog beberapa jam
        # setelah akuisisi; siklus tanpa scene baru murah (satu query katalog).
        self._scheduler.add_job(
            func=self.run_live_areas,
            trigger=CronTrigger(hour=_AREA_CHECK_HOURS, minute=_CHECK_MINUTE),
            id="live_areas_check",
            name="Live Monitoring Check",
            replace_existing=True,
        )
        # Dataset LIVE tunggal versi lama (run_daily_check) sengaja tidak lagi
        # dijadwalkan: fitur itu digantikan Daerah Live. Kodenya dibiarkan
        # untuk endpoint lama /api/live/backfill.
        self._scheduler.start()
        logger.info("[LIVE] scheduler dimulai, cek Daerah Live jam %s %s",
                    _AREA_CHECK_HOURS, _TIMEZONE)
        self._recover_areas()

    def run_live_areas(self) -> dict:
        """Mulai siklus untuk setiap Daerah Live aktif. Siklusnya berjalan
        di thread per daerah dan diserialkan oleh live_monitor._CYCLE_LOCK."""
        from etl.live_monitor import LiveMonitor

        mon = LiveMonitor(self._db)
        started = []
        # Urut last_checked_at terlama (belum pernah dicek paling depan). Urutan
        # eksekusi sebenarnya dijaga _CycleGate; ini membuat log mengikutinya.
        areas = sorted(mon.list_areas(), key=lambda a: (
            a["last_checked_at"] is not None,
            a["last_checked_at"].timestamp() if a["last_checked_at"] else 0.0))
        for area in areas:
            if area["enabled"] and mon.start_cycle(area["area_id"]):
                started.append(area["area_id"])
        logger.info("[LIVE] cek terjadwal: siklus dimulai untuk area %s", started)
        return {"started": started}

    def _recover_areas(self) -> None:
        """Daerah yang siklusnya terputus restart (status BACKFILLING/RUNNING/WAITING)
        dilanjutkan. Siklus idempoten: scene yang sudah jadi dilewati."""
        import os
        from etl.live_monitor import LiveMonitor

        # Sakelar yang sama dengan pemulihan job dataset biasa (api/main.py);
        # tes mematikannya supaya tidak memicu unduhan sungguhan.
        if os.getenv("AUTO_RESUME_JOBS", "true").lower() not in ("1", "true", "yes"):
            return
        try:
            mon = LiveMonitor(self._db)
            for area in mon.list_areas():
                if area["enabled"] and area["status"] in ("BACKFILLING", "RUNNING", "WAITING"):
                    logger.warning("[LIVE] area=%d terputus (%s), dilanjutkan",
                                   area["area_id"], area["status"])
                    mon.start_cycle(area["area_id"])
        except Exception:
            logger.exception("[LIVE] gagal memulihkan Daerah Live")

    def shutdown(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            logger.info("[LIVE] scheduler dihentikan")

    def run_daily_check(self) -> dict:
        live = self._dsmgr.get_live_dataset()
        if live is None:
            logger.warning("[LIVE] dataset live belum ada, skip check")
            return {"checked": False, "reason": "dataset_live_not_found"}
        if not live["live_enabled"]:
            logger.info("[LIVE] live dataset nonaktif, skip check")
            return {"checked": False, "reason": "disabled"}

        with self._db.session() as sess:
            sources = sess.scalars(
                select(LiveDatasetSource).where(LiveDatasetSource.enabled == True)
            ).all()
            source_rows = [
                {
                    "source_name": s.source_name,
                    "last_check": s.last_check,
                    "last_ingest": s.last_ingest,
                    "source_config": s.source_config or {},
                }
                for s in sources
            ]

        results: dict[str, bool] = {}
        for source in source_rows:
            try:
                results[source["source_name"]] = self._check_and_ingest_source(live, source)
            except Exception:
                logger.exception("[LIVE] gagal cek sumber %s", source["source_name"])
                results[source["source_name"]] = False

        with self._db.session() as sess:
            dataset = sess.get(Dataset, live["dataset_id"])
            if dataset:
                dataset.live_last_checked_at = _now()

        if not any(results.values()):
            logger.info("[LIVE] tidak ada data baru dari semua sumber")

        return {"checked": True, "sources": results}

    def _check_and_ingest_source(self, live: dict, source: dict) -> bool:
        name = source["source_name"]
        if name == "SENTINEL1":
            # run_dataset_job opens the dataset run log itself.
            return self._check_and_ingest_sentinel1(live, source)
        # MODIS/GPM never pass through run_dataset_job, so without this wrapper
        # their events reached processing_logs only and never the .txt file.
        if name in ("MODIS", "GPM"):
            with dataset_log_file(live["dataset_id"], live["name"]):
                logger.info("[LIVE] %s: mulai cek & ingest dataset=%r", name, live["name"])
                if name == "MODIS":
                    return self._check_and_ingest_modis(live, source)
                return self._check_and_ingest_gpm(live, source)
        logger.warning("[LIVE] sumber tidak dikenal: %s", name)
        return False

    def _update_source_check(self, source_name: str, **fields) -> None:
        with self._db.session() as sess:
            source = sess.scalar(
                select(LiveDatasetSource).where(LiveDatasetSource.source_name == source_name)
            )
            if source is None:
                return
            for k, v in fields.items():
                setattr(source, k, v)

    def _create_live_job(self, dataset_id: int, date_start: date, date_end: date, job_type: str) -> int:
        with self._db.session() as sess:
            job = DatasetJob(
                dataset_id=dataset_id,
                job_type=job_type,
                status="QUEUED",
                date_range_start=date_start,
                date_range_end=date_end,
            )
            sess.add(job)
            sess.flush()
            return job.job_id

    def _check_and_ingest_sentinel1(self, live: dict, source: dict) -> bool:
        since = source["last_ingest"] or (_now() - timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        date_to = _now()
        try:
            scenes = discover_scenes(bbox_wkt=live["bbox_wkt"], date_from=since, date_to=date_to, max_results=200)
        except Exception:
            logger.exception("[LIVE] Sentinel-1: gagal cek ketersediaan")
            self._update_source_check("SENTINEL1", last_check=_now())
            return False

        self._update_source_check("SENTINEL1", last_check=_now())

        if not scenes:
            logger.info("[LIVE] Sentinel-1: tidak ada scene baru")
            return False

        logger.info("[LIVE] Sentinel-1: %d scene baru ditemukan, memulai ingest", len(scenes))
        job_id = self._create_live_job(live["dataset_id"], since.date(), date_to.date(), "LIVE_INGEST")
        run_dataset_job(self._db, job_id)
        self._update_source_check("SENTINEL1", last_ingest=_now())
        # Fusion (GOLD tier) is now built per-scene inside run_dataset_job's
        # own FUSION stage (etl/module5_orchestrator.py) for every dataset,
        # LIVE included — no separate post-ingest fusion pass needed here.
        return True

    def _check_and_ingest_modis(self, live: dict, source: dict) -> bool:
        """Ingest MODIS harian untuk dataset LIVE.

        Registrasi produk (SILVER -> ekspor COG GOLD -> baris data_products +
        lineage) dilakukan module9_fusion.ensure_modis_inputs_for_date, sama
        persis dengan jalur dataset biasa. Sebelumnya di sini ada salinan
        logika registrasi sendiri yang cuma mencatat band FLOOD ke SILVER —
        dan memanggil download_modis_scene tanpa argumen `dataset_name`,
        sehingga `since` terbaca sebagai nama dataset dan seluruh path
        output-nya salah."""
        dataset_id = live["dataset_id"]
        today = _now()
        since = source["last_ingest"] or (today - timedelta(days=_DEFAULT_LOOKBACK_DAYS))
        bbox_tuple = self._bbox_tuple(live["bbox_wkt"])

        self._update_source_check("MODIS", last_check=_now())
        self._create_live_job(dataset_id, since.date(), today.date(), "LIVE_INGEST")

        registered = 0
        d = since.date()
        while d <= today.date():
            produced = ensure_modis_inputs_for_date(
                self._db, dataset_id, live["name"], live["region_id"],
                bbox_tuple, d, plog=self._plog,
            )
            # Dihitung lintas tier, bukan cuma SILVER: sumber yang
            # dikonfigurasi RAW menulis ke BRONZE dan tidak pernah menyentuh
            # SILVER, jadi menghitung SILVER saja akan melaporkan "tidak ada
            # data baru" untuk ingest yang sebenarnya berhasil.
            registered += sum(len(paths) for paths in produced.values())
            d += timedelta(days=1)

        if registered:
            self._update_source_check("MODIS", last_ingest=_now())
        return registered > 0

    def _check_and_ingest_gpm(self, live: dict, source: dict) -> bool:
        today = _now()
        meta = MetadataManager(self._db)
        try:
            nasa_scene_id = meta.get_nasa_scene(GPM_SOURCE, GPM_TILE_ID, GPM_PRODUCT_SHORT_NAME, today.date())
        except Exception:
            nasa_scene_id = None

        self._update_source_check("GPM", last_check=_now())

        if nasa_scene_id:
            logger.info("[LIVE] GPM: data hari ini sudah ada")
            return False

        dataset_id = live["dataset_id"]
        bbox_tuple = self._bbox_tuple(live["bbox_wkt"])
        self._create_live_job(dataset_id, today.date(), today.date(), "LIVE_INGEST")

        produced = ensure_gpm_inputs_for_date(
            self._db, dataset_id, live["name"], live["region_id"],
            bbox_tuple, today.date(), plog=self._plog,
        )
        # Lintas tier — lihat catatan di _check_and_ingest_modis.
        registered = sum(len(paths) for paths in produced.values())

        if registered:
            self._update_source_check("GPM", last_ingest=_now())
        return registered > 0

    @staticmethod
    def _bbox_tuple(bbox_wkt: str) -> tuple[float, float, float, float]:
        from shapely import wkt as shapely_wkt

        return shapely_wkt.loads(bbox_wkt).bounds

    def handle_backfill_request(self, date_start: date, date_end: date) -> dict:
        return self._dsmgr.trigger_live_backfill(date_start, date_end)