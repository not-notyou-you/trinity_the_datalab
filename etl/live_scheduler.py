# etl/live_scheduler.py
from __future__ import annotations
import logging
from etl.database_client import DatabaseClient

logger = logging.getLogger(__name__)

_CHECK_MINUTE = 0
_TIMEZONE = "Asia/Jakarta"
_AREA_CHECK_HOURS = "1,7,13,19"


class LiveScheduler:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db
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
        # Dataset LIVE tunggal versi lama sudah digantikan Daerah Live; cek
        # hariannya (run_daily_check) dihapus. Endpoint lama /api/live/backfill
        # memanggil DatasetManager.trigger_live_backfill langsung.
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


