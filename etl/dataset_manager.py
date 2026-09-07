# etl/dataset_manager.py
from __future__ import annotations
import logging
import threading
from datetime import date, datetime, timezone
from sqlalchemy import func, select
from etl.database_client import (
    CleanupOperation,
    Dataset,
    DatabaseClient,
    DataProduct,
    DatasetJob,
    SatelliteScene,
    SceneJobState,
)
from etl import folder_manager as fm
from etl.location_resolver import resolve_location, resolve_region_id

logger = logging.getLogger(__name__)

# Tier yang bisa diminta user dan ikut aturan retensi. PREVIEW sengaja tidak
# ada di sini: dia tier turunan (PNG hasil render dari GOLD, lihat
# folder_manager.TIERS) yang tidak pernah diminta eksplisit dan tidak pernah
# ikut dihapus compute_tiers_to_delete.
TIER_ORDER = ["RAW", "BRONZE", "SILVER", "GOLD", "FUSION"]

# Tahap pipeline -> index tier tertinggi yang dibutuhkan tahap itu.
# QUALITY_ANALYTICS membaca SILVER (bukan menghasilkan tier baru), jadi
# index-nya sama dengan LEE_FILTER. PREVIEW membaca GOLD, jadi index-nya sama
# dengan GOLD_EXPORT: dataset yang berhenti di SILVER melewatinya, dataset
# yang sampai GOLD atau FUSION menjalankannya. Karena index PREVIEW (3) <=
# index FUSION (4), FUSION tidak pernah jalan tanpa PREVIEW ikut jalan.
STAGE_TIER_INDEX = {
    "DOWNLOAD": 0,
    "CROP": 1,
    "LEE_FILTER": 2,
    "QUALITY_ANALYTICS": 2,
    "GOLD_EXPORT": 3,
    "PREVIEW": 3,
    "FUSION": 4,
}

_active_threads: dict[str, threading.Thread] = {}
_threads_lock = threading.Lock()

_pause_events: dict[int, threading.Event] = {}
_cancel_events: dict[int, threading.Event] = {}
_events_lock = threading.Lock()


def _is_thread_alive(key: str) -> bool:
    with _threads_lock:
        t = _active_threads.get(key)
        return t is not None and t.is_alive()


def _register_thread(key: str, thread: threading.Thread) -> None:
    with _threads_lock:
        _active_threads[key] = thread


def get_pause_event(job_id: int) -> threading.Event:
    with _events_lock:
        ev = _pause_events.get(job_id)
        if ev is None:
            ev = threading.Event()
            ev.set()
            _pause_events[job_id] = ev
        return ev


def get_cancel_event(job_id: int) -> threading.Event:
    with _events_lock:
        ev = _cancel_events.get(job_id)
        if ev is None:
            ev = threading.Event()
            _cancel_events[job_id] = ev
        return ev


def release_job_events(job_id: int) -> None:
    with _events_lock:
        _pause_events.pop(job_id, None)
        _cancel_events.pop(job_id, None)


def _normalize_tiers(tiers: list[str]) -> list[str]:
    upper = {t.upper() for t in tiers}
    invalid = upper - set(TIER_ORDER)
    if invalid:
        raise ValueError(f"Tier tidak valid: {invalid}. Valid: {TIER_ORDER}")
    if not upper:
        raise ValueError("tiers tidak boleh kosong")
    return sorted(upper, key=TIER_ORDER.index)


def compute_max_tier(required_tiers: list[str]) -> str:
    return max(required_tiers, key=TIER_ORDER.index)


def compute_skip_stages(required_tiers: list[str]) -> set[str]:
    max_index = TIER_ORDER.index(compute_max_tier(required_tiers))
    return {stage for stage, idx in STAGE_TIER_INDEX.items() if idx > max_index}


def compute_tiers_to_delete(produced_tiers: list[str], required_tiers: list[str]) -> set[str]:
    return set(produced_tiers) - set(required_tiers)


class DatasetManager:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def create_dataset(
        self,
        date_start: date,
        date_end: date,
        tiers: list[str],
        name: str,
        location: str | None = None,
        region_id: int | None = None,
        description: str | None = None,
        quality_settings: dict | None = None,
        generate_preview: bool = True,
    ) -> dict:
        normalized_tiers = _normalize_tiers(tiers)
        # region_id = lokasi dipilih dari tabel (jalur UI). location = nama bebas
        # (pemanggil lama/CLI), di-resolve lewat nama lalu geocoding.
        if region_id is not None:
            bbox_wkt, region_id, location_label = resolve_region_id(self._db, region_id)
        elif location and location.strip():
            bbox_wkt, region_id, location_label = resolve_location(self._db, location)
        else:
            raise ValueError("Lokasi belum dipilih: isi region_id atau location")
        with self._db.session() as sess:
            dataset = Dataset(
                name=name,
                description=description,
                location_label=location_label,
                region_id=region_id,
                bbox=f"SRID=4326;{bbox_wkt}",
                bbox_wkt=bbox_wkt,
                date_start=date_start,
                date_end=date_end,
                required_tiers=normalized_tiers,
                quality_settings=quality_settings or {},
                generate_preview=generate_preview,
                dataset_kind="STANDARD",
                status="QUEUED",
            )
            sess.add(dataset)
            sess.flush()
            dataset_id = dataset.dataset_id
            job = DatasetJob(
                dataset_id=dataset_id,
                job_type="CREATE",
                status="QUEUED",
                date_range_start=date_start,
                date_range_end=date_end,
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id
        logger.info(
            "[DATASET] created dataset_id=%d job_id=%d name=%s tiers=%s",
            dataset_id, job_id, name, normalized_tiers,
        )
        # metadata.json ditulis sejak dataset dibuat, bukan menunggu job
        # pertama selesai: sebuah dataset yang masih mengunduh (berjam-jam
        # untuk scene S1) atau yang discovery-nya nol scene tidak boleh
        # membuat /api/datasets/{id}/metadata membalas 404.
        try:
            self.write_metadata_file(dataset_id, total_size_bytes=0)
        except OSError:
            logger.warning("[DATASET] gagal tulis metadata.json awal dataset_id=%d", dataset_id, exc_info=True)
        self._spawn_job_runner(job_id)
        with self._db.session() as sess:
            job = sess.get(DatasetJob, job_id)
            status = job.status if job else "QUEUED"
        return {"dataset_id": dataset_id, "job_id": job_id, "status": status}

    def list_datasets(
        self,
        limit: int = 20,
        offset: int = 0,
        include_deleted: bool = False,
        dataset_kind: str | None = None,
    ) -> dict:
        with self._db.session() as sess:
            stmt = select(Dataset)
            if not include_deleted:
                stmt = stmt.where(Dataset.status != "DELETED")
            if dataset_kind:
                stmt = stmt.where(Dataset.dataset_kind == dataset_kind)
            total = sess.scalar(select(func.count()).select_from(stmt.subquery()))
            rows = sess.scalars(
                stmt.order_by(Dataset.created_at.desc()).limit(limit).offset(offset)
            ).all()
            items = [self._dataset_to_dict(d) for d in rows]
        return {"total": total or 0, "limit": limit, "offset": offset, "items": items}

    def get_dataset(self, dataset_id: int) -> dict | None:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                return None
            return self._dataset_to_dict(dataset, detail=True)

    def get_acquisition_dates(self, dataset_id: int) -> list[str]:
        """Semua tanggal akuisisi (YYYYMMDD) scene yang punya data_products
        untuk dataset ini — dipakai untuk ringkasan metadata.json, karena
        layout on-disk (tier-first) tidak lagi punya folder tanggal di
        level teratas untuk dijelajahi langsung."""
        with self._db.session() as sess:
            rows = sess.scalars(
                select(SatelliteScene.acquisition_datetime)
                .join(DataProduct, DataProduct.scene_id == SatelliteScene.scene_id)
                .where(DataProduct.dataset_id == dataset_id)
                .distinct()
            ).all()
        return sorted({dt.strftime("%Y%m%d") for dt in rows})

    def get_live_dataset(self) -> dict | None:
        with self._db.session() as sess:
            dataset = sess.scalar(select(Dataset).where(Dataset.dataset_kind == "LIVE"))
            if dataset is None:
                return None
            return self._dataset_to_dict(dataset, detail=True)

    def get_progress(self, dataset_id: int) -> dict | None:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                return None
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            if job is None:
                return {
                    "dataset_id": dataset_id,
                    "job_id": None,
                    "status": dataset.status,
                    "total_scenes": 0,
                    "downloaded_count": 0,
                    "processed_count": 0,
                    "failed_count": 0,
                    "cleaned_count": 0,
                    "progress_percent": 0,
                    "paused": False,
                    "pause_reason": None,
                    "scenes": [],
                }
            scene_rows = sess.scalars(
                select(SceneJobState)
                .where(SceneJobState.job_id == job.job_id)
                .order_by(SceneJobState.created_at)
            ).all()
            scenes = [self._scene_state_to_dict(r) for r in scene_rows]
            job_dict = self._job_to_dict(job)
        total = job_dict["total_scenes"] or 0
        if total > 0:
            progress_percent = int(
                (job_dict["downloaded_count"] + job_dict["processed_count"] + job_dict["cleaned_count"])
                / (total * 3) * 100
            )
        else:
            progress_percent = 0
        return {
            "dataset_id": dataset_id,
            "job_id": job_dict["job_id"],
            "status": job_dict["status"],
            "total_scenes": total,
            "downloaded_count": job_dict["downloaded_count"],
            "processed_count": job_dict["processed_count"],
            "failed_count": job_dict["failed_count"],
            "cleaned_count": job_dict["cleaned_count"],
            "progress_percent": min(progress_percent, 100),
            "paused": job_dict["status"] == "PAUSED",
            "pause_reason": job_dict["pause_reason"],
            "scenes": scenes,
        }

    def toggle_live(self, enabled: bool) -> dict:
        live = self.get_live_dataset()
        if live is None:
            raise ValueError("Dataset live belum ada")
        dataset_id = live["dataset_id"]
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset:
                dataset.live_enabled = enabled
        logger.info("[DATASET] live dataset_id=%d enabled=%s", dataset_id, enabled)
        return {"enabled": enabled}

    def clear_live_dataset(self) -> dict:
        live = self.get_live_dataset()
        if live is None:
            raise ValueError("Dataset live belum ada")
        dataset_id = live["dataset_id"]

        from etl.deletion_manager import DeletionManager
        file_result = DeletionManager(self._db, dataset_id, live["name"]).clear_files_only()

        with self._db.session() as sess:
            sess.query(DataProduct).filter(DataProduct.dataset_id == dataset_id).delete(synchronize_session=False)
            sess.query(DatasetJob).filter(DatasetJob.dataset_id == dataset_id).delete(synchronize_session=False)
            dataset = sess.get(Dataset, dataset_id)
            if dataset:
                dataset.total_scenes = 0
                dataset.completed_scenes = 0
                dataset.failed_scenes = 0
                dataset.total_size_bytes = 0
                dataset.status = "DRAFT"

        logger.info("[DATASET] live dataset_id=%d dikosongkan", dataset_id)
        return {"cleared": True, **file_result}

    def trigger_live_backfill(self, date_start: date, date_end: date) -> dict:
        live = self.get_live_dataset()
        if live is None:
            raise ValueError("Dataset live belum ada")
        dataset_id = live["dataset_id"]
        with self._db.session() as sess:
            job = DatasetJob(
                dataset_id=dataset_id,
                job_type="BACKFILL",
                status="QUEUED",
                date_range_start=date_start,
                date_range_end=date_end,
            )
            sess.add(job)
            sess.flush()
            job_id = job.job_id
        self._spawn_job_runner(job_id)
        logger.info("[DATASET] live backfill job_id=%d dataset_id=%d range=%s..%s",
                    job_id, dataset_id, date_start, date_end)
        return {"job_id": job_id, "status": "QUEUED"}

    def pause_dataset(self, dataset_id: int, reason: str = "user_requested") -> dict:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")
            if dataset.dataset_kind == "LIVE":
                raise ValueError("Dataset live tidak bisa dipause, gunakan toggle enable/disable")
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            if job is None:
                raise ValueError("Belum ada job berjalan untuk dataset ini")
            pausable = {"QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING"}
            if job.status not in pausable:
                raise ValueError(f"Job berstatus {job.status}, tidak bisa dipause")
            job.status = "PAUSED"
            job.paused_at = datetime.now(timezone.utc)
            job.paused_by = "user"
            job.pause_reason = reason
            dataset.status = "PAUSED"
            job_id = job.job_id
        get_pause_event(job_id).clear()
        logger.info("[DATASET] job_id=%d paused reason=%s", job_id, reason)
        return {"status": "PAUSED", "reason": reason}

    def resume_dataset(self, dataset_id: int) -> dict:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")
            if dataset.dataset_kind == "LIVE":
                raise ValueError("Dataset live tidak menggunakan resume, gunakan toggle enable/disable")
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            if job is None:
                raise ValueError("Belum ada job untuk dataset ini")
            if job.status != "PAUSED":
                raise ValueError(f"Job berstatus {job.status}, bukan PAUSED")
            # Mirror retry_dataset_job: queue it and let run_dataset_job set the
            # real stage-specific status, instead of hardcoding "PROCESSING".
            job.status = "QUEUED"
            job.resumed_at = datetime.now(timezone.utc)
            job.resume_count = (job.resume_count or 0) + 1
            dataset.status = "QUEUED"
            job_id = job.job_id
            resume_count = job.resume_count
        get_pause_event(job_id).set()
        self._spawn_job_runner(job_id)
        logger.info("[DATASET] job_id=%d resumed count=%d", job_id, resume_count)
        return {"status": "QUEUED", "resume_count": resume_count}

    def retry_dataset_job(self, dataset_id: int) -> dict:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")
            if dataset.dataset_kind == "LIVE":
                raise ValueError("Dataset live tidak menggunakan retry, gunakan toggle enable/disable")
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            if job is None:
                raise ValueError("Belum ada job untuk dataset ini")
            if job.status != "FAILED":
                raise ValueError(f"Job berstatus {job.status}, bukan FAILED")
            job.status = "QUEUED"
            job.started_at = None
            job.completed_at = None
            dataset.status = "QUEUED"
            job_id = job.job_id
        get_pause_event(job_id).set()
        self._spawn_job_runner(job_id)
        logger.info("[DATASET] job_id=%d retried", job_id)
        return {"status": "QUEUED", "job_id": job_id}

    def cancel_dataset(self, dataset_id: int, cascade_delete: bool = True) -> dict:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")
            if dataset.dataset_kind == "LIVE":
                raise ValueError("Dataset live tidak bisa dibatalkan, gunakan toggle enable/disable")
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            cancellable = {"QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING", "PAUSED"}
            if job is None or job.status not in cancellable:
                raise ValueError(
                    f"Job berstatus {job.status if job else 'tidak ada'}, tidak bisa dibatalkan"
                )
            job.status = "CANCELLED"
            job.completed_at = datetime.now(timezone.utc)
            dataset.status = "CANCELLED"
            job_id = job.job_id
            dataset_name = dataset.name
        get_cancel_event(job_id).set()
        get_pause_event(job_id).set()

        deleted_files = 0
        if cascade_delete:
            from etl.deletion_manager import DeletionManager
            # Sisakan GOLD + FUSION: keduanya deliverable, sisanya antara.
            tier_result = DeletionManager(self._db, dataset_id, dataset_name).delete_tiers(
                ["RAW", "BRONZE", "SILVER"]
            )
            deleted_files = tier_result["deleted_count"]

        logger.info(
            "[DATASET] dataset_id=%d job_id=%d dibatalkan cascade_delete=%s deleted_files=%d",
            dataset_id, job_id, cascade_delete, deleted_files,
        )
        return {"status": "CANCELLED", "deleted_files": deleted_files, "retained_tier": "GOLD+FUSION"}

    def delete_dataset(self, dataset_id: int, force: bool = False) -> dict:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                raise ValueError(f"dataset_id={dataset_id} tidak ditemukan")
            if not dataset.is_deletable:
                raise ValueError("Dataset ini tidak bisa dihapus (dataset live gunakan clear)")
            job = sess.scalar(
                select(DatasetJob)
                .where(DatasetJob.dataset_id == dataset_id)
                .order_by(DatasetJob.created_at.desc())
            )
            active_statuses = {"QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING"}
            if job and job.status in active_statuses and not force:
                raise ValueError("Dataset sedang diproses. Gunakan force=True atau pause dulu.")
            dataset.status = "DELETING"
            job_id = job.job_id if job else None
            job_was_paused = job.status == "PAUSED" if job else False
            if job and job.status in active_statuses:
                job.status = "CANCELLED"
        if job_id is not None and (force or job_was_paused):
            # A paused job's thread is parked on pause_event.wait(); wake it
            # (with cancel set) so it exits instead of leaking forever.
            get_cancel_event(job_id).set()
            get_pause_event(job_id).set()
        self._spawn_deletion_runner(dataset_id)
        logger.info("[DATASET] dataset_id=%d deletion triggered force=%s", dataset_id, force)
        return {"status": "DELETING", "dataset_id": dataset_id}

    def get_deletion_progress(self, dataset_id: int) -> dict | None:
        with self._db.session() as sess:
            op = sess.scalar(
                select(CleanupOperation)
                .where(
                    CleanupOperation.dataset_id == dataset_id,
                    CleanupOperation.operation_type == "FULL_DELETE",
                )
                .order_by(CleanupOperation.created_at.desc())
            )
            if op is None:
                return None
            total_files = op.total_files
            progress_percent = int(op.deleted_count / total_files * 100) if total_files > 0 else 0
            return {
                "status": op.status,
                "total_files": total_files,
                "deleted_count": op.deleted_count,
                "freed_bytes": op.freed_bytes,
                "progress_percent": min(progress_percent, 100),
            }

    def create_scene_job_states(self, job_id: int, product_identifiers: list[str]) -> int:
        with self._db.session() as sess:
            existing = set(
                sess.scalars(
                    select(SceneJobState.product_identifier).where(SceneJobState.job_id == job_id)
                ).all()
            )
            created = 0
            for pid in product_identifiers:
                if pid in existing:
                    continue
                sess.add(SceneJobState(job_id=job_id, product_identifier=pid, stage_status="PENDING"))
                created += 1
            job = sess.get(DatasetJob, job_id)
            if job:
                job.total_scenes = len(product_identifiers)
                dataset = sess.get(Dataset, job.dataset_id)
                if dataset:
                    dataset.total_scenes = len(product_identifiers)
        return created

    def get_scene_job_state(self, job_id: int, product_identifier: str) -> dict | None:
        with self._db.session() as sess:
            row = sess.scalar(
                select(SceneJobState).where(
                    SceneJobState.job_id == job_id,
                    SceneJobState.product_identifier == product_identifier,
                )
            )
            if row is None:
                return None
            return self._scene_state_to_dict(row)

    def upsert_scene_job_state(self, job_id: int, product_identifier: str, **fields) -> int:
        with self._db.session() as sess:
            row = sess.scalar(
                select(SceneJobState).where(
                    SceneJobState.job_id == job_id,
                    SceneJobState.product_identifier == product_identifier,
                )
            )
            if row is None:
                row = SceneJobState(job_id=job_id, product_identifier=product_identifier)
                sess.add(row)
                sess.flush()
            for k, v in fields.items():
                setattr(row, k, v)
            sess.flush()
            return row.id

    def increment_job_counters(
        self,
        job_id: int,
        downloaded: int = 0,
        processed: int = 0,
        failed: int = 0,
        cleaned: int = 0,
    ) -> None:
        with self._db.session() as sess:
            job = sess.get(DatasetJob, job_id)
            if job is None:
                return
            job.downloaded_count += downloaded
            job.processed_count += processed
            job.failed_count += failed
            job.cleaned_count += cleaned
            dataset = sess.get(Dataset, job.dataset_id)
            if dataset:
                dataset.completed_scenes += processed
                dataset.failed_scenes += failed

    def set_job_status(
        self,
        job_id: int,
        status: str,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        with self._db.session() as sess:
            job = sess.get(DatasetJob, job_id)
            if job is None:
                return
            job.status = status
            if started_at is not None:
                job.started_at = started_at
            if completed_at is not None:
                job.completed_at = completed_at
            dataset = sess.get(Dataset, job.dataset_id)
            if dataset:
                dataset.status = status

    def write_metadata_file(self, dataset_id: int, total_size_bytes: int | None = None) -> None:
        """Tulis ulang metadata.json level-dataset.

        Ringkasan read-only turunan tabel `datasets` + isi disk — kalau isinya
        berbeda dari API, database yang benar. `storage_usage` dipecah per tier
        lalu per source lewat folder_manager.storage_breakdown, sumber angka
        yang sama dengan endpoint /api/datasets/{id}/storage/summary.

        Dipanggil saat dataset dibuat (supaya file-nya ada sejak awal, bukan
        cuma setelah job pertama selesai) dan tiap kali job berhenti — termasuk
        di jalur berhenti-awal seperti discovery gagal atau nol scene, yang
        dulu keluar tanpa pernah menulis file ini sama sekali sehingga
        /api/datasets/{id}/metadata membalas 404 selamanya."""
        dataset = self.get_dataset(dataset_id)
        if dataset is None:
            return
        breakdown = fm.storage_breakdown(dataset_id, dataset["name"])
        if total_size_bytes is None:
            total_size_bytes = breakdown["total_size_bytes"]
        quality_settings = dataset.get("quality_settings") or {}
        fm.write_dataset_metadata(dataset_id, dataset["name"], {
            "dataset_id": dataset_id,
            "name": dataset["name"],
            "location_label": dataset["location_label"],
            "bbox_wkt": dataset.get("bbox_wkt"),
            "date_range": {"start": dataset["date_start"], "end": dataset["date_end"]},
            "date_start": dataset["date_start"],
            "date_end": dataset["date_end"],
            "mode": dataset.get("dataset_kind"),
            "quality_threshold": quality_settings.get("min_quality_score"),
            "required_tiers": dataset["required_tiers"],
            "sources": sorted(breakdown["sources"]),
            "status": dataset["status"],
            "total_scenes": dataset["total_scenes"],
            "completed_scenes": dataset["completed_scenes"],
            "failed_scenes": dataset["failed_scenes"],
            "total_size_bytes": total_size_bytes,
            "storage_usage": {
                tier: {
                    "size_bytes": info["size_bytes"],
                    "file_count": info["file_count"],
                    "scene_count": info["scene_count"],
                    "sources": {
                        src: {"size_bytes": v["size_bytes"], "file_count": v["file_count"]}
                        for src, v in info["sources"].items()
                    },
                }
                for tier, info in breakdown["tiers"].items()
            },
            "storage_by_source": breakdown["sources"],
            "acquisition_dates": self.get_acquisition_dates(dataset_id),
            "updated_at": datetime.now(timezone.utc),
        })

    def set_dataset_size(self, dataset_id: int, total_size_bytes: int) -> None:
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset:
                dataset.total_size_bytes = total_size_bytes

    def _spawn_job_runner(self, job_id: int) -> None:
        key = f"job-{job_id}"
        if _is_thread_alive(key):
            return
        get_pause_event(job_id).set()
        try:
            from etl.module5_orchestrator import run_dataset_job
        except ImportError as exc:
            logger.error("[DATASET] run_dataset_job belum tersedia di module5_orchestrator: %s", exc)
            with self._db.session() as sess:
                job = sess.get(DatasetJob, job_id)
                if job:
                    job.status = "FAILED"
                    dataset = sess.get(Dataset, job.dataset_id)
                    if dataset:
                        dataset.status = "FAILED"
            return

        def _runner() -> None:
            try:
                run_dataset_job(self._db, job_id)
            except Exception:
                logger.exception("[DATASET] job thread gagal job_id=%d", job_id)
            finally:
                release_job_events(job_id)

        t = threading.Thread(target=_runner, daemon=True)
        _register_thread(key, t)
        t.start()

    def _spawn_deletion_runner(self, dataset_id: int) -> None:
        key = f"delete-{dataset_id}"
        if _is_thread_alive(key):
            return
        with self._db.session() as sess:
            dataset = sess.get(Dataset, dataset_id)
            if dataset is None:
                return
            dataset_name = dataset.name
        try:
            from etl.deletion_manager import DeletionManager
        except ImportError as exc:
            logger.error("[DATASET] deletion_manager belum tersedia: %s", exc)
            with self._db.session() as sess:
                dataset = sess.get(Dataset, dataset_id)
                if dataset:
                    dataset.status = "FAILED"
            return

        def _runner() -> None:
            try:
                DeletionManager(self._db, dataset_id, dataset_name).delete_all()
            except Exception:
                logger.exception("[DATASET] deletion gagal dataset_id=%d", dataset_id)

        t = threading.Thread(target=_runner, daemon=True)
        _register_thread(key, t)
        t.start()

    def _dataset_to_dict(self, d: Dataset, detail: bool = False) -> dict:
        base = {
            "dataset_id": d.dataset_id,
            "dataset_uuid": str(d.dataset_uuid),
            "name": d.name,
            "description": d.description,
            "location_label": d.location_label,
            "date_start": d.date_start,
            "date_end": d.date_end,
            "required_tiers": list(d.required_tiers or []),
            "dataset_kind": d.dataset_kind,
            "status": d.status,
            "total_scenes": d.total_scenes,
            "completed_scenes": d.completed_scenes,
            "failed_scenes": d.failed_scenes,
            "total_size_bytes": d.total_size_bytes,
            "is_deletable": d.is_deletable,
            # Ikut di `base`, bukan cuma di `detail`: kartu dataset memakainya
            # untuk membedakan "preview sengaja dimatikan" dari "preview belum
            # sempat dibuat", dan daftar kartu tidak mengambil detail.
            "generate_preview": d.generate_preview,
            "live_enabled": d.live_enabled,
            "created_at": d.created_at,
            "updated_at": d.updated_at,
        }
        if detail:
            base.update({
                "bbox_wkt": d.bbox_wkt,
                "region_id": d.region_id,
                "quality_settings": d.quality_settings or {},
                "live_last_checked_at": d.live_last_checked_at,
                "deleted_at": d.deleted_at,
            })
        return base

    def _job_to_dict(self, j: DatasetJob) -> dict:
        return {
            "job_id": j.job_id,
            "job_uuid": str(j.job_uuid),
            "dataset_id": j.dataset_id,
            "job_type": j.job_type,
            "status": j.status,
            "paused_at": j.paused_at,
            "paused_by": j.paused_by,
            "pause_reason": j.pause_reason,
            "resumed_at": j.resumed_at,
            "resume_count": j.resume_count,
            "total_scenes": j.total_scenes,
            "downloaded_count": j.downloaded_count,
            "processed_count": j.processed_count,
            "failed_count": j.failed_count,
            "cleaned_count": j.cleaned_count,
            "created_at": j.created_at,
            "started_at": j.started_at,
            "completed_at": j.completed_at,
        }

    def _scene_state_to_dict(self, s: SceneJobState) -> dict:
        return {
            "id": s.id,
            "job_id": s.job_id,
            "product_identifier": s.product_identifier,
            "scene_id": s.scene_id,
            "current_stage": s.current_stage,
            "stage_status": s.stage_status,
            "attempt_number": s.attempt_number,
            "max_retries": s.max_retries,
            "last_error": s.last_error,
            "created_at": s.created_at,
            "started_at": s.started_at,
            "completed_at": s.completed_at,
        }