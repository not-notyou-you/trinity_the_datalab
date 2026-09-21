# etl/dataset_manager.py
from __future__ import annotations
import logging
import threading
from datetime import date, datetime, timezone
from sqlalchemy import func, select, update
from etl.database_client import (
    CleanupOperation,
    Dataset,
    DatabaseClient,
    DataProduct,
    DatasetJob,
    SatelliteScene,
    SceneJobState,
    SOURCE_NAME_ORDER,
    SOURCE_NAME_TO_API_KEY,
    normalize_source_configs,
)
from etl import folder_manager as fm
from etl.location_resolver import resolve_location, resolve_region_id

from etl import tier_names as tn
from etl.fusion_strategies import FULL_COVERAGE, HYBRID

logger = logging.getLogger(__name__)

# Tier yang bisa diminta user dan ikut aturan retensi. PREVIEW sengaja tidak
# ada di sini: dia tier turunan (PNG hasil render dari GOLD, lihat
# folder_manager.TIERS) yang tidak pernah diminta eksplisit dan tidak pernah
# ikut dihapus compute_tiers_to_delete.
# Peringkat tier dipegang etl/tier_names.rank(): sejak rank 2 bercabang
# per-source (DESPECKLED/INDICES/ACCUMULATED, D14), list datar tidak lagi bisa
# mewakilinya. rank() menerima kedua kosakata, jadi job yang mulai sebelum
# migrasi tetap terbandingkan dengan benar.

# Tahap pipeline -> RANK tier tertinggi yang dibutuhkan tahap itu. Angkanya
# tidak berubah: sejak dulu ini memang peringkat, bukan indeks ke sebuah list.
# QUALITY_ANALYTICS membaca hasil rank 2 (bukan menghasilkan tier baru), jadi
# peringkatnya sama dengan LEE_FILTER. PREVIEW membaca COG, jadi sama dengan
# COG_EXPORT: dataset yang berhenti di rank 2 melewatinya, dataset yang sampai
# COG atau FUSED menjalankannya. Karena PREVIEW (3) <= FUSED (4), FUSED tidak
# pernah jalan tanpa PREVIEW ikut jalan.
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


def _wait_thread_exit(key: str, timeout_s: float) -> bool:
    """Tunggu thread terdaftar `key` selesai. True kalau sudah tidak hidup
    (atau memang tidak ada), False kalau timeout."""
    with _threads_lock:
        t = _active_threads.get(key)
    if t is None or t is threading.current_thread():
        return True
    t.join(timeout_s)
    return not t.is_alive()


# Batas tunggu job berhenti sebelum file dataset dihapus. Cancel hanya dicek
# di antara tahap, dan satu tahap (ekstrak/kalibrasi GRD, download 1.7 GB)
# bisa berjalan belasan menit.
JOB_STOP_TIMEOUT_S = 30 * 60


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
    if not upper:
        raise ValueError("tiers tidak boleh kosong")
    invalid = set()
    for t in upper:
        try:
            tn.rank(t)
        except ValueError:
            invalid.add(t)
    if invalid:
        raise ValueError(f"Tier tidak valid: {invalid}. Valid: {tn.ALL_TIERS}")
    return sorted(upper, key=tn.sort_key)


def compute_max_tier(required_tiers: list[str]) -> str:
    return max(required_tiers, key=tn.rank)


def compute_skip_stages(required_tiers: list[str]) -> set[str]:
    max_rank = tn.rank(compute_max_tier(required_tiers))
    return {stage for stage, r in STAGE_TIER_INDEX.items() if r > max_rank}


def compute_tiers_to_delete(produced_tiers: list[str], required_tiers: list[str]) -> set[str]:
    """Tier yang dihasilkan tapi tidak diminta.

    Dibandingkan lewat RANK, bukan NAMA: sebuah job yang mulai sebelum migrasi
    D14 memegang nama lama di memori sementara required_tiers-nya sudah
    dihitung ulang dengan nama baru. Set difference atas nama akan menganggap
    SEMUA yang dihasilkan job itu tidak diminta, lalu menghapusnya.

    Nilai baliknya memakai nama seperti yang ADA di produced_tiers, karena
    itulah yang dipakai pemanggil untuk menemukan berkasnya.
    """
    keep = {tn.rank(t) for t in required_tiers}
    return {t for t in produced_tiers if tn.rank(t) not in keep}


class DatasetManager:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    def create_dataset(
        self,
        date_start: date,
        date_end: date,
        name: str,
        sources: dict | None = None,
        tiers: list[str] | None = None,
        fusion_strategy: str | None = None,
        fusion_output_only: bool = False,
        s1_match_tolerance_days: int | None = None,
        preview_options: list[str] | None = None,
        location: str | None = None,
        region_id: int | None = None,
        description: str | None = None,
        quality_settings: dict | None = None,
        generate_preview: bool = True,
    ) -> dict:
        """Buat dataset + job pertamanya.

        Jalur utama sekarang lewat `sources` (konfigurasi per-satelit).
        `tiers` dipertahankan sebagai jalur lama untuk pemanggil yang belum
        pindah; kalau `sources` diisi, `required_tiers` diturunkan darinya dan
        `tiers` diabaikan (DOCS/PROTOTYPE_CHANGELOG.md: "`tiers` is no longer
        user-facing -- derived internally").

        Returns:
            dict berisi dataset_id, job_id, status, dan source_configs.
        """
        if sources is None and not tiers:
            raise ValueError("Isi sources (konfigurasi per-satelit) atau tiers")
        # region_id = lokasi dipilih dari tabel (jalur UI). location = nama bebas
        # (pemanggil lama/CLI), di-resolve lewat nama lalu geocoding.
        if region_id is not None:
            bbox_wkt, region_id, location_label = resolve_region_id(self._db, region_id)
        elif location and location.strip():
            bbox_wkt, region_id, location_label = resolve_location(self._db, location)
        else:
            raise ValueError("Lokasi belum dipilih: isi region_id atau location")

        dataset_fields = dict(
            name=name,
            description=description,
            location_label=location_label,
            region_id=region_id,
            bbox=f"SRID=4326;{bbox_wkt}",
            bbox_wkt=bbox_wkt,
            date_start=date_start,
            date_end=date_end,
            quality_settings=quality_settings or {},
            generate_preview=generate_preview,
            dataset_kind="STANDARD",
            status="QUEUED",
        )

        if sources is not None:
            # Dataset + baris dataset_source_config ditulis dalam SATU
            # transaksi oleh database_client: dataset tanpa config adalah
            # dataset yang tidak bisa diproses ETL, jadi keduanya harus jadi
            # atau tidak sama sekali.
            configs = normalize_source_configs(sources)
            dataset_fields["fusion_strategy"] = fusion_strategy
            dataset_fields["fusion_output_only"] = fusion_output_only
            # Hanya diteruskan kalau user benar-benar menyatakannya: None di
            # sini berarti "pakai default kolom", dan menuliskannya eksplisit
            # akan menghilangkan server_default.
            if s1_match_tolerance_days is not None:
                dataset_fields["s1_match_tolerance_days"] = s1_match_tolerance_days
            dataset_fields["preview_options"] = preview_options
            dataset = self._db.create_dataset_with_sources(dataset_fields, sources)
            dataset_id = dataset.dataset_id
            normalized_tiers = list(dataset.required_tiers or [])
            source_configs = [
                {"source": SOURCE_NAME_TO_API_KEY[name_], "processing": levels}
                for name_, levels in configs.items()
            ]
        else:
            normalized_tiers = _normalize_tiers(tiers)
            source_configs = []
            with self._db.session() as sess:
                dataset = Dataset(required_tiers=normalized_tiers, **dataset_fields)
                sess.add(dataset)
                sess.flush()
                dataset_id = dataset.dataset_id

        with self._db.session() as sess:
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
            "[DATASET] created dataset_id=%d job_id=%d name=%s tiers=%s sources=%s",
            dataset_id, job_id, name, normalized_tiers, source_configs,
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
        return {
            "dataset_id": dataset_id,
            "job_id": job_id,
            "status": status,
            "source_configs": source_configs,
        }

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
            # Satu query agregat untuk SELURUH halaman, bukan satu per kartu:
            # kartu dirender ulang tiap polling, jadi fan-out per dataset akan
            # mengalikan beban database dengan jumlah kartu di layar.
            per_source = self._per_source_stats([d.dataset_id for d in rows], sess)
            items = []
            for d in rows:
                item = self._dataset_to_dict(d)
                stats = per_source.get(d.dataset_id, {})
                item["scenes_by_source"] = {
                    k: v["scenes"] for k, v in stats.items()
                }
                item["bytes_by_source"] = {
                    k: v["bytes"] for k, v in stats.items()
                }
                items.append(item)
        return {"total": total or 0, "limit": limit, "offset": offset, "items": items}

    @staticmethod
    def _per_source_stats(dataset_ids: list[int], sess) -> dict[int, dict[str, dict]]:
        """{dataset_id: {source_api_key: {"scenes": n, "bytes": n}}}.

        Dihitung dari `data_products`, bukan dari disk: storage_breakdown()
        menyusuri folder, dan melakukannya untuk tiap kartu pada tiap polling
        akan membaca ribuan entri direktori hanya untuk menampilkan dua angka.

        Hanya baris `is_latest` yang dihitung — baris lama masih ada di tabel
        untuk keperluan lineage, tapi berkasnya sudah digantikan, jadi
        menjumlahkannya akan melaporkan disk yang tidak terpakai.

        Scene dihitung DISTINCT: satu scene menghasilkan banyak produk (VV, VH,
        beberapa tier), dan menghitung barisnya akan melaporkan angka berkali
        lipat dari jumlah scene yang sebenarnya.
        """
        if not dataset_ids:
            return {}

        rows = sess.execute(
            select(
                DataProduct.dataset_id,
                DataProduct.source,
                func.count(func.distinct(DataProduct.scene_id)),
                func.coalesce(func.sum(DataProduct.file_size_mb), 0),
            )
            .where(
                DataProduct.dataset_id.in_(dataset_ids),
                DataProduct.is_latest.is_(True),
            )
            .group_by(DataProduct.dataset_id, DataProduct.source)
        ).all()

        out: dict[int, dict[str, dict]] = {}
        for ds_id, source, n_scenes, total_mb in rows:
            key = SOURCE_NAME_TO_API_KEY.get(source, str(source).lower())
            out.setdefault(ds_id, {})[key] = {
                "scenes": int(n_scenes or 0),
                "bytes": int(float(total_mb or 0) * 1024 * 1024),
            }
        return out

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
                    "layers": [],
                }
            scene_rows = sess.scalars(
                select(SceneJobState)
                .where(SceneJobState.job_id == job.job_id)
                .order_by(SceneJobState.created_at)
            ).all()
            scenes = [self._scene_state_to_dict(r) for r in scene_rows]
            job_dict = self._job_to_dict(job)
            layers = self._progress_layers(sess, dataset, job_dict, scenes)
        total = job_dict["total_scenes"] or 0
        if layers:
            # Counter job mencampur hari aux dengan scene S1 sehingga bisa
            # melampaui total_scenes; rata-rata lapisan mengikuti sumbu yang
            # benar-benar dikerjakan.
            progress_percent = int(sum(l["ratio"] for l in layers) / len(layers) * 100)
        elif total > 0:
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
            "layers": layers,
        }

    # Urutan tahap per scene S1, sama dengan orchestrator. Scene yang sudah
    # melewati sebuah tahap dianggap menyelesaikannya.
    _S1_STAGE_ORDER = (
        "DOWNLOAD", "CROP", "LEE_FILTER", "QUALITY_ANALYTICS",
        "GOLD_EXPORT", "PREVIEW", "FUSION", "CLEANUP",
    )

    def _progress_layers(self, sess, dataset, job_dict: dict, scenes: list[dict]) -> list[dict]:
        """Lapisan radar progres di kartu dataset: unduh/proses per satelit + fusi.

        Hanya lapisan yang benar-benar dikerjakan dataset ini yang dikembalikan:
        satelit yang tidak dikonfigurasi tidak punya lapisan, "processing"
        hanya ada kalau level PROCESSED diminta, dan "fusion" hanya ada kalau
        strategi fusi dipilih dengan >= 2 satelit.

        S1 dihitung dari scene_job_state (tahap per scene). MODIS/GPM tidak
        punya baris scene_job_state -- mereka ditarik per tanggal -- jadi
        progresnya dihitung dari data_products: scene berbeda yang sudah punya
        produk apa pun (unduh) atau produk rank >= 2 (proses).
        """
        configs = {c.source_name: list(c.processing_levels or []) for c in dataset.source_configs}
        if not configs:
            return []

        counts: dict[tuple[str, str], int] = {}
        rows = sess.execute(
            select(DataProduct.source, DataProduct.product_tier,
                   func.count(func.distinct(DataProduct.scene_id)))
            .where(DataProduct.dataset_id == dataset.dataset_id,
                   DataProduct.is_latest.is_(True))
            .group_by(DataProduct.source, DataProduct.product_tier)
        ).all()
        for source, tier, n in rows:
            counts[(str(source).upper(), tn._upper(tier))] = int(n or 0)

        def n_scenes(source: str, min_rank: int) -> int:
            # Distinct per tier lalu diambil maksimum: satu scene punya banyak
            # tier, jadi menjumlahkan antar-tier akan menghitungnya berkali-kali.
            best = 0
            for (src, tier), n in counts.items():
                if src != source:
                    continue
                try:
                    r = tn.rank(tier)
                except ValueError:
                    continue
                if r >= min_rank:
                    best = max(best, n)
            return best

        done = job_dict["status"] == "COMPLETED"
        total = job_dict["total_scenes"] or len(scenes)
        has_s1 = "SENTINEL1" in configs
        range_days = (dataset.date_end - dataset.date_start).days + 1
        strategy = str(dataset.fusion_strategy or "").strip().upper()
        # Sumbu unduh aux mengikuti plan_fusion: HYBRID dan FULL_COVERAGE
        # menarik MODIS/GPM tiap hari di rentang, bukan per scene S1. Membagi
        # dengan jumlah scene membuat cincin aux penuh setelah hari pertama.
        if has_s1 and strategy not in (HYBRID, FULL_COVERAGE):
            expected_aux = total
        else:
            expected_aux = range_days
        expected_fused = range_days if strategy == FULL_COVERAGE or not has_s1 else total

        def ratio(n: int, expected: int) -> float:
            if done:
                return 1.0
            if expected <= 0:
                return 0.0
            return round(min(n / expected, 1.0), 4)

        order = self._S1_STAGE_ORDER

        def s1_reached(stage: str) -> int:
            idx = order.index(stage)
            reached = 0
            for sc in scenes:
                cur = sc.get("current_stage")
                if cur not in order:
                    continue
                ci = order.index(cur)
                if ci > idx or (ci == idx and sc.get("stage_status") == "COMPLETED"):
                    reached += 1
            return reached

        layers: list[dict] = []
        for source_name in SOURCE_NAME_ORDER:
            if source_name not in configs:
                continue
            key = SOURCE_NAME_TO_API_KEY.get(source_name, source_name.lower())
            wants_processed = "PROCESSED" in configs[source_name]
            if source_name == "SENTINEL1":
                dl = s1_reached("DOWNLOAD")
                pr = s1_reached("GOLD_EXPORT")
                expected = total
            else:
                dl = n_scenes(source_name, 0)
                pr = n_scenes(source_name, 2)
                expected = max(expected_aux, dl)
            layers.append({"key": key + "_download", "source": key,
                           "phase": "download", "ratio": ratio(dl, expected)})
            if wants_processed:
                layers.append({"key": key + "_processing", "source": key,
                               "phase": "processing", "ratio": ratio(pr, expected)})

        if dataset.fusion_strategy and len(configs) >= 2:
            fused = max((n for (_, tier), n in counts.items()
                         if tier in ("FUSED", "FUSION")), default=0)
            layers.append({"key": "fusion", "source": "fusion", "phase": "fusion",
                           "ratio": ratio(fused, expected_fused)})
        return layers

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

    def recover_interrupted_jobs(self) -> list[int]:
        """Lanjutkan job yang terputus karena proses server mati/restart.

        Thread job hidup di dalam proses API. Kalau proses berhenti, job tetap
        berstatus QUEUED/PREPARING/DOWNLOADING/PROCESSING di database padahal
        tidak ada yang mengerjakannya lagi (kasus try2/try3). Dipanggil sekali
        saat startup: tiap job aktif terbaru per dataset STANDARD di-queue ulang.
        run_dataset_job idempoten — scene yang selesai dilewati, file yang ada
        di disk tidak diunduh ulang, dan download S1 melanjutkan .part.

        Returns: job_id yang dilanjutkan."""
        active = {"QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING"}
        to_resume: list[int] = []
        with self._db.session() as sess:
            jobs = sess.scalars(
                select(DatasetJob)
                .where(DatasetJob.status.in_(active))
                .order_by(DatasetJob.created_at.desc())
            ).all()
            seen: set[int] = set()
            for job in jobs:
                if job.dataset_id in seen:
                    continue
                seen.add(job.dataset_id)
                dataset = sess.get(Dataset, job.dataset_id)
                if (
                    dataset is None
                    or dataset.deleted_at is not None
                    or dataset.dataset_kind == "LIVE"
                    or dataset.status == "DELETING"
                ):
                    continue
                if _is_thread_alive(f"job-{job.job_id}"):
                    continue
                job.status = "QUEUED"
                job.resumed_at = datetime.now(timezone.utc)
                job.resume_count = (job.resume_count or 0) + 1
                dataset.status = "QUEUED"
                to_resume.append(job.job_id)
        for job_id in to_resume:
            logger.warning("[DATASET] job_id=%d terputus oleh restart, dilanjutkan", job_id)
            self._spawn_job_runner(job_id)
        return to_resume

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
            # Sisakan COG + FUSED (rank >= 3): keduanya deliverable, sisanya
            # antara. Kedua kosakata disapu supaya baris pra-D14 ikut terhapus.
            tier_result = DeletionManager(self._db, dataset_id, dataset_name).delete_tiers(
                list(tn.tiers_up_to_rank(2))
            )
            deleted_files = tier_result["deleted_count"]

        logger.info(
            "[DATASET] dataset_id=%d job_id=%d dibatalkan cascade_delete=%s deleted_files=%d",
            dataset_id, job_id, cascade_delete, deleted_files,
        )
        return {"status": "CANCELLED", "deleted_files": deleted_files, "retained_tier": f"{tn.COG}+{tn.FUSED}"}

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
        self._spawn_deletion_runner(dataset_id, job_id=job_id)
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

    @staticmethod
    def scene_is_done(current_stage: str | None, stage_status: str | None) -> bool:
        """Scene benar-benar selesai hanya di CLEANUP/COMPLETED (tahap terakhir
        _cleanup_worker). stage_status COMPLETED saja tidak cukup: nilai itu
        juga ditulis di akhir tiap tahap antara (CROP, GOLD_EXPORT, ...), jadi
        scene yang terputus sesudah tahap seperti itu dulu dilewati selamanya."""
        return current_stage == "CLEANUP" and stage_status == "COMPLETED"

    def begin_job_run(self, job_id: int) -> int:
        """Siapkan counter & state untuk satu eksekusi run_dataset_job.

        Counter job/dataset dulu hanya bertambah dan tidak pernah di-reset saat
        retry/resume, sehingga kegagalan eksekusi lama (mis. MemoryError)
        terbawa selamanya: status akhir tetap FAILED walau semua scene sudah
        berhasil, dan UI menampilkan "4 gagal" untuk dataset 2 scene. Counter
        diturunkan ulang dari scene yang sudah selesai dan error lama
        dibersihkan; kegagalan di eksekusi ini menambah counter seperti biasa.

        Returns: jumlah scene yang sudah selesai."""
        with self._db.session() as sess:
            job = sess.get(DatasetJob, job_id)
            if job is None:
                return 0
            rows = sess.scalars(
                select(SceneJobState).where(SceneJobState.job_id == job_id)
            ).all()
            done = sum(1 for r in rows if self.scene_is_done(r.current_stage, r.stage_status))
            for r in rows:
                r.last_error = None
            job.downloaded_count = done
            job.processed_count = done
            job.cleaned_count = done
            job.failed_count = 0
            dataset = sess.get(Dataset, job.dataset_id)
            if dataset:
                dataset.completed_scenes = done
                dataset.failed_scenes = 0
        return done

    def increment_job_counters(
        self,
        job_id: int,
        downloaded: int = 0,
        processed: int = 0,
        failed: int = 0,
        cleaned: int = 0,
    ) -> None:
        # UPDATE x = x + n di database, bukan baca-tambah-tulis di Python:
        # thread download (paralel), pipeline, dan cleanup memanggil ini
        # bersamaan, dan versi baca-tulis kehilangan hitungan saat dua
        # transaksi membaca nilai yang sama.
        with self._db.session() as sess:
            dataset_id = sess.scalar(
                select(DatasetJob.dataset_id).where(DatasetJob.job_id == job_id)
            )
            if dataset_id is None:
                return
            sess.execute(
                update(DatasetJob)
                .where(DatasetJob.job_id == job_id)
                .values(
                    downloaded_count=DatasetJob.downloaded_count + downloaded,
                    processed_count=DatasetJob.processed_count + processed,
                    failed_count=DatasetJob.failed_count + failed,
                    cleaned_count=DatasetJob.cleaned_count + cleaned,
                )
                .execution_options(synchronize_session=False)
            )
            if processed or failed:
                sess.execute(
                    update(Dataset)
                    .where(Dataset.dataset_id == dataset_id)
                    .values(
                        completed_scenes=Dataset.completed_scenes + processed,
                        failed_scenes=Dataset.failed_scenes + failed,
                    )
                    .execution_options(synchronize_session=False)
                )

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

    def _spawn_deletion_runner(self, dataset_id: int, job_id: int | None = None) -> None:
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
            # Hapus file hanya setelah thread job berhenti. Cancel cuma sinyal:
            # tahap yang sedang jalan tetap menulis sampai selesai, dan file
            # yang lahir sesudah manifest penghapusan dibuat akan tertinggal
            # sebagai folder yatim tanpa baris dataset (kasus jakarta_part2:
            # 3.5 GB raw/_work tersisa setelah dataset dihapus di tengah
            # kalibrasi).
            if job_id is not None and not _wait_thread_exit(f"job-{job_id}", JOB_STOP_TIMEOUT_S):
                logger.warning(
                    "[DATASET] job_id=%d belum berhenti setelah %ds; penghapusan "
                    "dataset_id=%d tetap dilanjutkan", job_id, JOB_STOP_TIMEOUT_S, dataset_id,
                )
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
            # Ikut di `base`: kartu dataset (Tab 2) menampilkan satelit,
            # level pemrosesan, dan strategi fusi, dan kartu itu dirender
            # dari listing tanpa menarik detail per dataset. Dimuat lewat
            # relasi lazy="selectin", jadi tetap satu query tambahan untuk
            # seluruh halaman, bukan satu per baris.
            "source_configs": [
                {"source": SOURCE_NAME_TO_API_KEY.get(c.source_name, c.source_name.lower()),
                 "processing": list(c.processing_levels or [])}
                for c in sorted(
                    d.source_configs,
                    key=lambda c: SOURCE_NAME_ORDER.index(c.source_name)
                    if c.source_name in SOURCE_NAME_ORDER else len(SOURCE_NAME_ORDER),
                )
            ],
            "fusion_strategy": d.fusion_strategy,
            # Ikut di `base` bersama fusion_strategy: keduanya menjelaskan
            # bentuk output dataset, dan orchestrator membaca dict ini (bukan
            # objek ORM-nya) untuk memutuskan penghematan disk.
            "fusion_output_only": d.fusion_output_only,
            "s1_match_tolerance_days": d.s1_match_tolerance_days,
            "preview_options": list(d.preview_options or []),
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
                # Konfigurasi per-satelit + strategi fusi ikut di detail karena
                # orchestrator memutuskan cabang pipeline dari keduanya
                # (DOCS/ETL.md, "Pipeline Branching Logic"). Dimuat lewat
                # relasi lazy="selectin", jadi tidak menambah query per baris.
                "sources": {
                    c.source_name: list(c.processing_levels or [])
                    for c in d.source_configs
                },
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