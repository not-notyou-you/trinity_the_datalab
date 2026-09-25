# etl/live_monitor.py
"""Live Monitoring: pemantauan otomatis per Daerah Live (LIVE_MONITORING.md).

BENTUK DATA
    Satu Daerah Live = satu baris `datasets` (dataset_kind='LIVE_AREA') + satu
    baris `live_areas`. Dataset-nya diproses pipeline biasa
    (module5_orchestrator.run_dataset_job) dengan konfigurasi tetap:

        sumber      : Sentinel-1, MODIS, GPM -- semuanya PROCESSED
        tier simpan : COG saja (rank 3). FUSION otomatis dilewati karena
                      compute_skip_stages() memangkas tahap di atas tier
                      tertinggi yang diminta; tidak ada HDF5 yang ditulis.
        strategi    : CO_OCCURRENCE. Validasi multi-sumber mewajibkan strategi,
                      dan CO_OCCURRENCE berjangkar pada tanggal S1 -- persis
                      definisi "scene" di dokumen -- sehingga MODIS/GPM hanya
                      diunduh untuk tanggal S1, tanpa hari tambahan.
        preview     : tahap PREVIEW pipeline dimatikan; Live merender 8 PNG-nya
                      sendiri (etl/live_preview.py) dari COG yang tersisa.

    Scene = tanggal akuisisi S1. Log per scene ada di `live_scenes` (tidak
    pernah dihapus, hanya ditandai deleted_at) dan log langkah di `live_events`.

PENGHAPUSAN
    Retensi melakukan hard delete berkas satu scene. Berkas hanya dihapus kalau
    path-nya (setelah resolve) berada di dalam root dataset LIVE_AREA milik
    daerah itu -- lihat _LiveFiles. Semua berkas dataset biasa tinggal di root
    dataset-nya sendiri (data/datasets/{id}_{slug}/), jadi tidak pernah bisa
    tersentuh.
"""
from __future__ import annotations

import logging
import heapq
import os
import math
import re
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import func, select

from etl import folder_manager as fm
from etl.database_client import (
    DataProduct,
    Dataset,
    DatabaseClient,
    DatasetJob,
    LiveArea,
    LiveEvent,
    LiveScene,
)

logger = logging.getLogger(__name__)

# Batas jumlah daerah aktif sekaligus. Tiap daerah mengunduh scene S1 (~1 GB
# sebelum crop) + granule MODIS/GPM tiap siklus, jadi angka ini menjaga
# storage dan kuota API NASA/Copernicus.
MAX_AREAS = 5
MIN_RETENTION = 1
MAX_RETENTION = 12
DEFAULT_RETENTION = 6

LIVE_SOURCES = {"SENTINEL1": ["PROCESSED"], "MODIS": ["PROCESSED"], "GPM": ["PROCESSED"]}
LIVE_REQUIRED_TIERS = ["COG"]
LIVE_FUSION_STRATEGY = "CO_OCCURRENCE"

# Revisit S1 ~6-12 hari. Backfill mencari mundur retensi x ini (+ cadangan),
# dibatasi supaya discovery tidak menjelajah bertahun-tahun di AOI yang
# jarang dilewati.
S1_REVISIT_DAYS = 12
BACKFILL_MARGIN_DAYS = 14
MAX_LOOKBACK_DAYS = 200

LIVE_DIRNAME = "live"

class _CycleGate:
    """Kunci satu-siklus-sekaligus yang adil. threading.Lock tidak menjamin
    urutan: saat beberapa daerah jatuh tempo bersamaan, daerah yang sama bisa
    terus menang dan yang lain terlambat melewati slot cron berikutnya. Di
    sini penunggu dilayani urut `key` terkecil (last_checked_at terlama),
    lalu urutan datang."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._held = False
        self._waiters: list[tuple[float, int]] = []
        self._seq = 0

    def busy(self) -> bool:
        with self._cond:
            return self._held or bool(self._waiters)

    @contextmanager
    def hold(self, key: float = float("inf")):
        with self._cond:
            self._seq += 1
            entry = (key, self._seq)
            heapq.heappush(self._waiters, entry)
            while self._held or self._waiters[0] != entry:
                self._cond.wait()
            heapq.heappop(self._waiters)
            self._held = True
        try:
            yield
        finally:
            with self._cond:
                self._held = False
                self._cond.notify_all()


# Satu siklus pada satu waktu, lintas daerah: kuota unduhan dan disk lebih
# penting daripada kecepatan, dan run_dataset_job sendiri sudah multi-thread.
_CYCLE_LOCK = _CycleGate()
_area_threads: dict[int, threading.Thread] = {}
_threads_guard = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def forecast_steps(n_scenes: int) -> int:
    """Jumlah scene yang diramal (LIVE_MONITORING.md 5.1): ceil(n/3)."""
    return max(1, math.ceil(n_scenes / 3)) if n_scenes > 0 else 0


# ---------------------------------------------------------------------------
# Berkas milik satu Daerah Live
# ---------------------------------------------------------------------------

class UnsafeDeletion(RuntimeError):
    """Penghapusan ditolak karena target di luar root dataset LIVE_AREA."""


class _LiveFiles:
    """Pencari & penghapus berkas satu dataset LIVE_AREA.

    Semua operasi disaring lewat _inside(): path di-resolve dulu (symlink,
    '..') lalu wajib berada di bawah root dataset ini. Kalau dataset-nya bukan
    LIVE_AREA, konstruktor menolak -- penjaga kedua selain filter path."""

    def __init__(self, dataset_id: int, dataset_name: str, dataset_kind: str) -> None:
        if dataset_kind != "LIVE_AREA":
            raise UnsafeDeletion(
                f"dataset_id={dataset_id} berjenis {dataset_kind!r}, bukan LIVE_AREA"
            )
        self.dataset_id = dataset_id
        self.dataset_name = dataset_name
        self.root = fm.get_dataset_root(dataset_id, dataset_name)

    def _inside(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.root.resolve())
            return True
        except (ValueError, OSError):
            return False

    def preview_dir(self, scene_date: date) -> Path:
        return self.root / LIVE_DIRNAME / scene_date.strftime("%Y%m%d")

    def files_for_date(self, scene_date: date) -> list[Path]:
        """Seluruh berkas yang dimiliki satu scene: COG S1 (semua frame hari
        itu), COG MODIS/GPM tanggal itu, dan PNG Live-nya."""
        if not self.root.is_dir():
            return []
        dk = scene_date.strftime("%Y%m%d")
        found: set[Path] = set()
        patterns = (
            # S1: product_identifier memuat stempel akuisisi YYYYMMDDTHHMMSS.
            (fm.SOURCE_DIR_NAMES["sentinel1"], f"*_{dk}T*"),
            (fm.SOURCE_DIR_NAMES["modis"], f"*_{dk}_*"),
            (fm.SOURCE_DIR_NAMES["gpm"], f"*_{dk}.*"),
            (fm.PREVIEW_DIRNAME, f"*{dk}*"),
        )
        for sub, pattern in patterns:
            base = self.root / sub
            if base.is_dir():
                found.update(p for p in base.rglob(pattern) if p.is_file())
        pdir = self.preview_dir(scene_date)
        if pdir.is_dir():
            found.update(p for p in pdir.rglob("*") if p.is_file())
        return sorted(p for p in found if self._inside(p))

    def stale_granules(self, keep_from: date) -> list[Path]:
        """Granule mentah di cache yang tidak lagi dibutuhkan scene mana pun.

        Window GPM 7 hari scene tertua masih membaca granule 6 hari sebelum
        tanggalnya, jadi batasnya keep_from - 7 hari."""
        cutoff = keep_from - timedelta(days=7)
        cache = fm.get_granule_cache_root(self.root)
        if not cache.is_dir():
            return []
        out = []
        for p in cache.rglob("*"):
            if not p.is_file():
                continue
            d = _granule_date(p.name)
            if d is not None and d < cutoff and self._inside(p):
                out.append(p)
        return out

    def delete(self, paths: list[Path]) -> tuple[list[dict], int]:
        """Hapus `paths`. Mengembalikan ([{path, bytes}], total byte)."""
        deleted: list[dict] = []
        freed = 0
        for p in paths:
            if not self._inside(p):
                raise UnsafeDeletion(f"{p} di luar {self.root}")
            try:
                size = p.stat().st_size
                p.unlink()
            except FileNotFoundError:
                continue
            except OSError as exc:
                logger.error("[LIVE] gagal hapus %s: %s", p, exc)
                continue
            deleted.append({"path": str(p), "bytes": size})
            freed += size
        return deleted, freed


_GPM_GRANULE_RE = re.compile(r"\.(\d{8})-S\d{6}")
_MODIS_GRANULE_RE = re.compile(r"\.A(\d{4})(\d{3})\.")


def _granule_date(name: str) -> date | None:
    m = _GPM_GRANULE_RE.search(name)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y%m%d").date()
        except ValueError:
            return None
    m = _MODIS_GRANULE_RE.search(name)
    if m:
        try:
            return date(int(m.group(1)), 1, 1) + timedelta(days=int(m.group(2)) - 1)
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# LiveMonitor
# ---------------------------------------------------------------------------

class LiveMonitor:
    def __init__(self, db: DatabaseClient) -> None:
        self._db = db

    # --- log --------------------------------------------------------------

    def log(self, area_id: int, step: str, status: str, message: str,
            scene_date: date | None = None, **details) -> None:
        level = logging.WARNING if status in ("FAILED", "WARNING") else logging.INFO
        logger.log(level, "[LIVE] area=%d %s %s: %s", area_id, step, status, message)
        try:
            with self._db.session() as sess:
                sess.add(LiveEvent(
                    area_id=area_id, scene_date=scene_date, step=step,
                    status=status, message=message, details=_jsonable(details),
                ))
        except Exception:
            logger.exception("[LIVE] gagal menulis live_events")

    # --- manajemen daerah -------------------------------------------------

    def create_area(self, region_id: int, name: str | None = None,
                    retention: int = DEFAULT_RETENTION, start: bool = True) -> dict:
        from etl.location_resolver import resolve_region_id

        retention = _check_retention(retention)
        with self._db.session() as sess:
            active = sess.scalar(
                select(func.count())
                .select_from(LiveArea).where(LiveArea.deleted_at.is_(None))
            )
        if active >= MAX_AREAS:
            raise ValueError(f"Maksimal {MAX_AREAS} Daerah Live. Hapus salah satu dulu.")

        bbox_wkt, region_id, label = resolve_region_id(self._db, region_id)
        name = (name or "").strip() or label
        today = _now().date()
        dataset = self._db.create_dataset_with_sources(
            dict(
                name=f"live_{name}",
                description=f"Dataset Daerah Live '{name}' (dikelola Live Monitoring)",
                location_label=label,
                region_id=region_id,
                bbox=f"SRID=4326;{bbox_wkt}",
                bbox_wkt=bbox_wkt,
                date_start=today,
                date_end=today,
                required_tiers=LIVE_REQUIRED_TIERS,
                fusion_strategy=LIVE_FUSION_STRATEGY,
                preview_options=[],
                generate_preview=False,
                dataset_kind="LIVE_AREA",
                is_deletable=False,
                status="DRAFT",
                quality_settings={},
            ),
            LIVE_SOURCES,
        )
        with self._db.session() as sess:
            area = LiveArea(
                dataset_id=dataset.dataset_id, name=name, region_id=region_id,
                location_label=label, bbox_wkt=bbox_wkt, retention=retention,
                enabled=True, status="BACKFILLING", forecast={},
            )
            sess.add(area)
            sess.flush()
            area_id = area.area_id
        self.log(area_id, "CREATE", "OK",
                 f"Daerah '{name}' dibuat, retensi {retention} scene",
                 dataset_id=dataset.dataset_id, region_id=region_id)
        if start:
            self.start_cycle(area_id)
        return self.get_area(area_id)

    def list_areas(self) -> list[dict]:
        with self._db.session() as sess:
            rows = sess.scalars(
                select(LiveArea).where(LiveArea.deleted_at.is_(None))
                .order_by(LiveArea.created_at)
            ).all()
            return [self._area_dict(a) for a in rows]

    def get_area(self, area_id: int) -> dict:
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            if a is None or a.deleted_at is not None:
                raise LookupError(f"Daerah Live {area_id} tidak ditemukan")
            return self._area_dict(a)

    def update_area(self, area_id: int, retention: int | None = None,
                    name: str | None = None, enabled: bool | None = None) -> dict:
        grew = False
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            if a is None or a.deleted_at is not None:
                raise LookupError(f"Daerah Live {area_id} tidak ditemukan")
            if name is not None and name.strip():
                a.name = name.strip()
            if enabled is not None:
                a.enabled = bool(enabled)
            if retention is not None:
                retention = _check_retention(retention)
                grew = retention > a.retention
                old = a.retention
                a.retention = retention
            a.updated_at = _now()
        if retention is not None:
            self.log(area_id, "RETENTION", "OK", f"Retensi diubah {old} -> {retention}")
            # Turun: kelebihan langsung dihapus. Naik: isi kekurangannya.
            self.enforce_retention(area_id, reason=f"retensi diturunkan ke {retention}")
            self.refresh_forecast(area_id)
            if grew:
                self.start_cycle(area_id)
        return self.get_area(area_id)

    def delete_area(self, area_id: int) -> dict:
        """Hapus daerah: semua berkas scene-nya dihapus permanen, lalu baris
        dataset LIVE_AREA-nya. live_areas/live_scenes/live_events tetap ada."""
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            if a is None or a.deleted_at is not None:
                raise LookupError(f"Daerah Live {area_id} tidak ditemukan")
            a.enabled = False
            dataset_id = a.dataset_id
            name = a.name
        self._cancel_running_job(dataset_id)

        total_freed = 0
        with self._db.session() as sess:
            dates = [s.scene_date for s in sess.scalars(
                select(LiveScene).where(LiveScene.area_id == area_id,
                                        LiveScene.deleted_at.is_(None))
            ).all()]
        for d in dates:
            total_freed += self.delete_scene(area_id, d, reason="daerah dihapus")

        # Sisa berkas (cache granule, metadata.json, scratch) + baris dataset.
        # DeletionManager bekerja di root dataset ini saja.
        leftover = {"deleted_count": 0, "freed_bytes": 0}
        info = self._dataset_info(dataset_id)
        if info is not None:
            _LiveFiles(*info)  # penjaga: menolak kalau bukan LIVE_AREA
            from etl.deletion_manager import DeletionManager
            # is_deletable=False hanya menutup jalur "Dataset Saya"; penghapus
            # yang sah untuk dataset ini adalah LiveMonitor sendiri.
            with self._db.session() as sess:
                sess.get(Dataset, dataset_id).is_deletable = True
            try:
                leftover = DeletionManager(self._db, dataset_id, info[1]).delete_all()
            except Exception:
                logger.exception("[LIVE] gagal hapus sisa dataset %s", dataset_id)
        total_freed += int(leftover.get("freed_bytes") or 0)

        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            a.status = "DELETED"
            a.deleted_at = _now()
        self.log(area_id, "DELETE_AREA", "OK",
                 f"Daerah '{name}' dihapus, {len(dates)} scene, "
                 f"{total_freed / 1e6:.1f} MB dibebaskan",
                 freed_bytes=total_freed, scenes=[d.isoformat() for d in dates],
                 leftover_files=leftover.get("deleted_count"))
        return {"area_id": area_id, "status": "DELETED", "freed_bytes": total_freed,
                "deleted_scenes": len(dates)}

    # --- retensi ------------------------------------------------------------

    def enforce_retention(self, area_id: int, reason: str = "melebihi retensi") -> int:
        """Hapus scene tertua sampai jumlah scene tersimpan <= retensi."""
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            if a is None:
                return 0
            retention = a.retention
            kept = sess.scalars(
                select(LiveScene).where(
                    LiveScene.area_id == area_id,
                    LiveScene.deleted_at.is_(None),
                    LiveScene.status.in_(("READY", "PARTIAL")),
                ).order_by(LiveScene.scene_date.desc())
            ).all()
            excess = [s.scene_date for s in kept[retention:]]
            keep_from = kept[min(retention, len(kept)) - 1].scene_date if kept else None
        for d in excess:
            self.delete_scene(area_id, d, reason=reason)
        if keep_from is not None:
            self._prune_granules(area_id, keep_from)
        return len(excess)

    def delete_scene(self, area_id: int, scene_date: date, reason: str) -> int:
        """Hard delete berkas satu scene + tandai log-nya. Mengembalikan byte."""
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            dataset_id = a.dataset_id if a else None
        info = self._dataset_info(dataset_id)
        deleted: list[dict] = []
        freed = 0
        if info is not None:
            files = _LiveFiles(*info)
            deleted, freed = files.delete(files.files_for_date(scene_date))
            self._drop_product_rows(dataset_id, [d["path"] for d in deleted])
            _remove_empty_dirs(files.root)
        with self._db.session() as sess:
            s = sess.scalar(select(LiveScene).where(
                LiveScene.area_id == area_id, LiveScene.scene_date == scene_date))
            if s is not None:
                s.status = "DELETED"
                s.deleted_at = _now()
                s.delete_reason = reason
                s.deleted_files = list(s.deleted_files or []) + deleted
                s.freed_bytes = int(s.freed_bytes or 0) + freed
                s.previews = {}
        self.log(area_id, "DELETE_SCENE", "OK",
                 f"Scene {scene_date} dihapus ({reason}): {len(deleted)} berkas, "
                 f"{freed / 1e6:.1f} MB", scene_date=scene_date,
                 files=len(deleted), freed_bytes=freed)
        return freed

    def _prune_granules(self, area_id: int, keep_from: date) -> None:
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            info = self._dataset_info(a.dataset_id if a else None)
        if info is None:
            return
        files = _LiveFiles(*info)
        deleted, freed = files.delete(files.stale_granules(keep_from))
        if deleted:
            self.log(area_id, "PRUNE_CACHE", "OK",
                     f"{len(deleted)} granule cache lama dihapus ({freed / 1e6:.1f} MB)",
                     freed_bytes=freed, files=deleted)

    def _drop_product_rows(self, dataset_id: int, paths: list[str]) -> None:
        """Baris data_products yang berkasnya baru dihapus. Dicocokkan lewat
        nama berkas: kolom file_path bisa relatif atau absolut."""
        if not paths:
            return
        names = {Path(p).name for p in paths}
        try:
            with self._db.session() as sess:
                rows = sess.scalars(select(DataProduct).where(
                    DataProduct.dataset_id == dataset_id)).all()
                for r in rows:
                    if Path(str(r.file_path or "")).name in names:
                        r.is_valid = False
        except Exception:
            logger.exception("[LIVE] gagal menandai data_products dataset=%s", dataset_id)

    # --- siklus (etl/live_cycle.py) -----------------------------------------

    def start_cycle(self, area_id: int) -> bool:
        """Jalankan siklus satu daerah di thread latar. False kalau sudah jalan."""
        with _threads_guard:
            t = _area_threads.get(area_id)
            if t is not None and t.is_alive():
                return False

            def _run() -> None:
                try:
                    self.run_cycle(area_id)
                except Exception:
                    logger.exception("[LIVE] siklus area=%d gagal", area_id)

            t = threading.Thread(target=_run, name=f"live-area-{area_id}", daemon=True)
            _area_threads[area_id] = t
            t.start()
            return True

    def is_running(self, area_id: int) -> bool:
        t = _area_threads.get(area_id)
        return t is not None and t.is_alive()

    def run_cycle(self, area_id: int) -> dict:
        from etl.live_cycle import run_cycle
        return run_cycle(self, area_id)

    def refresh_forecast(self, area_id: int) -> None:
        from etl.live_cycle import refresh_forecast
        refresh_forecast(self, area_id)

    # --- kartu & log ---------------------------------------------------------

    def get_card(self, area_id: int, scene_date: date | None = None) -> dict:
        """Isi kartu daerah: scene terpilih (default terbaru) + daftar tanggal
        + forecast tersimpan. Tidak menghitung apa pun -- semua sudah disimpan
        siklus."""
        area = self.get_area(area_id)
        with self._db.session() as sess:
            rows = sess.scalars(select(LiveScene).where(
                LiveScene.area_id == area_id, LiveScene.deleted_at.is_(None),
                LiveScene.status.in_(("READY", "PARTIAL")),
            ).order_by(LiveScene.scene_date.desc())).all()
            a = sess.get(LiveArea, area_id)
            forecast = a.forecast or {}
            forecast_at = a.forecast_updated_at
            dates = [{"date": r.scene_date.isoformat(), "status": r.status,
                      "level": (r.area_status or {}).get("level")} for r in rows]
            sel = None
            if rows:
                sel = next((r for r in rows if r.scene_date == scene_date), rows[0])
            scene = None
            if sel is not None:
                dk = sel.scene_date.isoformat()
                items = ((sel.previews or {}).get("items") or {})
                scene = {
                    "date": dk,
                    "status": sel.status,
                    "source_status": sel.source_status or {},
                    "metrics": sel.metrics or {},
                    "interpretations": sel.interpretations or {},
                    "area_status": sel.area_status or {},
                    "previews": {
                        k: {**v, "url": f"/api/live/areas/{area_id}/preview/{dk}/{k}.png"}
                        for k, v in items.items()
                    },
                    "previews_skipped": (sel.previews or {}).get("skipped") or {},
                    "updated_at": sel.updated_at,
                }
        return {"area": area, "scene": scene, "dates": dates,
                "forecast": forecast, "forecast_updated_at": forecast_at}

    def preview_path(self, area_id: int, scene_date: date, key: str) -> Path | None:
        from etl.live_interpret import PREVIEW_KEYS
        if key not in PREVIEW_KEYS:
            return None
        with self._db.session() as sess:
            a = sess.get(LiveArea, area_id)
            info = self._dataset_info(a.dataset_id if a else None)
        if info is None:
            return None
        files = _LiveFiles(*info)
        p = files.preview_dir(scene_date) / f"{key}.png"
        return p if p.is_file() and files._inside(p) else None

    def events(self, area_id: int, limit: int = 100) -> list[dict]:
        with self._db.session() as sess:
            rows = sess.scalars(select(LiveEvent).where(LiveEvent.area_id == area_id)
                                .order_by(LiveEvent.event_id.desc()).limit(limit)).all()
            return [{"at": r.created_at, "step": r.step, "status": r.status,
                     "message": r.message,
                     "scene_date": r.scene_date.isoformat() if r.scene_date else None}
                    for r in rows]

    def scene_log(self, area_id: int) -> list[dict]:
        """Seluruh scene termasuk yang sudah dihapus (audit, 6.1)."""
        with self._db.session() as sess:
            rows = sess.scalars(select(LiveScene).where(LiveScene.area_id == area_id)
                                .order_by(LiveScene.scene_date.desc())).all()
            return [{
                "date": r.scene_date.isoformat(), "status": r.status,
                "created_at": r.created_at, "deleted_at": r.deleted_at,
                "delete_reason": r.delete_reason,
                "deleted_files": len(r.deleted_files or []),
                "freed_bytes": int(r.freed_bytes or 0),
                "source_status": r.source_status or {},
                "metrics": r.metrics or {},
                "area_status": r.area_status or {},
                "interpretations": {k: v.get("text") for k, v in (r.interpretations or {}).items()},
            } for r in rows]

    def retry_scene(self, area_id: int, scene_date: date) -> bool:
        """Coba ulang MODIS/GPM satu scene di thread latar. False kalau siklus
        atau retry daerah ini masih berjalan -- klik berulang dulu menumpuk
        retry yang sama di antrean _CYCLE_LOCK."""
        from etl.live_cycle import retry_scene_locked

        self.get_area(area_id)
        with _threads_guard:
            t = _area_threads.get(area_id)
            if t is not None and t.is_alive():
                return False

            def _run() -> None:
                try:
                    retry_scene_locked(self, area_id, scene_date)
                except Exception:
                    logger.exception("[LIVE] retry area=%d %s gagal", area_id, scene_date)

            # Didaftarkan di registry yang sama dengan siklus: is_running()
            # jadi True (kartu menampilkan "Memproses") dan start_cycle
            # menolak siklus baru selama retry berjalan.
            t = threading.Thread(target=_run, name=f"live-retry-{area_id}", daemon=True)
            _area_threads[area_id] = t
            t.start()
            return True

    # --- progres siklus (loading bar di kartu) -------------------------------

    # Tahap siklus dibaca dari event live_events terakhir. Persen hanya ada di
    # tahap unduh+proses (counter job LIVE_INGEST); tahap lain tidak punya
    # ukuran yang jujur, jadi bar-nya indeterminate (percent None).
    _PHASES = {
        "RECOVER": "Memeriksa scene baru",
        "CYCLE": "Memeriksa scene baru",
        "DISCOVER": "Memeriksa scene baru",
        "INGEST": "Mengunduh & memproses",
        "SCENE": "Menyusun metrik & preview",
        "PREVIEW": "Menyusun metrik & preview",
        "RETRY": "Mencoba ulang MODIS/GPM",
        "DELETE_SCENE": "Merapikan scene lama",
        "PRUNE_CACHE": "Merapikan scene lama",
        "FORECAST": "Menghitung forecast",
    }

    @staticmethod
    def _alerts() -> list[dict]:
        from etl import download_guard as dg
        return [{"source": s, "message": m} for s, m in dg.active_auth_failures().items()]

    def _progress(self, a: LiveArea) -> dict | None:
        prog = self._progress_phase(a)
        if prog is None:
            return None
        from etl import download_guard as dg
        prog["waiting"] = dg.current_wait(a.dataset_id)
        prog["timing"] = self._cycle_timing(a)
        return prog

    def _cycle_timing(self, a: LiveArea) -> dict:
        """Durasi siklus (sejak event CYCLE STARTED terakhir; untuk WAITING
        sejak status diset) dan jeda sejak kemajuan terakhir (event Live atau
        detak unduhan/pipeline dataset-nya)."""
        from etl import download_guard as dg

        with self._db.session() as sess:
            started = sess.scalar(select(LiveEvent.created_at).where(
                LiveEvent.area_id == a.area_id, LiveEvent.step == "CYCLE",
                LiveEvent.status == "STARTED").order_by(LiveEvent.event_id.desc()).limit(1))
            last_ev = sess.scalar(select(LiveEvent.created_at).where(
                LiveEvent.area_id == a.area_id).order_by(LiveEvent.event_id.desc()).limit(1))
        if a.status == "WAITING" or started is None:
            start = a.updated_at
        else:
            start = started
        t = dg.timing(a.dataset_id, start.timestamp() if start else None,
                      last_ev.timestamp() if last_ev else None)
        if a.status == "WAITING":
            t["stalled"] = False  # mengantre di belakang daerah lain bukan macet
        return t

    # Ringkasan hasil siklus terakhir tampil sekian detik setelah selesai.
    RESULT_SHOW_S = int(os.getenv("LIVE_RESULT_SHOW_S", "900"))

    def _last_result(self, area_id: int) -> dict | None:
        with self._db.session() as sess:
            ev = sess.scalar(select(LiveEvent).where(
                LiveEvent.area_id == area_id, LiveEvent.step == "CYCLE",
                LiveEvent.status.in_(("COMPLETED", "FAILED")))
                .order_by(LiveEvent.event_id.desc()).limit(1))
            if ev is None:
                return None
            age = (_now() - ev.created_at).total_seconds()
            if age > self.RESULT_SHOW_S:
                return None
            res = (ev.details or {}).get("result")
            if not res:
                res = {"level": "error" if ev.status == "FAILED" else "ok", "text": ev.message}
            return {**res, "at": ev.created_at}

    def _progress_phase(self, a: LiveArea) -> dict | None:
        running = self.is_running(a.area_id)
        if a.status == "WAITING":
            return {"phase": "Menunggu giliran", "percent": None}
        if not running:
            return None
        with self._db.session() as sess:
            ev = sess.scalar(select(LiveEvent).where(LiveEvent.area_id == a.area_id)
                             .order_by(LiveEvent.event_id.desc()).limit(1))
            step, status = (ev.step, ev.status) if ev is not None else ("CYCLE", "STARTED")
        phase = self._PHASES.get(step, "Memproses")
        if step == "INGEST" and status == "STARTED" and a.dataset_id is not None:
            from etl.dataset_manager import DatasetManager
            try:
                prog = DatasetManager(self._db).get_progress(a.dataset_id) or {}
            except Exception:
                logger.exception("[LIVE] progres area=%d gagal dibaca", a.area_id)
                prog = {}
            total = int(prog.get("total_scenes") or 0)
            failed = min(total, int(prog.get("failed_count") or 0))
            ok = min(total - failed, int(prog.get("processed_count") or 0))
            if not total:
                return {"phase": f"{phase} (menyiapkan)", "percent": None}
            return {"phase": phase, "percent": prog.get("progress_percent"),
                    "ok": ok, "failed": failed, "total": total}
        return {"phase": phase, "percent": None}

    # --- util ---------------------------------------------------------------

    def _dataset_info(self, dataset_id: int | None) -> tuple[int, str, str] | None:
        if dataset_id is None:
            return None
        with self._db.session() as sess:
            d = sess.get(Dataset, dataset_id)
            if d is None:
                return None
            return d.dataset_id, d.name, d.dataset_kind

    def _cancel_running_job(self, dataset_id: int | None) -> None:
        if dataset_id is None:
            return
        from etl.dataset_manager import get_cancel_event, get_pause_event
        active = {"QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING"}
        with self._db.session() as sess:
            jobs = sess.scalars(select(DatasetJob).where(
                DatasetJob.dataset_id == dataset_id, DatasetJob.status.in_(active))).all()
            ids = [j.job_id for j in jobs]
            for j in jobs:
                j.status = "CANCELLED"
        for jid in ids:
            get_cancel_event(jid).set()
            get_pause_event(jid).set()

    def _area_dict(self, a: LiveArea) -> dict:
        with self._db.session() as sess:
            scenes = sess.scalars(
                select(LiveScene).where(
                    LiveScene.area_id == a.area_id, LiveScene.deleted_at.is_(None),
                    LiveScene.status.in_(("READY", "PARTIAL")),
                ).order_by(LiveScene.scene_date.desc())
            ).all()
            dates = [s.scene_date.isoformat() for s in scenes]
            latest = scenes[0] if scenes else None
            size = 0
            if a.dataset_id is not None:
                d = sess.get(Dataset, a.dataset_id)
                size = int(d.total_size_bytes or 0) if d else 0
        return {
            "area_id": a.area_id,
            "dataset_id": a.dataset_id,
            "name": a.name,
            "region_id": a.region_id,
            "location_label": a.location_label,
            "bbox_wkt": a.bbox_wkt,
            "retention": a.retention,
            "enabled": a.enabled,
            "status": a.status,
            "status_message": a.status_message,
            "running": self.is_running(a.area_id),
            "progress": self._progress(a),
            # Token NASA yang ditolak tetap ditampilkan walau siklus sudah
            # selesai: sampai token diganti, setiap siklus akan gagal sama.
            "alerts": self._alerts(),
            "last_result": None if self.is_running(a.area_id) else self._last_result(a.area_id),
            "last_checked_at": a.last_checked_at,
            "scene_dates": dates,
            "scene_count": len(dates),
            "latest_scene_date": dates[0] if dates else None,
            "area_status": (latest.area_status or None) if latest else None,
            "total_size_bytes": size,
            "created_at": a.created_at,
        }


def _check_retention(value) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        raise ValueError("Retensi harus bilangan bulat 1-12")
    if not MIN_RETENTION <= n <= MAX_RETENTION:
        raise ValueError(f"Retensi harus {MIN_RETENTION}-{MAX_RETENTION} scene")
    return n


def _remove_empty_dirs(root: Path) -> None:
    if not root.is_dir():
        return
    for d in sorted((p for p in root.rglob("*") if p.is_dir()),
                    key=lambda p: len(p.parts), reverse=True):
        try:
            d.rmdir()
        except OSError:
            pass


def _jsonable(obj):
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, float) and not math.isfinite(obj):
        return None
    return obj
