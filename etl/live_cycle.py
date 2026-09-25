# etl/live_cycle.py
"""Siklus otomatis satu Daerah Live (LIVE_MONITORING.md 3.3).

    1. cek scene Sentinel-1 baru (discover_scenes)
    2. unduh + proses S1/MODIS/GPM lewat run_dataset_job (pipeline biasa)
    3. per scene: metrik -> 8 preview -> kalimat kondisi
    4. hitung ulang forecast
    5. retensi: hapus scene paling lama yang melebihi batas
    6. semua langkah dicatat ke live_events

Backfill awal adalah siklus yang sama: selama scene tersimpan < retensi,
discovery melihat mundur cukup jauh untuk mengisi kekurangannya.

Kegagalan satu sumber tidak menggagalkan scene: MODIS/GPM yang berkasnya tidak
ada ditandai FAILED di source_status, scene tetap PARTIAL (tampil), dan tiap
siklus berikutnya mencoba ulang sumber itu (retry_failed_sources).
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

from shapely import wkt as shapely_wkt
from sqlalchemy import select

from etl import download_guard as dg
from etl import folder_manager as fm
from etl.database_client import Dataset, DatasetJob, LiveArea, LiveScene
from etl.job_lock import LOCK_DIRNAME as JOB_LOCK_DIRNAME
from etl.job_lock import JobLock
from etl.live_monitor import (
    _CYCLE_LOCK,
    BACKFILL_MARGIN_DAYS,
    MAX_LOOKBACK_DAYS,
    S1_REVISIT_DAYS,
    LiveMonitor,
    _jsonable,
    _LiveFiles,
)

logger = logging.getLogger(__name__)

# Scene yang S1-nya gagal diproses dicoba ulang sampai sekian kali (siklus
# berbeda), lalu dibiarkan FAILED -- tetap di log, tidak tampil di kartu.
MAX_S1_ATTEMPTS = 3
# Jendela discovery minimum di siklus rutin: menangkap scene yang terbit
# terlambat di katalog Copernicus.
ROUTINE_LOOKBACK_DAYS = 10


def _now() -> datetime:
    return datetime.now(timezone.utc)


def run_cycle(mon: LiveMonitor, area_id: int) -> dict:
    """Satu siklus, terkunci dua lapis: _CYCLE_LOCK menyerialkan daerah DI
    DALAM proses ini; JobLock mencegah dua PROSES (mis. uvicorn --reload lama
    yang belum benar-benar berhenti + proses baru, lihat etl/job_lock.py)
    menjalankan siklus daerah yang sama bersamaan dan berebut menulis berkas
    scene yang sama. Tanpa ini, restart yang tumpang tindih bisa membuat dua
    proses mengunduh S1/MODIS/GPM ke folder yang sama sekaligus."""
    lock_dir = fm.DATA_ROOT.parent / JOB_LOCK_DIRNAME
    lock = JobLock.acquire_key(f"live-area-{area_id}", lock_dir)
    if lock is None:
        logger.info("[LIVE] area=%d sedang dikerjakan proses lain, dilewati", area_id)
        return {"skipped": "locked_by_other_process"}
    try:
        key, prev_status = _queue_key(mon, area_id)
        if _CYCLE_LOCK.busy():
            _set_area(mon, area_id, status="WAITING", status_message="Menunggu giliran")
        # Seluruh siklus (termasuk retry MODIS/GPM di luar run_dataset_job)
        # mengalah ke unduhan Dataset Saya di slot koneksi download_guard.
        with _CYCLE_LOCK.hold(key), dg.low_priority():
            started = time.time()
            try:
                _recover_interrupted_ingest(mon, area_id)
                return _run_cycle_locked(mon, area_id)
            finally:
                _report_auth_failures(mon, area_id, started)
                # Jalur keluar awal (daerah nonaktif/terhapus) tidak menulis
                # status; jangan biarkan kartu tertahan "Menunggu giliran".
                _restore_if_waiting(mon, area_id, prev_status)
    finally:
        lock.release()


def retry_scene_locked(mon: LiveMonitor, area_id: int, scene_date: date) -> None:
    """Coba ulang MODIS/GPM satu scene (tombol di kartu), antre di gerbang
    siklus yang sama. Kunci -inf: ini aksi pengguna yang sedang ditunggu."""
    with _CYCLE_LOCK.hold(float("-inf")), dg.low_priority():
        started = time.time()
        retry_failed_sources(mon, area_id, only=scene_date)
        reinterpret_all(mon, area_id)
        refresh_forecast(mon, area_id)
        _report_auth_failures(mon, area_id, started)


def _queue_key(mon: LiveMonitor, area_id: int) -> tuple[float, str | None]:
    """(kunci antrean FIFO, status sekarang). Daerah yang paling lama tidak
    dicek dilayani lebih dulu; yang belum pernah dicek paling depan."""
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        if a is None:
            return 0.0, None
        ts = a.last_checked_at.timestamp() if a.last_checked_at else 0.0
        return ts, a.status


def _restore_if_waiting(mon: LiveMonitor, area_id: int, prev_status: str | None) -> None:
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        if a is not None and a.status == "WAITING":
            a.status = prev_status if prev_status and prev_status != "WAITING" else "ACTIVE"
            a.status_message = None
            a.updated_at = _now()


def _report_auth_failures(mon: LiveMonitor, area_id: int, since: float) -> None:
    """401/403 NASA selama siklus ini. ensure_*_inputs_for_date menelan
    exception per sumber, jadi tanpa ini token kedaluwarsa cuma terlihat
    sebagai MODIS/GPM FAILED tanpa alasan di kartu."""
    for source, msg in dg.auth_failures_since(since).items():
        mon.log(area_id, "AUTH", "FAILED", msg, source=source)


def _set_area(mon: LiveMonitor, area_id: int, **fields) -> None:
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        if a is not None:
            for k, v in fields.items():
                setattr(a, k, v)
            a.updated_at = _now()


def _recover_interrupted_ingest(mon: LiveMonitor, area_id: int) -> None:
    """Pulihkan ingest yang terputus (proses mati/restart di tengah _ingest).

    Dipanggil sambil memegang JobLock daerah + _CYCLE_LOCK, jadi tidak ada
    ingest daerah ini yang sedang berjalan: setiap scene yang masih PROCESSING
    pasti yatim. Tanpa ini scene itu macet selamanya -- _discover_new_dates
    menganggap PROCESSING sudah "dikenal" sehingga tidak diunduh ulang, dan
    tidak pernah difinalisasi sehingga tidak tampil. Dijadikan FAILED dengan
    attempts +1 supaya ikut jalur coba-ulang MAX_S1_ATTEMPTS yang sudah ada.
    Job LIVE_INGEST-nya yang tertinggal DOWNLOADING/PROCESSING ditutup FAILED.
    """
    from etl.module5_orchestrator import LIVE_JOB_TYPE

    active = ("QUEUED", "PREPARING", "DOWNLOADING", "PROCESSING")
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        if a is None:
            return
        stuck = sess.scalars(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.status == "PROCESSING",
            LiveScene.deleted_at.is_(None))).all()
        stuck_dates = []
        for r in stuck:
            src = dict(r.source_status or {})
            s1 = dict(src.get("sentinel1") or {})
            s1["attempts"] = int(s1.get("attempts") or 0) + 1
            s1["status"] = "FAILED"
            s1["reason"] = "Ingest terputus (proses berhenti)"
            src["sentinel1"] = s1
            r.source_status = src
            r.status = "FAILED"
            r.updated_at = _now()
            stuck_dates.append(r.scene_date)
        jobs = []
        if a.dataset_id is not None:
            jobs = sess.scalars(select(DatasetJob).where(
                DatasetJob.dataset_id == a.dataset_id,
                DatasetJob.job_type == LIVE_JOB_TYPE,
                DatasetJob.status.in_(active))).all()
        for j in jobs:
            j.status = "FAILED"
            j.completed_at = _now()
        job_ids = [j.job_id for j in jobs]
    for d in sorted(stuck_dates):
        mon.log(area_id, "RECOVER", "WARNING",
                f"Scene {d} terputus di tengah ingest, akan dicoba ulang", scene_date=d)
    if job_ids:
        logger.warning("[LIVE] area=%d job LIVE_INGEST yatim ditutup FAILED: %s", area_id, job_ids)
        mon.log(area_id, "RECOVER", "WARNING",
                f"Job ingest terputus ditutup: {', '.join(map(str, job_ids))}", job_ids=job_ids)


def _run_cycle_locked(mon: LiveMonitor, area_id: int) -> dict:
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        if a is None or a.deleted_at is not None:
            return {"skipped": "deleted"}
        if not a.enabled:
            mon.log(area_id, "CYCLE", "SKIPPED", "Daerah nonaktif, siklus dilewati")
            return {"skipped": "disabled"}
        retention, bbox_wkt, dataset_id = a.retention, a.bbox_wkt, a.dataset_id
        prev_status = a.status
        rows = sess.scalars(select(LiveScene).where(LiveScene.area_id == area_id)).all()
        scenes = {r.scene_date: (r.status, r.source_status or {}) for r in rows}
        dataset = sess.get(Dataset, dataset_id) if dataset_id else None
        if dataset is None:
            a.status, a.status_message = "ERROR", "Dataset daerah hilang"
            return {"error": "dataset_missing"}
        dataset_name = dataset.name

    kept = sorted((d for d, (st, _) in scenes.items() if st in ("READY", "PARTIAL")), reverse=True)
    filling = len(kept) < retention
    _set_area(mon, area_id, status="BACKFILLING" if filling else "RUNNING",
              status_message="Mengisi scene awal" if filling else "Memeriksa scene baru")
    mon.log(area_id, "CYCLE", "STARTED",
            f"Siklus dimulai ({len(kept)}/{retention} scene tersimpan)")

    try:
        new_dates = _discover_new_dates(mon, area_id, bbox_wkt, retention, kept, scenes)
        processed: list[date] = []
        if new_dates:
            processed = _ingest(mon, area_id, dataset_id, new_dates)
        # Sumber yang gagal di scene lama dicoba ulang tiap siklus.
        retry_failed_sources(mon, area_id, skip=set(processed))
        reinterpret_all(mon, area_id)
        mon.enforce_retention(area_id)
        refresh_forecast(mon, area_id)
        _update_dataset_size(mon, dataset_id, dataset_name)
    except Exception as exc:
        logger.exception("[LIVE] siklus area=%d gagal", area_id)
        mon.log(area_id, "CYCLE", "FAILED", f"Siklus gagal: {exc}",
                result={"level": "error", "text": f"Siklus gagal: {str(exc)[:300]}"})
        _set_area(mon, area_id, status="ERROR", status_message=str(exc)[:500],
                  last_checked_at=_now())
        return {"error": str(exc)}

    with mon._db.session() as sess:
        n = len(sess.scalars(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.deleted_at.is_(None),
            LiveScene.status.in_(("READY", "PARTIAL")))).all())
    msg = (f"{n}/{retention} scene tersimpan"
           + ("" if n >= retention else " — scene Sentinel-1 yang tersedia belum cukup"))
    _set_area(mon, area_id, status="ACTIVE", status_message=msg, last_checked_at=_now())
    mon.log(area_id, "CYCLE", "COMPLETED",
            f"Siklus selesai: {len(processed)} scene baru, {msg}",
            new_scenes=[d.isoformat() for d in processed], previous_status=prev_status,
            result=_cycle_result(mon, area_id, new_dates, n, retention))
    return {"new_scenes": [d.isoformat() for d in processed], "stored": n}


_SOURCE_NAMES = {"sentinel1": "Sentinel-1", "modis": "MODIS", "gpm": "GPM"}


def _cycle_result(mon: LiveMonitor, area_id: int, targets: list[date],
                  stored: int, retention: int) -> dict:
    """Ringkasan hasil siklus untuk kartu: {level: ok|warn, text}.
    Dibedakan lengkap / sebagian (sumber mana yang gagal) / gagal, supaya
    siklus yang tidak mulus tidak terlihat sama dengan yang mulus."""
    from etl import download_guard as dg

    full, partial, failed = 0, [], 0
    if targets:
        with mon._db.session() as sess:
            rows = sess.scalars(select(LiveScene).where(
                LiveScene.area_id == area_id, LiveScene.scene_date.in_(targets))).all()
            for r in rows:
                if r.status == "READY":
                    full += 1
                elif r.status == "PARTIAL":
                    bad = [_SOURCE_NAMES.get(k, k) for k, v in (r.source_status or {}).items()
                           if isinstance(v, dict) and v.get("status") == "FAILED"]
                    partial.append(", ".join(bad) or "sumber")
                elif r.status == "FAILED":
                    failed += 1
    parts = []
    if not targets:
        parts.append(f"tidak ada scene baru ({stored}/{retention} tersimpan)")
    else:
        parts.append(f"{len(targets)} scene baru")
        detail = []
        if full:
            detail.append(f"{full} lengkap")
        if partial:
            srcs = sorted(set(", ".join(partial).split(", ")))
            detail.append(f"{len(partial)} sebagian ({'/'.join(srcs)} gagal)")
        if failed:
            detail.append(f"{failed} gagal Sentinel-1")
        if detail:
            parts[-1] += " — " + ", ".join(detail)
    auth = dg.active_auth_failures()
    if auth:
        parts.append("token NASA ditolak")
    level = "warn" if (partial or failed or auth) else "ok"
    return {"level": level, "text": "Selesai: " + "; ".join(parts)}


# ---------------------------------------------------------------------------
# 1. discovery
# ---------------------------------------------------------------------------

def _discover_new_dates(mon, area_id, bbox_wkt, retention, kept, scenes) -> list[date]:
    from etl.module1_download import discover_scenes
    from etl.module5_orchestrator import _drop_dates_barely_covering_aoi, _scene_date

    today = _now().date()
    if len(kept) < retention:
        lookback = min(MAX_LOOKBACK_DAYS, retention * S1_REVISIT_DAYS + BACKFILL_MARGIN_DAYS)
        date_from = today - timedelta(days=lookback)
    else:
        date_from = min(kept[0] - timedelta(days=1), today - timedelta(days=ROUTINE_LOOKBACK_DAYS))
    date_to = _now() + timedelta(days=1)

    try:
        found = discover_scenes(
            bbox_wkt=bbox_wkt,
            date_from=datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc),
            date_to=date_to, max_results=200,
        )
    except Exception as exc:
        mon.log(area_id, "DISCOVER", "FAILED", f"Gagal cek scene Sentinel-1: {exc}")
        raise
    # Filter cakupan yang sama dengan orchestrator, supaya tanggal yang pasti
    # dibuang job tidak dijadikan scene.
    found = _drop_dates_barely_covering_aoi(found, bbox_wkt, job_id=0)
    dates = sorted({d for s in found if (d := _scene_date(s))}, reverse=True)

    def known(d: date) -> bool:
        if d not in scenes:
            return False
        st, src = scenes[d]
        if st == "FAILED":
            return int((src.get("sentinel1") or {}).get("attempts") or 0) >= MAX_S1_ATTEMPTS
        return True  # READY/PARTIAL/PROCESSING/DELETED

    candidates = [d for d in dates if not known(d)]
    # Hanya yang akan masuk `retensi` terbaru; scene yang lebih tua dari yang
    # sudah tersimpan akan langsung terhapus lagi, jadi tidak diunduh.
    top = sorted(set(kept) | set(candidates), reverse=True)[:retention]
    targets = sorted(d for d in top if d in candidates)
    mon.log(area_id, "DISCOVER", "OK",
            f"{len(dates)} tanggal S1 ditemukan sejak {date_from}, {len(targets)} baru diproses",
            found=[d.isoformat() for d in dates], targets=[d.isoformat() for d in targets])
    return targets


# ---------------------------------------------------------------------------
# 2-3. ingest + finalisasi scene
# ---------------------------------------------------------------------------

def _ingest(mon: LiveMonitor, area_id: int, dataset_id: int, dates: list[date]) -> list[date]:
    from etl.module5_orchestrator import LIVE_JOB_TYPE, run_dataset_job

    with mon._db.session() as sess:
        for d in dates:
            row = sess.scalar(select(LiveScene).where(
                LiveScene.area_id == area_id, LiveScene.scene_date == d))
            if row is None:
                row = LiveScene(area_id=area_id, dataset_id=dataset_id, scene_date=d,
                                source_status={}, metrics={}, interpretations={},
                                area_status={}, previews={}, deleted_files=[])
                sess.add(row)
            row.status = "PROCESSING"
            row.updated_at = _now()
        ds = sess.get(Dataset, dataset_id)
        ds.date_start = min(ds.date_start, dates[0])
        ds.date_end = max(ds.date_end, dates[-1])
        job = DatasetJob(dataset_id=dataset_id, job_type=LIVE_JOB_TYPE, status="QUEUED",
                         date_range_start=dates[0], date_range_end=dates[-1])
        sess.add(job)
        sess.flush()
        job_id = job.job_id

    mon.log(area_id, "INGEST", "STARTED",
            f"Unduh & proses {len(dates)} scene ({dates[0]} s/d {dates[-1]}), job {job_id}",
            job_id=job_id)
    try:
        run_dataset_job(mon._db, job_id)
    except Exception as exc:
        # Scene yang sebagian berkasnya sudah jadi tetap difinalisasi di bawah.
        mon.log(area_id, "INGEST", "FAILED", f"Job {job_id} berhenti: {exc}", job_id=job_id)
    with mon._db.session() as sess:
        job = sess.get(DatasetJob, job_id)
        job_status = job.status if job else "?"
    mon.log(area_id, "INGEST", "OK" if job_status == "COMPLETED" else "WARNING",
            f"Job {job_id} selesai dengan status {job_status}", job_id=job_id)

    done = []
    for d in dates:
        if finalize_scene(mon, area_id, d) in ("READY", "PARTIAL"):
            done.append(d)
    return done


def finalize_scene(mon: LiveMonitor, area_id: int, scene_date: date) -> str:
    """Metrik + status sumber + preview untuk satu scene. Kalimat kondisi
    ditulis reinterpret_all (butuh scene sebelumnya)."""
    from etl import live_metrics as lmx

    info = _area_files(mon, area_id)
    if info is None:
        return "FAILED"
    files = info
    inputs = lmx.scene_inputs(files.root, scene_date)
    metrics = lmx.compute_metrics(inputs, scene_date)
    status = lmx.source_status(inputs, metrics)

    with mon._db.session() as sess:
        row = sess.scalar(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.scene_date == scene_date))
        prev_attempts = int(((row.source_status or {}).get("sentinel1") or {}).get("attempts") or 0) if row else 0

    if status["sentinel1"]["status"] != "OK":
        status["sentinel1"]["attempts"] = prev_attempts + 1
        scene_status = "FAILED"
        previews = {}
        mon.log(area_id, "SCENE", "FAILED",
                f"Scene {scene_date}: Sentinel-1 tidak berhasil diproses "
                f"(percobaan {prev_attempts + 1}/{MAX_S1_ATTEMPTS})", scene_date=scene_date)
    else:
        previews = {}
        try:
            from etl.live_preview import render_scene_previews
            previews = render_scene_previews(files, scene_date, inputs)
        except Exception as exc:
            logger.exception("[LIVE] preview %s gagal", scene_date)
            mon.log(area_id, "PREVIEW", "FAILED", f"Preview {scene_date} gagal: {exc}",
                    scene_date=scene_date)
        failed = [k for k in ("modis", "gpm") if status[k]["status"] == "FAILED"]
        scene_status = "PARTIAL" if failed else "READY"
        mon.log(area_id, "SCENE", "OK" if not failed else "WARNING",
                f"Scene {scene_date} {scene_status}: "
                + ", ".join(f"{k}={v['status']}" for k, v in status.items()),
                scene_date=scene_date, source_status=status,
                previews=len(previews.get("items", {})) if previews else 0)

    with mon._db.session() as sess:
        row = sess.scalar(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.scene_date == scene_date))
        if row is not None:
            row.status = scene_status
            row.metrics = _jsonable(metrics)
            row.source_status = _jsonable(status)
            row.previews = _jsonable(previews)
            row.s1_product_ids = [
                next(iter(f.values())).name.rsplit("_", 3)[0] for f in inputs["s1"]
            ]
            row.updated_at = _now()
    return scene_status


def retry_failed_sources(mon: LiveMonitor, area_id: int, skip: set[date] | None = None,
                         only: date | None = None) -> list[date]:
    """Unduh ulang MODIS/GPM yang FAILED di scene tersimpan, lalu finalisasi
    ulang scene-nya."""
    from etl.module9_fusion import ensure_gpm_inputs_for_date, ensure_modis_inputs_for_date
    from etl.pipeline_logger import PipelineLogger

    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        rows = sess.scalars(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.deleted_at.is_(None),
            LiveScene.status.in_(("PARTIAL",) if only is None else ("PARTIAL", "READY")))).all()
        todo = [(r.scene_date, r.source_status or {}) for r in rows
                if (only is None or r.scene_date == only) and r.scene_date not in (skip or set())]
        dataset = sess.get(Dataset, a.dataset_id) if a and a.dataset_id else None
        if dataset is None:
            return []
        ds_id, ds_name, region_id, bbox = dataset.dataset_id, dataset.name, dataset.region_id, dataset.bbox_wkt

    bbox_tuple = shapely_wkt.loads(bbox).bounds
    plog = PipelineLogger(mon._db)
    dg.set_context(ds_id)  # jeda retry MODIS/GPM tampil di bar daerah ini
    redone = []
    for d, st in todo:
        for src, fn in (("modis", ensure_modis_inputs_for_date), ("gpm", ensure_gpm_inputs_for_date)):
            if (st.get(src) or {}).get("status") != "FAILED" and only is None:
                continue
            try:
                fn(mon._db, ds_id, ds_name, region_id, bbox_tuple, d, plog=plog)
                mon.log(area_id, "RETRY", "OK", f"{src.upper()} {d} dicoba ulang", scene_date=d)
            except Exception as exc:
                mon.log(area_id, "RETRY", "FAILED", f"{src.upper()} {d} gagal lagi: {exc}",
                        scene_date=d)
        finalize_scene(mon, area_id, d)
        redone.append(d)
    return redone


def reinterpret_all(mon: LiveMonitor, area_id: int) -> None:
    """Tulis ulang kalimat kondisi semua scene tersimpan, urut tanggal, dengan
    scene tersimpan sebelumnya sebagai pembanding."""
    from etl.live_interpret import area_status, interpret_scene

    with mon._db.session() as sess:
        rows = sess.scalars(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.deleted_at.is_(None),
            LiveScene.status.in_(("READY", "PARTIAL"))).order_by(LiveScene.scene_date)).all()
        prev = None
        for r in rows:
            interp = interpret_scene(r.metrics or {}, prev, r.source_status or {})
            r.interpretations = _jsonable(interp)
            r.area_status = _jsonable(area_status(interp))
            prev = r.metrics or {}


# ---------------------------------------------------------------------------
# 4. forecast
# ---------------------------------------------------------------------------

def refresh_forecast(mon: LiveMonitor, area_id: int) -> None:
    from etl.live_forecast import build_area_forecast

    with mon._db.session() as sess:
        rows = sess.scalars(select(LiveScene).where(
            LiveScene.area_id == area_id, LiveScene.deleted_at.is_(None),
            LiveScene.status.in_(("READY", "PARTIAL"))).order_by(LiveScene.scene_date)).all()
        series = [(r.scene_date, r.metrics or {}) for r in rows]
    fc = build_area_forecast(series)
    _set_area(mon, area_id, forecast=_jsonable(fc), forecast_updated_at=_now())
    mon.log(area_id, "FORECAST", "OK",
            f"Forecast dihitung dari {len(series)} scene, {fc.get('steps', 0)} langkah")


# ---------------------------------------------------------------------------
# util
# ---------------------------------------------------------------------------

def _area_files(mon: LiveMonitor, area_id: int) -> _LiveFiles | None:
    with mon._db.session() as sess:
        a = sess.get(LiveArea, area_id)
        info = mon._dataset_info(a.dataset_id if a else None)
    return _LiveFiles(*info) if info else None


def _update_dataset_size(mon: LiveMonitor, dataset_id: int, dataset_name: str) -> None:
    root = fm.get_dataset_root(dataset_id, dataset_name)
    size = sum(p.stat().st_size for p in root.rglob("*") if p.is_file()) if root.is_dir() else 0
    with mon._db.session() as sess:
        d = sess.get(Dataset, dataset_id)
        if d is not None:
            d.total_size_bytes = size
