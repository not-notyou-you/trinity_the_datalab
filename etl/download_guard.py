# etl/download_guard.py
"""
Pengaman bersama untuk tahap download (M1 Sentinel-1, M7 MODIS, M8 GPM).

1. StallGuard — `timeout=` requests hanya membatasi jeda antar-paket, jadi
   koneksi yang masih mengalir beberapa KB/s tidak pernah timeout dan bisa
   menahan satu run berjam-jam. Guard ini membatalkan attempt kalau laju
   rata-rata dalam satu jendela waktu di bawah ambang, sehingga logika retry
   (dan resume Range di M1) yang sudah ada mengambil alih.

2. find_reusable_file / adopt_file — granule MODIS/GPM dan ZIP Sentinel-1
   identik antar-dataset (nama file = identitas produk). Sebelum mengunduh
   ulang dari NASA/ESA, cari salinan lengkap di dataset lain lalu hardlink
   (tanpa tambahan ruang disk; fallback copy kalau beda volume).
"""
from __future__ import annotations

import logging
import os
import random
import shutil
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

# Timeout (connect, read) untuk requests. 75 s: titik tengah antara 120 s lama
# (terlalu lambat mendeteksi koneksi mati) dan 60 s yang sempat dicoba
# (terlalu ketat begitu S1_PARALLEL_DOWNLOADS naik lagi ke 3 -- tiga transfer
# berbagi bandwidth yang sama, jadi laju per-koneksi yang sehat pun turun).
REQUEST_TIMEOUT = (30, 75)
# Chunk kecil supaya StallGuard dan callback progress sering diperiksa.
CHUNK_SIZE = 1024 * 1024


class DownloadStalledError(TimeoutError):
    """Laju download di bawah ambang terlalu lama. Subclass TimeoutError supaya
    tertangkap jalur retry yang sudah ada."""


class StallGuard:
    def __init__(
        self,
        min_bytes_per_sec: float | None = None,
        window_s: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.min_bps = (
            min_bytes_per_sec
            if min_bytes_per_sec is not None
            # 75 KB/s, bukan 100: dengan S1_PARALLEL_DOWNLOADS=3 tiga transfer
            # berbagi bandwidth yang sama, jadi laju per-koneksi yang SEHAT pun
            # ikut turun dibanding satu koneksi sendirian. Ambang 100 KB/s pas
            # di 2 paralel sempat memicu retry pada koneksi yang sebenarnya
            # cuma melambat karena berbagi jalur, bukan macet.
            else float(os.getenv("DOWNLOAD_MIN_KBPS", "75")) * 1024
        )
        self.window_s = (
            window_s if window_s is not None
            # Scene S1 sehat (~2 GB) yang berhasil di log konsisten mengalir
            # di kisaran MB/s dan selesai dalam 130-600 s total. 90 s -- titik
            # tengah antara 180 s lama (macet 3 menit sebelum ketahuan) dan
            # 60 s yang sempat dicoba (terlalu ketat begitu paralelisme naik
            # lagi ke 3 dan laju per-koneksi wajar ikut turun).
            else float(os.getenv("DOWNLOAD_STALL_WINDOW_S", "90"))
        )
        self._clock = clock
        self._window_start = clock()
        self._window_bytes = 0

    def update(self, nbytes: int) -> None:
        self._window_bytes += nbytes
        note_activity()  # byte mengalir = ada kemajuan (dataset dari set_context)
        elapsed = self._clock() - self._window_start
        if elapsed < self.window_s:
            return
        rate = self._window_bytes / elapsed
        if rate < self.min_bps:
            raise DownloadStalledError(
                f"download macet: {rate / 1024:.1f} KB/s selama {elapsed:.0f} s "
                f"(minimum {self.min_bps / 1024:.0f} KB/s)"
            )
        self._window_start = self._clock()
        self._window_bytes = 0


def find_reusable_file(
    file_name: str,
    patterns: Iterable[str],
    exclude: Path,
    root: Path,
    validate: Callable[[Path], bool] | None = None,
    fs_path: Callable[[Path], Path] = lambda p: p,
) -> Path | None:
    """Cari salinan lengkap `file_name` di bawah `root` memakai glob `patterns`
    (masing-masing berisi `{name}`). `exclude` = path tujuan sendiri.
    `fs_path` membungkus path sebelum akses disk (mis. prefix long-path
    Windows untuk path > 260 karakter)."""
    if not root.exists():
        return None
    exclude_resolved = exclude.resolve()
    for pattern in patterns:
        for candidate in root.glob(pattern.format(name=file_name)):
            try:
                if (
                    candidate.resolve() == exclude_resolved
                    or fs_path(candidate).stat().st_size <= 0
                ):
                    continue
                if validate is not None and not validate(candidate):
                    continue
            except OSError:
                continue
            return candidate
    return None


def adopt_file(src: Path, dst: Path) -> str:
    """Hardlink `src` ke `dst` (fallback copy). Mengembalikan 'link' / 'copy'."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    # Unik per proses+thread: nama tetap membuat dua pengadopsi berbarengan
    # saling menghapus dan menimpa berkas sementara yang sama.
    tmp = dst.with_name(f"{dst.name}.{os.getpid()}.{threading.get_ident()}.adopt")
    tmp.unlink(missing_ok=True)
    try:
        os.link(src, tmp)
        how = "link"
    except OSError:
        shutil.copy2(src, tmp)
        how = "copy"
    os.replace(tmp, dst)
    return how


def reuse_granule(out_path: Path, source: str, root: Path, log_prefix: str) -> bool:
    """Isi `out_path` dari cache granule dataset lain kalau tersedia."""
    from etl.folder_manager import GRANULE_CACHE_DIRNAME

    found = find_reusable_file(
        out_path.name, [f"*/{GRANULE_CACHE_DIRNAME}/{source}/{{name}}"], out_path, root,
    )
    if found is None:
        return False
    how = adopt_file(found, out_path)
    logger.info(
        "%s pakai ulang granule dari dataset lain (%s): %s", log_prefix, how, found,
    )
    return True


# ---------------------------------------------------------------------------
# Batas koneksi global per sumber + prioritas Dataset Saya di atas Live
# ---------------------------------------------------------------------------
#
# S1_PARALLEL_DOWNLOADS (module5_orchestrator) berlaku PER JOB: dua dataset
# yang jalan bersamaan + satu siklus Live sudah 9 koneksi CDSE, padahal CDSE
# cuma mengizinkan 4 unduhan paralel per akun -- sisanya dijawab 429 dan scene
# sehat gagal setelah jatah retry habis. Semaphore di sini batas atas lintas
# job dalam satu proses. Lintas proses tidak perlu: JobLock sudah memastikan
# satu job/daerah hanya dikerjakan satu proses.

class PrioritySemaphore:
    """Semaphore dengan dua kelas penunggu. Penunggu prioritas rendah (Live)
    baru boleh mengambil slot kalau tidak ada penunggu prioritas normal
    (Dataset Saya). Slot yang sudah dipegang tidak direbut."""

    def __init__(self, value: int) -> None:
        self._cond = threading.Condition()
        self._value = max(1, value)
        self._waiting_normal = 0

    def acquire(self, low: bool = False) -> None:
        with self._cond:
            if not low:
                self._waiting_normal += 1
            try:
                while self._value <= 0 or (low and self._waiting_normal > 0):
                    self._cond.wait()
                self._value -= 1
            finally:
                if not low:
                    self._waiting_normal -= 1
            # Penunggu rendah yang tadi tertahan oleh penunggu normal (bukan
            # oleh slot) harus dibangunkan begitu antrean normal kosong.
            if self._value > 0:
                self._cond.notify_all()

    def release(self) -> None:
        with self._cond:
            self._value += 1
            self._cond.notify_all()


_priority = threading.local()


def is_low_priority() -> bool:
    return getattr(_priority, "low", False)


def set_low_priority(flag: bool) -> None:
    """Untuk thread worker yang baru lahir: masuk kelas prioritas job-nya."""
    _priority.low = flag


@contextmanager
def low_priority(flag: bool = True):
    """Tandai thread ini sebagai kerja prioritas rendah (Live) selama blok.
    Thread worker (pool download, pipeline) tidak mewarisi thread-local, jadi
    masing-masing harus masuk sendiri."""
    prev = is_low_priority()
    _priority.low = flag
    try:
        yield
    finally:
        _priority.low = prev


CDSE = "CDSE"
LAADS = "LAADS"
GESDISC = "GESDISC"

_SOURCE_SLOTS = {
    # 3, bukan 4: menyisakan satu slot akun CDSE untuk browser/sesi lain.
    CDSE: PrioritySemaphore(int(os.getenv("CDSE_MAX_CONNECTIONS", "3"))),
    LAADS: PrioritySemaphore(int(os.getenv("LAADS_MAX_CONNECTIONS", "4"))),
    GESDISC: PrioritySemaphore(int(os.getenv("GESDISC_MAX_CONNECTIONS", "4"))),
}


@contextmanager
def source_slot(source: str):
    """Pegang satu slot koneksi `source` selama transfer berlangsung."""
    sem = _SOURCE_SLOTS[source]
    sem.acquire(low=is_low_priority())
    try:
        yield
    finally:
        sem.release()


# ---------------------------------------------------------------------------
# Jeda retry bersama (M1/M7/M8)
# ---------------------------------------------------------------------------

# Batas atas Retry-After: server yang meminta menunggu berjam-jam lebih baik
# dianggap gagal di run ini daripada menahan worker (dan slot koneksinya).
MAX_RETRY_AFTER_S = float(os.getenv("DOWNLOAD_MAX_RETRY_AFTER_S", "300"))
# Kode yang berarti "server sibuk / menjatah", bukan transfer rusak.
THROTTLE_STATUSES = frozenset({429, 503})


class DownloadCancelled(Exception):
    """Job dibatalkan selama menunggu jeda retry. Sengaja bukan OSError supaya
    tidak ditangkap jalur retry jaringan."""


def retry_delay(attempt: int, retry_after: str | None = None) -> float:
    """Retry-After dari server (dibatasi MAX_RETRY_AFTER_S) kalau ada; kalau
    tidak, backoff eksponensial + jitter supaya worker yang gagal berbarengan
    tidak menembak ulang di detik yang sama persis."""
    if retry_after is not None:
        try:
            delay = max(1.0, float(retry_after))
        except ValueError:
            delay = 15.0
        return min(delay, MAX_RETRY_AFTER_S)
    return min(60.0, 2.0 ** attempt) + random.uniform(0, 1.0)


def sleep_or_cancel(delay: float, cancel_event: threading.Event | None = None) -> None:
    """Tidur `delay` detik, tapi bangun dan lempar DownloadCancelled begitu
    job dibatalkan -- jeda 429 bisa sampai 5 menit."""
    if cancel_event is None:
        time.sleep(delay)
        return
    if cancel_event.wait(delay):
        raise DownloadCancelled("job dibatalkan saat menunggu retry")


# ---------------------------------------------------------------------------
# Kegagalan otentikasi NASA Earthdata
# ---------------------------------------------------------------------------

NASA_AUTH_MESSAGE = "NASA_EARTHDATA_TOKEN tidak valid atau kedaluwarsa"


class NasaAuthError(RuntimeError):
    """401/403 dari LAADS/GES DISC. Token dipakai bersama semua job, jadi
    mengulang request tidak ada gunanya -- gagal cepat."""


_auth_failures: dict[str, tuple[float, str]] = {}
_auth_lock = threading.Lock()


def record_auth_failure(source: str, message: str) -> None:
    """Catat 401/403 terakhir per sumber. Pemanggil tingkat modul (mis.
    ensure_*_inputs_for_date) menelan exception per sumber, jadi siklus Live
    membaca catatan ini untuk menampilkannya di live_events."""
    with _auth_lock:
        _auth_failures[source] = (time.time(), message)


def auth_failures_since(ts: float) -> dict[str, str]:
    with _auth_lock:
        return {s: m for s, (t, m) in _auth_failures.items() if t >= ts}


def raise_for_nasa_auth(resp, source: str, url: str) -> None:
    if resp.status_code in (401, 403):
        msg = f"{NASA_AUTH_MESSAGE} (HTTP {resp.status_code} dari {source})"
        record_auth_failure(source, msg)
        raise NasaAuthError(f"{msg}: {url}")


# ---------------------------------------------------------------------------
# Hambatan yang sedang terjadi, untuk loading bar di UI
# ---------------------------------------------------------------------------
#
# Jeda retry (429/503, koneksi putus, login ulang) dulu cuma terlihat di log
# server: di UI bar diam di persen yang sama sampai 5 menit dan tampak macet.
# Thread unduhan menandai dataset yang sedang dikerjakannya (set_context),
# lalu setiap jeda dicatat per dataset sampai waktunya habis. API membaca
# catatan ini; tidak ada yang perlu dibersihkan karena kedaluwarsa sendiri.

_ctx = threading.local()
_waits: dict[int, dict] = {}
_waits_lock = threading.Lock()

SOURCE_LABELS = {CDSE: "CDSE (Sentinel-1)", LAADS: "LAADS (MODIS)", GESDISC: "GES DISC (GPM)"}


def set_context(dataset_id: int | None) -> None:
    """Tandai thread ini sedang bekerja untuk `dataset_id`."""
    _ctx.dataset_id = dataset_id


def current_context() -> int | None:
    return getattr(_ctx, "dataset_id", None)


def note_wait(source: str, delay: float, reason: str,
              attempt: int | None = None, max_attempts: int | None = None) -> None:
    ds = current_context()
    if ds is None:
        return
    now = time.time()
    with _waits_lock:
        _waits[ds] = {
            "source": source, "source_label": SOURCE_LABELS.get(source, source),
            "reason": reason, "seconds": round(delay), "until": now + delay,
            "attempt": attempt, "max_attempts": max_attempts,
        }


def current_wait(dataset_id: int | None) -> dict | None:
    """Jeda yang sedang berlangsung untuk dataset ini, atau None."""
    if dataset_id is None:
        return None
    with _waits_lock:
        w = _waits.get(dataset_id)
        if w is None:
            return None
        left = w["until"] - time.time()
        if left <= 0:
            _waits.pop(dataset_id, None)
            return None
        return {**{k: v for k, v in w.items() if k != "until"}, "remaining_s": round(left)}


def backoff_wait(source: str, attempt: int, reason: str, *,
                 retry_after: str | None = None, max_attempts: int | None = None,
                 cancel_event: threading.Event | None = None) -> None:
    """retry_delay + catat ke UI + tidur (bisa dibatalkan)."""
    delay = retry_delay(attempt, retry_after)
    note_wait(source, delay, reason, attempt, max_attempts)
    sleep_or_cancel(delay, cancel_event)


def clear_auth_failure(source: str) -> None:
    """Request NASA berhasil: token sudah sehat lagi."""
    with _auth_lock:
        _auth_failures.pop(source, None)


def active_auth_failures() -> dict[str, str]:
    """401/403 yang belum dipulihkan request sukses sesudahnya."""
    with _auth_lock:
        return {s: m for s, (_, m) in _auth_failures.items()}


# ---------------------------------------------------------------------------
# Detak kemajuan per dataset (durasi & "tidak ada kemajuan sejak ...")
# ---------------------------------------------------------------------------
#
# Dua sumber detak: setiap event pipeline (PipelineLogger.log_event -- awal &
# akhir tiap tahap, progres unduhan) dan setiap potongan byte yang mengalir
# (StallGuard.update). Kalkulasi/kalibrasi panjang tanpa event tetap dianggap
# wajar sampai PROGRESS_STALL_AFTER_S (default 30 menit).

PROGRESS_STALL_AFTER_S = float(os.getenv("PROGRESS_STALL_AFTER_S", "1800"))
_activity: dict[int, float] = {}
_activity_lock = threading.Lock()


def note_activity(dataset_id: int | None = None) -> None:
    ds = dataset_id if dataset_id is not None else current_context()
    if ds is None:
        return
    with _activity_lock:
        _activity[ds] = time.time()


def last_activity(dataset_id: int | None) -> float | None:
    if dataset_id is None:
        return None
    with _activity_lock:
        return _activity.get(dataset_id)


def timing(dataset_id: int | None, started: float | None, *extra: float | None) -> dict:
    """{elapsed_s, idle_s, stalled, last_activity_at} untuk loading bar.
    `started` = awal run/tahap (epoch); `extra` = cap waktu lain yang juga
    menandakan kemajuan (mis. event Live terakhir). Semua dihitung di server
    supaya jam browser yang meleset tidak memengaruhi."""
    now = time.time()
    marks = [t for t in (started, last_activity(dataset_id), *extra) if t]
    last = max(marks) if marks else None
    idle = now - last if last else None
    return {
        "elapsed_s": round(now - started) if started else None,
        "idle_s": round(idle) if idle is not None else None,
        "stalled": bool(idle is not None and idle >= PROGRESS_STALL_AFTER_S),
        "last_activity_at": last,
    }
