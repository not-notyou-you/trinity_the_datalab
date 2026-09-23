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
import shutil
import threading
import time
from pathlib import Path
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

# Timeout (connect, read) untuk requests: read 120 s cukup untuk server lambat,
# tapi tidak lagi 300-600 s per jeda.
REQUEST_TIMEOUT = (30, 120)
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
            else float(os.getenv("DOWNLOAD_MIN_KBPS", "50")) * 1024
        )
        self.window_s = (
            window_s if window_s is not None
            else float(os.getenv("DOWNLOAD_STALL_WINDOW_S", "180"))
        )
        self._clock = clock
        self._window_start = clock()
        self._window_bytes = 0

    def update(self, nbytes: int) -> None:
        self._window_bytes += nbytes
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
