# etl/job_lock.py
"""Lock lintas-PROSES untuk job dataset.

_active_threads di dataset_manager cuma melindungi dalam satu proses Python.
Itu tidak cukup: `uvicorn --reload` menjalankan proses baru sementara proses
lama masih menutup diri, dan proses baru menjalankan recover_interrupted_jobs()
lalu me-resume job yang MASIH dikerjakan proses lama. Dua proses yang menggarap
job yang sama saling menimpa berkas — dataset 31/32 (21 Sep 2026) berakhir
dengan granule GPM korup dan dua event SCENE_PIPELINE COMPLETED parsial untuk
tanggal yang sama.

Lock-nya lock berkas tingkat OS, bukan penanda di database atau berkas PID,
karena satu-satunya properti yang benar-benar dibutuhkan adalah: kalau proses
pemegangnya mati dengan cara apa pun (exit normal, crash, taskkill), locknya
HARUS lepas sendiri. Kernel yang menjamin itu. Berkas PID tidak: PID yang
tertinggal dari proses yang di-kill tidak bisa dibedakan dari PID yang masih
hidup tanpa menebak, dan PID dipakai ulang OS.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

LOCK_DIRNAME = "_job_locks"

if os.name == "nt":
    import msvcrt

    def _try_lock(fd: int) -> bool:
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def _unlock(fd: int) -> None:
        try:
            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
else:
    import fcntl

    def _try_lock(fd: int) -> bool:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(fd: int) -> None:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass


class JobLock:
    """Kepemilikan eksklusif satu job_id lintas proses.

    Dipakai lewat `acquire()` yang mengembalikan None kalau proses lain sudah
    memegangnya. Pemanggil WAJIB memanggil `release()` saat job selesai --
    kalau prosesnya mati duluan, OS yang melepas.
    """

    def __init__(self, job_id: int, lock_dir: Path) -> None:
        self._job_id = job_id
        self._path = lock_dir / f"job-{job_id}.lock"
        self._fd: int | None = None

    @classmethod
    def acquire(cls, job_id: int, lock_dir: Path) -> "JobLock | None":
        lock = cls(job_id, lock_dir)
        try:
            lock_dir.mkdir(parents=True, exist_ok=True)
            fd = os.open(lock._path, os.O_RDWR | os.O_CREAT, 0o644)
        except OSError:
            # Tanpa lock lebih baik lanjut daripada job tidak pernah jalan:
            # disk penuh atau folder read-only bukan alasan menghentikan
            # pipeline, dan race-nya cuma terjadi saat ada dua proses.
            logger.warning(
                "[JOBLOCK] job_id=%d tidak bisa membuka berkas lock, lanjut tanpa lock",
                job_id, exc_info=True,
            )
            return lock

        if not _try_lock(fd):
            os.close(fd)
            return None

        lock._fd = fd
        try:
            # Lebar tetap supaya isi dari pemegang sebelumnya selalu tertimpa
            # penuh; tidak di-truncate karena byte 0 sedang dikunci. Isinya
            # murni untuk diagnosa, bukan bagian dari mekanisme lock.
            os.write(fd, f"pid={os.getpid()}".ljust(32)[:32].encode())
        except OSError:
            pass
        return lock

    def release(self) -> None:
        if self._fd is None:
            return
        _unlock(self._fd)
        try:
            os.close(self._fd)
        except OSError:
            pass
        self._fd = None

    def __repr__(self) -> str:
        return f"<JobLock job_id={self._job_id} held={self._fd is not None}>"
