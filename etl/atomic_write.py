# etl/atomic_write.py
"""Tulis berkas keluaran lewat berkas sementara, lalu pindahkan sekali jalan.

Menulis langsung ke path final berarti path itu sempat berisi berkas separuh
jadi selama penulisan berlangsung. Konsumen mana pun yang membacanya saat itu
-- tahap berikutnya, proses kedua yang menggarap job yang sama, atau run
berikutnya yang menganggap "berkas ada = berkas beres" -- mendapat data rusak
tanpa ada yang tahu. Itu yang terjadi pada cache granule dataset 31/32.

os.replace bersifat atomik di Windows maupun POSIX: path final selalu berisi
versi lama yang utuh atau versi baru yang utuh, tidak pernah di antaranya.
Nama sementaranya unik per proses+thread supaya dua penulis berbarengan tidak
malah berebut berkas sementara yang sama.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def atomic_path(final_path: Path | str) -> Iterator[Path]:
    """Berikan path sementara untuk ditulis; dipindahkan ke `final_path` saat
    blok selesai tanpa error.

    Kalau bloknya melempar, berkas sementaranya dibuang dan `final_path` tidak
    tersentuh -- berkas lama yang masih utuh tetap di tempatnya, dan tidak ada
    berkas separuh jadi yang tertinggal untuk dipercaya run berikutnya.
    """
    final = Path(final_path)
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp = final.with_name(
        f"{final.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    tmp.unlink(missing_ok=True)
    try:
        yield tmp
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, final)
