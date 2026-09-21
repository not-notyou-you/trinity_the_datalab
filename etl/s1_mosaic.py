# etl/s1_mosaic.py
"""
Mosaik frame Sentinel-1 yang jatuh di tanggal yang sama.

MASALAH YANG DISELESAIKAN
CDSE memotong satu lintasan menjadi beberapa frame. AOI yang lebih panjang
dari satu frame (Jabodetabek, 0,8 derajat) karena itu tertutup DUA scene
dengan tanggal dan orbit yang sama, masing-masing menutup separuh AOI yang
saling melengkapi. Keduanya scene yang sah dan keduanya diproses penuh, tapi
nama berkas keluaran PREVIEW dan FUSION hanya memuat tanggal:

    preview/PROCESSED/colored/20250123_s1_vv.png
    fusion/hybrid/fusion_20250123_hybrid_processed.h5

sehingga scene yang selesai belakangan menimpa yang duluan. Di dataset
22_try6, frame dengan cakupan 68,7% ditimpa frame 51,7% hanya karena selesai
belakangan — separuh AOI hilang dari deliverable, dan 335 MB hasil kerja
dibuang.

Modul ini menyatukan frame-frame itu jadi satu raster per band SEBELUM
PREVIEW dan FUSION berjalan, jadi satu tanggal tetap menghasilkan satu
keluaran — tapi keluaran yang memuat seluruh AOI, bukan salah satu frame.

KEPUTUSAN
- `method="first"`: frame diurutkan dari cakupan valid terbesar ke terkecil
  (lihat `_coverage`), jadi di daerah tumpang tindih yang menang adalah frame
  yang datanya paling utuh. `method="last"` akan membuat hasilnya bergantung
  pada urutan penyelesaian thread — persis penyakit yang sedang diperbaiki.
- `use_highest_res=True`: dua frame satu lintasan bisa beda resolusi beberapa
  angka di belakang koma (9,141e-5 vs 9,092e-5 derajat di 22_try6). Memakai
  resolusi terhalus berarti tidak ada frame yang kehilangan detail.
- Resampling nearest: nilai backscatter tidak boleh dirata-rata dengan
  tetangganya hanya karena grid-nya digeser sepersekian piksel.
- Satu frame saja -> path aslinya dikembalikan apa adanya, tanpa menyalin.
  Mosaik satu frame cuma menggandakan gigabyte tanpa mengubah isi.

Keluarannya artefak antara, bukan tier: ditulis ke `_work/` dan disapu di
akhir job (`_sweep_scratch`). Yang tersimpan permanen tetap COG per scene —
mosaik bisa dibangun ulang dari sana kapan saja.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.merge import merge

logger = logging.getLogger(__name__)

MODULE = "S1_MOSAIC"


def _coverage(path: str) -> float:
    """Fraksi piksel valid sebuah raster, 0.0 kalau tidak terbaca.

    Dipakai mengurutkan frame, jadi pembacaannya sengaja di-decimate: yang
    dibutuhkan urutan, bukan angka presisi, dan membaca penuh dua raster
    8000x8800 hanya untuk mengurutkan dua berkas itu mahal tanpa alasan.
    """
    try:
        with rasterio.open(path) as src:
            out_h = min(src.height, 512)
            out_w = min(src.width, 512)
            data = src.read(1, out_shape=(1, out_h, out_w))
            if src.nodata is None or np.isnan(src.nodata):
                valid = np.isfinite(data)
            else:
                valid = np.isfinite(data) & (data != src.nodata)
            return float(valid.mean())
    except Exception:
        logger.exception("[%s] gagal membaca cakupan %s", MODULE, path)
        return 0.0


def mosaic_frames(
    frames: list[dict[str, str]], out_dir: Path, *, date_key: str, level: str
) -> dict[str, str]:
    """Satukan `frames` ({band: path} per scene) jadi satu {band: path}.

    Band diproses sendiri-sendiri: sebuah frame yang punya VV tapi VH-nya
    gagal ditulis tetap menyumbang VV-nya, alih-alih menggugurkan seluruh
    tanggal.

    Frame yang berkasnya tidak ada di disk dilewati dengan peringatan — itu
    kondisi yang mungkin (ekspor gagal setelah scene lain selesai) dan bukan
    alasan membuang frame yang sehat.

    Returns {band: path} — path mosaik untuk band yang punya lebih dari satu
    frame, path asli untuk yang cuma satu. Kosong kalau tidak ada satu pun
    berkas yang bisa dibaca.
    """
    by_band: dict[str, list[str]] = {}
    for frame in frames:
        for band, path in (frame or {}).items():
            if not path:
                continue
            if not Path(path).exists():
                logger.warning(
                    "[%s] %s %s: berkas %s band %s tidak ada, frame dilewati",
                    MODULE, date_key, level, path, band,
                )
                continue
            by_band.setdefault(band, []).append(path)

    out: dict[str, str] = {}
    for band, paths in by_band.items():
        if len(paths) == 1:
            out[band] = paths[0]
            continue
        # Cakupan terbesar lebih dulu supaya method="first" deterministik.
        ordered = sorted(paths, key=_coverage, reverse=True)
        # merge() menganggap semua sumber satu CRS dan tidak memeriksanya;
        # menyatukan CRS berbeda akan menaruh piksel di tempat yang salah
        # tanpa satu pun error. Yang dipakai CRS milik frame dengan cakupan
        # terbesar; frame ber-CRS lain dibuang dengan peringatan, bukan
        # direproyeksi diam-diam.
        ordered = _drop_foreign_crs(ordered, date_key=date_key, level=level, band=band)
        if len(ordered) == 1:
            out[band] = ordered[0]
            continue
        out_path = out_dir / f"{date_key}_{level}_{band}_mosaic.tif"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            out[band] = str(
                _merge_to(ordered, out_path, date_key=date_key, level=level, band=band)
            )
        except Exception:
            # Frame dengan cakupan terbesar dipakai apa adanya: setengah AOI
            # masih jauh lebih baik daripada tanggal ini hilang sama sekali,
            # dan pilihannya tetap deterministik (bukan "yang selesai
            # belakangan").
            logger.exception(
                "[%s] %s %s band %s: mosaik gagal, memakai frame dengan cakupan "
                "terbesar (%s)", MODULE, date_key, level, band, ordered[0],
            )
            out[band] = ordered[0]

    return out


def _merge_to(
    ordered: list[str], out_path: Path, *, date_key: str, level: str, band: str
) -> Path:
    with rasterio.open(ordered[0]) as first:
        profile = first.profile.copy()

    profile.update(driver="GTiff", nodata=float("nan"), count=1, compress="deflate")
    merge(
        sources=ordered,
        nodata=float("nan"),
        method="first",
        use_highest_res=True,
        resampling=Resampling.nearest,
        # Default 64 MB memecah mosaik Jawa (32k x 36k float32 = 4,6 GB) jadi
        # ratusan potongan: ~20 menit per band di 26_JAWA. Lebih parah, di
        # rasterio 1.4.3 hasil merge berpotongan TIDAK sama dengan merge utuh
        # (piksel bergeser di batas potongan). 8 GB cukup untuk satu band AOI
        # Jawa dalam satu potongan -> cepat dan identik dengan merge di memori.
        mem_limit=8192,
        dst_path=str(out_path),
        dst_kwds={k: profile[k] for k in ("driver", "compress", "nodata") if k in profile},
    )
    with rasterio.open(out_path) as dst:
        logger.info(
            "[%s] %s %s band %s: %d frame -> %dx%d (%.1f%% valid)",
            MODULE, date_key, level, band, len(ordered), dst.height, dst.width,
            _coverage(str(out_path)) * 100,
        )
    return out_path


def _crs_of(path: str):
    with rasterio.open(path) as src:
        return src.crs


def _drop_foreign_crs(
    ordered: list[str], *, date_key: str, level: str, band: str
) -> list[str]:
    """Sisakan frame yang CRS-nya sama dengan frame pertama (cakupan
    terbesar). `ordered` harus sudah urut cakupan menurun."""
    keep_crs = _crs_of(ordered[0])
    kept = [p for p in ordered if _crs_of(p) == keep_crs]
    dropped = [p for p in ordered if p not in kept]
    if dropped:
        logger.warning(
            "[%s] %s %s band %s: %d frame dibuang karena CRS-nya bukan %s: %s",
            MODULE, date_key, level, band, len(dropped), keep_crs, dropped,
        )
    return kept
