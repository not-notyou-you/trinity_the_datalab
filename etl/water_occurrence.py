# etl/water_occurrence.py
"""
Layer frekuensi air permanen — referensi "selebar apa air ini biasanya".

Pasangan etl/land_mask.py, dengan peran yang berbeda dan tidak saling
menggantikan:

    land_distance.tif      membuang LAUT              batas geometris
    water_occurrence.tif   mengukur air DARATAN       referensi historis

Sumbernya JRC Global Surface Water (Pekel dkk., 2016), layer `occurrence`:
persentase 0-100 seberapa sering tiap piksel tampak berair sepanjang
1984-2021, dari ~4 juta citra Landsat.

PENTING — ini BUKAN mask untuk membuang sungai dan danau. Luapan sungai dan
danau justru kejadian yang ingin ditangkap penelitian ini, jadi membuangnya
berarti membuang sinyalnya sendiri. Perannya baseline:

    sungai lebar normal   occurrence ~95   berair, tapi bukan anomali
    sawah bantaran banjir occurrence ~5    berair, dan itu anomali

Selisih antara "terdeteksi air hari ini" dan "occurrence rendah" adalah
luapan. Itu memberi definisi "terlalu lebar dibanding biasanya" sebuah
pembanding historis 38 tahun, bukan ambang yang dipilih sendiri.

Keterbatasan yang harus disadari konsumen, dan sengaja dicatat di metadata:

    - sumbernya 30 m, grid fusion ~10 m: nilai dinaikkan dengan NEAREST,
      jadi tidak ada nilai antara yang dikarang; satu piksel JRC menutupi
      sekitar 3x3 piksel fusion
    - datanya berhenti 2021: normalisasi kali, betonisasi, dan sedimentasi
      setelah itu tidak terekam
    - tambak dan sawah irigasi ikut ber-occurrence tinggi meski bukan
      badan air alami

Lisensi sumber bebas dipakai dengan atribusi (EC JRC / Google).
"""
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
from rasterio.transform import Affine

from etl.land_mask import write_cog

logger = logging.getLogger(__name__)

MODULE = "WATER_OCCURRENCE"

WATER_OCCURRENCE_STEM = "water_occurrence"
REFERENCE_DIR = Path("data") / "reference" / "jrc_gsw"

# JRC memakai 255 sebagai "tidak ada data" pada skala 0-100. Dipertahankan apa
# adanya sebagai nodata supaya "tidak pernah teramati" tidak tertukar dengan
# "teramati dan tidak pernah berair" — keduanya nol kalau dipaksa jadi 0.
NODATA = 255

SOURCE_URL_TEMPLATE = (
    "https://storage.googleapis.com/global-surface-water/downloads2021/"
    "occurrence/occurrence_{tile}v1_4_2021.tif"
)


def tiles_for_bbox(bbox: tuple[float, float, float, float]) -> list[str]:
    """Nama tile JRC yang menutupi bbox. Tile berukuran 10x10 derajat dan
    dinamai dari sudut KIRI-ATAS-nya, jadi batas lintangnya dibulatkan ke atas
    sementara bujurnya ke bawah."""
    min_lon, min_lat, max_lon, max_lat = bbox
    names = []
    lon0 = int(np.floor(min_lon / 10.0) * 10)
    lon1 = int(np.floor(max_lon / 10.0) * 10)
    lat0 = int(np.ceil(max_lat / 10.0) * 10)
    lat1 = int(np.ceil(min_lat / 10.0) * 10)
    for lon in range(lon0, lon1 + 1, 10):
        for lat in range(lat1, lat0 + 1, 10):
            lon_s = f"{abs(lon)}{'E' if lon >= 0 else 'W'}"
            lat_s = f"{abs(lat)}{'N' if lat >= 0 else 'S'}"
            names.append(f"{lon_s}_{lat_s}")
    return names


def local_tile_path(tile: str, reference_dir: Path = REFERENCE_DIR) -> Path:
    return Path(reference_dir) / f"occurrence_{tile}.tif"


def build_occurrence(
    bbox: tuple[float, float, float, float],
    transform: Affine,
    shape: tuple[int, int],
    reference_dir: Path = REFERENCE_DIR,
) -> tuple[np.ndarray, dict]:
    """Proyeksikan tile JRC ke grid fusion dataset.

    NEAREST dipakai, bukan bilinear, dan itu keputusan sadar: naik dari 30 m ke
    10 m berarti setiap nilai antara yang dihasilkan interpolasi adalah angka
    yang tidak pernah diukur siapa pun. Untuk besaran yang akan dipakai sebagai
    pembanding historis dalam penelitian, mengulang nilai asli lebih jujur
    daripada memuluskannya. Bonusnya, nodata 255 tidak merembes ke tetangganya.
    """
    import rasterio
    from rasterio.warp import Resampling, reproject

    tiles = tiles_for_bbox(bbox)
    out = np.full(shape, NODATA, dtype=np.uint8)
    used, missing = [], []

    for tile in tiles:
        path = local_tile_path(tile, reference_dir)
        if not path.exists():
            missing.append(tile)
            continue
        with rasterio.open(path) as src:
            chunk = np.full(shape, NODATA, dtype=np.uint8)
            reproject(
                source=rasterio.band(src, 1),
                destination=chunk,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs="EPSG:4326",
                src_nodata=NODATA,
                dst_nodata=NODATA,
                resampling=Resampling.nearest,
            )
        # Tile bersebelahan tidak tumpang tindih, jadi menimpa hanya di tempat
        # yang masih kosong sudah cukup dan tidak ada nilai sah yang tertimpa.
        fill = (out == NODATA) & (chunk != NODATA)
        out[fill] = chunk[fill]
        used.append(tile)
        del chunk

    valid = out != NODATA
    n_valid = int(np.count_nonzero(valid))
    note = {
        "tiles_used": used,
        "tiles_missing": missing,
        "resampling": "nearest",
        "source_resolution_m": 30,
        "nodata": NODATA,
        "pixels_total": int(out.size),
        "pixels_valid": n_valid,
        "pct_nodata": round((out.size - n_valid) / out.size * 100.0, 4),
    }
    if n_valid:
        vals = out[valid]
        for thr in (50, 80, 90):
            n = int(np.count_nonzero(vals >= thr))
            note[f"pct_occurrence_ge_{thr}"] = round(n / out.size * 100.0, 4)
    return out, note


def render_preview(data: np.ndarray, path: Path, max_side: int = 1024) -> Path:
    """PNG pemeriksaan cepat: makin sering berair makin biru, nodata abu.

    Sequential, bukan divergen — occurrence adalah besaran berurut 0-100 tanpa
    titik tengah yang berarti, kebalikan dari land_distance yang justru
    dua-kutub. Memakai divergen di sini akan mengarang batas di 50% yang tidak
    punya arti fisik apa pun.
    """
    from PIL import Image

    step = max(1, int(np.ceil(max(data.shape) / max_side)))
    d = data[::step, ::step]

    valid = d != NODATA
    frac = np.where(valid, np.clip(d, 0, 100), 0).astype(np.float32) / 100.0
    rgb = np.zeros((*d.shape, 3), np.uint8)
    rgb[..., 0] = np.where(valid, (240 - frac * 230), 128).astype(np.uint8)
    rgb[..., 1] = np.where(valid, (240 - frac * 150), 128).astype(np.uint8)
    rgb[..., 2] = np.where(valid, (240 - frac * 20), 128).astype(np.uint8)

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.fromarray(rgb).save(path)
    return path


def write_manifest(path: Path, note: dict, extra: dict | None = None) -> Path:
    """Asal-usul layer occurrence, sejajar dengan manifest land_distance."""
    import json

    payload = {
        "layer": WATER_OCCURRENCE_STEM,
        "semantics": (
            "percent of time (1984-2021) the pixel was observed as water; "
            "255 = no data"
        ),
        "role": (
            "baseline for normal water extent — flood is water detected today "
            "where occurrence is LOW"
        ),
        "not_a_filter": (
            "raw and fusion data are untouched; rivers and lakes must NOT be "
            "masked out because their overflow is the signal being studied"
        ),
        "caveats": [
            "source is 30 m, upsampled to the fusion grid with nearest — one "
            "source pixel covers roughly 3x3 fusion pixels",
            "record ends in 2021; river normalisation and sedimentation after "
            "that are not captured",
            "fish ponds and irrigated paddy also score high; they are managed "
            "water, not natural water bodies",
        ],
        "source": {
            "name": "JRC Global Surface Water v1.4 (2021), occurrence",
            "citation": "Pekel et al. (2016), Nature 540:418-422",
            "url": SOURCE_URL_TEMPLATE,
            "license": "free to use with attribution (EC JRC / Google)",
            "local_dir": str(REFERENCE_DIR),
        },
        "statistics": note,
    }
    payload.update(extra or {})
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def write_occurrence(
    data: np.ndarray, path: Path, transform: Affine, note: dict
) -> Path:
    """Tulis sebagai COG ber-tile, memakai penulis yang sama dengan land_mask
    supaya kedua layer referensi punya bentuk berkas yang identik."""
    tags = dict(note)
    tags.update(
        {
            "source": "JRC Global Surface Water v1.4 (2021), occurrence",
            "source_citation": "Pekel et al. (2016), Nature 540:418-422",
            "source_license": "free to use with attribution (EC JRC / Google)",
            "generated_by": "etl/water_occurrence.py",
            "semantics": "percent of time (1984-2021) the pixel was observed as water; 255 = no data",
            "note": (
                "baseline for normal water extent — NOT a mask; rivers and "
                "lakes must stay in the data because their overflow is the signal"
            ),
        }
    )
    return write_cog(
        data,
        path,
        transform,
        tags=tags,
        nodata=NODATA,
        description="JRC surface water occurrence 1984-2021 (%)",
    )
