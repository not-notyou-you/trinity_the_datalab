# etl/module7_modis_download.py
"""
Downloads NASA LAADS DAAC MODIS products, reprojects/crops them to the
dataset AOI, and writes one GeoTIFF per band per day for lineage tracking.

Dua produk berbeda diambil di sini:

  MCDWD_L3_F2_NRT (250 m)  -> band FLOOD, langsung dari subdataset
                              "Flood 2-Day 250m" (komposit 2 hari).
  MOD09A1         (500 m)  -> band NDVI dan NDWI, dihitung dari surface
                              reflectance komposit 8 hari:
                                  NDVI = (b02_NIR   - b01_red)   / (b02 + b01)
                                  NDWI = (b04_green - b02_NIR)   / (b04 + b02)
                              piksel awan/bayangan/cirrus (QA state) dibuang
                              jadi NaN. Kalau komposit periode itu belum
                              terbit, jatuh balik ke MOD09GA_NRT harian.

NDWI di sini adalah formulasi McFeeters (green/NIR) yang menyorot badan air
terbuka — bukan NDWI Gao (NIR/SWIR) yang mengukur kelembapan vegetasi.
Pipeline ini soal banjir, jadi indeks air permukaan yang relevan.

Output ditulis ke data/datasets/{id}_{slug}/{YYYYMMDD}/silver/modis/ dan
granule mentahnya di-cache di _granule_cache/modis/. Semuanya adalah input fusion
(dikonsumsi module9_fusion.py lewat tier GOLD), bukan deliverable akhir.

Kegagalan satu produk tidak menjatuhkan produk lain: kalau reflectance hari
itu tidak tersedia (MOD09A1 maupun MOD09GA) tapi MCDWD ada, hari itu tetap
menghasilkan FLOOD dan cuma kehilangan NDVI/NDWI.

LEVEL PEMROSESAN (DOCS/ETL.md, "MODIS Pipeline")
    RAW        cuma peta banjir MCDWD -> reproject -> crop -> tier BRONZE.
               Reflectance tidak diunduh sama sekali: NDVI/NDWI adalah indeks
               turunan, dan level RAW justru didefinisikan sebagai "tanpa
               indeks turunan".
    PROCESSED  peta banjir + NDVI + NDWI -> tier SILVER (lalu COG GOLD lewat
               module9_fusion._promote_aux_to_gold).

Dataset yang meminta KEDUANYA mendapat kedua artefak berdampingan: FLOOD
ditulis dua kali (bronze/ sebagai deliverable RAW, silver/ sebagai lapisan
pertama jalur PROCESSED). Granule-nya cuma diunduh dan diproses sekali —
salinan kedua adalah copy file, bukan build ulang.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject

from etl import download_guard as dg
from etl import folder_manager as fm
from etl.pipeline_logger import PipelineLogger
from etl.processing_plan import MODIS as MODIS_SOURCE_NAME
from etl.processing_plan import PROCESSED, SourcePlan, normalize_levels

from etl import tier_names as tn

logger = logging.getLogger(__name__)

LAADS_NRT_BASE = "https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61"

# Standard (science-quality, reprocessed) archive. NRT collections only keep
# a rolling retention window (days-to-weeks) before granules are pulled from
# nrt3.modaps.eosdis.nasa.gov; backfill jobs for older dates land past that
# window and must fall back here instead.
LAADS_STANDARD_BASE = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61"

MODIS_FLOOD_PRODUCT = "MCDWD_L3_F2_NRT"

# NDVI/NDWI utamanya dari MOD09A1: komposit surface reflectance 8 hari yang
# per piksel memilih observasi terbaik (awan & sudut pandang minimum) dalam
# periodenya. Di musim hujan Jakarta, MOD09GA harian tertutup awan 75-100%
# hampir setiap tanggal, sehingga setelah cloud mask indeksnya kosong total;
# komposit 8 hari menaikkan peluang ada piksel cerah. Produk ini hanya ada di
# arsip standar (tidak ada versi NRT) dan terbit ~1-2 minggu setelah periodenya
# berakhir, jadi tanggal yang komposit-nya belum terbit jatuh balik ke
# MOD09GA_NRT harian (juga dengan cloud mask).
MODIS_REFLECTANCE_PRODUCT = "MOD09A1"
MODIS_REFLECTANCE_FALLBACK_PRODUCT = "MOD09GA_NRT"
MODIS_REFLECTANCE_PRODUCTS = (MODIS_REFLECTANCE_PRODUCT, MODIS_REFLECTANCE_FALLBACK_PRODUCT)
MOD09A1_PERIOD_DAYS = 8

# NRT product -> standard-archive equivalent. MCDWD's standard archive is a
# single consolidated "MCDWD_L3" product (the 1-day/2-day/3-day composites
# that are separate NRT products live as subdatasets inside one granule);
# MOD09GA's standard equivalent just drops the "_NRT" suffix.
MODIS_STANDARD_PRODUCT = {
    MODIS_FLOOD_PRODUCT: "MCDWD_L3",
    MODIS_REFLECTANCE_FALLBACK_PRODUCT: "MOD09GA",
}

# Nama produk "utama" modul ini — dipakai untuk product_id lineage dan
# etl/constants.py:MODIS_PRODUCT_SHORT_NAME.
MODIS_PRODUCT = MODIS_FLOOD_PRODUCT

# Tile default untuk AOI Jabodetabek. Nilai lama ["h30v08", "h31v08"] menunjuk
# ke 120-140E / 0-10N (utara khatulistiwa), bukan Jakarta (~106.8E, 6S), jadi
# crop ke AOI selalu gagal. Kalau pemanggil tidak memberi `tiles`, tile dihitung
# dari AOI per produk lewat modis_tiles_for_bbox().
MODIS_TILES = ["h28v09"]
MODULE = "MODULE7_MODIS_DOWNLOAD"

# Komposit 2 hari (hari itu + sehari sebelumnya), sesuai produk NRT yang
# dipakai (MCDWD_L3_F2_NRT = 2-day). Granule standar MCDWD_L3 memuat 1/2/3-day
# sekaligus; versi 1-day tidak dipakai karena bayangan awan dan piksel gelap
# perkotaan lolos sebagai "Flood (unusual)" — di AOI Jakarta 20250111 ada 200
# piksel flood 1-day yang tidak muncul di komposit multi-hari. Nama dicocokkan
# tanpa memedulikan huruf besar/spasi/underscore/strip (_norm_name), supaya
# varian penamaan antar koleksi ("Flood 2-Day 250m" vs "Flood_2Day_250m") cocok.
FLOOD_SUBDATASET = "Flood_2Day_250m"

# Grid sinusoidal MODIS (MOD09GA dkk): tile 10 derajat = 1111950.52 m.
MODIS_SINUSOIDAL_CRS = "+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +R=6371007.181 +units=m +no_defs"
_SIN_TILE_SIZE_M = 1111950.5196666666
# Produk flood MCDWD memakai grid geografis 10x10 derajat, bukan sinusoidal.
_GEOGRAPHIC_TILE_PRODUCTS = {"MCDWD_L3", "MCDWD_L3_F2_NRT"}

# Nama SDS surface reflectance + QA state per keluarga produk (grid 500 m;
# state MOD09GA ada di grid 1 km, state MOD09A1 di 500 m).
#   red   = band 1, 620-670 nm
#   nir   = band 2, 841-876 nm
#   green = band 4, 545-565 nm
REFLECTANCE_SDS: dict[str, dict[str, str]] = {
    "MOD09A1": {
        "red": "sur_refl_b01",
        "nir": "sur_refl_b02",
        "green": "sur_refl_b04",
        "state": "sur_refl_state_500m",
    },
    "MOD09GA": {
        "red": "sur_refl_b01_1",
        "nir": "sur_refl_b02_1",
        "green": "sur_refl_b04_1",
        "state": "state_1km_1",
    },
}

# MOD09: fill -28672, rentang valid -100..16000 (scale 0.0001). Skala
# saling meniadakan di indeks ternormalisasi, jadi tidak perlu di-apply —
# tapi fill dan nilai di luar rentang valid tetap wajib dibuang dulu.
REFL_FILL = -28672
REFL_VALID_MIN = -100
REFL_VALID_MAX = 16000

# QA awan. Tanpa mask ini NDVI/NDWI musim hujan dihitung dari puncak awan: di
# AOI Jakarta Jan-Apr 2025 tutupan awan MOD09GA 75-100% per tanggal, dan
# NDVI-nya turun ke ~0 (awan putih = reflectance merah ~ NIR). state_1km
# (MOD09GA) dan sur_refl_state_500m (MOD09A1) memakai tata letak bit yang sama
# (MOD09 User Guide, tabel "State QA"):
STATE_FILL = 65535
_STATE_CLOUD_MASK = 0b11          # bit 0-1: 00 clear, 01 cloudy, 10 mixed, 11 not set (clear)
_STATE_CLOUD_SHADOW = 1 << 2      # bit 2
_STATE_CIRRUS_SHIFT = 8           # bit 8-9: 00 none, 01 small, 10 average, 11 high
_STATE_INTERNAL_CLOUD = 1 << 10   # bit 10: internal cloud algorithm flag

# band_name -> (kanal A, kanal B); indeks = (A - B) / (A + B)
MODIS_INDICES: dict[str, tuple[str, str]] = {
    "NDVI": ("nir", "red"),
    "NDWI": ("green", "nir"),
}


def _reflectance_family(product: str) -> str:
    """Nama produk (termasuk varian _NRT / arsip standar) -> kunci REFLECTANCE_SDS."""
    family = product.removesuffix("_NRT")
    if family not in REFLECTANCE_SDS:
        raise ValueError(f"produk reflectance tidak dikenal: {product!r}")
    return family


def _product_query_date(product: str, date: datetime) -> datetime:
    """Tanggal granule yang dicari untuk `date`. MOD09A1 diberi nama menurut
    hari pertama periode 8 harinya (DOY 1, 9, 17, ...), jadi tanggal target
    dipetakan ke awal periode yang memuatnya."""
    if product != MODIS_REFLECTANCE_PRODUCT:
        return date
    doy = date.timetuple().tm_yday
    start_doy = (doy - 1) // MOD09A1_PERIOD_DAYS * MOD09A1_PERIOD_DAYS + 1
    return datetime(date.year, 1, 1) + timedelta(days=start_doy - 1)

# band_name -> nilai data_products.product_type
MODIS_PRODUCT_TYPES: dict[str, str] = {
    "FLOOD": "MODIS_FLOOD",
    "NDVI": "MODIS_NDVI",
    "NDWI": "MODIS_NDWI",
}

# Jabodetabek bounding box, WGS84 (min_lon, min_lat, max_lon, max_lat)
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)

DST_CRS = "EPSG:4326"
MAX_RETRIES = 3
# Di bawah porsi ini band dianggap degraded: file tetap ditulis (awan memang
# data yang sah), tapi hari itu tidak boleh dilaporkan GOOD. Jakarta musim
# hujan sering 100% tertutup awan menurut QA state MOD09.
MIN_VALID_FRACTION = 0.05


def band_filename(band: str, date_key: str) -> str:
    """Nama file GeoTIFF harian untuk satu band MODIS. Satu-satunya tempat
    pola nama ini didefinisikan — module9_fusion.py mencari file input
    lewat fungsi ini, bukan lewat string literal-nya sendiri."""
    return f"modis_{date_key}_{band.lower()}.tif"


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "NASA_EARTHDATA_TOKEN belum diset. Generate app token di "
            "urs.earthdata.nasa.gov -> Generate Token."
        )
    return {"Authorization": f"Bearer {token}"}


def _daterange(date_start: datetime, date_end: datetime):
    d = date_start
    while d.date() <= date_end.date():
        yield d
        d += timedelta(days=1)


def _plog_event(
    plog: PipelineLogger | None,
    dataset_id: int | None,
    scene_id: str,
    stage: str,
    status: str,
    message: str,
    details: dict | None = None,
) -> None:
    if plog is None or dataset_id is None:
        return
    plog.log_event(dataset_id, scene_id, MODULE, stage, status, message, details or {})


def _md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def _discover_tile_files(
    date: datetime, tiles: list[str], product: str, base: str = LAADS_NRT_BASE
) -> list[dict]:
    """List available granules of `product` for `date` by scraping the LAADS
    directory index, one entry per requested tile."""
    import requests

    doy = date.timetuple().tm_yday
    url = f"{base}/{product}/{date.year}/{doy:03d}/"
    resp = None
    last_error: str = ""
    for attempt in range(1, MAX_RETRIES + 1):
        # Listing dulu satu request tanpa retry, dan exception jaringannya
        # (ReadTimeout, ConnectionError) bukan RuntimeError sehingga lolos dari
        # fallback NRT -> arsip standar di pemanggil. Satu timeout LAADS karena
        # itu menghapus satu band sehari penuh (try1: FLOOD 2025-01-08).
        try:
            resp = requests.get(url, headers=_auth_headers(), timeout=dg.REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            resp = None
        else:
            # Hanya 5xx yang diulang: 4xx (mis. 404 = direktori tanggal itu
            # memang tidak ada) adalah jawaban final, bukan gangguan.
            if resp.status_code < 500:
                break
            last_error = f"HTTP {resp.status_code}"
        if attempt < MAX_RETRIES:
            logger.warning(
                "[M7] listing LAADS gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, url, last_error,
            )
            time.sleep(2 ** attempt)
    if resp is None:
        raise RuntimeError(
            f"gagal listing LAADS setelah {MAX_RETRIES} percobaan ({last_error}): {url}"
        )
    if resp.status_code != 200:
        raise RuntimeError(f"gagal listing LAADS ({resp.status_code}): {url}")

    found = []
    for tile in tiles:
        for line in resp.text.splitlines():
            if tile not in line or ".hdf" not in line or ".hdf.xml" in line:
                continue
            href = line.split('"')[1] if '"' in line else None
            if not href:
                continue
            # nrt3's index uses relative hrefs ("FILE.hdf"); ladsweb's
            # (standard archive) uses absolute ones (full "https://...").
            fname = href.rsplit("/", 1)[-1]
            download_url = href if href.startswith("http") else url + href
            found.append({"tile": tile, "file_name": fname, "download_url": download_url})
            break
    return found


def _discover_tile_files_with_fallback(
    date: datetime, tiles: list[str], product: str
) -> tuple[list[dict], str]:
    """Try the NRT archive first (lowest latency), then fall back to the
    standard/reprocessed archive if NRT has nothing for `date` — this is the
    normal case for backfill jobs on dates past the NRT retention window.

    Produk tanpa varian NRT (mis. MOD09A1) langsung dicari di arsip standar.

    Returns (items, product_used)."""
    if not product.endswith("_NRT"):
        items = _discover_tile_files(date, tiles, product, base=LAADS_STANDARD_BASE)
        if not items:
            raise RuntimeError(
                f"tidak ada granule {product} untuk {date.date().isoformat()}"
            )
        return items, product

    try:
        items = _discover_tile_files(date, tiles, product, base=LAADS_NRT_BASE)
        if items:
            return items, product
    except RuntimeError as exc:
        nrt_error = exc
    else:
        nrt_error = RuntimeError(f"tidak ada granule NRT {product} untuk {date.date().isoformat()}")

    std_product = MODIS_STANDARD_PRODUCT.get(product)
    if not std_product:
        raise nrt_error

    try:
        items = _discover_tile_files(date, tiles, std_product, base=LAADS_STANDARD_BASE)
    except RuntimeError as exc:
        raise RuntimeError(f"{nrt_error}; fallback standar juga gagal: {exc}") from exc

    if not items:
        raise RuntimeError(
            f"{nrt_error}; fallback standar {std_product} juga tidak punya granule "
            f"untuk {date.date().isoformat()}"
        )
    return items, std_product


def _download_with_retry(
    url: str,
    out_path: Path,
    *,
    plog: PipelineLogger | None = None,
    dataset_id: int | None = None,
    scene_id: str = "",
    item_label: str = "",
) -> str:
    """Download `url` to `out_path`, retrying up to MAX_RETRIES times on
    network error or truncated transfer. Returns the file's MD5 checksum.
    Skips the download entirely if `out_path` already exists on disk.

    When `plog`/`dataset_id` are given, emits a RUNNING event per attempt
    (with periodic progress ticks), a terminal FAILED event only once all
    retries are exhausted, and a COMPLETED event on success."""
    import requests

    if out_path.exists() and out_path.stat().st_size > 0:
        logger.info("[M7] sudah ada di disk, lewati download: %s", out_path.name)
        return _md5(out_path)
    if dg.reuse_granule(out_path, "modis", fm.DATA_ROOT, "[M7]"):
        return _md5(out_path)

    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
    last_exc: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        attempt_started = time.monotonic()
        _plog_event(
            plog, dataset_id, scene_id, "DOWNLOAD", "RUNNING",
            f"{item_label}: downloading (attempt {attempt}/{MAX_RETRIES})",
            {"item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES, "url": url},
        )
        try:
            with requests.get(
                url, headers=_auth_headers(), stream=True, timeout=dg.REQUEST_TIMEOUT
            ) as r:
                r.raise_for_status()
                expected_size = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                guard = dg.StallGuard()
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=dg.CHUNK_SIZE):
                        f.write(chunk)
                        downloaded += len(chunk)
                        guard.update(len(chunk))
                        if expected_size and downloaded % (50 * 1024 * 1024) < dg.CHUNK_SIZE:
                            _plog_event(
                                plog, dataset_id, scene_id, "DOWNLOAD", "RUNNING",
                                f"{item_label}: {downloaded / 1e6:.0f}/{expected_size / 1e6:.0f} MB",
                                {
                                    "item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES,
                                    "progress_percent": round(downloaded / expected_size * 100, 1),
                                },
                            )

            if expected_size and downloaded != expected_size:
                raise IOError(
                    f"ukuran file tidak sesuai: got {downloaded} bytes, expected {expected_size}"
                )

            tmp_path.rename(out_path)
            checksum = _md5(out_path)
            logger.info("[M7] downloaded %s (md5=%s...)", out_path.name, checksum[:12])
            _plog_event(
                plog, dataset_id, scene_id, "DOWNLOAD", "COMPLETED",
                f"{item_label}: downloaded",
                {
                    "item": item_label, "attempt": attempt, "file_name": out_path.name,
                    "file_size_mb": round(downloaded / (1024 ** 2), 2), "checksum_md5": checksum,
                    "duration_seconds": round(time.monotonic() - attempt_started, 3),
                },
            )
            return checksum

        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[M7] download gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, out_path.name, exc,
            )
            tmp_path.unlink(missing_ok=True)
            is_final = attempt == MAX_RETRIES
            _plog_event(
                plog, dataset_id, scene_id, "DOWNLOAD", "FAILED" if is_final else "RUNNING",
                f"{item_label}: attempt {attempt}/{MAX_RETRIES} failed ({exc})",
                {
                    "item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES,
                    "error_type": type(exc).__name__, "error_message": str(exc),
                    "duration_seconds": round(time.monotonic() - attempt_started, 3),
                },
            )
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"gagal download {url} setelah {MAX_RETRIES} percobaan: {last_exc}")


def modis_tiles_for_bbox(
    bbox: tuple[float, float, float, float],
    product: str = MODIS_REFLECTANCE_PRODUCT,
) -> list[str]:
    """Tile MODIS (hXXvYY) yang memotong `bbox` WGS84 (min_lon, min_lat,
    max_lon, max_lat). MCDWD memakai grid geografis 10x10 derajat, MOD09GA
    grid sinusoidal — nomornya sering sama tapi tidak selalu, jadi dihitung
    per produk. Tepi bbox disampel rapat supaya lengkungan sinusoidal tidak
    melewatkan tile."""
    min_lon, min_lat, max_lon, max_lat = bbox
    lon_g, lat_g = np.meshgrid(
        np.linspace(min_lon, max_lon, 9), np.linspace(min_lat, max_lat, 9)
    )
    if product in _GEOGRAPHIC_TILE_PRODUCTS:
        h = np.floor((lon_g + 180.0) / 10.0)
        v = np.floor((90.0 - lat_g) / 10.0)
    else:
        from pyproj import Transformer

        x, y = Transformer.from_crs(
            "EPSG:4326", MODIS_SINUSOIDAL_CRS, always_xy=True
        ).transform(lon_g, lat_g)
        h = np.floor(np.asarray(x) / _SIN_TILE_SIZE_M + 18)
        v = np.floor(9 - np.asarray(y) / _SIN_TILE_SIZE_M)
    h = np.clip(h, 0, 35).astype(int)
    v = np.clip(v, 0, 17).astype(int)
    return sorted({f"h{hh:02d}v{vv:02d}" for hh, vv in zip(h.ravel(), v.ravel())})


def _norm_name(name: str) -> str:
    return "".join(ch for ch in name.lower() if ch.isalnum())


def _dms_to_deg(packed: float) -> float:
    """GCTP packed DMS (DDDMMMSSS.SS) -> derajat desimal."""
    sign = -1.0 if packed < 0 else 1.0
    v = abs(packed)
    return sign * (int(v // 1_000_000) + int((v % 1_000_000) // 1_000) / 60.0 + (v % 1_000) / 3600.0)


def _eos_grid_georef(struct_meta: str, field: str) -> dict:
    """Georeferensi grid HDF-EOS yang memuat `field`, dari StructMetadata."""
    import re

    from rasterio.coords import BoundingBox
    from rasterio.crs import CRS
    from rasterio.transform import from_bounds

    for block in re.findall(r"GROUP=GRID_\d+\s(.*?)END_GROUP=GRID_\d+", struct_meta, re.S):
        fields = re.findall(r'DataFieldName="([^"]+)"', block)
        if not any(_norm_name(f) == _norm_name(field) for f in fields):
            continue

        def value(key: str, default: str | None = None) -> str:
            m = re.search(rf"^\s*{key}=(.+)$", block, re.M)
            if not m:
                if default is not None:
                    return default
                raise RuntimeError(f"StructMetadata grid untuk {field} tidak punya {key}")
            return m.group(1).strip()

        xdim, ydim = int(value("XDim")), int(value("YDim"))
        ulx, uly = (float(s) for s in value("UpperLeftPointMtrs").strip("()").split(","))
        lrx, lry = (float(s) for s in value("LowerRightMtrs").strip("()").split(","))
        # GridOrigin opsional di HDF-EOS dan default-nya UL; granule MOD09A1
        # memang tidak menuliskannya (MOD09GA/MCDWD menulis eksplisit).
        origin = value("GridOrigin", "HDFE_GD_UL")
        if origin != "HDFE_GD_UL":
            raise RuntimeError(f"GridOrigin {origin} belum didukung ({field})")
        projection = value("Projection")
        if projection == "GCTP_SNSOID":
            crs = CRS.from_user_input(MODIS_SINUSOIDAL_CRS)
        elif projection == "GCTP_GEO":
            crs = CRS.from_epsg(4326)
            ulx, uly, lrx, lry = (_dms_to_deg(c) for c in (ulx, uly, lrx, lry))
        else:
            raise RuntimeError(f"proyeksi grid {projection} belum didukung ({field})")
        return {
            "crs": crs,
            "transform": from_bounds(ulx, lry, lrx, uly, xdim, ydim),
            "width": xdim,
            "height": ydim,
            "bounds": BoundingBox(ulx, lry, lrx, uly),
        }
    raise RuntimeError(f"field {field} tidak ditemukan di StructMetadata")


def _require_hdf4_reader() -> None:
    """Pastikan pyhdf bisa diimport sebelum mulai download. Tanpa ini setiap
    granule tetap diunduh lalu gagal dibaca satu per satu."""
    try:
        import pyhdf.SD  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "pyhdf tidak terpasang (dibutuhkan untuk membaca HDF4 MODIS) — "
            "jalankan `pip install -r requirements.txt`"
        ) from exc


def _read_eos_grid_field(hdf_path: Path, subdataset: str) -> tuple[np.ndarray, dict]:
    """Baca satu field grid HDF4-EOS -> (array, georef + nodata).

    Dibaca lewat pyhdf, bukan path GDAL 'HDF4_EOS:EOS_GRID:...': wheel
    rasterio (Windows/pip) tidak menyertakan driver HDF4, sehingga path itu
    selalu gagal "does not exist in the file system". `subdataset` boleh
    berbentuk "Grid:field" atau "field"."""
    from pyhdf.SD import SD, SDC

    field = subdataset.rsplit(":", 1)[-1]
    sd = SD(str(hdf_path), SDC.READ)
    try:
        names = list(sd.datasets())
        match = next((n for n in names if _norm_name(n) == _norm_name(field)), None)
        if match is None:
            raise RuntimeError(f"SDS {field!r} tidak ada di {hdf_path.name} (tersedia: {names})")
        sds = sd.select(match)
        try:
            data = sds.get()
            attrs = sds.attributes()
        finally:
            sds.endaccess()
        global_attrs = sd.attributes()
        struct_meta = "".join(
            global_attrs[k] for k in sorted(global_attrs) if k.startswith("StructMetadata")
        )
    finally:
        sd.end()

    grid = _eos_grid_georef(struct_meta, match)
    if data.shape != (grid["height"], grid["width"]):
        raise RuntimeError(
            f"ukuran {match} {data.shape} tidak cocok dengan grid "
            f"{(grid['height'], grid['width'])} di {hdf_path.name}"
        )
    grid["nodata"] = attrs.get("_FillValue")
    return data, grid


def _hdf_subdataset_to_geotiff(
    hdf_path: Path,
    subdataset: str,
    output_path: Path,
    dst_crs: str = DST_CRS,
) -> Path:
    data, grid = _read_eos_grid_field(hdf_path, subdataset)
    nodata = grid["nodata"]
    transform, width, height = calculate_default_transform(
        grid["crs"], dst_crs, grid["width"], grid["height"], *grid["bounds"]
    )
    dest = np.full((height, width), nodata if nodata is not None else 0, dtype=data.dtype)
    reproject(
        source=data,
        destination=dest,
        src_transform=grid["transform"],
        src_crs=grid["crs"],
        src_nodata=nodata,
        dst_transform=transform,
        dst_crs=dst_crs,
        dst_nodata=nodata,
        resampling=Resampling.nearest,
    )
    with rasterio.open(
        output_path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype=data.dtype.name, crs=dst_crs, transform=transform, nodata=nodata,
    ) as dst:
        dst.write(dest, 1)
    return output_path


def _read_reflectance(hdf_path: Path, subdataset: str) -> tuple[np.ndarray, dict]:
    """Baca satu subdataset surface reflectance MOD09GA sebagai float32
    dengan fill/out-of-range diganti NaN. Mengembalikan (array, profil grid
    sumber) supaya pemanggil bisa reproject hasil hitungannya."""
    raw, grid = _read_eos_grid_field(hdf_path, subdataset)
    data = raw.astype("float32")
    invalid = (raw == REFL_FILL) | (raw < REFL_VALID_MIN) | (raw > REFL_VALID_MAX)
    data[invalid] = np.nan
    return data, grid


def _cloud_mask(hdf_path: Path, shape: tuple[int, int], state_sds: str) -> np.ndarray:
    """Mask True = piksel yang tidak boleh dipakai indeks, menurut QA state.

    Dibuang: cloudy/mixed, bayangan awan, cirrus average/high, flag awan
    internal, dan fill. Grid state dan grid reflectance menutupi tile yang
    sama, jadi state 1 km MOD09GA diperbesar ke `shape` dengan pengulangan blok
    (1 piksel 1 km = 2x2 piksel 500 m), bukan resampling; state 500 m MOD09A1
    sudah seukuran."""
    state, _ = _read_eos_grid_field(hdf_path, state_sds)
    fy, ry = divmod(shape[0], state.shape[0])
    fx, rx = divmod(shape[1], state.shape[1])
    if ry or rx or not fy or not fx:
        raise RuntimeError(
            f"grid {state_sds} {state.shape} tidak kelipatan grid reflectance {shape} "
            f"di {hdf_path.name}"
        )
    state = state.astype(np.uint16)
    cloud_state = state & _STATE_CLOUD_MASK
    bad = (
        (state == STATE_FILL)
        | (cloud_state == 0b01)
        | (cloud_state == 0b10)
        | ((state & _STATE_CLOUD_SHADOW) != 0)
        | (((state >> _STATE_CIRRUS_SHIFT) & 0b11) >= 0b10)
        | ((state & _STATE_INTERNAL_CLOUD) != 0)
    )
    return np.repeat(np.repeat(bad, fy, axis=0), fx, axis=1)


def _normalized_index_tile(
    hdf_path: Path,
    product: str,
    band: str,
    output_path: Path,
    dst_crs: str = DST_CRS,
) -> Path:
    """Hitung indeks ternormalisasi `band` (lihat MODIS_INDICES) dari granule
    reflectance `product` (MOD09A1 / MOD09GA), lalu reproject ke `dst_crs`.

    Indeksnya dihitung dulu di grid sinusoidal asli baru direproject —
    bukan sebaliknya. Meresample tiap band dulu lalu membagi akan
    mencampur reflectance tetangga di pembilang dan penyebut secara
    berbeda, yang menggeser nilai indeks di tepi tiap fitur."""
    sds = REFLECTANCE_SDS[_reflectance_family(product)]
    chan_a, chan_b = MODIS_INDICES[band]
    a, grid = _read_reflectance(hdf_path, sds[chan_a])
    b, _ = _read_reflectance(hdf_path, sds[chan_b])

    denom = a + b
    with np.errstate(invalid="ignore", divide="ignore"):
        index = (a - b) / denom
    # Penyebut nol = kedua band nol: tidak ada sinyal, bukan indeks 0.
    index[~np.isfinite(index)] = np.nan
    # Rentang valid MOD09GA memuat reflectance negatif (artefak koreksi
    # atmosfer). Dengan salah satu band negatif, penyebutnya bisa mendekati
    # nol dan indeks meledak jauh di luar [-1, 1] (terukur -8..11 di AOI
    # Jakarta). Pixel seperti itu tidak punya indeks yang bermakna.
    index[(a < 0) | (b < 0)] = np.nan
    # Awan/bayangan dibuang SEBELUM reproject: bilinear di tahap berikut
    # mengabaikan NaN, jadi nilai awan tidak ikut merembes ke piksel cerah.
    index[_cloud_mask(hdf_path, index.shape, sds["state"])] = np.nan

    transform, width, height = calculate_default_transform(
        grid["crs"], dst_crs, grid["width"], grid["height"], *grid["bounds"]
    )
    dest = np.full((height, width), np.nan, dtype="float32")
    reproject(
        source=index,
        destination=dest,
        src_transform=grid["transform"],
        src_crs=grid["crs"],
        src_nodata=np.nan,
        dst_transform=transform,
        dst_crs=dst_crs,
        dst_nodata=np.nan,
        resampling=Resampling.bilinear,
    )

    with rasterio.open(
        output_path, "w", driver="GTiff", height=height, width=width, count=1,
        dtype="float32", crs=dst_crs, transform=transform, nodata=np.nan,
    ) as dst:
        dst.write(dest, 1)
    return output_path


def _mosaic_and_crop(
    tile_tif_paths: list[Path],
    aoi_bbox: tuple[float, float, float, float],
    output_path: Path,
) -> Path:
    """Merge per-tile GeoTIFFs (already reprojected to DST_CRS) and crop
    the mosaic to `aoi_bbox`, matching Sentinel-1 resolution/projection."""
    from rasterio.mask import mask
    from shapely.geometry import box, mapping

    srcs = [rasterio.open(p) for p in tile_tif_paths]
    try:
        mosaic, out_transform = merge(srcs)
        meta = srcs[0].meta.copy()
    finally:
        for s in srcs:
            s.close()

    meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_transform,
    })
    mosaic_path = output_path.with_name(output_path.stem + "_mosaic.tif")
    with rasterio.open(mosaic_path, "w", **meta) as dst:
        dst.write(mosaic)

    geom = mapping(box(*aoi_bbox))
    with rasterio.open(mosaic_path) as src:
        out_image, crop_transform = mask(src, [geom], crop=True)
        crop_meta = src.meta.copy()
        crop_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": crop_transform,
        })
        with rasterio.open(output_path, "w", **crop_meta) as dst:
            dst.write(out_image)

    mosaic_path.unlink(missing_ok=True)
    return output_path


def _build_band_for_date(
    *,
    band: str,
    product: str,
    date: datetime,
    date_key: str,
    tiles: list[str],
    raw_dir: Path,
    out_path: Path,
    aoi_bbox: tuple[float, float, float, float],
    plog: PipelineLogger | None,
    dataset_id: int | None,
    scene_label: str,
) -> dict:
    """Bangun satu band MODIS untuk satu tanggal: listing granule -> download
    per tile -> ekstrak/hitung -> mosaic -> crop ke AOI.

    Mengembalikan dict hasil. Melempar RuntimeError kalau band ini tidak bisa
    dibangun sama sekali untuk tanggal tsb; pemanggil memutuskan apakah itu
    fatal (tidak, per band) atau tidak."""
    query_date = _product_query_date(product, date)
    items, product_used = _discover_tile_files_with_fallback(query_date, tiles, product)
    if not items:
        raise RuntimeError(f"tidak ada granule {product} untuk {date.date().isoformat()}")
    if product_used != product:
        logger.info(
            "[M7] %s tanggal %s: NRT tidak tersedia, pakai arsip standar %s",
            band, date.date().isoformat(), product_used,
        )

    tile_tifs: list[Path] = []
    source_checksums: dict[str, str] = {}
    failed_tiles: list[str] = []

    for item in items:
        try:
            hdf_path = raw_dir / item["file_name"]
            source_checksums[item["tile"]] = _download_with_retry(
                item["download_url"], hdf_path,
                plog=plog, dataset_id=dataset_id, scene_id=scene_label,
                item_label=f"{band} tile {item['tile']}",
            )
            stem = Path(item["file_name"]).stem
            tile_tif = raw_dir / f"{stem}_{band.lower()}.tif"
            if band == "FLOOD":
                _hdf_subdataset_to_geotiff(hdf_path, FLOOD_SUBDATASET, tile_tif)
            else:
                _normalized_index_tile(hdf_path, product_used, band, tile_tif)
            tile_tifs.append(tile_tif)
        except ImportError:
            # Masalah environment, bukan data: tile lain pasti gagal juga.
            raise
        except Exception as exc:
            logger.warning(
                "[M7] %s tile %s gagal (tanggal %s): %s",
                band, item["tile"], date.date().isoformat(), exc,
            )
            failed_tiles.append(item["tile"])

    if not tile_tifs:
        raise RuntimeError(f"semua tile {band} gagal ({', '.join(failed_tiles)})")

    _mosaic_and_crop(tile_tifs, aoi_bbox, out_path)
    valid_fraction = _valid_fraction(out_path)
    low_coverage = valid_fraction < MIN_VALID_FRACTION
    logger.log(
        logging.WARNING if low_coverage else logging.INFO,
        "[M7] %s tanggal %s: %.1f%% piksel AOI valid%s",
        band, date.date().isoformat(), valid_fraction * 100,
        " (sisanya awan/tanpa data)" if band != "FLOOD" else " (sisanya insufficient data)",
    )

    entry = {
        "band": band,
        "product": product_used,
        "path": str(out_path),
        "checksum_md5": _md5(out_path),
        "source_tiles": source_checksums,
        "skipped": False,
        "degraded": bool(failed_tiles) or low_coverage,
        "failed_tiles": failed_tiles,
        "valid_fraction": round(valid_fraction, 4),
        "low_coverage": low_coverage,
    }
    if product == MODIS_REFLECTANCE_PRODUCT:
        period_end = query_date + timedelta(days=MOD09A1_PERIOD_DAYS - 1)
        entry["composite_period"] = [
            query_date.date().isoformat(), period_end.date().isoformat()
        ]
    return entry


def _valid_fraction(path: Path) -> float:
    """Porsi piksel AOI yang punya nilai (bukan nodata/NaN)."""
    with rasterio.open(path) as src:
        data = src.read(1, masked=True)
    if data.size == 0:
        return 0.0
    values = np.ma.masked_invalid(data) if data.dtype.kind == "f" else data
    return float(values.count()) / data.size


def _band_targets(
    dataset_id: int,
    dataset_name: str,
    band: str,
    date_key: str,
    targets: tuple[tuple[str, str], ...],
) -> list[tuple[str, str, Path]]:
    """(tier, processing_level, path) untuk satu band, tier tertinggi dulu.

    Tier tertinggi jadi yang pertama karena dialah yang dibangun; target lain
    (kalau ada) diisi dengan menyalin berkas itu."""
    ordered = sorted(targets, key=lambda t: 0 if tn.rank(t[0]) == 2 else 1)
    out = []
    for tier, level in ordered:
        scene_dir = fm.ensure_scene_dir(
            dataset_id, dataset_name, tier.lower(), "modis", date_key
        )
        out.append((tier, level, scene_dir / band_filename(band, date_key)))
    return out


def download_modis_scene(
    dataset_id: int,
    dataset_name: str,
    date_start: datetime,
    date_end: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    tiles: list[str] | None = None,
    plog: PipelineLogger | None = None,
    processing_levels=(PROCESSED,),
) -> tuple[str, dict]:
    """
    Download MODIS flood (MCDWD) + surface reflectance (MOD09GA) dari NASA
    LAADS DAAC untuk setiap hari di [date_start, date_end], hitung NDVI/NDWI,
    reproject/crop tiap hari ke `aoi_bbox`, dan tulis GeoTIFF ke
    data/datasets/{id}_{slug}/{YYYYMMDD}/silver/modis/modis_{date}_{band}.tif
    (ini input fusion, dikonsumsi module9_fusion.py — bukan deliverable akhir).

    `processing_levels` (dari dataset_source_config) menentukan band mana yang
    dibangun dan ke tier mana ditulis:
        {"RAW"}              FLOOD saja  -> bronze/modis/{date}/
        {"PROCESSED"}        FLOOD+NDVI+NDWI -> silver/modis/{date}/
        {"RAW","PROCESSED"}  keduanya; FLOOD ada di bronze/ DAN silver/

    Kegagalan diisolasi dua lapis: satu tile yang gagal masih menyisakan
    mosaic degraded dari tile lain, dan satu band yang gagal (mis. MOD09GA
    belum terbit untuk hari itu) tidak menjatuhkan band lain di hari yang
    sama. Satu hari baru dihitung gagal kalau tidak ada band sama sekali.
    Pass `plog` untuk ikut mengirim event terstruktur per tile/hari/ringkasan
    ke tabel `processing_logs`.

    Returns:
        (product_id, metadata_dict) — product_id mengidentifikasi produk NASA
        sumber untuk lineage; metadata_dict membawa path output per band per
        hari (termasuk tier & processing_level tiap salinan di
        `outputs[i]["bands"][band]["targets"]`), checksum MD5, dan ringkasan
        `quality`/`failed_days`.
    """
    plan = SourcePlan(
        source_name=MODIS_SOURCE_NAME,
        levels=normalize_levels(processing_levels) or (PROCESSED,),
    )
    band_targets = plan.targets()
    wanted_bands = plan.modis_bands()
    logger.info(
        "[M7] dataset_id=%s level=%s band=%s",
        dataset_id, list(plan.levels), list(wanted_bands),
    )

    try:
        _require_hdf4_reader()
    except RuntimeError as exc:
        _plog_event(
            plog, dataset_id, f"MODIS_{date_start.strftime('%Y%m%d')}",
            "DOWNLOAD", "FAILED", str(exc),
            {"error_type": "MissingDependency", "error_message": str(exc)},
        )
        raise

    raw_dir = fm.get_granule_cache_dir(dataset_id, dataset_name, "modis")
    raw_dir.mkdir(parents=True, exist_ok=True)

    # tiles=None -> hitung dari AOI per produk (grid MCDWD dan MOD09GA berbeda).
    tiles_by_product = {
        p: list(tiles) if tiles else modis_tiles_for_bbox(aoi_bbox, p)
        for p in (MODIS_FLOOD_PRODUCT, *MODIS_REFLECTANCE_PRODUCTS)
    }

    daily_outputs = []
    failed_days: list[dict] = []

    for date in _daterange(date_start, date_end):
        date_key = date.strftime("%Y%m%d")
        scene_label = f"MODIS_{date_key}"

        bands: dict[str, dict] = {}
        band_errors: dict[str, str] = {}

        for band, products in (
            ("FLOOD", (MODIS_FLOOD_PRODUCT,)),
            ("NDVI", MODIS_REFLECTANCE_PRODUCTS),
            ("NDWI", MODIS_REFLECTANCE_PRODUCTS),
        ):
            product = products[0]
            if band not in wanted_bands:
                continue

            # Target pertama = tier tertinggi; di situlah band dibangun.
            # Sisanya salinan (lihat _band_targets).
            targets = _band_targets(
                dataset_id, dataset_name, band, date_key, band_targets[band]
            )
            build_tier, build_level, out_path = targets[0]

            def _record(entry: dict) -> dict:
                """Lengkapi entry band dengan salinan ke target lain."""
                written = {
                    build_tier: {
                        "path": str(out_path),
                        "processing_level": build_level,
                        "checksum_md5": entry["checksum_md5"],
                    }
                }
                for tier, level, copy_path in targets[1:]:
                    if not copy_path.exists():
                        shutil.copy2(out_path, copy_path)
                    written[tier] = {
                        "path": str(copy_path),
                        "processing_level": level,
                        "checksum_md5": entry["checksum_md5"],
                    }
                entry["targets"] = written
                return entry

            if out_path.exists():
                logger.info("[M7] output sudah ada, skip: %s", out_path.name)
                valid_fraction = _valid_fraction(out_path)
                low_coverage = valid_fraction < MIN_VALID_FRACTION
                bands[band] = _record({
                    "band": band,
                    "product": product,
                    "path": str(out_path),
                    "checksum_md5": _md5(out_path),
                    "skipped": True,
                    "degraded": low_coverage,
                    "failed_tiles": [],
                    "valid_fraction": round(valid_fraction, 4),
                    "low_coverage": low_coverage,
                })
                continue

            try:
                # Produk dicoba berurutan (reflectance: MOD09A1 lalu MOD09GA);
                # error terakhir yang dilaporkan kalau semuanya gagal.
                attempt_errors: list[str] = []
                for candidate in products:
                    try:
                        bands[band] = _record(_build_band_for_date(
                            band=band, product=candidate, date=date, date_key=date_key,
                            tiles=tiles_by_product[candidate], raw_dir=raw_dir,
                            out_path=out_path, aoi_bbox=aoi_bbox,
                            plog=plog, dataset_id=dataset_id, scene_label=scene_label,
                        ))
                        break
                    except ImportError:
                        # Jangan fallback (download produk lain) untuk error environment.
                        raise
                    except Exception as exc:
                        attempt_errors.append(f"{candidate}: {exc}")
                        if candidate != products[-1]:
                            logger.info(
                                "[M7] %s tanggal %s: %s gagal (%s), coba %s",
                                band, date.date().isoformat(), candidate, exc,
                                products[products.index(candidate) + 1],
                            )
                else:
                    raise RuntimeError("; ".join(attempt_errors))
            except ImportError:
                raise
            except Exception as exc:
                logger.warning(
                    "[M7] band %s gagal tanggal %s: %s", band, date.date().isoformat(), exc
                )
                band_errors[band] = str(exc)
                _plog_event(
                    plog, dataset_id, scene_label, "DOWNLOAD", "RUNNING",
                    f"MODIS {date_key}: band {band} gagal ({exc})",
                    {
                        "date": date.date().isoformat(), "band": band,
                        "error_type": type(exc).__name__, "error_message": str(exc),
                    },
                )

        if not bands:
            _plog_event(
                plog, dataset_id, scene_label, "DOWNLOAD", "FAILED",
                f"MODIS {date_key}: semua band gagal",
                {"date": date.date().isoformat(), "band_errors": band_errors},
            )
            failed_days.append({
                "date": date.date().isoformat(),
                "reason": f"all bands failed: {band_errors}",
            })
            continue

        degraded = bool(band_errors) or any(b.get("degraded") for b in bands.values())
        daily_outputs.append({
            "date": date.date().isoformat(),
            # `products` = path tier tertinggi per band, dipertahankan untuk
            # pemanggil yang cuma butuh "satu file per band". Registrasi
            # data_products memakai `bands[band]["targets"]` supaya tiap
            # salinan tercatat dengan tier & processing_level-nya sendiri.
            "products": {band: b["path"] for band, b in bands.items()},
            "checksums": {band: b["checksum_md5"] for band, b in bands.items()},
            "bands": bands,
            "band_errors": band_errors,
            "skipped": all(b.get("skipped") for b in bands.values()),
            "degraded": degraded,
        })
        _plog_event(
            plog, dataset_id, scene_label, "DOWNLOAD", "COMPLETED",
            f"MODIS {date_key}: {'selesai (degraded)' if degraded else 'selesai'} "
            f"({len(bands)}/{len(wanted_bands)} band)",
            {
                "date": date.date().isoformat(),
                "bands_ok": sorted(bands), "bands_failed": sorted(band_errors),
                "bands_low_coverage": sorted(
                    band for band, b in bands.items() if b.get("low_coverage")
                ),
                "valid_fraction": {
                    band: b["valid_fraction"] for band, b in bands.items()
                    if "valid_fraction" in b
                },
                "degraded": degraded,
            },
        )

    if not daily_outputs:
        raise RuntimeError(
            f"tidak ada produk MODIS ditemukan untuk rentang "
            f"{date_start.date()}..{date_end.date()} di tiles {tiles_by_product}"
            + (f" (gagal: {failed_days})" if failed_days else "")
        )

    degraded_days = sum(1 for d in daily_outputs if d.get("degraded"))
    quality = "GOOD" if not failed_days and not degraded_days else "DEGRADED"
    total_days = len(daily_outputs) + len(failed_days)
    _plog_event(
        plog, dataset_id, f"MODIS_{date_start.strftime('%Y%m%d')}_{date_end.strftime('%Y%m%d')}",
        "DOWNLOAD_SUMMARY", "COMPLETED",
        f"MODIS selesai: {len(daily_outputs)}/{total_days} hari berhasil"
        + (f", {len(failed_days)} gagal" if failed_days else "")
        + (f", {degraded_days} degraded" if degraded_days else ""),
        {
            "days_ok": len(daily_outputs), "days_degraded": degraded_days,
            "days_failed": len(failed_days), "failed_days": failed_days, "quality": quality,
        },
    )

    product_id = (
        f"{MODIS_PRODUCT}.{date_start.strftime('%Y%m%d')}_{date_end.strftime('%Y%m%d')}"
        ".jabodetabek"
    )
    metadata = {
        "product": MODIS_PRODUCT,
        "products": (
            [MODIS_FLOOD_PRODUCT, *MODIS_REFLECTANCE_PRODUCTS]
            if plan.has_processed else [MODIS_FLOOD_PRODUCT]
        ),
        "processing_levels": list(plan.levels),
        "bands_requested": list(wanted_bands),
        "dataset_id": dataset_id,
        "date_start": date_start.date().isoformat(),
        "date_end": date_end.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "tiles": tiles_by_product,
        "crs": DST_CRS,
        "outputs": daily_outputs,
        "quality": quality,
        "failed_days": failed_days,
    }

    logger.info("[M7] selesai: %d hari diproses untuk dataset_id=%s", len(daily_outputs), dataset_id)
    return product_id, metadata
