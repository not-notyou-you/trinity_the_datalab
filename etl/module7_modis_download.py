# etl/module7_modis_download.py
"""
Downloads NASA LAADS DAAC MODIS products, reprojects/crops them to the
dataset AOI, and writes one GeoTIFF per band per day for lineage tracking.

Dua produk berbeda diambil di sini:

  MCDWD_L3_F2_NRT (250 m)  -> band FLOOD, langsung dari subdataset
                              "Flood 1-day 250m".
  MOD09GA_NRT     (500 m)  -> band NDVI dan NDWI, dihitung dari surface
                              reflectance:
                                  NDVI = (b02_NIR   - b01_red)   / (b02 + b01)
                                  NDWI = (b04_green - b02_NIR)   / (b04 + b02)

NDWI di sini adalah formulasi McFeeters (green/NIR) yang menyorot badan air
terbuka — bukan NDWI Gao (NIR/SWIR) yang mengukur kelembapan vegetasi.
Pipeline ini soal banjir, jadi indeks air permukaan yang relevan.

Output ditulis ke data/datasets/{id}_{slug}/silver/modis/{YYYYMMDD}/ dan
granule mentahnya di-cache di raw/modis/. Semuanya adalah input fusion
(dikonsumsi module9_fusion.py lewat tier GOLD), bukan deliverable akhir.

Kegagalan satu produk tidak menjatuhkan produk lain: kalau MOD09GA hari itu
belum tersedia di NRT archive tapi MCDWD ada, hari itu tetap menghasilkan
FLOOD dan cuma kehilangan NDVI/NDWI.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.merge import merge
from rasterio.warp import calculate_default_transform, reproject

from etl import folder_manager as fm
from etl.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)

LAADS_NRT_BASE = "https://nrt3.modaps.eosdis.nasa.gov/archive/allData/61"

MODIS_FLOOD_PRODUCT = "MCDWD_L3_F2_NRT"
MODIS_REFLECTANCE_PRODUCT = "MOD09GA_NRT"

# Nama produk "utama" modul ini — dipakai untuk product_id lineage dan
# etl/constants.py:MODIS_PRODUCT_SHORT_NAME.
MODIS_PRODUCT = MODIS_FLOOD_PRODUCT

MODIS_TILES = ["h30v08", "h31v08"]
MODULE = "MODULE7_MODIS_DOWNLOAD"

FLOOD_SUBDATASET = "Flood 1-day 250m"

# Subdataset surface reflectance MOD09GA (grid 500 m).
_REFL_GRID = "MODIS_Grid_500m_2D"
REFL_RED = f"{_REFL_GRID}:sur_refl_b01_1"    # 620-670 nm
REFL_NIR = f"{_REFL_GRID}:sur_refl_b02_1"    # 841-876 nm
REFL_GREEN = f"{_REFL_GRID}:sur_refl_b04_1"  # 545-565 nm

# MOD09GA: fill -28672, rentang valid -100..16000 (scale 0.0001). Skala
# saling meniadakan di indeks ternormalisasi, jadi tidak perlu di-apply —
# tapi fill dan nilai di luar rentang valid tetap wajib dibuang dulu.
REFL_FILL = -28672
REFL_VALID_MIN = -100
REFL_VALID_MAX = 16000

# band_name -> (subdataset A, subdataset B); indeks = (A - B) / (A + B)
MODIS_INDICES: dict[str, tuple[str, str]] = {
    "NDVI": (REFL_NIR, REFL_RED),
    "NDWI": (REFL_GREEN, REFL_NIR),
}

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


def _discover_tile_files(date: datetime, tiles: list[str], product: str) -> list[dict]:
    """List available granules of `product` for `date` by scraping the LAADS
    directory index, one entry per requested tile."""
    import requests

    doy = date.timetuple().tm_yday
    url = f"{LAADS_NRT_BASE}/{product}/{date.year}/{doy:03d}/"
    resp = requests.get(url, headers=_auth_headers(), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"gagal listing LAADS ({resp.status_code}): {url}")

    found = []
    for tile in tiles:
        for line in resp.text.splitlines():
            if tile not in line or ".hdf" not in line or ".hdf.xml" in line:
                continue
            fname = line.split('"')[1] if '"' in line else None
            if fname:
                found.append({"tile": tile, "file_name": fname, "download_url": url + fname})
                break
    return found


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
            with requests.get(url, headers=_auth_headers(), stream=True, timeout=300) as r:
                r.raise_for_status()
                expected_size = int(r.headers.get("Content-Length", 0))
                downloaded = 0
                with open(tmp_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        if expected_size and downloaded % (50 * 1024 * 1024) < 8 * 1024 * 1024:
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


def _eos_grid_path(hdf_path: Path, subdataset: str) -> str:
    return f'HDF4_EOS:EOS_GRID:"{hdf_path}":{subdataset}'


def _hdf_subdataset_to_geotiff(
    hdf_path: Path,
    subdataset: str,
    output_path: Path,
    dst_crs: str = DST_CRS,
) -> Path:
    src_path = _eos_grid_path(hdf_path, subdataset)
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            "driver": "GTiff",
            "crs": dst_crs,
            "transform": transform,
            "width": width,
            "height": height,
        })
        with rasterio.open(output_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.nearest,
                )
    return output_path


def _read_reflectance(hdf_path: Path, subdataset: str) -> tuple[np.ndarray, dict]:
    """Baca satu subdataset surface reflectance MOD09GA sebagai float32
    dengan fill/out-of-range diganti NaN. Mengembalikan (array, profil grid
    sumber) supaya pemanggil bisa reproject hasil hitungannya."""
    with rasterio.open(_eos_grid_path(hdf_path, subdataset)) as src:
        raw = src.read(1)
        grid = {
            "crs": src.crs,
            "transform": src.transform,
            "width": src.width,
            "height": src.height,
            "bounds": src.bounds,
        }
    data = raw.astype("float32")
    invalid = (raw == REFL_FILL) | (raw < REFL_VALID_MIN) | (raw > REFL_VALID_MAX)
    data[invalid] = np.nan
    return data, grid


def _normalized_index_tile(
    hdf_path: Path,
    sub_a: str,
    sub_b: str,
    output_path: Path,
    dst_crs: str = DST_CRS,
) -> Path:
    """Hitung indeks ternormalisasi (A - B) / (A + B) dari dua subdataset
    reflectance, lalu reproject hasilnya ke `dst_crs`.

    Indeksnya dihitung dulu di grid sinusoidal asli baru direproject —
    bukan sebaliknya. Meresample tiap band dulu lalu membagi akan
    mencampur reflectance tetangga di pembilang dan penyebut secara
    berbeda, yang menggeser nilai indeks di tepi tiap fitur."""
    a, grid = _read_reflectance(hdf_path, sub_a)
    b, _ = _read_reflectance(hdf_path, sub_b)

    denom = a + b
    with np.errstate(invalid="ignore", divide="ignore"):
        index = (a - b) / denom
    # Penyebut nol = kedua band nol: tidak ada sinyal, bukan indeks 0.
    index[~np.isfinite(index)] = np.nan

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
    items = _discover_tile_files(date, tiles, product)
    if not items:
        raise RuntimeError(f"tidak ada granule {product} untuk {date.date().isoformat()}")

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
                sub_a, sub_b = MODIS_INDICES[band]
                _normalized_index_tile(hdf_path, sub_a, sub_b, tile_tif)
            tile_tifs.append(tile_tif)
        except Exception as exc:
            logger.warning(
                "[M7] %s tile %s gagal (tanggal %s): %s",
                band, item["tile"], date.date().isoformat(), exc,
            )
            failed_tiles.append(item["tile"])

    if not tile_tifs:
        raise RuntimeError(f"semua tile {band} gagal ({', '.join(failed_tiles)})")

    _mosaic_and_crop(tile_tifs, aoi_bbox, out_path)

    return {
        "band": band,
        "product": product,
        "path": str(out_path),
        "checksum_md5": _md5(out_path),
        "source_tiles": source_checksums,
        "skipped": False,
        "degraded": bool(failed_tiles),
        "failed_tiles": failed_tiles,
    }


def download_modis_scene(
    dataset_id: int,
    dataset_name: str,
    date_start: datetime,
    date_end: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    tiles: list[str] = MODIS_TILES,
    plog: PipelineLogger | None = None,
) -> tuple[str, dict]:
    """
    Download MODIS flood (MCDWD) + surface reflectance (MOD09GA) dari NASA
    LAADS DAAC untuk setiap hari di [date_start, date_end], hitung NDVI/NDWI,
    reproject/crop tiap hari ke `aoi_bbox`, dan tulis GeoTIFF ke
    data/datasets/{id}_{slug}/silver/modis/{YYYYMMDD}/modis_{date}_{band}.tif
    (ini input fusion, dikonsumsi module9_fusion.py — bukan deliverable akhir).

    Kegagalan diisolasi dua lapis: satu tile yang gagal masih menyisakan
    mosaic degraded dari tile lain, dan satu band yang gagal (mis. MOD09GA
    belum terbit untuk hari itu) tidak menjatuhkan band lain di hari yang
    sama. Satu hari baru dihitung gagal kalau tidak ada band sama sekali.
    Pass `plog` untuk ikut mengirim event terstruktur per tile/hari/ringkasan
    ke tabel `processing_logs`.

    Returns:
        (product_id, metadata_dict) — product_id mengidentifikasi produk NASA
        sumber untuk lineage; metadata_dict membawa path output per band per
        hari, checksum MD5, dan ringkasan `quality`/`failed_days`.
    """
    raw_dir = fm.get_granule_cache_dir(dataset_id, dataset_name, "modis")
    raw_dir.mkdir(parents=True, exist_ok=True)

    daily_outputs = []
    failed_days: list[dict] = []

    for date in _daterange(date_start, date_end):
        date_key = date.strftime("%Y%m%d")
        scene_label = f"MODIS_{date_key}"
        silver_dir = fm.ensure_scene_dir(dataset_id, dataset_name, "silver", "modis", date_key)

        bands: dict[str, dict] = {}
        band_errors: dict[str, str] = {}

        for band, product in (
            ("FLOOD", MODIS_FLOOD_PRODUCT),
            ("NDVI", MODIS_REFLECTANCE_PRODUCT),
            ("NDWI", MODIS_REFLECTANCE_PRODUCT),
        ):
            out_path = silver_dir / band_filename(band, date_key)

            if out_path.exists():
                logger.info("[M7] output sudah ada, skip: %s", out_path.name)
                bands[band] = {
                    "band": band,
                    "product": product,
                    "path": str(out_path),
                    "checksum_md5": _md5(out_path),
                    "skipped": True,
                    "degraded": False,
                    "failed_tiles": [],
                }
                continue

            try:
                bands[band] = _build_band_for_date(
                    band=band, product=product, date=date, date_key=date_key,
                    tiles=tiles, raw_dir=raw_dir, out_path=out_path, aoi_bbox=aoi_bbox,
                    plog=plog, dataset_id=dataset_id, scene_label=scene_label,
                )
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
            f"({len(bands)}/{len(MODIS_PRODUCT_TYPES)} band)",
            {
                "date": date.date().isoformat(),
                "bands_ok": sorted(bands), "bands_failed": sorted(band_errors),
                "degraded": degraded,
            },
        )

    if not daily_outputs:
        raise RuntimeError(
            f"tidak ada produk MODIS ditemukan untuk rentang "
            f"{date_start.date()}..{date_end.date()} di tiles {tiles}"
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
        "products": [MODIS_FLOOD_PRODUCT, MODIS_REFLECTANCE_PRODUCT],
        "dataset_id": dataset_id,
        "date_start": date_start.date().isoformat(),
        "date_end": date_end.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "tiles": tiles,
        "crs": DST_CRS,
        "outputs": daily_outputs,
        "quality": quality,
        "failed_days": failed_days,
    }

    logger.info("[M7] selesai: %d hari diproses untuk dataset_id=%s", len(daily_outputs), dataset_id)
    return product_id, metadata
