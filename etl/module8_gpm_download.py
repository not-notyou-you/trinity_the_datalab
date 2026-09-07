# etl/module8_gpm_download.py
"""
Downloads NASA GES DISC GPM IMERG daily rainfall product (Final Run
GPM_3IMERGDF, falling back to Late Run GPM_3IMERGDL for dates not yet
published in Final), aggregates it into 24h/72h/7-day accumulation windows,
reprojects/crops it to the dataset AOI at the Sentinel-1 grid resolution, and
writes one GeoTIFF per window for lineage tracking.

Output ditulis ke data/datasets/{id}_{slug}/silver/gpm/{YYYYMMDD}/ dan
granule mentahnya di-cache di raw/gpm/. Cache-nya flat (bukan per-tanggal)
karena satu granule harian ikut dipakai window 72h/7d tanggal-tanggal
berikutnya — lihat folder_manager.get_granule_cache_dir.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import rasterio
from rasterio.enums import Resampling
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rasterio.warp import calculate_default_transform, reproject
from shapely.geometry import box, mapping

from etl import folder_manager as fm
from etl.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)

MODULE = "MODULE8_GPM_DOWNLOAD"
IMERG_VERSION = "07"
GES_DISC_ROOT = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3"
IMERG_SUBDATASET = "precipitation"  # mm, HDF5/NetCDF variable name

# IMERG Final Run (GPM_3IMERGDF) is the primary, gauge-calibrated product but
# is published with ~3-4 months of latency. For dates not yet covered by
# Final, fall back to Late Run (GPM_3IMERGDL, ~14h latency, satellite-only).
# Falling back is recorded per-day/window so downstream consumers know the
# accumulation isn't built purely from the calibrated product.
IMERG_RUNS = {
    "F": {"product": "GPM_3IMERGDF", "file_infix": ""},
    "L": {"product": "GPM_3IMERGDL", "file_infix": "-L"},
}
IMERG_RUN_ORDER = ["F", "L"]

# Jabodetabek bounding box, WGS84 (min_lon, min_lat, max_lon, max_lat)
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)

DST_CRS = "EPSG:4326"
S1_RESOLUTION_M = 10
S1_RESOLUTION_DEG = S1_RESOLUTION_M / 111_320.0  # meters -> degrees at the equator
MAX_RETRIES = 3
DEFAULT_NODATA = -9999.9

GPM_PRODUCT_TYPE = "GPM_RAINFALL"


def band_filename(window_name: str, date_key: str) -> str:
    """Nama file GeoTIFF harian untuk satu window akumulasi. Satu-satunya
    tempat pola nama ini didefinisikan — module9_fusion.py mencari file
    input lewat fungsi ini, bukan lewat string literal-nya sendiri."""
    return f"gpm_rain_{window_name}_{date_key}.tif"


def band_name(window_name: str) -> str:
    """Window akumulasi -> data_products.band_name, mis. "24h" -> RAIN_24H."""
    return f"RAIN_{window_name.upper()}"

# window name -> number of trailing days to accumulate, ending on the target date
WINDOWS = {
    "24h": 1,
    "72h": 3,
    "7d": 7,
}


def _auth_headers() -> dict:
    token = os.getenv("NASA_EARTHDATA_TOKEN")
    if not token:
        raise RuntimeError(
            "NASA_EARTHDATA_TOKEN belum diset. Generate app token di "
            "urs.earthdata.nasa.gov -> Generate Token."
        )
    return {"Authorization": f"Bearer {token}"}


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


class _GranuleNotFound(Exception):
    """Raised when GES DISC returns 404 for a granule — the file doesn't
    exist for that run/date, so retrying the same URL is pointless."""


def _daily_granule_filename(date: datetime, run: str) -> str:
    date_str = date.strftime("%Y%m%d")
    infix = IMERG_RUNS[run]["file_infix"]
    return f"3B-DAY{infix}.MS.MRG.3IMERG.{date_str}-S000000-E235959.V{IMERG_VERSION}B.nc4"


def _daily_granule_url(date: datetime, run: str) -> str:
    product = IMERG_RUNS[run]["product"]
    base = f"{GES_DISC_ROOT}/{product}.{IMERG_VERSION}"
    return f"{base}/{date.year}/{date.month:02d}/{_daily_granule_filename(date, run)}"


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
        logger.info("[M8] sudah ada di disk, lewati download: %s", out_path.name)
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
            logger.info("[M8] downloaded %s (md5=%s...)", out_path.name, checksum[:12])
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
            not_found = isinstance(exc, requests.exceptions.HTTPError) and (
                exc.response is not None and exc.response.status_code == 404
            )
            logger.warning(
                "[M8] download gagal (attempt %d/%d) %s: %s",
                attempt, MAX_RETRIES, out_path.name, exc,
            )
            tmp_path.unlink(missing_ok=True)
            is_final = not_found or attempt == MAX_RETRIES
            _plog_event(
                plog, dataset_id, scene_id, "DOWNLOAD", "FAILED" if is_final else "RUNNING",
                f"{item_label}: attempt {attempt}/{MAX_RETRIES} failed ({exc})",
                {
                    "item": item_label, "attempt": attempt, "max_retries": MAX_RETRIES,
                    "error_type": type(exc).__name__, "error_message": str(exc),
                    "duration_seconds": round(time.monotonic() - attempt_started, 3),
                },
            )
            if not_found:
                # granule genuinely doesn't exist for this run/date yet (e.g. Final
                # Run not published) — retrying the same URL won't help.
                raise _GranuleNotFound(str(exc)) from exc
            if attempt < MAX_RETRIES:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"gagal download {url} setelah {MAX_RETRIES} percobaan: {last_exc}")


def _read_daily_precip(nc4_path: Path):
    """Read the daily precipitation band (mm/day) from an IMERG NetCDF granule.
    Nodata pixels are filled with 0 mm so they contribute nothing to the
    accumulation sums downstream."""
    src_path = f'NETCDF:"{nc4_path}":{IMERG_SUBDATASET}'
    with rasterio.open(src_path) as src:
        data = src.read(1).astype("float64")
        transform = src.transform
        crs = str(src.crs) if src.crs else DST_CRS
        nodata = src.nodata if src.nodata is not None else DEFAULT_NODATA
        data[data == nodata] = 0.0
    return data, transform, crs


def _fetch_daily_precip(
    date: datetime,
    raw_dir: Path,
    *,
    plog: PipelineLogger | None = None,
    dataset_id: int | None = None,
    scene_id: str = "",
    window_name: str = "",
) -> tuple:
    """Try each run in `IMERG_RUN_ORDER` (Final, then Late) for `date`,
    falling through to the next run only when the granule genuinely doesn't
    exist (404) for the previous one."""
    not_found_reasons = []
    for run in IMERG_RUN_ORDER:
        filename = _daily_granule_filename(date, run)
        nc4_path = raw_dir / filename
        try:
            checksum = _download_with_retry(
                _daily_granule_url(date, run), nc4_path,
                plog=plog, dataset_id=dataset_id, scene_id=scene_id,
                item_label=f"{window_name} day {date.date().isoformat()} ({run})",
            )
        except _GranuleNotFound as exc:
            not_found_reasons.append(f"{run}: {exc}")
            continue
        data, transform, crs = _read_daily_precip(nc4_path)
        return data, transform, crs, checksum, run

    raise RuntimeError(
        f"tidak ada produk IMERG (Final/Late) untuk tanggal {date.date().isoformat()}: "
        + "; ".join(not_found_reasons)
    )


def _accumulate_window(
    end_date: datetime,
    num_days: int,
    raw_dir: Path,
    *,
    plog: PipelineLogger | None = None,
    dataset_id: int | None = None,
    scene_id: str = "",
    window_name: str = "",
) -> tuple:
    """Sum daily IMERG rainfall over the `num_days` ending on `end_date`
    (inclusive). All daily granules share the same fixed global grid, so the
    per-pixel sums line up without any resampling at this stage."""
    accum = None
    transform = crs = None
    source_checksums = {}

    for offset in range(num_days):
        day = end_date - timedelta(days=offset)
        data, day_transform, day_crs, checksum, run = _fetch_daily_precip(
            day, raw_dir, plog=plog, dataset_id=dataset_id, scene_id=scene_id, window_name=window_name,
        )
        source_checksums[day.date().isoformat()] = {"checksum_md5": checksum, "run": run}

        if accum is None:
            accum = data
            transform, crs = day_transform, day_crs
        else:
            accum = accum + data

    return accum, transform, crs, source_checksums


def _reproject_and_crop_to_s1_grid(
    accum,
    src_transform,
    src_crs: str,
    aoi_bbox: tuple[float, float, float, float],
    output_path: Path,
) -> Path:
    """Reproject the accumulated rainfall grid to the Sentinel-1 target
    resolution/CRS and crop it to the AOI, matching module7's mosaic/crop
    pattern for MODIS."""
    height, width = accum.shape

    with MemoryFile() as memfile:
        with memfile.open(
            driver="GTiff", height=height, width=width, count=1,
            dtype="float64", crs=src_crs, transform=src_transform,
            nodata=DEFAULT_NODATA,
        ) as tmp:
            tmp.write(accum, 1)

        with memfile.open() as src:
            dst_transform, dst_width, dst_height = calculate_default_transform(
                src.crs, DST_CRS, src.width, src.height, *src.bounds,
                resolution=S1_RESOLUTION_DEG,
            )
            kwargs = src.meta.copy()
            kwargs.update({
                "driver": "GTiff",
                "crs": DST_CRS,
                "transform": dst_transform,
                "width": dst_width,
                "height": dst_height,
            })
            reproj_path = output_path.with_name(output_path.stem + "_reproj.tif")
            with rasterio.open(reproj_path, "w", **kwargs) as dst:
                reproject(
                    source=rasterio.band(src, 1),
                    destination=rasterio.band(dst, 1),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=DST_CRS,
                    resampling=Resampling.bilinear,
                )

    geom = mapping(box(*aoi_bbox))
    with rasterio.open(reproj_path) as src:
        out_image, crop_transform = mask(src, [geom], crop=True, nodata=DEFAULT_NODATA)
        crop_meta = src.meta.copy()
        crop_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": crop_transform,
        })
        with rasterio.open(output_path, "w", **crop_meta) as dst:
            dst.write(out_image)

    reproj_path.unlink(missing_ok=True)
    return output_path


def download_gpm_scene(
    dataset_id: int,
    dataset_name: str,
    date: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    plog: PipelineLogger | None = None,
) -> tuple[list[str], dict]:
    """
    Build 24h/72h/7-day rainfall accumulation GeoTIFFs for `date` from NASA
    GES DISC GPM IMERG Final daily granules, reprojected/cropped to `aoi_bbox`
    at the Sentinel-1 grid resolution.

    Writes (fusion *inputs*, consumed by module9_fusion.py — not a GOLD
    deliverable themselves):
        data/datasets/{id}_{slug}/silver/{date}/gpm_rain_24h_{date}.tif
        data/datasets/{id}_{slug}/silver/{date}/gpm_rain_72h_{date}.tif
        data/datasets/{id}_{slug}/silver/{date}/gpm_rain_7d_{date}.tif

    Each window (24h/72h/7d) is built independently: a window whose daily
    granules fail to download (after retries) is logged and skipped rather
    than aborting the other windows. Pass `plog` to also emit structured
    per-window/per-day/summary events to the `processing_logs` table
    (visible in the live UI panel).

    Returns:
        (product_ids, metadata_dict) — product_ids covers only the windows
        that succeeded; metadata_dict carries per-window output paths,
        checksums, the source daily granules each window was built from, and
        an overall `quality`/`failed_windows` summary.
    """
    date_key = date.strftime("%Y%m%d")
    silver_dir = fm.ensure_scene_dir(dataset_id, dataset_name, "silver", "gpm", date_key)
    raw_dir = fm.get_granule_cache_dir(dataset_id, dataset_name, "gpm")
    raw_dir.mkdir(parents=True, exist_ok=True)

    scene_label = f"GPM_{date_key}"
    product_ids = []
    window_outputs = {}
    failed_windows: list[dict] = []

    for window_name, num_days in WINDOWS.items():
        out_path = silver_dir / band_filename(window_name, date_key)
        product_id = f"GPM_3IMERGD.{window_name}.{date_key}.jabodetabek"

        if out_path.exists():
            logger.info("[M8] output sudah ada, skip: %s", out_path.name)
            window_outputs[window_name] = {
                "path": str(out_path),
                "checksum_md5": _md5(out_path),
                "days_aggregated": num_days,
                "skipped": True,
            }
            product_ids.append(product_id)
            continue

        try:
            accum, transform, crs, source_checksums = _accumulate_window(
                date, num_days, raw_dir,
                plog=plog, dataset_id=dataset_id, scene_id=scene_label, window_name=window_name,
            )
            _reproject_and_crop_to_s1_grid(accum, transform, crs, aoi_bbox, out_path)
        except Exception as exc:
            logger.warning("[M8] window %s gagal tanggal %s: %s", window_name, date.date().isoformat(), exc)
            _plog_event(
                plog, dataset_id, scene_label, "DOWNLOAD", "FAILED",
                f"GPM {window_name}: gagal ({exc})",
                {
                    "window": window_name, "days_aggregated": num_days,
                    "error_type": type(exc).__name__, "error_message": str(exc),
                },
            )
            failed_windows.append({"window": window_name, "reason": str(exc)})
            continue

        runs_used = {entry["run"] for entry in source_checksums.values()}
        window_outputs[window_name] = {
            "path": str(out_path),
            "checksum_md5": _md5(out_path),
            "days_aggregated": num_days,
            "source_checksums": source_checksums,
            "runs_used": sorted(runs_used),
            "skipped": False,
        }
        product_ids.append(product_id)
        _plog_event(
            plog, dataset_id, scene_label, "DOWNLOAD", "COMPLETED",
            f"GPM {window_name}: selesai ({num_days} hari)",
            {"window": window_name, "days_aggregated": num_days},
        )

    if not window_outputs:
        raise RuntimeError(
            f"semua produk GPM gagal untuk dataset_id={dataset_id} tanggal={date.date().isoformat()} "
            f"({failed_windows})"
        )

    used_late_run = any(
        "L" in output.get("runs_used", [])
        for output in window_outputs.values()
        if not output.get("skipped")
    )
    if failed_windows:
        quality = "DEGRADED"
    elif used_late_run:
        quality = "LATE_RUN"
    else:
        quality = "GOOD"
    _plog_event(
        plog, dataset_id, scene_label, "DOWNLOAD_SUMMARY", "COMPLETED",
        f"GPM selesai: {len(window_outputs)}/{len(WINDOWS)} produk"
        + (f", gagal: {', '.join(w['window'] for w in failed_windows)}" if failed_windows else ""),
        {
            "windows_ok": list(window_outputs.keys()),
            "windows_failed": failed_windows, "quality": quality,
        },
    )

    metadata = {
        "product": "GPM_3IMERGD",
        "dataset_id": dataset_id,
        "date": date.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "crs": DST_CRS,
        "resolution_m": S1_RESOLUTION_M,
        "windows": window_outputs,
        "quality": quality,
        "failed_windows": failed_windows,
    }

    logger.info(
        "[M8] selesai: %d/%d produk rainfall dibuat untuk dataset_id=%s tanggal=%s",
        len(window_outputs), len(WINDOWS), dataset_id, date.date().isoformat(),
    )
    return product_ids, metadata
