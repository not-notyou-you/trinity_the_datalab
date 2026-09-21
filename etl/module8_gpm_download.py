# etl/module8_gpm_download.py
"""
Downloads NASA GES DISC GPM IMERG daily rainfall product (Final Run
GPM_3IMERGDF, falling back to Late Run GPM_3IMERGDL for dates not yet
published in Final), aggregates it into 24h/72h/7-day accumulation windows,
reprojects/crops it to the dataset AOI at the Sentinel-1 grid resolution, and
writes one GeoTIFF per window for lineage tracking.

Output ditulis ke data/datasets/{id}_{slug}/{YYYYMMDD}/silver/gpm/ dan
granule mentahnya di-cache di _granule_cache/gpm/. Cache-nya flat (bukan per-tanggal)
karena satu granule harian ikut dipakai window 72h/7d tanggal-tanggal
berikutnya — lihat folder_manager.get_granule_cache_dir.

LEVEL PEMROSESAN (DOCS/ETL.md, "GPM IMERG Pipeline")
    RAW        cuma curah hujan hari itu (window 24h = 1 granule) -> BRONZE.
               Hari-hari sebelumnya TIDAK diunduh: yang membuat sebuah window
               "akumulasi" justru granule tetangga itu, dan level RAW
               didefinisikan sebagai "tanpa akumulasi multi-hari".
    PROCESSED  window 24h + 72h + 7d -> SILVER (lalu COG GOLD lewat
               module9_fusion._promote_aux_to_gold). Butuh hari target + 6
               hari sebelumnya.

Jumlah granule yang diunduh karena itu turun dari 7 menjadi 1 untuk dataset
GPM RAW-only — penghematan yang justru jadi alasan level RAW ada.

Dataset yang meminta KEDUANYA mendapat kedua artefak berdampingan: window 24h
ditulis dua kali (bronze/ sebagai deliverable RAW, silver/ sebagai lapisan
pertama jalur PROCESSED), tanpa build ulang.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import shutil
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import reproject

from etl import download_guard as dg
from etl import folder_manager as fm
from etl.pipeline_logger import PipelineLogger
from etl.processing_plan import GPM as GPM_SOURCE_NAME
from etl.processing_plan import PROCESSED, SourcePlan, normalize_levels

from etl import tier_names as tn

logger = logging.getLogger(__name__)

MODULE = "MODULE8_GPM_DOWNLOAD"
IMERG_VERSION = "07"
GES_DISC_ROOT = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3"
IMERG_SUBDATASET = "precipitation"  # mm, HDF5/NetCDF variable name

# IMERG Final Run (GPM_3IMERGDF) is the primary, gauge-calibrated product but
# is published with months of latency (per Sep 2026, V07 Final on GES DISC
# ends at 2025-09). For dates not covered by Final, fall back to Late Run
# (GPM_3IMERGDL, ~14h latency, satellite-only), then Early Run (GPM_3IMERGDE,
# ~4h latency). Falling back is recorded per-day/window so downstream
# consumers know the accumulation isn't built purely from the calibrated
# product.
IMERG_RUNS = {
    "F": {"product": "GPM_3IMERGDF", "file_infix": ""},
    "L": {"product": "GPM_3IMERGDL", "file_infix": "-L"},
    "E": {"product": "GPM_3IMERGDE", "file_infix": "-E"},
}
IMERG_RUN_ORDER = ["F", "L", "E"]

# Listing folder bulanan GES DISC di-cache per proses: satu window 7d
# menyentuh folder yang sama sampai 7 hari x 3 run. TTL supaya granule yang
# baru terbit (live scheduler harian) tetap terlihat.
_LISTING_TTL_S = 3600
_listing_cache: dict[tuple[str, int, int], tuple[float, frozenset[str]]] = {}
_listing_lock = threading.Lock()

# Jabodetabek bounding box, WGS84 (min_lon, min_lat, max_lon, max_lat)
JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)

DST_CRS = "EPSG:4326"
# Resolusi asli IMERG. Berkas rainfall disimpan di resolusi ini (lihat
# _crop_to_aoi), jadi inilah yang dilaporkan sidecar -- bukan lagi resolusi
# Sentinel-1, yang dulu benar hanya karena berkasnya di-upsample ke sana.
IMERG_RESOLUTION_DEG = 0.1
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


def _daily_granule_filename(date: datetime, run: str, minor: str = "B") -> str:
    """Nama granule dengan huruf minor versi TEBAKAN. Hanya dipakai sebagai
    cadangan kalau listing folder gagal — lihat _resolve_daily_granule."""
    date_str = date.strftime("%Y%m%d")
    infix = IMERG_RUNS[run]["file_infix"]
    return f"3B-DAY{infix}.MS.MRG.3IMERG.{date_str}-S000000-E235959.V{IMERG_VERSION}{minor}.nc4"


def _run_base_url(run: str) -> str:
    return f"{GES_DISC_ROOT}/{IMERG_RUNS[run]['product']}.{IMERG_VERSION}"


def _daily_granule_url(date: datetime, run: str) -> str:
    return f"{_run_base_url(run)}/{date.year}/{date.month:02d}/{_daily_granule_filename(date, run)}"


def _granule_pattern(date: datetime, run: str) -> re.Pattern:
    infix = re.escape(IMERG_RUNS[run]["file_infix"])
    return re.compile(
        rf"^3B-DAY{infix}\.MS\.MRG\.3IMERG\.{date:%Y%m%d}-S000000-E235959"
        rf"\.V{IMERG_VERSION}([A-Z])\.nc4$"
    )


def _list_month_granules(run: str, year: int, month: int) -> frozenset[str]:
    """Nama berkas .nc4 di folder bulanan GES DISC untuk satu run. Folder
    yang 404 (run itu belum/tidak menerbitkan bulan tsb) = himpunan kosong."""
    import requests

    key = (run, year, month)
    now = time.monotonic()
    with _listing_lock:
        hit = _listing_cache.get(key)
        if hit and now - hit[0] < _LISTING_TTL_S:
            return hit[1]

    resp = requests.get(
        f"{_run_base_url(run)}/{year}/{month:02d}/", headers=_auth_headers(), timeout=60
    )
    if resp.status_code == 404:
        names: frozenset[str] = frozenset()
    else:
        resp.raise_for_status()
        names = frozenset(re.findall(r'href="(?:[^"]*/)?([^"/?#]+\.nc4)"', resp.text))
    with _listing_lock:
        _listing_cache[key] = (now, names)
    return names


def _resolve_daily_granule(date: datetime, run: str, raw_dir: Path) -> tuple[str, str] | None:
    """(nama berkas, URL) granule harian `run` untuk `date`, atau None kalau
    run itu memang belum menerbitkannya.

    Huruf minor versi TIDAK di-hardcode: GES DISC mengganti V07B -> V07C di
    awal Maret 2026, dan nama tebakan "V07B" membuat Final maupun Late 404
    untuk semua tanggal sesudahnya walau datanya ada. Nama dibaca dari listing
    folder bulanannya; kalau satu hari punya beberapa minor, yang terbaru
    dipakai. Granule yang sudah ada di cache lokal dipakai tanpa listing."""
    import requests

    pat = _granule_pattern(date, run)
    month_url = f"{_run_base_url(run)}/{date.year}/{date.month:02d}"

    local = sorted(
        (m.group(1), p.name) for p in raw_dir.glob("3B-DAY*.nc4") if (m := pat.match(p.name))
    )
    if local:
        return local[-1][1], f"{month_url}/{local[-1][1]}"

    try:
        names = _list_month_granules(run, date.year, date.month)
    except requests.RequestException as exc:
        guess = _daily_granule_filename(date, run)
        logger.warning(
            "[M8] listing %s gagal (%s), coba nama tebakan %s", month_url, exc, guess
        )
        return guess, f"{month_url}/{guess}"

    candidates = sorted((m.group(1), n) for n in names if (m := pat.match(n)))
    if not candidates:
        return None
    return candidates[-1][1], f"{month_url}/{candidates[-1][1]}"


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
    if dg.reuse_granule(out_path, "gpm", fm.DATA_ROOT, "[M8]"):
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


def _snap_resolution(res: float) -> float:
    """Resolusi grid IMERG dari selisih koordinat, dijepit ke 0.1 derajat.

    Koordinat lat/lon IMERG disimpan float32, jadi (lon[-1] - lon[0]) / (n-1)
    menghasilkan 0.0999999983, bukan 0.1. Selisih 1.7e-9 itu terakumulasi
    sepanjang 2.870 sel dari -180 sampai AOI, sehingga tepi sel di 107.2
    bergeser beberapa mikroderajat -- cukup untuk membuat crop AOI 106.4-107.2
    ikut mengambil kolom ke-9 (107.2-107.3) yang seluruhnya nodata (dataset
    24_try8: semua berkas GPM 9x8, bukan 8x8). Kalau hasil hitungnya berbeda
    jauh dari resolusi resmi, nilai hitungnya yang dipakai: berarti berkasnya
    memang bukan grid 0.1 derajat, dan menjepitnya akan menggeser seluruh
    raster."""
    if abs(res - IMERG_RESOLUTION_DEG) < 1e-6:
        return IMERG_RESOLUTION_DEG
    return res


def _snap_edge(edge: float, res: float) -> float:
    """Tepi grid ke kelipatan `res` terdekat (-180/90 untuk IMERG global)."""
    return round(round(edge / res) * res, 9)


def _read_daily_precip(nc4_path: Path):
    """Read the daily precipitation band (mm/day) from an IMERG NetCDF granule.
    Nodata pixels are filled with 0 mm so they contribute nothing to the
    accumulation sums downstream.

    Dibaca lewat h5py dari koordinat lat/lon file, bukan driver NETCDF GDAL:
    variabel IMERG berdimensi (time, lon, lat), dan GDAL membacanya tanpa
    geotransform (matriks identitas, CRS kosong) serta tertransposisi
    3600x1800 — crop ke AOI lalu selalu gagal "Input shapes do not overlap
    raster"."""
    import h5py
    import numpy as np
    from rasterio.transform import from_origin

    with h5py.File(nc4_path, "r") as h5:
        var = h5[IMERG_SUBDATASET]
        arr = var[0] if var.ndim == 3 else var[()]
        lat = h5["lat"][:].astype("float64")
        lon = h5["lon"][:].astype("float64")

    data = np.asarray(arr, dtype="float64")
    if data.shape == (lon.size, lat.size) and lon.size != lat.size:
        data = data.T  # (lon, lat) -> (lat, lon)
    elif data.shape != (lat.size, lon.size):
        raise RuntimeError(
            f"dimensi {IMERG_SUBDATASET} {data.shape} tidak cocok dengan "
            f"lat={lat.size} lon={lon.size} di {nc4_path.name}"
        )
    if lat[0] < lat[-1]:  # utara di atas
        data = data[::-1]
        lat = lat[::-1]
    if lon[0] > lon[-1]:
        data = data[:, ::-1]
        lon = lon[::-1]

    res_x = _snap_resolution(abs(lon[-1] - lon[0]) / (lon.size - 1))
    res_y = _snap_resolution(abs(lat[0] - lat[-1]) / (lat.size - 1))
    # Tepi grid di-snap ke kelipatan resolusi: koordinat IMERG disimpan
    # float32, jadi lon[0] - res/2 hasilnya -179.999997, bukan -180 persis.
    transform = from_origin(
        _snap_edge(lon[0] - res_x / 2, res_x), _snap_edge(lat[0] + res_y / 2, res_y),
        res_x, res_y,
    )

    # _FillValue IMERG = -9999.9; curah hujan tidak pernah negatif.
    data[~np.isfinite(data) | (data < 0)] = 0.0
    return np.ascontiguousarray(data), transform, DST_CRS


def _fetch_daily_precip(
    date: datetime,
    raw_dir: Path,
    *,
    plog: PipelineLogger | None = None,
    dataset_id: int | None = None,
    scene_id: str = "",
    window_name: str = "",
) -> tuple:
    """Try each run in `IMERG_RUN_ORDER` (Final, Late, Early) for `date`,
    falling through to the next run only when the previous one genuinely
    hasn't published the granule (absent from its monthly listing, or 404).
    A run that hasn't published is not a failure, so it isn't attempted —
    and therefore never logged as a FAILED download."""
    not_found_reasons = []
    for run in IMERG_RUN_ORDER:
        resolved = _resolve_daily_granule(date, run, raw_dir)
        if resolved is None:
            not_found_reasons.append(f"{run}: belum terbit di {IMERG_RUNS[run]['product']}")
            continue
        filename, url = resolved
        nc4_path = raw_dir / filename
        try:
            checksum = _download_with_retry(
                url, nc4_path,
                plog=plog, dataset_id=dataset_id, scene_id=scene_id,
                item_label=f"{window_name} day {date.date().isoformat()} ({run})",
            )
        except _GranuleNotFound as exc:
            not_found_reasons.append(f"{run}: {exc}")
            continue
        if not_found_reasons:
            logger.info(
                "[M8] %s: %s -> pakai %s",
                date.date().isoformat(), "; ".join(not_found_reasons), filename,
            )
        data, transform, crs = _read_daily_precip(nc4_path)
        return data, transform, crs, checksum, run

    raise RuntimeError(
        f"tidak ada produk IMERG ({'/'.join(IMERG_RUN_ORDER)}) untuk tanggal "
        f"{date.date().isoformat()}: " + "; ".join(not_found_reasons)
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


def _crop_to_aoi(
    accum,
    src_transform,
    src_crs: str,
    aoi_bbox: tuple[float, float, float, float],
    output_path: Path,
    tags: dict[str, str] | None = None,
) -> Path:
    """Crop akumulasi hujan ke AOI dan simpan PADA RESOLUSI ASLINYA (0.1
    derajat IMERG).

    Sebelumnya berkas ini di-resample ke grid Sentinel-1 ~10 m supaya bisa
    ditumpuk langsung dengan S1. Itu menggandakan nilai yang sama jutaan kali:
    AOI 0,8 derajat memuat 8x8 sel IMERG, tapi berkasnya ditulis 8906x8906
    piksel -- 64 nilai unik dalam 2,4 MB, dan 214 MB untuk satu bulan (93
    berkas di dataset 22_try6). Data aslinya muat di bawah 1 KB.

    Penumpukan tidak hilang karena itu: kedua konsumennya mereproyeksi sendiri
    ke grid tujuan masing-masing -- module9 lewat _reproject_to_grid ke grid
    dataset, module10 ke grid preview -- dan keduanya memakai nearest, jadi
    hasil akhirnya identik dengan resample dini ini. Yang hilang cuma salinan
    perantara yang kebesaran.

    Crop tetap dilakukan: akumulasi IMERG mentah jauh lebih luas dari AOI, dan
    menyimpan seluruh globe per tanggal jauh lebih mahal daripada memotongnya.
    Setiap sel yang tersentuh AOI ikut, termasuk sel tepi yang pusatnya di
    luar bbox -- tanpa itu 12,5% AOI jadi nodata. Konsekuensinya berkas ini
    bisa sedikit LEBIH LUAS dari AOI (sebatas sel IMERG yang tersentuh), dan
    itu memang yang diinginkan: pemotongan presisi dilakukan saat reproyeksi
    ke grid tujuan. Sel yang cuma BERSINGGUNGAN di tepi tidak ikut.

    `tags` ditulis sebagai metadata GeoTIFF (lihat _window_tags): berkas
    akumulasi tidak menyebut sendiri rentang waktu yang dijumlahkannya, dan
    module9 menyalin tag ini ke atribut lapisan HDF5.

    Disimpan float32 + DEFLATE: presisi float64 tidak bermakna untuk mm hujan.
    """
    from rasterio.windows import Window
    from rasterio.windows import transform as window_transform

    # Jendela sel dihitung langsung dari affine, bukan lewat rasterize
    # all_touched: tepi AOI di sini jatuh TEPAT di tepi sel (106.4, 107.2, ...),
    # dan all_touched pada garis yang berimpit dengan tepi piksel ikut
    # mengambil sel tetangganya -- kolom nodata ekstra di 24_try8. Toleransi
    # 1e-6 sel membuang sisa pembulatan float tanpa pernah membuang sel yang
    # benar-benar tersentuh AOI.
    height, width = accum.shape
    min_lon, min_lat, max_lon, max_lat = aoi_bbox
    inv = ~src_transform
    col_a, row_a = inv * (min_lon, max_lat)
    col_b, row_b = inv * (max_lon, min_lat)
    eps = 1e-6
    col0 = max(0, int(np.floor(min(col_a, col_b) + eps)))
    col1 = min(width, int(np.ceil(max(col_a, col_b) - eps)))
    row0 = max(0, int(np.floor(min(row_a, row_b) + eps)))
    row1 = min(height, int(np.ceil(max(row_a, row_b) - eps)))
    if col1 <= col0 or row1 <= row0:
        raise ValueError(f"AOI {aoi_bbox} tidak beririsan dengan grid IMERG")
    window = Window(col0, row0, col1 - col0, row1 - row0)
    crop_image = accum[row0:row1, col0:col1][np.newaxis, ...]
    crop_transform = window_transform(window, src_transform)
    crop_crs = src_crs

    dest = crop_image[0].astype("float32")
    dst_height, dst_width = dest.shape

    with rasterio.open(
        output_path, "w", driver="GTiff", height=dst_height, width=dst_width,
        count=1, dtype="float32", crs=crop_crs, transform=crop_transform,
        nodata=DEFAULT_NODATA, compress="deflate", predictor=3,
        tiled=True, blockxsize=512, blockysize=512,
    ) as dst:
        dst.write(dest, 1)
        if tags:
            dst.update_tags(**tags)

    return output_path


def _window_tags(date: datetime, window_name: str, num_days: int, runs: list[str]) -> dict[str, str]:
    """Metadata waktu satu berkas akumulasi.

    Granule harian IMERG mencakup satu hari UTC penuh (00:00-24:00), jadi
    window `num_days` yang berakhir di `date` menjumlahkan hujan dari 00:00
    UTC hari pertama sampai 00:00 UTC hari SETELAH `date`. Ditulis eksplisit
    karena konsumen fusion perlu tahu berapa jam hujan di window ini jatuh
    SETELAH citra Sentinel-1 diambil."""
    day = datetime(date.year, date.month, date.day)
    start = day - timedelta(days=num_days - 1)
    end = day + timedelta(days=1)
    return {
        "SOURCE_PRODUCT": "GPM_3IMERGD",
        "WINDOW": window_name,
        "WINDOW_DAYS": str(num_days),
        "WINDOW_START_UTC": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "WINDOW_END_UTC": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "IMERG_RUNS": ",".join(runs),
        "UNITS": "mm",
    }


def _is_current_format(path: Path) -> bool:
    """True kalau berkas akumulasi ditulis versi yang mencatat jendela
    waktunya (tag WINDOW_END_UTC, lihat _window_tags)."""
    try:
        with rasterio.open(path) as src:
            return "WINDOW_END_UTC" in src.tags()
    except Exception:
        return False


def _window_targets(
    dataset_id: int,
    dataset_name: str,
    window_name: str,
    date_key: str,
    targets: tuple[tuple[str, str], ...],
) -> list[tuple[str, str, Path]]:
    """(tier, processing_level, path) untuk satu window, tier tertinggi dulu.

    Tier tertinggi dibangun; target lain diisi dengan menyalin berkas itu."""
    ordered = sorted(targets, key=lambda t: 0 if tn.rank(t[0]) == 2 else 1)
    out = []
    for tier, level in ordered:
        scene_dir = fm.ensure_scene_dir(
            dataset_id, dataset_name, tier.lower(), "gpm", date_key
        )
        out.append((tier, level, scene_dir / band_filename(window_name, date_key)))
    return out


def download_gpm_scene(
    dataset_id: int,
    dataset_name: str,
    date: datetime,
    aoi_bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
    plog: PipelineLogger | None = None,
    processing_levels=(PROCESSED,),
) -> tuple[list[str], dict]:
    """
    Build 24h/72h/7-day rainfall accumulation GeoTIFFs for `date` from NASA
    GES DISC GPM IMERG Final daily granules, reprojected/cropped to `aoi_bbox`
    at the Sentinel-1 grid resolution.

    Writes (fusion *inputs*, consumed by module9_fusion.py — not a GOLD
    deliverable themselves):
        data/datasets/{id}_{slug}/{date}/silver/gpm/gpm_rain_24h_{date}.tif
        data/datasets/{id}_{slug}/{date}/silver/gpm/gpm_rain_72h_{date}.tif
        data/datasets/{id}_{slug}/{date}/silver/gpm/gpm_rain_7d_{date}.tif

    `processing_levels` (dari dataset_source_config) menentukan window mana
    yang dibangun — dan karena itu berapa granule harian yang diunduh:
        {"RAW"}              24h saja (1 granule)   -> bronze/gpm/{date}/
        {"PROCESSED"}        24h+72h+7d (7 granule) -> silver/gpm/{date}/
        {"RAW","PROCESSED"}  keduanya; 24h ada di bronze/ DAN silver/

    Each window (24h/72h/7d) is built independently: a window whose daily
    granules fail to download (after retries) is logged and skipped rather
    than aborting the other windows. Pass `plog` to also emit structured
    per-window/per-day/summary events to the `processing_logs` table
    (visible in the live UI panel).

    Returns:
        (product_ids, metadata_dict) — product_ids covers only the windows
        that succeeded; metadata_dict carries per-window output paths (with
        the tier & processing_level of each copy under `windows[w]["targets"]`),
        checksums, the source daily granules each window was built from, and
        an overall `quality`/`failed_windows` summary.
    """
    plan = SourcePlan(
        source_name=GPM_SOURCE_NAME,
        levels=normalize_levels(processing_levels) or (PROCESSED,),
    )
    window_targets = plan.targets()
    wanted_windows = plan.gpm_windows()
    logger.info(
        "[M8] dataset_id=%s level=%s window=%s granule_hari=%d",
        dataset_id, list(plan.levels), list(wanted_windows), plan.gpm_days(),
    )

    date_key = date.strftime("%Y%m%d")
    raw_dir = fm.get_granule_cache_dir(dataset_id, dataset_name, "gpm")
    raw_dir.mkdir(parents=True, exist_ok=True)

    scene_label = f"GPM_{date_key}"
    product_ids = []
    window_outputs = {}
    failed_windows: list[dict] = []

    for window_name, num_days in WINDOWS.items():
        if window_name not in wanted_windows:
            continue

        targets = _window_targets(
            dataset_id, dataset_name, window_name, date_key, window_targets[window_name]
        )
        build_tier, build_level, out_path = targets[0]
        product_id = f"GPM_3IMERGD.{window_name}.{date_key}.jabodetabek"

        def _record(entry: dict, _out=out_path, _targets=targets,
                    _tier=build_tier, _level=build_level) -> dict:
            """Lengkapi entry window dengan salinan ke target lain."""
            written = {
                _tier: {
                    "path": str(_out),
                    "processing_level": _level,
                    "checksum_md5": entry["checksum_md5"],
                }
            }
            for tier, level, copy_path in _targets[1:]:
                if not copy_path.exists():
                    shutil.copy2(_out, copy_path)
                written[tier] = {
                    "path": str(copy_path),
                    "processing_level": level,
                    "checksum_md5": entry["checksum_md5"],
                }
            entry["targets"] = written
            return entry

        if out_path.exists() and not _is_current_format(out_path):
            # Berkas dari versi sebelum snap grid 0.1 derajat (kolom nodata
            # ekstra) dan tanpa tag jendela waktu: bangun ulang, jangan pakai.
            logger.info("[M8] format lama, bangun ulang: %s", out_path.name)
            for _tier, _level, stale in targets:
                stale.unlink(missing_ok=True)
        if out_path.exists():
            logger.info("[M8] output sudah ada, skip: %s", out_path.name)
            window_outputs[window_name] = _record({
                "path": str(out_path),
                "checksum_md5": _md5(out_path),
                "days_aggregated": num_days,
                "skipped": True,
            })
            product_ids.append(product_id)
            continue

        try:
            accum, transform, crs, source_checksums = _accumulate_window(
                date, num_days, raw_dir,
                plog=plog, dataset_id=dataset_id, scene_id=scene_label, window_name=window_name,
            )
            _crop_to_aoi(
                accum, transform, crs, aoi_bbox, out_path,
                tags=_window_tags(
                    date, window_name, num_days,
                    sorted({e["run"] for e in source_checksums.values()}),
                ),
            )
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
        window_outputs[window_name] = _record({
            "path": str(out_path),
            "checksum_md5": _md5(out_path),
            "days_aggregated": num_days,
            "source_checksums": source_checksums,
            "runs_used": sorted(runs_used),
            "skipped": False,
        })
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
        run in ("L", "E")
        for output in window_outputs.values()
        if not output.get("skipped")
        for run in output.get("runs_used", [])
    )
    if failed_windows:
        quality = "DEGRADED"
    elif used_late_run:
        quality = "LATE_RUN"
    else:
        quality = "GOOD"
    _plog_event(
        plog, dataset_id, scene_label, "DOWNLOAD_SUMMARY", "COMPLETED",
        f"GPM selesai: {len(window_outputs)}/{len(wanted_windows)} produk"
        + (f", gagal: {', '.join(w['window'] for w in failed_windows)}" if failed_windows else ""),
        {
            "windows_ok": list(window_outputs.keys()),
            "windows_failed": failed_windows, "quality": quality,
        },
    )

    metadata = {
        "product": "GPM_3IMERGD",
        "processing_levels": list(plan.levels),
        "windows_requested": list(wanted_windows),
        "dataset_id": dataset_id,
        "date": date.date().isoformat(),
        "aoi_bbox": aoi_bbox,
        "crs": DST_CRS,
        "resolution_deg": IMERG_RESOLUTION_DEG,
        "windows": window_outputs,
        "quality": quality,
        "failed_windows": failed_windows,
    }

    logger.info(
        "[M8] selesai: %d/%d produk rainfall dibuat untuk dataset_id=%s tanggal=%s",
        len(window_outputs), len(wanted_windows), dataset_id, date.date().isoformat(),
    )
    return product_ids, metadata
