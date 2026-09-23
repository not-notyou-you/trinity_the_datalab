# etl/module1_download.py
"""
Tahap DOWNLOAD Sentinel-1: discovery CDSE + unduh SAFE ZIP + ekstrak VV/VH.

TAHAP INI TIDAK BERCABANG PER LEVEL. DOWNLOAD, CALIBRATE (module1b), dan CROP
(module2) jalan sama persis untuk level RAW maupun PROCESSED: nilai DN mentah
tanpa LUT sigma-nought tidak punya arti fisik, jadi "RAW" untuk SAR pun berarti
terkalibrasi dan ter-crop (DOCS/PIPELINE.md, "What RAW means for Sentinel-1").

Percabangan level Sentinel-1 ada satu lapis di atas, di mana urutan tahap
memang disusun:
    keputusan  -> etl/processing_plan.py (SourcePlan.s1_skip_stages)
    eksekusi   -> etl/module5_orchestrator.py (_run_s1_chain)
LEE_FILTER, QUALITY_ANALYTICS, dan GOLD_EXPORT-lah yang dilewati saat SENTINEL1
dikonfigurasi RAW-only; modul ini tidak perlu tahu levelnya sama sekali.
"""
from __future__ import annotations

import hashlib
import logging
import os
import random
import shutil
import threading
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from etl import download_guard as dg

logger = logging.getLogger(__name__)


def _long(path: Path) -> Path:
    """Path aman dari batas MAX_PATH (260) Windows. Lihat
    folder_manager.long_path."""
    from etl.folder_manager import long_path

    return long_path(path)


@dataclass
class DownloadResult:
    product_identifier: str
    zip_path: str
    vv_tif_path: str
    vh_tif_path: str
    file_size_mb: float
    checksum_md5: str
    acquisition_datetime: datetime
    orbit_direction: str
    orbit_number: int | None
    relative_orbit: int | None
    cloud_cover: float | None
    incidence_near: float | None
    incidence_far: float | None
    download_url: str = ""
    kept_raw: bool = False


# Status yang berarti "server tidak mau melayani permintaan lanjutan ini".
# 501 yang dijawab CDSE untuk header Range, 405 kalau metodenya ditolak, dan
# 400 yang dipakai sebagian gateway untuk Range yang tidak dikenal.
_RANGE_UNSUPPORTED = frozenset({400, 405, 501})

# 429 dibatasi lebih longgar daripada error jaringan biasa: itu bukan
# kegagalan transfer, melainkan akun sedang dijatah CDSE (maksimal 4 unduhan
# paralel -- lihat module5_orchestrator.S1_PARALLEL_DOWNLOADS) dan pasti pulih
# kalau didiamkan sebentar. Kalau dipukul rata dengan MAX_RETRIES=3 tanpa
# jeda, 3 worker yang throttle bersamaan menghabiskan jatahnya dalam
# hitungan detik dan scene yang sehat gagal permanen (okt_dec_2025_hybrid
# 2026-09-23: 12 scene begitu; jul_sep_2025_hybrid: 236).
MAX_RATE_LIMIT_RETRIES = 8


def _retry_sleep(attempt: int, retry_after: str | None = None) -> None:
    """Jeda sebelum percobaan ulang. Pakai Retry-After dari server kalau ada;
    kalau tidak, backoff eksponensial + jitter supaya beberapa worker yang
    gagal berbarengan tidak menembak ulang di detik yang sama persis."""
    if retry_after is not None:
        try:
            delay = max(1.0, float(retry_after))
        except ValueError:
            delay = 15.0
    else:
        delay = min(60.0, 2.0 ** attempt) + random.uniform(0, 1.0)
    logger.info("[M1] Menunggu %.1f s sebelum mencoba lagi...", delay)
    time.sleep(delay)


def _get_cdse_token(user: str, password: str) -> str:
    import requests

    r = requests.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE"
        "/protocol/openid-connect/token",
        data={
            "client_id": "cdse-public",
            "username": user,
            "password": password,
            "grant_type": "password",
        },
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"CDSE auth gagal ({r.status_code}): {r.text[:300]}\n"
            "Pastikan email dan password di .env sudah benar.\n"
            "Daftar: https://dataspace.copernicus.eu"
        )
    return r.json()["access_token"]


# Di bawah porsi AOI ini sebuah TANGGAL S1 (gabungan semua frame-nya) tidak
# diunduh. Sama dengan ambang "lapisan nyaris kosong" di module9: 24_try8
# mengunduh 1,6 GB untuk scene descending 14 Jan yang cuma menyentuh 0,6% AOI,
# lalu menulis stack fusion 455 MB yang 99,4% NaN. Footprint sudah ada di
# respons katalog, jadi ini bisa diputuskan sebelum unduhan dimulai.
MIN_S1_AOI_COVERAGE = 0.05


def _footprint_wkt(item: dict) -> str | None:
    """Footprint scene dari respons OData CDSE sebagai WKT, atau None.

    `GeoFootprint` (GeoJSON) dipakai lebih dulu karena bisa langsung dibaca
    shapely; `Footprint` berbentuk "geography'SRID=4326;POLYGON(...)'" dan
    hanya jadi cadangan."""
    from shapely import wkt as shapely_wkt
    from shapely.geometry import shape

    geo = item.get("GeoFootprint")
    if isinstance(geo, dict) and geo.get("coordinates"):
        try:
            return shape(geo).wkt
        except Exception:
            pass
    raw = item.get("Footprint")
    if isinstance(raw, str) and ";" in raw:
        try:
            return shapely_wkt.loads(raw.split(";", 1)[1].rstrip("'")).wkt
        except Exception:
            pass
    return None


def aoi_coverage(footprints_wkt: list[str], bbox_wkt: str) -> float | None:
    """Porsi luas AOI yang tertutup gabungan `footprints_wkt` (0..1), atau
    None kalau tidak ada satu pun footprint yang bisa dibaca.

    Dihitung di derajat lon/lat: yang dicari rasio, dan distorsi luas di
    dalam AOI selebar <1 derajat di dekat ekuator bisa diabaikan."""
    from shapely import wkt as shapely_wkt
    from shapely.ops import unary_union

    aoi = shapely_wkt.loads(bbox_wkt)
    if aoi.area <= 0:
        return None
    geoms = []
    for fp in footprints_wkt:
        if not fp:
            continue
        try:
            geoms.append(shapely_wkt.loads(fp))
        except Exception:
            continue
    if not geoms:
        return None
    return float(unary_union(geoms).intersection(aoi).area / aoi.area)


def discover_scenes(
    bbox_wkt: str,
    date_from: datetime,
    date_to: datetime,
    orbit_direction: str | None = None,
    max_results: int = 50,
    product_type: str = "GRD",
    instrument_mode: str = "IW",
) -> list[dict]:
    import requests

    dt_from = date_from.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    dt_to = date_to.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    filters = [
        "Collection/Name eq 'SENTINEL-1'",
        f"OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}')",
        f"ContentDate/Start gt {dt_from}",
        f"ContentDate/Start lt {dt_to}",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' "
        f"and att/OData.CSC.StringAttribute/Value eq '{product_type}')",
        f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'operationalMode' "
        f"and att/OData.CSC.StringAttribute/Value eq '{instrument_mode}')",
    ]

    if orbit_direction:
        filters.append(
            f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'orbitDirection' "
            f"and att/OData.CSC.StringAttribute/Value eq '{orbit_direction}')"
        )

    filter_str = " and ".join(filters)
    url = (
        "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        f"?$filter={filter_str}"
        f"&$orderby=ContentDate/Start desc"
        f"&$top={min(max_results, 1000)}"
        "&$expand=Attributes"
    )

    logger.info("[M1] Querying CDSE: area=%s... from=%s to=%s", bbox_wkt[:40], date_from.date(), date_to.date())

    r = requests.get(url, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"CDSE query gagal ({r.status_code}): {r.text[:300]}")

    items = r.json().get("value", [])
    logger.info("[M1] Ditemukan %d scene di CDSE.", len(items))

    results = []
    for item in items:
        attrs = {a["Name"]: a.get("Value") for a in item.get("Attributes", [])}
        acq_raw = item.get("ContentDate", {}).get("Start", "")
        try:
            acq_dt = datetime.fromisoformat(acq_raw.replace("Z", "+00:00"))
        except Exception:
            acq_dt = datetime.now(tz=timezone.utc)

        results.append({
            "product_identifier": item.get("Name", item.get("Id", "")),
            "acquisition_datetime": acq_dt,
            "orbit_direction": attrs.get("orbitDirection", "ASCENDING").upper(),
            "orbit_number": attrs.get("absoluteOrbit"),
            "relative_orbit": attrs.get("relativeOrbit"),
            "cloud_cover": attrs.get("cloudCover"),
            "size_mb": item.get("ContentLength", 0) / (1024 ** 2),
            "download_url": f"https://download.dataspace.copernicus.eu/odata/v1/Products({item['Id']})/$value",
            "_id": item["Id"],
            "footprint_wkt": (fp := _footprint_wkt(item)),
            "aoi_coverage": aoi_coverage([fp], bbox_wkt) if fp else None,
        })

    return results


def _zip_is_complete(path: Path) -> bool:
    """ZIP lengkap = central directory terbaca. Cepat (tidak membaca isi)
    dan menolak file terpotong."""
    try:
        with zipfile.ZipFile(_long(path)) as zf:
            return bool(zf.namelist())
    except (zipfile.BadZipFile, OSError):
        return False


def find_reusable_scene_zip(product_identifier: str, data_root: Path, exclude: Path) -> Path | None:
    """Cari ZIP SAFE yang sama, sudah lengkap, di folder dataset lain
    (layout {ds}/{tanggal}/raw/sentinel1/{pid}/ dan {ds}/_work/{pid}/raw/sentinel1/,
    plus varian tanpa subfolder pid)."""
    return dg.find_reusable_file(
        f"{product_identifier}.zip",
        [
            "*/*/raw/sentinel1/{name}",
            "*/*/raw/sentinel1/*/{name}",
            "*/*/*/raw/sentinel1/{name}",
            "*/*/*/raw/sentinel1/*/{name}",
        ],
        exclude, data_root, validate=_zip_is_complete, fs_path=_long,
    )


def download_scene(
    scene_meta: dict,
    output_dir: str = "recovered_temp",
    keep_raw: bool = False,
    progress_cb: Callable[[float, str], None] | None = None,
    reuse_root: Path | None = None,
) -> DownloadResult:
    import requests

    user = os.getenv("COPERNICUS_USER")
    pwd = os.getenv("COPERNICUS_PASSWORD")
    if not user or not pwd:
        raise RuntimeError(
            "COPERNICUS_USER dan COPERNICUS_PASSWORD harus ada di .env\n"
            "Daftar gratis: https://dataspace.copernicus.eu"
        )

    out = Path(output_dir)
    _long(out).mkdir(parents=True, exist_ok=True)
    name = scene_meta["product_identifier"]
    url = scene_meta["download_url"]
    zip_path = out / f"{name}.zip"

    if reuse_root is not None and not _long(zip_path).exists():
        found = find_reusable_scene_zip(name, reuse_root, zip_path)
        if found is not None:
            how = dg.adopt_file(_long(found), _long(zip_path))
            logger.info("[M1] ZIP dipakai ulang dari dataset lain (%s): %s", how, found)
            _long(out / f"{name}.zip.part").unlink(missing_ok=True)

    if _long(zip_path).exists():
        logger.info("[M1] ZIP sudah ada di disk, lewati download: %s", zip_path.name)
        file_size_mb = _long(zip_path).stat().st_size / (1024 ** 2)
    else:
        logger.info("[M1] Downloading: %s (%.0f MB)", name[:50], scene_meta.get("size_mb", 0))

        token = _get_cdse_token(user, pwd)

        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})

        download_url = url.replace(
            "catalogue.dataspace.copernicus.eu",
            "download.dataspace.copernicus.eu"
        )

        part_path = out / f"{name}.zip.part"
        resume_from = _long(part_path).stat().st_size if _long(part_path).exists() else 0

        if resume_from > 0:
            logger.info("[M1] Melanjutkan download dari %.0f MB...", resume_from / 1e6)
            session.headers.update({"Range": f"bytes={resume_from}-"})

        MAX_RETRIES = 3
        attempt = 0
        # Percobaan ulang yang tidak menghabiskan jatah MAX_RETRIES: server
        # menolak Range. Dibatasi sendiri supaya server yang terus-menerus
        # menolak tidak membuat loop tak berujung.
        restarts_left = 2
        # 429 punya jatah dan ritme retry sendiri -- lihat MAX_RATE_LIMIT_RETRIES.
        rate_limit_attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                with session.get(
                    download_url, stream=True, timeout=dg.REQUEST_TIMEOUT, allow_redirects=True
                ) as resp:
                    if resp.status_code == 401:
                        logger.info("[M1] Token expired, refreshing (attempt %d)...", attempt)
                        token = _get_cdse_token(user, pwd)
                        session.headers.update({"Authorization": f"Bearer {token}"})
                        continue

                    if resp.status_code == 416:
                        logger.info("[M1] File sudah lengkap di .part, rename saja.")
                        os.replace(_long(part_path), _long(zip_path))
                        break

                    # CDSE tidak selalu melayani permintaan lanjutan: endpoint
                    # /$value menjawab 501 Not Implemented untuk header Range.
                    # Karena 501 bukan error jaringan, retry berikutnya
                    # mengirim Range yang sama dan ditolak lagi -- scene sehat
                    # yang cuma putus sekali di tengah jadi gagal permanen
                    # (26_JAWA 2026-09-20: IncompleteRead di attempt 1, lalu
                    # dua 501 berturut-turut). Kalau server menolak Range,
                    # .part dibuang dan berkas diunduh ulang dari nol.
                    if (
                        resp.status_code in _RANGE_UNSUPPORTED
                        and resume_from > 0
                        and restarts_left > 0
                    ):
                        logger.warning(
                            "[M1] Server menolak resume (HTTP %d), download "
                            "diulang dari awal.", resp.status_code,
                        )
                        session.headers.pop("Range", None)
                        resume_from = 0
                        _long(part_path).unlink(missing_ok=True)
                        # Penolakan Range bukan kegagalan transfer, jadi tidak
                        # menghabiskan jatah percobaan.
                        restarts_left -= 1
                        attempt -= 1
                        continue

                    if resp.status_code == 429 and rate_limit_attempt < MAX_RATE_LIMIT_RETRIES:
                        rate_limit_attempt += 1
                        logger.warning(
                            "[M1] Download ditolak (429 rate limit, attempt %d/%d).",
                            rate_limit_attempt, MAX_RATE_LIMIT_RETRIES,
                        )
                        _retry_sleep(rate_limit_attempt, resp.headers.get("Retry-After"))
                        # Throttle bukan kegagalan transfer, jadi tidak
                        # menghabiskan jatah MAX_RETRIES.
                        attempt -= 1
                        continue

                    resp.raise_for_status()

                    # A Range request is only honoured when the server answers
                    # 206. CDSE sometimes replies 200 with the whole file
                    # anyway; appending that to the existing .part silently
                    # produces a corrupt ZIP (and a nonsense total, e.g.
                    # "948 / 2922 MB" for a 2024 MB product), so fall back to
                    # restarting the file from scratch.
                    resumed = resume_from > 0 and resp.status_code == 206
                    if resume_from > 0 and not resumed:
                        logger.warning(
                            "[M1] Server mengabaikan Range (HTTP %d), "
                            "download diulang dari awal.", resp.status_code,
                        )
                        session.headers.pop("Range", None)
                        resume_from = 0

                    total = int(resp.headers.get("Content-Length", 0)) + resume_from
                    downloaded = resume_from
                    write_mode = "ab" if resumed else "wb"

                    guard = dg.StallGuard()
                    with open(_long(part_path), write_mode) as fout:
                        for chunk in resp.iter_content(chunk_size=dg.CHUNK_SIZE):
                            if chunk:
                                fout.write(chunk)
                                downloaded += len(chunk)
                                guard.update(len(chunk))
                                if total:
                                    pct = downloaded / total * 100
                                    if downloaded % (50 * 1024 * 1024) < dg.CHUNK_SIZE:
                                        logger.info("[M1] Download: %.0f%%  (%.0f / %.0f MB)", pct, downloaded / 1e6, total / 1e6)
                                        if progress_cb:
                                            progress_cb(pct, f"{downloaded / 1e6:.0f} / {total / 1e6:.0f} MB")

                    os.replace(_long(part_path), _long(zip_path))
                    logger.info("[M1] Download selesai.")
                    break

            except (ConnectionError, TimeoutError, OSError) as exc:
                if attempt < MAX_RETRIES:
                    logger.warning("[M1] Download terputus (attempt %d/%d): %s. Retry...", attempt, MAX_RETRIES, exc)
                    if _long(part_path).exists():
                        resume_from = _long(part_path).stat().st_size
                        session.headers.update({"Range": f"bytes={resume_from}-"})
                        logger.info("[M1] Akan resume dari %.0f MB", resume_from / 1e6)
                    # Backoff + jitter: retry instan terhadap server yang
                    # baru saja memutus koneksi (SSL EOF, 429 yang habis
                    # jatahnya di atas) cuma menabrak kondisi yang sama lagi.
                    _retry_sleep(attempt)
                else:
                    logger.error("[M1] Download gagal setelah %d attempts: %s", MAX_RETRIES, exc)
                    logger.info("[M1] File .part tersimpan di: %s", part_path)
                    raise

        if not _long(zip_path).exists():
            raise RuntimeError(f"Download tidak lengkap. Cek file: {part_path}")

        file_size_mb = _long(zip_path).stat().st_size / (1024 ** 2)
        logger.info("[M1] Download selesai: %.1f MB", file_size_mb)

    checksum_md5 = _md5(zip_path)

    vv_path, vh_path = _extract_bands(zip_path, out)

    if not keep_raw:
        _long(zip_path).unlink()
        logger.info("[M1] ZIP dihapus (keep_raw=False). Dihemat %.1f MB.", file_size_mb)
        zip_stored = ""
    else:
        zip_stored = str(zip_path)
        logger.info("[M1] ZIP disimpan (keep_raw=True): %s", zip_stored)

    return DownloadResult(
        product_identifier=name,
        zip_path=zip_stored,
        vv_tif_path=str(vv_path),
        vh_tif_path=str(vh_path),
        file_size_mb=file_size_mb,
        checksum_md5=checksum_md5,
        acquisition_datetime=scene_meta["acquisition_datetime"],
        orbit_direction=scene_meta.get("orbit_direction", "ASCENDING"),
        orbit_number=scene_meta.get("orbit_number"),
        relative_orbit=scene_meta.get("relative_orbit"),
        cloud_cover=scene_meta.get("cloud_cover"),
        incidence_near=None,
        incidence_far=None,
        download_url=url,
        kept_raw=keep_raw,
    )


def _extract_bands(zip_path: Path, output_dir: Path) -> tuple[Path, Path]:
    logger.info("[M1] Mengekstrak band dari %s", zip_path.name)
    with zipfile.ZipFile(_long(zip_path), "r") as zf:
        all_files = zf.namelist()
        vv_files = [f for f in all_files
                    if "/measurement/" in f and "-vv-" in f.lower() and f.endswith(".tiff")]
        vh_files = [f for f in all_files
                    if "/measurement/" in f and "-vh-" in f.lower() and f.endswith(".tiff")]
        if not vv_files:
            raise RuntimeError(f"Band VV tidak ditemukan dalam {zip_path.name}")
        if not vh_files:
            raise RuntimeError(f"Band VH tidak ditemukan dalam {zip_path.name}")

        stem = zip_path.stem[:35]
        vv_out = output_dir / f"{stem}_VV.tif"
        vh_out = output_dir / f"{stem}_VH.tif"

        # Diekstrak ke berkas antara lalu dipindahkan sekali jalan: tahap
        # berikutnya menilai keberadaan _VV.tif/_VH.tif sebagai "ekstraksi
        # beres", jadi ekstraksi yang terhenti di tengah tidak boleh
        # meninggalkan berkas berukuran wajar di path final.
        for member, out_path in ((vv_files[0], vv_out), (vh_files[0], vh_out)):
            tmp_out = out_path.with_name(
                f"{out_path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
            )
            try:
                with zf.open(member) as src, open(_long(tmp_out), "wb") as dst:
                    shutil.copyfileobj(src, dst)
                os.replace(_long(tmp_out), _long(out_path))
            except BaseException:
                Path(_long(tmp_out)).unlink(missing_ok=True)
                raise

    logger.info("[M1] Ekstraksi selesai: VV=%s | VH=%s", vv_out.name, vh_out.name)
    return vv_out, vh_out


def _md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(_long(path), "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def run(
    bbox_wkt: str,
    date_from: datetime,
    date_to: datetime,
    output_dir: str = "recovered_temp",
    orbit_direction: str | None = None,
    keep_raw: bool = False,
    max_scenes: int = 50,
) -> list[DownloadResult]:
    scenes = discover_scenes(
        bbox_wkt=bbox_wkt,
        date_from=date_from,
        date_to=date_to,
        orbit_direction=orbit_direction,
        max_results=max_scenes,
    )

    if not scenes:
        logger.info("[M1] Tidak ada scene baru ditemukan.")
        return []

    results = []
    for i, scene in enumerate(scenes, 1):
        pid = scene["product_identifier"]
        logger.info("[M1] Proses scene %d/%d: %s", i, len(scenes), pid[:50])
        try:
            result = download_scene(scene, output_dir=output_dir, keep_raw=keep_raw)
            results.append(result)
        except Exception as exc:
            logger.error("[M1] Gagal: %s -> %s", pid[:40], exc)

    logger.info("[M1] Selesai: %d/%d berhasil.", len(results), len(scenes))
    return results