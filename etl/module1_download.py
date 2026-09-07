# etl/module1_download.py
from __future__ import annotations

import hashlib
import logging
import os
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)


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
        })

    return results


def download_scene(
    scene_meta: dict,
    output_dir: str = "recovered_temp",
    keep_raw: bool = False,
    progress_cb: Callable[[float, str], None] | None = None,
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
    out.mkdir(parents=True, exist_ok=True)
    name = scene_meta["product_identifier"]
    url = scene_meta["download_url"]
    zip_path = out / f"{name}.zip"

    if zip_path.exists():
        logger.info("[M1] ZIP sudah ada di disk, lewati download: %s", zip_path.name)
        file_size_mb = zip_path.stat().st_size / (1024 ** 2)
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
        resume_from = part_path.stat().st_size if part_path.exists() else 0

        if resume_from > 0:
            logger.info("[M1] Melanjutkan download dari %.0f MB...", resume_from / 1e6)
            session.headers.update({"Range": f"bytes={resume_from}-"})

        MAX_RETRIES = 3
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                with session.get(download_url, stream=True, timeout=600, allow_redirects=True) as resp:
                    if resp.status_code == 401:
                        logger.info("[M1] Token expired, refreshing (attempt %d)...", attempt)
                        token = _get_cdse_token(user, pwd)
                        session.headers.update({"Authorization": f"Bearer {token}"})
                        continue

                    if resp.status_code == 416:
                        logger.info("[M1] File sudah lengkap di .part, rename saja.")
                        part_path.rename(zip_path)
                        break

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

                    with open(part_path, write_mode) as fout:
                        for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                            if chunk:
                                fout.write(chunk)
                                downloaded += len(chunk)
                                if total:
                                    pct = downloaded / total * 100
                                    if downloaded % (50 * 1024 * 1024) < 8 * 1024 * 1024:
                                        logger.info("[M1] Download: %.0f%%  (%.0f / %.0f MB)", pct, downloaded / 1e6, total / 1e6)
                                        if progress_cb:
                                            progress_cb(pct, f"{downloaded / 1e6:.0f} / {total / 1e6:.0f} MB")

                    part_path.rename(zip_path)
                    logger.info("[M1] Download selesai.")
                    break

            except (ConnectionError, TimeoutError, OSError) as exc:
                if attempt < MAX_RETRIES:
                    logger.warning("[M1] Download terputus (attempt %d/%d): %s. Retry...", attempt, MAX_RETRIES, exc)
                    if part_path.exists():
                        resume_from = part_path.stat().st_size
                        session.headers.update({"Range": f"bytes={resume_from}-"})
                        logger.info("[M1] Akan resume dari %.0f MB", resume_from / 1e6)
                else:
                    logger.error("[M1] Download gagal setelah %d attempts: %s", MAX_RETRIES, exc)
                    logger.info("[M1] File .part tersimpan di: %s", part_path)
                    raise

        if not zip_path.exists():
            raise RuntimeError(f"Download tidak lengkap. Cek file: {part_path}")

        file_size_mb = zip_path.stat().st_size / (1024 ** 2)
        logger.info("[M1] Download selesai: %.1f MB", file_size_mb)

    checksum_md5 = _md5(zip_path)

    vv_path, vh_path = _extract_bands(zip_path, out)

    if not keep_raw:
        zip_path.unlink()
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
    with zipfile.ZipFile(zip_path, "r") as zf:
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

        with zf.open(vv_files[0]) as src, open(vv_out, "wb") as dst:
            shutil.copyfileobj(src, dst)
        with zf.open(vh_files[0]) as src, open(vh_out, "wb") as dst:
            shutil.copyfileobj(src, dst)

    logger.info("[M1] Ekstraksi selesai: VV=%s | VH=%s", vv_out.name, vh_out.name)
    return vv_out, vh_out


def _md5(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
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