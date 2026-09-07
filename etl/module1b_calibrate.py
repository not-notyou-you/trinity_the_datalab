# etl/module1b_calibrate.py
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import numpy as np
import rasterio
from rasterio.warp import Resampling, calculate_default_transform, reproject
from scipy.interpolate import RegularGridInterpolator

# CRS lookups below (calculate_default_transform/reproject to EPSG:4326) hit
# rasterio's PROJ database. If you see "Cannot find proj.db" / "unknown EPSG
# code" here, it's a conflicting PROJ_LIB/PROJ_DATA/GDAL_DATA env var — see
# the fix and full explanation in etl/__init__.py.

logger = logging.getLogger(__name__)


def _find_calibration_xml(zip_path: str, polarisation: str) -> bytes:
    pol = polarisation.lower()
    with zipfile.ZipFile(zip_path) as zf:
        candidates = [
            n for n in zf.namelist()
            if "annotation/calibration/calibration-" in n
            and f"-{pol}-" in n
            and n.endswith(".xml")
        ]
        if not candidates:
            raise RuntimeError(
                f"Calibration XML tidak ditemukan untuk polarisasi {polarisation} di {zip_path}"
            )
        return zf.read(candidates[0])


def _parse_calibration_lut(xml_bytes: bytes) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    root = ET.fromstring(xml_bytes)
    lines = []
    pixel_rows = []
    sigma_rows = []
    for vec in root.findall(".//calibrationVector"):
        line = int(vec.find("line").text)
        pixels = [int(x) for x in vec.find("pixel").text.split()]
        sigmas = [float(x) for x in vec.find("sigmaNought").text.split()]
        lines.append(line)
        pixel_rows.append(pixels)
        sigma_rows.append(sigmas)
    if not lines:
        raise RuntimeError("calibrationVectorList kosong atau format XML tidak dikenali")
    return np.array(lines), np.array(pixel_rows[0]), np.array(sigma_rows)


def apply_calibration(dn: np.ndarray, lines: np.ndarray, pixels: np.ndarray, sigma_lut: np.ndarray) -> np.ndarray:
    interp = RegularGridInterpolator(
        (lines, pixels), sigma_lut, bounds_error=False, fill_value=None
    )
    rows, cols = dn.shape
    grid_r, grid_c = np.meshgrid(np.arange(rows), np.arange(cols), indexing="ij")
    points = np.stack([grid_r.ravel(), grid_c.ravel()], axis=-1)
    sigma_full = interp(points).reshape(rows, cols)

    dn64 = dn.astype(np.float64)
    sigma0 = np.where(sigma_full > 0, (dn64 ** 2) / (sigma_full ** 2), 0.0)
    return sigma0.astype(np.float32)


def _reproject_with_gcps(data: np.ndarray, src_path: str, output_path: str, dst_crs: str = "EPSG:4326") -> None:
    with rasterio.open(src_path) as src:
        gcps, gcp_crs = src.gcps
        if not gcps:
            raise RuntimeError(f"Tidak ada GCP pada {src_path}, tidak bisa reproject tanpa itu")

        transform, width, height = calculate_default_transform(
            gcp_crs, dst_crs, src.width, src.height, gcps=gcps
        )

        dst_meta = {
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": 1,
            "dtype": "float32",
            "crs": dst_crs,
            "transform": transform,
            "nodata": float("nan"),
        }

        with rasterio.open(output_path, "w", **dst_meta) as dst:
            reproject(
                source=data,
                destination=rasterio.band(dst, 1),
                src_crs=gcp_crs,
                gcps=gcps,
                dst_transform=transform,
                dst_crs=dst_crs,
                dst_nodata=float("nan"),
                resampling=Resampling.bilinear,
            )

    logger.info("[M1b] reprojected -> %s (%dx%d)", Path(output_path).name, width, height)


def calibrate_and_reproject(tif_path: str, calib_xml: bytes, output_path: str) -> str:
    lines, pixels, sigma_lut = _parse_calibration_lut(calib_xml)
    with rasterio.open(tif_path) as src:
        dn = src.read(1)
    sigma0 = apply_calibration(dn, lines, pixels, sigma_lut)
    _reproject_with_gcps(sigma0, tif_path, output_path)
    return output_path


def run(
    zip_path: str,
    vv_tif_path: str,
    vh_tif_path: str,
    output_dir: str,
) -> tuple[str, str]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_tif_path).stem.replace("_VV", "")
    vv_out = str(Path(output_dir) / f"{stem}_VV_calibrated.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_calibrated.tif")

    vv_xml = _find_calibration_xml(zip_path, "vv")
    vh_xml = _find_calibration_xml(zip_path, "vh")

    logger.info("[M1b] calibrating VV: %s", Path(vv_tif_path).name)
    calibrate_and_reproject(vv_tif_path, vv_xml, vv_out)
    logger.info("[M1b] calibrating VH: %s", Path(vh_tif_path).name)
    calibrate_and_reproject(vh_tif_path, vh_xml, vh_out)

    return vv_out, vh_out