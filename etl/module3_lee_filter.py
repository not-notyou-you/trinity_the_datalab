# etl/module3_lee_filter.py
from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import rasterio
from scipy.ndimage import uniform_filter

logger = logging.getLogger(__name__)


def lee_filter(img: np.ndarray, window_size: int = 7, looks: int = 1) -> np.ndarray:
    original_dtype = img.dtype
    img = img.astype(np.float64)
    img_mean = uniform_filter(img, size=window_size)
    img_sqr = uniform_filter(img ** 2, size=window_size)
    img_var = img_sqr - img_mean ** 2
    noise_var = img_mean ** 2 / looks
    denom = img_var + noise_var
    safe_denom = np.where(denom > 0, denom, 1.0)
    weight = np.where(denom > 0, np.clip(img_var / safe_denom, 0, 1), 0.0)
    filtered = img_mean + weight * (img - img_mean)
    return filtered.astype(original_dtype)


def apply_lee_to_tiff(
    input_path: str,
    output_path: str,
    window_size: int = 7,
    looks: int = 1,
) -> str:
    with rasterio.open(input_path) as src:
        meta = src.meta.copy()
        band = src.read(1)
    filtered = lee_filter(band, window_size, looks)
    with rasterio.open(output_path, "w", **meta) as dst:
        dst.write(filtered, 1)
    logger.info("[M3] %s -> %s (window=%d looks=%d)",
                Path(input_path).name, Path(output_path).name, window_size, looks)
    return output_path


def run(
    vv_path: str,
    vh_path: str,
    output_dir: str,
    window_size: int = 7,
    looks: int = 1,
) -> tuple[str, str]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_path).stem.replace("_VV_crop", "")
    vv_out = str(Path(output_dir) / f"{stem}_VV_lee.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_lee.tif")
    apply_lee_to_tiff(vv_path, vv_out, window_size, looks)
    apply_lee_to_tiff(vh_path, vh_out, window_size, looks)
    return vv_out, vh_out