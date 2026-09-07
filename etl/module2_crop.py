# etl/module2_crop.py
from __future__ import annotations

import logging
from pathlib import Path

import rasterio
from rasterio.mask import mask
from shapely.geometry import box, mapping

logger = logging.getLogger(__name__)

JABODETABEK_BBOX = (106.4, -6.7, 107.2, -5.9)


def crop_to_bbox(
    input_path: str,
    bbox: tuple[float, float, float, float],
    output_path: str,
) -> str:
    geom = mapping(box(*bbox))
    with rasterio.open(input_path) as src:
        out_image, out_transform = mask(src, [geom], crop=True)
        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform,
        })
        with rasterio.open(output_path, "w", **out_meta) as dst:
            dst.write(out_image)
    logger.info("[M2] %s -> %s", Path(input_path).name, Path(output_path).name)
    return output_path


def run(
    vv_path: str,
    vh_path: str,
    output_dir: str,
    bbox: tuple[float, float, float, float] = JABODETABEK_BBOX,
) -> tuple[str, str]:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    stem = Path(vv_path).stem.replace("_VV", "")
    vv_out = str(Path(output_dir) / f"{stem}_VV_crop.tif")
    vh_out = str(Path(output_dir) / f"{stem}_VH_crop.tif")
    crop_to_bbox(vv_path, bbox, vv_out)
    crop_to_bbox(vh_path, bbox, vh_out)
    return vv_out, vh_out