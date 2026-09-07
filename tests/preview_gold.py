# tests/preview_gold.py
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import rasterio


def render_preview(tif_path: str, output_png: str, width: int = 800) -> str:
    with rasterio.open(tif_path) as src:
        scale = width / src.width
        out_h = max(1, int(src.height * scale))
        out_w = max(1, int(src.width * scale))
        data = src.read(1, out_shape=(out_h, out_w), resampling=rasterio.enums.Resampling.average)
        nodata = src.nodata

    data = data.astype(np.float64)
    mask = data == nodata if nodata is not None else ~np.isfinite(data)
    valid = data[~mask]

    if valid.size == 0:
        raise RuntimeError("Semua piksel NoData, tidak ada yang bisa ditampilkan")

    lo, hi = np.percentile(valid, [2, 98])
    if hi == lo:
        hi = lo + 1
    stretched = np.clip((data - lo) / (hi - lo) * 255, 0, 255)
    img_arr = np.where(mask, 0, stretched).astype(np.uint8)

    from PIL import Image
    img = Image.fromarray(img_arr, mode="L")
    Path(output_png).parent.mkdir(parents=True, exist_ok=True)
    img.save(output_png)

    nodata_pct = 100 * mask.sum() / mask.size
    print(f"ukuran asli: {src.width}x{src.height}")
    print(f"nodata: {nodata_pct:.1f}%")
    print(f"disimpan: {output_png}")
    return output_png


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m tests.preview_gold <path_ke_tif> [output.png]")
        sys.exit(1)
    tif_path = sys.argv[1]
    output_png = sys.argv[2] if len(sys.argv) > 2 else "preview.png"
    render_preview(tif_path, output_png)