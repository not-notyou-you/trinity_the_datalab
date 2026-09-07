# api/routes/preview.py
"""
Preview endpoints: generate thumbnail PNG dari COG dan tampilkan N gambar terbaru.

GET /api/preview/latest        — 3-5 gambar terbaru (metadata + thumbnail URL)
GET /api/preview/{product_id}  — thumbnail PNG satu produk
GET /api/preview/storage       — ringkasan penggunaan storage

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara
"""

from __future__ import annotations

import io
import logging
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse, Response, StreamingResponse
from sqlalchemy import select, and_

from api.deps import get_db
from etl.database_client import (
    DataProduct,
    DatabaseClient,
    ProductTierEnum,
    QualityMetric,
    SatelliteScene,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _generate_thumbnail(
    tif_path: str,
    band: str = "VV",
    width: int = 400,
    percentile_low: float = 2.0,
    percentile_high: float = 98.0,
) -> bytes:
    """
    Generate thumbnail PNG dari GeoTIFF / COG.

    Melakukan:
    1. Baca raster dengan rasterio (bisa baca COG langsung tanpa load semua)
    2. Stretch kontras (percentile stretch untuk visualisasi backscatter SAR)
    3. Konversi ke PNG via Pillow

    Args:
        tif_path        : Path ke file COG/TIFF
        band            : 'VV' atau 'VH' (untuk label saja, file sudah single band)
        width           : Lebar thumbnail output dalam pixel
        percentile_low  : Percentile bawah untuk contrast stretch
        percentile_high : Percentile atas untuk contrast stretch

    Returns:
        PNG bytes
    """
    try:
        import numpy as np
        import rasterio
        from rasterio.enums import Resampling
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as e:
        raise ImportError(f"Dependency tidak terinstall: {e}. pip install rasterio Pillow numpy")

    path = Path(tif_path)
    if not path.exists():
        raise FileNotFoundError(f"File tidak ditemukan: {tif_path}")

    with rasterio.open(tif_path) as src:
        # Hitung skala downsample untuk thumbnail
        scale = width / src.width
        out_h = max(1, int(src.height * scale))
        out_w = max(1, int(src.width  * scale))

        # Baca dengan downsampling langsung (efisien untuk COG)
        data = src.read(
            1,
            out_shape   = (out_h, out_w),
            resampling  = Resampling.average,
        ).astype(float)

        nodata = src.nodata

    # Mask nodata
    if nodata is not None:
        mask = data == nodata
        data = np.ma.masked_where(mask, data)
    else:
        mask = ~np.isfinite(data)
        data = np.ma.masked_where(mask, data)

    # Percentile contrast stretch (SAR backscatter bisa sangat lebar range-nya)
    valid = data.compressed()
    if len(valid) == 0:
        # Semua nodata — return abu-abu
        img_arr = np.zeros((out_h, out_w), dtype=np.uint8) + 128
    else:
        lo = np.percentile(valid, percentile_low)
        hi = np.percentile(valid, percentile_high)
        if hi == lo:
            hi = lo + 1

        stretched = np.clip((data - lo) / (hi - lo) * 255, 0, 255)
        img_arr   = np.where(mask, 0, stretched).astype(np.uint8)

    # Konversi ke PIL Image
    img = Image.fromarray(img_arr, mode="L").convert("RGB")

    # Watermark minimal: band name + ukuran kecil
    try:
        draw = ImageDraw.Draw(img)
        draw.text((6, 6), f"Sentinel-1 {band}", fill=(200, 230, 200))
    except Exception:
        pass

    # Encode ke PNG bytes
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# ROUTES
# ---------------------------------------------------------------------------

@router.get(
    "/latest",
    summary="Gambar terbaru",
    description=(
        "Mengembalikan metadata 3–5 scene terbaru dengan SILVER product, "
        "beserta URL thumbnail untuk ditampilkan di dashboard."
    ),
)
async def get_latest_previews(
    db:    DatabaseClient = Depends(get_db),
    limit: int            = Query(5, ge=1, le=20, description="Jumlah scene terbaru (1-20)"),
    band:  str            = Query("VV", pattern="^(VV|VH)$", description="Band yang ditampilkan"),
) -> JSONResponse:
    """
    Ambil N scene terbaru yang sudah punya SILVER (LEE-filtered) product.
    GOLD tier sekarang berisi HDF5 fusion stack (bukan GeoTIFF per-band), jadi
    tidak bisa di-thumbnail langsung — SILVER adalah tier raster per-band
    terakhir yang tersedia untuk preview.
    Response berisi metadata + URL thumbnail.
    """
    with db.session() as sess:
        # Ambil SILVER products terbaru
        products = sess.scalars(
            select(DataProduct)
            .where(
                and_(
                    DataProduct.product_tier == ProductTierEnum.SILVER,
                    DataProduct.band_name    == band,
                    DataProduct.is_latest    == True,
                    DataProduct.is_valid     == True,
                )
            )
            .order_by(DataProduct.created_at.desc())
            .limit(limit)
        ).all()

        items = []
        for p in products:
            # Ambil scene info
            scene = sess.get(SatelliteScene, p.scene_id)
            # Ambil quality
            qm = sess.scalar(
                select(QualityMetric).where(
                    and_(
                        QualityMetric.product_id == p.product_id,
                        QualityMetric.band_name  == band,
                    )
                )
            )

            file_exists = Path(p.file_path).exists() if p.file_path else False

            items.append({
                "product_id":           p.product_id,
                "scene_id":             p.scene_id,
                "product_identifier":   scene.product_identifier if scene else None,
                "acquisition_datetime": scene.acquisition_datetime.isoformat() if scene else None,
                "orbit_direction":      str(scene.orbit_direction) if scene else None,
                "band_name":            p.band_name,
                "product_tier":         p.product_tier.value,
                "file_path":            p.file_path,
                "file_size_mb":         float(p.file_size_mb),
                "file_exists_on_disk":  file_exists,
                "quality_score":        float(qm.quality_score) if qm else None,
                "quality_flag":         qm.quality_flag if qm else None,
                # URL untuk ambil thumbnail — client call ini
                "thumbnail_url":        f"/api/preview/{p.product_id}?band={band}",
                "created_at":           p.created_at.isoformat(),
            })

    return JSONResponse(content={
        "total": len(items),
        "band":  band,
        "items": items,
    })


@router.get(
    "/{product_id}",
    response_class=Response,
    summary="Thumbnail PNG",
    description=(
        "Generate dan return thumbnail PNG dari file COG/TIFF. "
        "Melakukan contrast stretch otomatis untuk visualisasi SAR backscatter. "
        "Gambar dikembalikan langsung sebagai PNG."
    ),
)
async def get_product_thumbnail(
    product_id: int,
    db:         DatabaseClient = Depends(get_db),
    width:      int            = Query(400, ge=100, le=2000, description="Lebar thumbnail (pixel)"),
    band:       str            = Query("VV", pattern="^(VV|VH)$"),
) -> Response:
    """
    Return PNG thumbnail dari satu produk COG.
    Otomatis stretch kontras untuk SAR backscatter.
    """
    with db.session() as sess:
        p = sess.get(DataProduct, product_id)
        if not p:
            raise HTTPException(404, f"Product {product_id} tidak ditemukan")
        file_path = p.file_path
        band_name = p.band_name

    if not file_path or not Path(file_path).exists():
        # Return placeholder abu-abu jika file tidak ada
        try:
            from PIL import Image
            import io
            placeholder = Image.new("RGB", (width, int(width * 0.6)), color=(40, 40, 40))
            draw = __import__("PIL.ImageDraw", fromlist=["ImageDraw"]).ImageDraw.Draw(placeholder)
            draw.text((10, 10), f"File tidak ada: {Path(file_path).name if file_path else 'unknown'}", fill=(150, 150, 150))
            buf = io.BytesIO()
            placeholder.save(buf, "PNG")
            buf.seek(0)
            return Response(content=buf.read(), media_type="image/png")
        except Exception:
            raise HTTPException(404, f"File tidak ditemukan: {file_path}")

    try:
        png_bytes = _generate_thumbnail(file_path, band=band_name, width=width)
        return Response(
            content      = png_bytes,
            media_type   = "image/png",
            headers      = {
                "Cache-Control": "public, max-age=3600",  # cache 1 jam
                "X-Product-Id":  str(product_id),
                "X-Band":        band_name,
            },
        )
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc))
    except ImportError as exc:
        raise HTTPException(503, f"Library tidak terinstall di server: {exc}")
    except Exception as exc:
        logger.error("Thumbnail error product=%d: %s", product_id, exc)
        raise HTTPException(500, f"Gagal generate thumbnail: {exc}")


@router.get(
    "/storage/summary",
    summary="Ringkasan storage",
    description="Menampilkan penggunaan disk per tier (RAW, BRONZE, SILVER, GOLD) dan total.",
)
async def get_storage_summary() -> JSONResponse:
    """
    Hitung ukuran folder per tier dan return dalam MB dan GB.
    """
    from etl.config import cfg as pipeline_cfg

    def dir_size(path: Path) -> float:
        if not path.exists():
            return 0.0
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file()) / (1024 ** 2)

    out = Path(pipeline_cfg.pipeline.output_dir)
    raw = Path(pipeline_cfg.pipeline.recovered_dir)

    bronze_mb = dir_size(out / "bronze")
    silver_mb = dir_size(out / "silver")
    gold_mb   = dir_size(out / "gold")
    raw_mb    = dir_size(raw)
    total_mb  = bronze_mb + silver_mb + gold_mb + raw_mb

    def to_gb(mb: float) -> str:
        return f"{mb / 1024:.2f} GB" if mb >= 1024 else f"{mb:.1f} MB"

    return JSONResponse(content={
        "tiers": {
            "raw":    {"mb": round(raw_mb, 1),    "human": to_gb(raw_mb),    "note": "File ZIP asli + TIF hasil ekstrak"},
            "bronze": {"mb": round(bronze_mb, 1), "human": to_gb(bronze_mb), "note": "Setelah crop ke AOI bbox"},
            "silver": {"mb": round(silver_mb, 1), "human": to_gb(silver_mb), "note": "Setelah Lee filter"},
            "gold":   {"mb": round(gold_mb, 1),   "human": to_gb(gold_mb),   "note": "COG production-ready"},
        },
        "total": {
            "mb":    round(total_mb, 1),
            "human": to_gb(total_mb),
        },
        "tips": [
            "Set keep_raw=false di config.json untuk hapus ZIP setelah ekstrak (~800 MB per scene)",
            "Set keep_bronze=false dan keep_silver=false untuk simpan GOLD saja",
            "Hanya GOLD tier yang dibutuhkan untuk ML — bronze/silver bisa dihapus",
        ],
    })