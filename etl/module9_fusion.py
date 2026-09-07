# etl/module9_fusion.py
"""
Builds a multi-modal HDF5 feature stack (Sentinel-1 SAR + MODIS flood/NDVI/NDWI
+ GPM rainfall) aligned to a common grid for ML training. This is the FUSION
tier deliverable — the last stage of the pipeline, downstream of GOLD.

Semua input diambil dari tier GOLD (COG per-source, ditulis
module4_gold_export.py), bukan dari SILVER: GOLD adalah kontrak
"analysis-ready per band" dan fusion adalah konsumen pertamanya. Kalau fusion
membaca SILVER, dia akan diam-diam melewati tahap yang justru menjamin
band-band itu sudah final.

  - Sentinel-1 GOLD (VV/VH) dicari lewat tabel `data_products` dan menentukan
    grid referensi.
  - MODIS/GPM GOLD dicari di disk di bawah
    data/datasets/{id}_{slug}/gold/{modis,gpm}/{YYYYMMDD}/, dicocokkan ke
    waktu akuisisi S1 dalam jendela 24 jam, direproject ke grid S1, dan
    didaftarkan sebagai baris `nasa_scenes`.

Hasilnya ditulis ke data/datasets/{id}_{slug}/fusion/{date}/ sebagai .h5 +
metadata JSON, dicatat sebagai baris `fusion_products` (untuk lineage
`fusion_id`) dan baris `data_products` (tier=FUSION, source=FUSION).

Struktur HDF5 dikelompokkan per source, bukan datar:

    /sentinel1/VV, /sentinel1/VH
    /modis/FLOOD, /modis/NDVI, /modis/NDWI
    /gpm/rainfall_24h, /gpm/rainfall_72h, /gpm/rainfall_7d
"""

from __future__ import annotations

import json
import logging
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import h5py
import numpy as np
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from shapely.geometry import box
from sqlalchemy import select

from etl import folder_manager as fm
from etl import module4_gold_export as m4
from etl.database_client import (
    DatabaseClient,
    DataProduct,
    FusionProduct,
    NasaScene,
    ProductTierEnum,
    SatelliteScene,
)
from etl.constants import (
    GPM_PRODUCT_SHORT_NAME,
    GPM_SOURCE,
    GPM_TILE_ID,
    MODIS_PRODUCT_SHORT_NAME,
    MODIS_SOURCE,
    MODIS_TILE_ID,
)
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager
from etl.pipeline_logger import PipelineLogger

logger = logging.getLogger(__name__)

ALIGNMENT_WINDOW_HOURS = 24
MODIS_NODATA_U8 = 255  # uint8 can't hold NaN; 255 marks a missing/nodata pixel
HDF5_CHUNK_MAX = 256

# Path dataset di dalam file HDF5, dikelompokkan per source.
FUSION_LAYERS = [
    "sentinel1/VV",
    "sentinel1/VH",
    "modis/FLOOD",
    "modis/NDVI",
    "modis/NDWI",
    "gpm/rainfall_24h",
    "gpm/rainfall_72h",
    "gpm/rainfall_7d",
]

# Band MODIS yang ikut difusikan, dan cara meresamplenya ke grid S1.
# FLOOD kategorikal (kelas banjir) jadi nearest; NDVI/NDWI kontinu jadi bilinear.
MODIS_FUSION_BANDS: dict[str, Resampling] = {
    "FLOOD": Resampling.nearest,
    "NDVI": Resampling.bilinear,
    "NDWI": Resampling.bilinear,
}

# Window GPM yang ikut difusikan — sama dengan kunci module8.WINDOWS.
GPM_FUSION_WINDOWS = ("24h", "72h", "7d")


def _find_s1_gold(db: DatabaseClient, dataset_id: int, scene_id: int) -> dict | None:
    """Find the Sentinel-1 GOLD (COG) VV/VH products for `scene_id` within
    `dataset_id`. Returns None if the scene doesn't exist.

    Looked up by the exact scene_id the caller just processed, rather than
    re-derived from the acquisition date: adjacent Sentinel-1 slices from the
    same orbit pass can both land on the same UTC day over the same AOI, and
    picking "whichever scene happened that day" would silently grab the
    wrong one whenever more than one scene qualifies."""
    with db.session() as sess:
        scene = sess.get(SatelliteScene, scene_id)
        if scene is None:
            return None

        def _gold_band(band: str) -> DataProduct | None:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id == scene.scene_id,
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.product_tier == ProductTierEnum.GOLD,
                    DataProduct.source == "SENTINEL1",
                    DataProduct.band_name == band,
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
            )

        vv = _gold_band("VV")
        vh = _gold_band("VH")

        return {
            "scene_id": scene.scene_id,
            "region_id": scene.region_id,
            "acquisition_datetime": scene.acquisition_datetime,
            "vv_product_id": vv.product_id if vv else None,
            "vv_path": vv.file_path if vv else None,
            "vh_product_id": vh.product_id if vh else None,
            "vh_path": vh.file_path if vh else None,
        }


def _find_nearest_daily_file(
    dataset_id: int,
    dataset_name: str,
    source: str,
    filename_fn,
    center_dt: datetime,
    tolerance_hours: int = ALIGNMENT_WINDOW_HOURS // 2,
) -> tuple[Path, date_type] | None:
    """File GOLD MODIS/GPM distempel satu per hari pada tengah malam lokal,
    masing-masing di folder gold/{source}/{YYYYMMDD}/ sendiri. Kembalikan
    (path, tanggal) kandidat hari terdekat yang tengah malamnya masih dalam
    `tolerance_hours` dari `center_dt`, atau None kalau tidak ada di disk."""
    best: tuple[Path, date_type] | None = None
    best_diff = None
    for offset in (0, -1, 1):
        candidate_date = (center_dt + timedelta(days=offset)).date()
        candidate_midnight = datetime.combine(candidate_date, datetime.min.time(),
                                              tzinfo=center_dt.tzinfo)
        diff_hours = abs((candidate_midnight - center_dt).total_seconds()) / 3600.0
        if diff_hours > tolerance_hours:
            continue
        gold_dir = fm.get_scene_dir(
            dataset_id, dataset_name, "gold", source, candidate_date.strftime("%Y%m%d")
        )
        path = gold_dir / filename_fn(candidate_date)
        if path.exists() and (best_diff is None or diff_hours < best_diff):
            best, best_diff = (path, candidate_date), diff_hours
    return best


def _read_band_or_nan(path: str | None, shape: tuple[int, int]) -> np.ndarray:
    if not path or not Path(path).exists():
        return np.full(shape, np.nan, dtype=np.float32)
    with rasterio.open(path) as src:
        data = src.read(1).astype(np.float32)
        if src.nodata is not None:
            data[data == src.nodata] = np.nan
    return data


def _reproject_to_grid(
    src_path: Path,
    ref_transform,
    ref_crs,
    ref_shape: tuple[int, int],
    resampling: Resampling,
    fill_value: float,
) -> np.ndarray:
    """Reproject a single-band raster onto the S1 reference grid, filling
    pixels outside the source extent with `fill_value`."""
    height, width = ref_shape
    dest = np.full((height, width), fill_value, dtype=np.float32)
    with rasterio.open(src_path) as src:
        reproject(
            source=rasterio.band(src, 1),
            destination=dest,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=ref_transform,
            dst_crs=ref_crs,
            dst_nodata=fill_value,
            resampling=resampling,
        )
    return dest


def _get_or_create_nasa_scene(
    db: DatabaseClient,
    source: str,
    tile_id: str,
    product_short_name: str,
    acquisition_date: date_type,
    region_id: int,
    file_path: Path,
) -> int:
    with db.session() as sess:
        existing = sess.scalar(
            select(NasaScene.nasa_scene_id).where(
                NasaScene.source == source,
                NasaScene.tile_id == tile_id,
                NasaScene.product_short_name == product_short_name,
                NasaScene.acquisition_date == acquisition_date,
            )
        )
        if existing:
            return existing

        scene = NasaScene(
            source=source,
            tile_id=tile_id,
            product_short_name=product_short_name,
            acquisition_date=acquisition_date,
            region_id=region_id,
            raw_file_path=str(file_path),
            is_available=True,
        )
        sess.add(scene)
        sess.flush()
        return scene.nasa_scene_id


def _write_fusion_h5(
    h5_path: Path,
    layers: dict[str, np.ndarray],
    acquisition_datetime: datetime,
    processing_datetime: datetime,
    aoi_bbox: tuple[float, float, float, float],
    crs: object | None = None,
    transform: object | None = None,
) -> None:
    """Tulis stack fusion. `layers` memetakan path dataset HDF5
    ("modis/NDVI") ke arraynya; h5py membuat group perantaranya sendiri.

    `crs`/`transform` adalah grid referensi scene S1 yang semua layer sudah
    direproject ke sana. Keduanya wajib ikut ditulis: `aoi_bbox` adalah kotak
    AOI yang diminta, bukan batas raster hasilnya, jadi tanpa affine transform
    yang sebenarnya konsumen tidak bisa memetakan piksel ke koordinat bumi
    selain dengan menebak."""
    ref = layers["sentinel1/VV"]
    height, width = ref.shape
    chunks = (min(HDF5_CHUNK_MAX, height), min(HDF5_CHUNK_MAX, width))

    h5_path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(h5_path, "w") as f:
        for name, array in layers.items():
            ds = f.create_dataset(
                name, data=array, dtype=array.dtype, chunks=chunks, compression="gzip"
            )
            if array.dtype == np.uint8:
                ds.attrs["nodata"] = MODIS_NODATA_U8
            else:
                ds.attrs["nodata"] = "NaN"

        f.attrs["acquisition_datetime"] = acquisition_datetime.isoformat()
        f.attrs["processing_datetime"] = processing_datetime.isoformat()
        f.attrs["aoi_bbox"] = list(aoi_bbox)
        f.attrs["layers"] = list(layers)
        f.attrs["height"] = height
        f.attrs["width"] = width
        if crs is not None:
            # WKT + string CRS: WKT supaya tidak bergantung ke lookup EPSG di
            # sisi pembaca, string pendek ("EPSG:4326") untuk keterbacaan.
            f.attrs["crs"] = str(crs)
            try:
                f.attrs["crs_wkt"] = crs.to_wkt()
            except AttributeError:
                pass
        if transform is not None:
            # Urutan GDAL-style 6 elemen (a, b, c, d, e, f) — sama dengan
            # rasterio.Affine, jadi bisa langsung Affine(*attrs["transform"]).
            f.attrs["transform"] = [float(v) for v in tuple(transform)[:6]]

    logger.info(
        "[M9] Saving fusion H5 to FUSION tier: %s shape=(%d, %d) layers=%d",
        h5_path, height, width, len(layers),
    )


def _write_fusion_metadata_json(
    json_path: Path,
    fusion_id: int,
    dataset_id: int,
    region_id: int,
    s1_date: date_type,
    s1: dict,
    layer_sources: dict[str, dict],
    days_since_s1: int,
    acquisition_datetime: datetime,
    processing_datetime: datetime,
    aoi_bbox: tuple[float, float, float, float],
    h5_path: Path,
    height: int,
    width: int,
) -> None:
    metadata = {
        "fusion_id": fusion_id,
        "dataset_id": dataset_id,
        "region_id": region_id,
        "feature_date": s1_date.isoformat(),
        "acquisition_datetime": acquisition_datetime.isoformat(),
        "processing_datetime": processing_datetime.isoformat(),
        "aoi_bbox": list(aoi_bbox),
        "shape": {"height": height, "width": width},
        "layers": FUSION_LAYERS,
        "layer_sources": layer_sources,
        "source_scenes": {"s1_scene_id": s1["scene_id"]},
        "days_since_s1": days_since_s1,
        "file_name": h5_path.name,
        "file_size_mb": round(h5_path.stat().st_size / (1024 ** 2), 3),
    }
    with open(json_path, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


def _product_exists(db: DatabaseClient, dataset_id: int, file_path: str) -> bool:
    with db.session() as sess:
        return sess.scalar(
            select(DataProduct.product_id).where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.file_path == file_path,
                DataProduct.is_latest == True,
            )
        ) is not None


def _resolve_aux_scene(
    db: DatabaseClient,
    dataset_id: int,
    region_id: int,
    bbox_wkt: str,
    source: str,
    target_date: date_type,
) -> int:
    """Placeholder SatelliteScene untuk menempelkan data_products MODIS/GPM
    (yang tidak terikat ke satu scene Sentinel-1 mana pun) ke scene_id valid.

    Satu placeholder per source per tanggal, bukan satu per dataset. Dedup
    di MetadataManager.insert_data_product berjalan atas
    (scene_id, band_name, product_tier, dataset_id): kalau semua tanggal
    berbagi satu scene_id, mendaftarkan MODIS FLOOD tanggal ke-2 akan
    menandai FLOOD tanggal ke-1 `is_latest=False` walaupun keduanya masih
    valid dan dipakai fusion hari masing-masing."""
    meta = MetadataManager(db)
    date_key = target_date.strftime("%Y%m%d")
    pid = f"NASA_AUX_{source.upper()}_{dataset_id}_{date_key}"
    existing = meta.get_scene_by_pid(pid)
    if existing:
        return existing["scene_id"]
    return meta.insert_satellite_scene(
        product_identifier=pid,
        acquisition_datetime=datetime.combine(
            target_date, datetime.min.time(), tzinfo=timezone.utc
        ),
        region_id=region_id,
        bbox_wkt=bbox_wkt,
        orbit_direction="ASCENDING",
        polarization_vv=False,
        polarization_vh=False,
        resolution_m=250,
        instrument_mode="AUX",
    )


def _register_aux_products(
    db: DatabaseClient,
    *,
    dataset_id: int,
    region_id: int,
    source: str,
    nasa_source: str,
    nasa_tile_id: str,
    nasa_product_short_name: str,
    acquisition_date: date_type,
    aux_scene_id: int,
    silver_paths: dict[str, str],
    product_type: str,
) -> tuple[dict[str, int], int]:
    """Daftarkan band SILVER satu source/tanggal sebagai data_products.
    Mengembalikan ({band: product_id SILVER}, job_id) — product_id dipakai
    _promote_aux_to_gold untuk mencatat lineage SILVER -> GOLD."""
    meta = MetadataManager(db)
    lineage = LineageTracker(db)

    job_id = meta.insert_processing_job(
        aux_scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": source.upper()}
    )
    silver_product_ids: dict[str, int] = {}
    for band, path in silver_paths.items():
        if not Path(path).exists():
            continue
        if _product_exists(db, dataset_id, path):
            continue
        meta.insert_nasa_scene(
            source=nasa_source, tile_id=nasa_tile_id,
            product_short_name=nasa_product_short_name,
            acquisition_date=acquisition_date, region_id=region_id, raw_file_path=path,
        )
        silver_product_ids[band] = meta.insert_data_product(
            scene_id=aux_scene_id, job_id=job_id, dataset_id=dataset_id,
            product_tier="SILVER", source=fm.db_source(source),
            product_type=product_type, band_name=band,
            file_path=path, file_name=Path(path).name,
            file_size_mb=round(Path(path).stat().st_size / (1024 ** 2), 3),
            data_hash_sha256=lineage.compute_sha256(path),
        )

    return silver_product_ids, job_id


def _promote_aux_to_gold(
    db: DatabaseClient,
    *,
    dataset_id: int,
    dataset_name: str,
    source: str,
    date_key: str,
    aux_scene_id: int,
    silver_paths: dict[str, str],
    silver_product_ids: dict[str, int],
) -> dict[str, str]:
    """Ekspor band SILVER MODIS/GPM satu tanggal ke COG di tier GOLD dan
    catat produknya + lineage-nya."""
    meta = MetadataManager(db)
    lineage = LineageTracker(db)

    gold_paths = m4.export_scene_to_gold(
        dataset_id, dataset_name, source, date_key, silver_paths
    )
    if not gold_paths:
        return {}

    gold_job_id = meta.insert_processing_job(
        aux_scene_id, "GOLD_EXPORT",
        parameters={"dataset_id": dataset_id, "source": source.upper(), "date": date_key},
    )
    meta.start_job(gold_job_id)
    for band, path in gold_paths.items():
        if _product_exists(db, dataset_id, path):
            continue
        gold_product_id = meta.insert_data_product(
            scene_id=aux_scene_id, job_id=gold_job_id, dataset_id=dataset_id,
            product_tier="GOLD", source=fm.db_source(source),
            product_type=m4.gold_product_type(source), band_name=band,
            file_path=path, file_name=Path(path).name,
            file_size_mb=round(Path(path).stat().st_size / (1024 ** 2), 3),
            data_hash_sha256=lineage.compute_sha256(path),
            file_format="COG",
        )
        if band in silver_product_ids:
            lineage.record_transformation(
                silver_product_ids[band], gold_product_id, "GOLD_EXPORT", gold_job_id,
                {"source": source, "date": date_key},
            )
    meta.complete_job(gold_job_id)
    return gold_paths


def ensure_modis_inputs_for_date(
    db: DatabaseClient,
    dataset_id: int,
    dataset_name: str,
    region_id: int,
    aoi_bbox: tuple[float, float, float, float],
    target_date: date_type,
    plog: PipelineLogger | None = None,
) -> dict[str, list[str]]:
    """Siapkan input MODIS (FLOOD/NDVI/NDWI) untuk satu tanggal: download
    kalau belum ada di disk, daftarkan sebagai data_products SILVER, ekspor
    ke COG di tier GOLD, lalu daftarkan produk GOLD-nya.

    Lihat ensure_aux_inputs_for_date untuk kontrak idempotensi & error."""
    from etl.module7_modis_download import MODIS_PRODUCT_TYPES, download_modis_scene

    bbox_wkt = box(*aoi_bbox).wkt
    target_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    produced: dict[str, list[str]] = {"SILVER": [], "GOLD": []}

    try:
        _, modis_meta = download_modis_scene(
            dataset_id, dataset_name, target_dt, target_dt, aoi_bbox, plog=plog
        )
    except Exception:
        logger.exception(
            "[M9] gagal siapkan input MODIS dataset=%s tanggal=%s", dataset_id, target_date
        )
        return produced

    for output in modis_meta["outputs"]:
        output_date = date_type.fromisoformat(output["date"])
        aux_scene_id = _resolve_aux_scene(
            db, dataset_id, region_id, bbox_wkt, "MODIS", output_date
        )
        # product_type dibedakan per band supaya FLOOD/NDVI/NDWI tetap bisa
        # dipisahkan tanpa mengandalkan nama file.
        for band, path in output["products"].items():
            silver_ids, _ = _register_aux_products(
                db, dataset_id=dataset_id, region_id=region_id,
                source="modis", nasa_source=MODIS_SOURCE, nasa_tile_id=MODIS_TILE_ID,
                nasa_product_short_name=MODIS_PRODUCT_SHORT_NAME,
                acquisition_date=output_date, aux_scene_id=aux_scene_id,
                silver_paths={band: path},
                product_type=MODIS_PRODUCT_TYPES[band],
            )
            gold_paths = _promote_aux_to_gold(
                db, dataset_id=dataset_id, dataset_name=dataset_name, source="modis",
                date_key=output_date.strftime("%Y%m%d"), aux_scene_id=aux_scene_id,
                silver_paths={band: path}, silver_product_ids=silver_ids,
            )
            produced["SILVER"].append(path)
            produced["GOLD"].extend(gold_paths.values())

    return produced


def ensure_gpm_inputs_for_date(
    db: DatabaseClient,
    dataset_id: int,
    dataset_name: str,
    region_id: int,
    aoi_bbox: tuple[float, float, float, float],
    target_date: date_type,
    plog: PipelineLogger | None = None,
) -> dict[str, list[str]]:
    """Siapkan input GPM (rainfall 24h/72h/7d) untuk satu tanggal, SILVER
    lalu GOLD. Lihat ensure_aux_inputs_for_date untuk kontraknya."""
    from etl.module8_gpm_download import (
        GPM_PRODUCT_TYPE,
        band_name as gpm_band_name,
        download_gpm_scene,
    )

    bbox_wkt = box(*aoi_bbox).wkt
    target_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    date_key = target_date.strftime("%Y%m%d")
    produced: dict[str, list[str]] = {"SILVER": [], "GOLD": []}

    try:
        _, gpm_meta = download_gpm_scene(dataset_id, dataset_name, target_dt, aoi_bbox, plog=plog)
    except Exception:
        logger.exception(
            "[M9] gagal siapkan input GPM dataset=%s tanggal=%s", dataset_id, target_date
        )
        return produced

    aux_scene_id = _resolve_aux_scene(db, dataset_id, region_id, bbox_wkt, "GPM", target_date)
    silver_paths = {
        gpm_band_name(window_name): output["path"]
        for window_name, output in gpm_meta["windows"].items()
    }
    silver_ids, _ = _register_aux_products(
        db, dataset_id=dataset_id, region_id=region_id,
        source="gpm", nasa_source=GPM_SOURCE, nasa_tile_id=GPM_TILE_ID,
        nasa_product_short_name=GPM_PRODUCT_SHORT_NAME,
        acquisition_date=target_date, aux_scene_id=aux_scene_id,
        silver_paths=silver_paths, product_type=GPM_PRODUCT_TYPE,
    )
    gold_paths = _promote_aux_to_gold(
        db, dataset_id=dataset_id, dataset_name=dataset_name, source="gpm",
        date_key=date_key, aux_scene_id=aux_scene_id,
        silver_paths=silver_paths, silver_product_ids=silver_ids,
    )
    produced["SILVER"].extend(silver_paths.values())
    produced["GOLD"].extend(gold_paths.values())
    return produced


def ensure_aux_inputs_for_date(
    db: DatabaseClient,
    dataset_id: int,
    dataset_name: str,
    region_id: int,
    aoi_bbox: tuple[float, float, float, float],
    target_date: date_type,
    plog: PipelineLogger | None = None,
) -> dict[str, list[str]]:
    """
    Siapkan input MODIS (flood/NDVI/NDWI) + GPM (rainfall 24h/72h/7d) yang
    dibutuhkan untuk memfusikan scene S1 tanggal `target_date`: download
    kalau belum ada di disk, daftarkan sebagai data_products SILVER, ekspor
    ke COG di tier GOLD, lalu daftarkan produk GOLD-nya.

    Idempotent: module7/module8 melewati file yang sudah ada di disk, dan
    dedup lewat file_path melewati registrasi ulang baris data_products
    untuk file yang sudah tercatat di dataset ini.

    Kegagalan download di-log lalu ditelan di sini — create_fusion_stack
    sudah mentoleransi input MODIS/GPM yang hilang dengan mengisi
    NaN/nodata, jadi satu gangguan server NASA tidak boleh menjatuhkan
    seluruh pipeline scene (hanya referensi S1 GOLD yang hilang yang fatal,
    dicek di create_fusion_stack). Kegagalan MODIS dan GPM juga terisolasi
    satu sama lain.

    Returns:
        {tier: [path, ...]} untuk file MODIS/GPM yang ditulis di sini.
        Orchestrator memakainya untuk membersihkan tier aux yang tidak
        diminta dataset — tanpa ini, file gold/modis + gold/gpm akan
        tertinggal di disk saat user cuma meminta tier FUSION.
    """
    produced: dict[str, list[str]] = {"SILVER": [], "GOLD": []}
    for part in (
        ensure_modis_inputs_for_date(
            db, dataset_id, dataset_name, region_id, aoi_bbox, target_date, plog=plog
        ),
        ensure_gpm_inputs_for_date(
            db, dataset_id, dataset_name, region_id, aoi_bbox, target_date, plog=plog
        ),
    ):
        for tier, paths in part.items():
            produced[tier].extend(paths)
    return produced


def create_fusion_stack(
    dataset_id: int,
    dataset_name: str,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    scene_id: int,
    db: DatabaseClient | None = None,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> int:
    """
    Build a fused HDF5 feature stack for Sentinel-1 `scene_id` (acquired on
    `s1_date`), aligning MODIS and GPM GOLD products found within 24h of the
    S1 acquisition time. This is the FUSION tier deliverable.

    Writes:
        data/datasets/{id}_{slug}/fusion/{s1_date}/fusion_{s1_date}.h5
        data/datasets/{id}_{slug}/fusion/{s1_date}/fusion_metadata.json

    Returns:
        fusion_id (int) — primary key of the `fusion_products` row, used
        for lineage tracking.
    """
    from etl.module7_modis_download import band_filename as modis_band_filename
    from etl.module8_gpm_download import band_filename as gpm_band_filename

    owns_db = db is None
    db = db or DatabaseClient.from_env()
    lineage = LineageTracker(db)
    meta = MetadataManager(db)

    try:
        s1 = _find_s1_gold(db, dataset_id, scene_id)
        if s1 is None or (not s1["vv_path"] and not s1["vh_path"]):
            raise RuntimeError(
                f"No S1 GOLD product found for scene_id={scene_id} "
                f"s1_date={s1_date.isoformat()}"
            )

        ref_path = s1["vv_path"] or s1["vh_path"]
        with rasterio.open(ref_path) as ref:
            ref_transform, ref_crs = ref.transform, ref.crs
            ref_shape = (ref.height, ref.width)

        total_layers = len(FUSION_LAYERS)
        done = 0
        layers: dict[str, np.ndarray] = {}
        layer_sources: dict[str, dict] = {}
        found_dates: list[date_type] = []

        def _tick(name: str) -> None:
            nonlocal done
            done += 1
            if progress_cb:
                progress_cb(name, done, total_layers)

        # --- Sentinel-1 -----------------------------------------------------
        for band, path_key in (("VV", "vv_path"), ("VH", "vh_path")):
            name = f"sentinel1/{band}"
            layers[name] = _read_band_or_nan(s1[path_key], ref_shape)
            if s1[path_key] is None:
                logger.warning("[M9] S1 %s missing for scene=%s, filled with NaN", band, s1["scene_id"])
            layer_sources[name] = {"path": s1[path_key], "date": s1_date.isoformat()}
            _tick(name)

        center_dt = s1["acquisition_datetime"]

        # --- MODIS ----------------------------------------------------------
        for band, resampling in MODIS_FUSION_BANDS.items():
            name = f"modis/{band}"
            hit = _find_nearest_daily_file(
                dataset_id, dataset_name, "modis",
                lambda d, b=band: modis_band_filename(b, d.strftime("%Y%m%d")),
                center_dt,
            )
            if hit:
                values = _reproject_to_grid(
                    hit[0], ref_transform, ref_crs, ref_shape,
                    resampling=resampling, fill_value=np.nan,
                )
                if band == "FLOOD":
                    # Kelas banjir itu kategorikal: NaN tidak muat di uint8,
                    # jadi pakai sentinel 255 yang sama dengan module7.
                    values = np.where(np.isnan(values), MODIS_NODATA_U8, values).astype("uint8")
                layers[name] = values
                layer_sources[name] = {"path": str(hit[0]), "date": hit[1].isoformat()}
                found_dates.append(hit[1])
            else:
                logger.warning(
                    "[M9] no MODIS %s GOLD product within %dh of %s",
                    band, ALIGNMENT_WINDOW_HOURS, center_dt,
                )
                layers[name] = (
                    np.full(ref_shape, MODIS_NODATA_U8, dtype="uint8") if band == "FLOOD"
                    else np.full(ref_shape, np.nan, dtype="float32")
                )
                layer_sources[name] = {"path": None, "date": None}
            _tick(name)

        # --- GPM ------------------------------------------------------------
        for window in GPM_FUSION_WINDOWS:
            name = f"gpm/rainfall_{window}"
            hit = _find_nearest_daily_file(
                dataset_id, dataset_name, "gpm",
                lambda d, w=window: gpm_band_filename(w, d.strftime("%Y%m%d")),
                center_dt,
            )
            if hit:
                layers[name] = _reproject_to_grid(
                    hit[0], ref_transform, ref_crs, ref_shape,
                    resampling=Resampling.bilinear, fill_value=np.nan,
                )
                layer_sources[name] = {"path": str(hit[0]), "date": hit[1].isoformat()}
                found_dates.append(hit[1])
            else:
                logger.warning(
                    "[M9] no GPM %s GOLD product within %dh of %s",
                    window, ALIGNMENT_WINDOW_HOURS, center_dt,
                )
                layers[name] = np.full(ref_shape, np.nan, dtype="float32")
                layer_sources[name] = {"path": None, "date": None}
            _tick(name)

        date_key = s1_date.strftime("%Y%m%d")
        out_dir = fm.ensure_fusion_dir(dataset_id, dataset_name, date_key)
        h5_path = out_dir / f"fusion_{date_key}.h5"
        json_path = out_dir / "fusion_metadata.json"
        processing_dt = datetime.now(tz=timezone.utc)

        _write_fusion_h5(
            h5_path, layers,
            acquisition_datetime=center_dt, processing_datetime=processing_dt,
            aoi_bbox=aoi_bbox, crs=ref_crs, transform=ref_transform,
        )

        modis_scene_id = None
        modis_hit_date = layer_sources["modis/FLOOD"]["date"]
        if modis_hit_date:
            modis_scene_id = _get_or_create_nasa_scene(
                db, MODIS_SOURCE, MODIS_TILE_ID, MODIS_PRODUCT_SHORT_NAME,
                date_type.fromisoformat(modis_hit_date), s1["region_id"],
                Path(layer_sources["modis/FLOOD"]["path"]),
            )
        gpm_scene_id = None
        gpm_hit_date = layer_sources["gpm/rainfall_24h"]["date"]
        if gpm_hit_date:
            gpm_scene_id = _get_or_create_nasa_scene(
                db, GPM_SOURCE, GPM_TILE_ID, GPM_PRODUCT_SHORT_NAME,
                date_type.fromisoformat(gpm_hit_date), s1["region_id"],
                Path(layer_sources["gpm/rainfall_24h"]["path"]),
            )

        days_since_s1 = max((abs((d - s1_date).days) for d in found_dates), default=0)

        with db.session() as sess:
            existing = sess.scalar(
                select(FusionProduct).where(
                    FusionProduct.feature_date == s1_date,
                    FusionProduct.region_id == s1["region_id"],
                )
            )
            if existing:
                existing.s1_scene_id = s1["scene_id"]
                existing.modis_scene_id = modis_scene_id
                existing.gpm_scene_id = gpm_scene_id
                existing.days_since_s1 = days_since_s1
                existing.feature_stack_path = str(h5_path)
                sess.flush()
                fusion_id = existing.fusion_id
            else:
                fusion = FusionProduct(
                    feature_date=s1_date,
                    region_id=s1["region_id"],
                    s1_scene_id=s1["scene_id"],
                    modis_scene_id=modis_scene_id,
                    gpm_scene_id=gpm_scene_id,
                    days_since_s1=days_since_s1,
                    feature_stack_path=str(h5_path),
                )
                sess.add(fusion)
                sess.flush()
                fusion_id = fusion.fusion_id

        height, width = ref_shape
        _write_fusion_metadata_json(
            json_path, fusion_id, dataset_id, s1["region_id"], s1_date, s1,
            layer_sources, days_since_s1,
            center_dt, processing_dt, aoi_bbox, h5_path, height, width,
        )

        fusion_job_id = meta.insert_processing_job(
            s1["scene_id"], "FUSION",
            parameters={"dataset_id": dataset_id, "s1_date": s1_date.isoformat()},
        )
        meta.start_job(fusion_job_id)
        fusion_product_id = meta.insert_data_product(
            scene_id=s1["scene_id"], job_id=fusion_job_id, dataset_id=dataset_id,
            product_tier="FUSION", source=fm.FUSION_DB_SOURCE,
            product_type="FUSION_H5", band_name="FUSION",
            file_path=str(h5_path), file_name=h5_path.name,
            file_size_mb=round(h5_path.stat().st_size / (1024 ** 2), 3),
            data_hash_sha256=lineage.compute_sha256(h5_path),
            file_format="HDF5", rows=height, cols=width,
        )
        if s1["vv_product_id"]:
            lineage.record_transformation(
                s1["vv_product_id"], fusion_product_id, "FUSION", fusion_job_id,
                {"aoi_bbox": list(aoi_bbox)},
            )
        if s1["vh_product_id"]:
            lineage.record_transformation(
                s1["vh_product_id"], fusion_product_id, "FUSION", fusion_job_id,
                {"aoi_bbox": list(aoi_bbox)},
            )
        meta.complete_job(fusion_job_id)

        logger.info(
            "[M9] fusion_id=%d dataset=%s date=%s path=%s hash=%s",
            fusion_id, dataset_id, s1_date.isoformat(), h5_path,
            lineage.compute_sha256(h5_path)[:12],
        )
        return fusion_id

    finally:
        if owns_db:
            db.dispose()
