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
    data/datasets/{id}_{slug}/{YYYYMMDD}/gold/{modis,gpm}/, dicocokkan ke
    waktu akuisisi S1 dalam jendela 24 jam, direproject ke grid S1, dan
    didaftarkan sebagai baris `nasa_scenes`.

Hasilnya ditulis ke data/datasets/{id}_{slug}/{date}/fusion/ sebagai .h5 +
metadata JSON, dicatat sebagai baris `fusion_products` (untuk lineage
`fusion_id`) dan baris `data_products` (tier=FUSION, source=FUSION).

Struktur HDF5 dikelompokkan per source, bukan datar, dan hanya memuat group
untuk sumber yang benar-benar dikonfigurasi dataset ini (DOCS/ETL.md, "Fusion
Process" langkah 4). Isi tiap group ikut level sumbernya:

    sentinel1 RAW / PROCESSED  ->  /sentinel1/VV, /sentinel1/VH
    modis     RAW              ->  /modis/FLOOD
    modis     PROCESSED        ->  /modis/FLOOD, /modis/NDVI, /modis/NDWI
    gpm       RAW              ->  /gpm/rainfall_daily
    gpm       PROCESSED        ->  /gpm/rainfall_24h, /gpm/rainfall_72h,
                                   /gpm/rainfall_7d

Stack yang levelnya RAW dibaca dari BRONZE, yang PROCESSED dari GOLD. Sebuah
dataset yang meminta salah satu sumbernya di KEDUA level menghasilkan dua
stack per tanggal (satu RAW, satu PROCESSED) — lihat
ProcessingPlan.output_levels() di etl/processing_plan.py untuk aturan
lengkapnya.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
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
from etl.processing_plan import GPM as GPM_PLAN_NAME
from etl.processing_plan import MODIS as MODIS_PLAN_NAME
from etl.processing_plan import SENTINEL1 as S1_PLAN_NAME
from etl.processing_plan import (
    PROCESSED,
    RAW,
    ProcessingPlan,
    SourcePlan,
    load_processing_plan,
)

logger = logging.getLogger(__name__)

ALIGNMENT_WINDOW_HOURS = 24
MODIS_NODATA_U8 = 255  # uint8 can't hold NaN; 255 marks a missing/nodata pixel
HDF5_CHUNK_MAX = 256

# Band Sentinel-1 yang ikut difusikan. Sama di kedua level: "RAW" untuk SAR
# tetap berarti terkalibrasi (DOCS/ETL.md), yang berubah cuma tier sumbernya.
S1_FUSION_BANDS: tuple[str, ...] = ("VV", "VH")


@dataclass(frozen=True)
class _AuxLayer:
    """Satu dataset HDF5 dari sumber aux (MODIS/GPM).

    `name` adalah nama di dalam group HDF5, `file_key` adalah kunci yang
    dipakai `band_filename()` modul sumbernya. Keduanya sengaja dipisah: pada
    level RAW, GPM menulis window "24h" ke disk tapi menyajikannya sebagai
    /gpm/rainfall_daily di HDF5 — nama itu jujur soal isinya (curah hujan satu
    hari, tanpa akumulasi) dan mencegah konsumen menyangka stack RAW punya
    window 24h yang sebanding dengan milik stack PROCESSED.
    """

    name: str
    file_key: str
    resampling: Resampling
    categorical: bool = False


# Lapisan aux per level. Level RAW hanya memuat artefak mentah sumbernya;
# turunannya (NDVI/NDWI, akumulasi 72h/7d) tidak pernah dihitung di jalur RAW
# jadi tidak ada berkasnya untuk dimasukkan (DOCS/DESIGN.md, tabel RAW vs
# PROCESSED per satelit).
_FLOOD = _AuxLayer("FLOOD", "FLOOD", Resampling.nearest, categorical=True)

MODIS_FUSION_LAYERS: dict[str, tuple[_AuxLayer, ...]] = {
    RAW: (_FLOOD,),
    PROCESSED: (
        _FLOOD,
        _AuxLayer("NDVI", "NDVI", Resampling.bilinear),
        _AuxLayer("NDWI", "NDWI", Resampling.bilinear),
    ),
}

GPM_FUSION_LAYERS: dict[str, tuple[_AuxLayer, ...]] = {
    RAW: (_AuxLayer("rainfall_daily", "24h", Resampling.bilinear),),
    PROCESSED: (
        _AuxLayer("rainfall_24h", "24h", Resampling.bilinear),
        _AuxLayer("rainfall_72h", "72h", Resampling.bilinear),
        _AuxLayer("rainfall_7d", "7d", Resampling.bilinear),
    ),
}

_AUX_LAYERS_BY_SOURCE: dict[str, dict[str, tuple[_AuxLayer, ...]]] = {
    MODIS_PLAN_NAME: MODIS_FUSION_LAYERS,
    GPM_PLAN_NAME: GPM_FUSION_LAYERS,
}

# Nama group HDF5 per sumber (lowercase), dan sekaligus nama folder tier-nya
# di disk — folder_manager memakai konvensi yang sama.
GROUP_BY_SOURCE: dict[str, str] = {
    S1_PLAN_NAME: "sentinel1",
    MODIS_PLAN_NAME: "modis",
    GPM_PLAN_NAME: "gpm",
}

# Seluruh lapisan yang MUNGKIN muncul, dipakai untuk logging/UI saat
# konfigurasi dataset belum diketahui. Isi berkas sebenarnya ditentukan
# fusion_layers_for(); jangan pakai konstanta ini sebagai kontrak isi HDF5.
FUSION_LAYERS = [
    *(f"sentinel1/{band}" for band in S1_FUSION_BANDS),
    *(f"modis/{layer.name}" for layer in MODIS_FUSION_LAYERS[PROCESSED]),
    *(f"gpm/{layer.name}" for layer in GPM_FUSION_LAYERS[PROCESSED]),
]


def fusion_layers_for(source_levels: dict[str, str]) -> list[str]:
    """Path dataset HDF5 yang akan ditulis untuk {SOURCE: level} ini.

    Ini adalah satu-satunya definisi "group apa yang ada di dalam HDF5".
    Sumber yang tidak ada di `source_levels` tidak menghasilkan group sama
    sekali — bukan group berisi NaN. Group kosong akan membuat konsumen
    (dan tabel `data_products`) tidak bisa membedakan "sensor ini tidak
    diminta" dari "sensor ini diminta tapi datanya hilang hari itu", padahal
    keduanya butuh penanganan berbeda saat training.
    """
    out: list[str] = []
    for source, level in source_levels.items():
        group = GROUP_BY_SOURCE.get(source)
        if group is None:
            logger.warning("[M9] sumber tanpa group HDF5, dilewati: %r", source)
            continue
        if source == S1_PLAN_NAME:
            out.extend(f"{group}/{band}" for band in S1_FUSION_BANDS)
        else:
            out.extend(
                f"{group}/{layer.name}"
                for layer in _AUX_LAYERS_BY_SOURCE[source][level]
            )
    return out


def _find_s1_products(
    db: DatabaseClient, dataset_id: int, scene_id: int, tier: str = "GOLD"
) -> dict | None:
    """Cari produk Sentinel-1 VV/VH scene `scene_id` di `tier`. None kalau
    scene-nya sendiri tidak ada.

    `tier` mengikuti level yang dikonfigurasi untuk SENTINEL1 pada run ini:
    GOLD untuk PROCESSED, BRONZE untuk RAW. BRONZE adalah artefak RAW S1 yang
    sah — sudah terkalibrasi, terreproyeksi, dan ter-crop, cuma belum
    di-Lee-filter (DOCS/ETL.md, "What RAW means for Sentinel-1") — jadi
    memfusikannya bukan kompromi, itu memang deliverable yang diminta user.

    Dicari lewat scene_id persis yang baru diproses pemanggil, bukan
    diturunkan ulang dari tanggal akuisisi: dua slice Sentinel-1 dari orbit
    yang sama bisa jatuh di hari UTC yang sama di atas AOI yang sama, dan
    memilih "scene mana pun yang hari itu" akan diam-diam mengambil yang
    keliru begitu ada lebih dari satu yang memenuhi syarat."""
    tier_enum = ProductTierEnum[str(tier).upper()]
    with db.session() as sess:
        scene = sess.get(SatelliteScene, scene_id)
        if scene is None:
            return None

        def _band(band: str) -> DataProduct | None:
            return sess.scalar(
                select(DataProduct).where(
                    DataProduct.scene_id == scene.scene_id,
                    DataProduct.dataset_id == dataset_id,
                    DataProduct.product_tier == tier_enum,
                    DataProduct.source == "SENTINEL1",
                    DataProduct.band_name == band,
                    DataProduct.is_latest == True,
                    DataProduct.is_valid == True,
                )
            )

        vv = _band("VV")
        vh = _band("VH")

        return {
            "scene_id": scene.scene_id,
            "region_id": scene.region_id,
            "acquisition_datetime": scene.acquisition_datetime,
            "tier": tier_enum.value,
            "vv_product_id": vv.product_id if vv else None,
            "vv_path": vv.file_path if vv else None,
            "vh_product_id": vh.product_id if vh else None,
            "vh_path": vh.file_path if vh else None,
        }


def _as_utc(dt: datetime) -> datetime:
    """Normalkan waktu akuisisi ke UTC sebelum diturunkan jadi tanggal.

    Wajib, bukan kosmetik: psycopg2 mengembalikan TIMESTAMPTZ dalam zona waktu
    SESI database, jadi `acquisition_datetime` sebuah scene bisa datang sebagai
    2024-03-06T05:50+07:00 padahal berkas MODIS/GPM-nya distempel dengan
    tanggal UTC-nya (2024-03-05) — orchestrator memakai `acq_date.date()` dari
    hasil download yang selalu UTC. Tanpa normalisasi ini, kunci tanggal yang
    dicari fusion bergeser satu hari di setiap deployment yang zona waktunya
    di timur UTC (termasuk Asia/Jakarta, AOI utama proyek ini), dan SEMUA
    lapisan aux terisi NaN tanpa satu pun error.

    Datetime naif dianggap sudah UTC — itu konvensi seluruh pipeline ini.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _find_nearest_daily_file(
    dataset_id: int,
    dataset_name: str,
    source: str,
    filename_fn,
    center_dt: datetime,
    tolerance_hours: int = ALIGNMENT_WINDOW_HOURS,
    tier: str = "gold",
) -> tuple[Path, date_type] | None:
    """File MODIS/GPM distempel satu per hari pada tengah malam lokal,
    masing-masing di folder {tier}/{source}/{YYYYMMDD}/ sendiri. Kembalikan
    (path, tanggal) kandidat hari terdekat yang tengah malamnya masih dalam
    `tolerance_hours` dari `center_dt`, atau None kalau tidak ada di disk.

    `tier` ikut level sumber pada run ini: "gold" untuk PROCESSED, "bronze"
    untuk RAW. Nama berkasnya sama persis di kedua tier (module7/module8
    memakai `band_filename()` yang sama untuk semua target), jadi yang berbeda
    hanya folder induknya.

    Toleransinya ALIGNMENT_WINDOW_HOURS penuh (24 jam), bukan setengahnya.
    Setengah jendela berarti "tengah malam terdekat", dan itu memutus justru
    kasus yang paling umum di AOI ini: pass descending Sentinel-1 di atas
    Jabodetabek turun sekitar 22:50 UTC, yang jaraknya 22,8 jam dari tengah
    malam HARI ITU tapi cuma 1,2 jam dari tengah malam hari BERIKUTNYA. Dengan
    toleransi 12 jam, berkas MODIS/GPM hari itu — satu-satunya yang memang
    diunduh pipeline (ensure_aux_inputs_for_date dipanggil dengan s1_date) —
    di luar jendela, jadi setiap stack fusion terisi NaN untuk semua lapisan
    aux. Kandidat tetap diurutkan berdasarkan selisih terkecil, jadi hari
    berikutnya tetap menang KALAU berkasnya ada."""
    center_dt = _as_utc(center_dt)
    best: tuple[Path, date_type] | None = None
    best_diff = None
    for offset in (0, -1, 1):
        candidate_date = (center_dt + timedelta(days=offset)).date()
        candidate_midnight = datetime.combine(candidate_date, datetime.min.time(),
                                              tzinfo=center_dt.tzinfo)
        diff_hours = abs((candidate_midnight - center_dt).total_seconds()) / 3600.0
        if diff_hours > tolerance_hours:
            continue
        tier_dir = fm.get_scene_dir(
            dataset_id, dataset_name, tier.lower(), source,
            candidate_date.strftime("%Y%m%d"),
        )
        path = tier_dir / filename_fn(candidate_date)
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
    ref_shape: tuple[int, int],
    acquisition_datetime: datetime,
    processing_datetime: datetime,
    aoi_bbox: tuple[float, float, float, float],
    processing_level: str,
    source_levels: dict[str, str],
    fusion_strategy: str | None = None,
    crs: object | None = None,
    transform: object | None = None,
) -> None:
    """Tulis stack fusion. `layers` memetakan path dataset HDF5
    ("modis/NDVI") ke arraynya; h5py membuat group perantaranya sendiri.

    `ref_shape` dioper terpisah, tidak lagi dibaca dari layers["sentinel1/VV"]:
    isi `layers` sekarang bergantung pada sumber apa yang dikonfigurasi, jadi
    tidak ada satu pun nama lapisan yang dijamin ada di setiap stack.

    `crs`/`transform` adalah grid referensi scene S1 yang semua layer sudah
    direproject ke sana. Keduanya wajib ikut ditulis: `aoi_bbox` adalah kotak
    AOI yang diminta, bukan batas raster hasilnya, jadi tanpa affine transform
    yang sebenarnya konsumen tidak bisa memetakan piksel ke koordinat bumi
    selain dengan menebak.

    `processing_level` dan `source_levels` ditulis sebagai atribut root supaya
    berkasnya bisa menjelaskan dirinya sendiri: dua stack tanggal yang sama
    dari dataset RAW+PROCESSED punya lapisan yang bisa bernama sama, dan tanpa
    atribut ini konsumen harus menebak dari nama berkas mana yang mana."""
    height, width = ref_shape
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
        f.attrs["processing_level"] = processing_level
        # Ditulis sebagai dua array sejajar, bukan JSON: h5py tidak punya tipe
        # map, dan array string bisa dibaca alat apa pun (h5dump, HDFView)
        # tanpa mem-parse ulang.
        f.attrs["sources"] = list(source_levels)
        f.attrs["source_levels"] = [source_levels[k] for k in source_levels]
        if fusion_strategy:
            f.attrs["fusion_strategy"] = fusion_strategy
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
        "[M9] Saving fusion H5 to FUSION tier: %s level=%s shape=(%d, %d) layers=%d %s",
        h5_path, processing_level, height, width, len(layers), sorted(layers),
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
    processing_level: str,
    source_levels: dict[str, str],
    source_tiers: dict[str, str],
    fusion_strategy: str | None,
    temporal_offsets: dict[str, int | None],
    checksum_sha256: str,
) -> None:
    """Tulis sidecar fusion_metadata.json.

    `layers` di sini adalah lapisan yang BENAR-BENAR ditulis (kunci
    `layer_sources`), bukan daftar semua lapisan yang mungkin. Sebelumnya
    berisi konstanta FUSION_LAYERS, yang pada dataset selektif berbohong
    tentang isi berkas — sidecar-nya menjanjikan delapan lapisan padahal HDF5
    di sebelahnya cuma punya tiga.
    """
    metadata = {
        "fusion_id": fusion_id,
        "dataset_id": dataset_id,
        "region_id": region_id,
        "feature_date": s1_date.isoformat(),
        "acquisition_datetime": acquisition_datetime.isoformat(),
        "processing_datetime": processing_datetime.isoformat(),
        "aoi_bbox": list(aoi_bbox),
        "shape": {"height": height, "width": width},
        "processing_level": processing_level,
        # Level per sumber, bukan cuma level stack-nya: pada dataset campuran
        # (mis. sentinel1[RAW] + modis[PROCESSED]) satu angka di level stack
        # tidak cukup untuk merekonstruksi asal tiap lapisan.
        "source_levels": source_levels,
        "source_tiers": source_tiers,
        "fusion_strategy": fusion_strategy,
        "layers": list(layer_sources),
        "layer_sources": layer_sources,
        "source_scenes": {"s1_scene_id": s1["scene_id"]},
        "days_since_s1": days_since_s1,
        "temporal_offsets": temporal_offsets,
        "file_name": h5_path.name,
        "file_size_mb": round(h5_path.stat().st_size / (1024 ** 2), 3),
        "checksum_sha256": checksum_sha256,
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


def _aux_plan(db: DatabaseClient, dataset_id: int, source_name: str) -> SourcePlan | None:
    """Konfigurasi satu sumber aux, dibaca dari dataset_source_config.

    Dipakai pemanggil yang tidak membawa ProcessingPlan sendiri (live_scheduler
    memanggil ensure_*_inputs_for_date langsung per sumber). None berarti
    sumber itu tidak dikonfigurasi untuk dataset ini — pemanggil harus
    melewatinya, bukan memprosesnya dengan default."""
    return load_processing_plan(db, dataset_id).get(source_name)


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
    band_paths: dict[str, str],
    product_type: str,
    tier: str = "SILVER",
    processing_level: str = PROCESSED,
) -> tuple[dict[str, int], int]:
    """Daftarkan band satu source/tanggal sebagai data_products di `tier`.

    `tier`/`processing_level` datang dari SourcePlan.targets(): band level RAW
    mendarat di BRONZE dan ditandai processing_level='RAW', band jalur penuh
    di SILVER dan ditandai 'PROCESSED' (DOCS/ETL.md). Sebelum model
    per-satelit keduanya selalu SILVER/PROCESSED, karena cuma ada satu jalur.

    Mengembalikan ({band: product_id}, job_id) — product_id dipakai
    _promote_aux_to_gold untuk mencatat lineage SILVER -> GOLD."""
    meta = MetadataManager(db)
    lineage = LineageTracker(db)

    job_id = meta.insert_processing_job(
        aux_scene_id, "DOWNLOAD", parameters={"dataset_id": dataset_id, "source": source.upper()}
    )
    product_ids: dict[str, int] = {}
    for band, path in band_paths.items():
        if not Path(path).exists():
            continue
        if _product_exists(db, dataset_id, path):
            continue
        meta.insert_nasa_scene(
            source=nasa_source, tile_id=nasa_tile_id,
            product_short_name=nasa_product_short_name,
            acquisition_date=acquisition_date, region_id=region_id, raw_file_path=path,
        )
        product_ids[band] = meta.insert_data_product(
            scene_id=aux_scene_id, job_id=job_id, dataset_id=dataset_id,
            product_tier=tier, source=fm.db_source(source),
            product_type=product_type, band_name=band,
            file_path=path, file_name=Path(path).name,
            file_size_mb=round(Path(path).stat().st_size / (1024 ** 2), 3),
            data_hash_sha256=lineage.compute_sha256(path),
            processing_level=processing_level,
        )

    return product_ids, job_id


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
            # GOLD hanya pernah lahir dari jalur PROCESSED: level RAW berhenti
            # di BRONZE dan tidak pernah sampai ke fungsi ini.
            processing_level=PROCESSED,
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
    plan: SourcePlan | None = None,
) -> dict[str, list[str]]:
    """Siapkan input MODIS untuk satu tanggal sesuai level yang dikonfigurasi.

    RAW      : FLOOD saja -> bronze/, didaftarkan sebagai data_products BRONZE
               dengan processing_level='RAW'. TIDAK diekspor ke GOLD — level
               RAW memang berhenti di BRONZE (DOCS/ETL.md).
    PROCESSED: FLOOD+NDVI+NDWI -> silver/, lalu COG GOLD, keduanya ditandai
               processing_level='PROCESSED'.

    `plan` boleh None; kalau begitu konfigurasinya dibaca dari
    dataset_source_config (jalur live_scheduler, yang tidak punya plan job).

    Lihat ensure_aux_inputs_for_date untuk kontrak idempotensi & error."""
    from etl.module7_modis_download import MODIS_PRODUCT_TYPES, download_modis_scene

    plan = plan or _aux_plan(db, dataset_id, MODIS_PLAN_NAME)
    produced: dict[str, list[str]] = {"BRONZE": [], "SILVER": [], "GOLD": []}
    if plan is None:
        logger.info(
            "[M9] MODIS tidak dikonfigurasi untuk dataset=%s, dilewati", dataset_id
        )
        return produced

    bbox_wkt = box(*aoi_bbox).wkt
    target_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)

    try:
        _, modis_meta = download_modis_scene(
            dataset_id, dataset_name, target_dt, target_dt, aoi_bbox, plog=plog,
            processing_levels=plan.levels,
        )
    except Exception:
        logger.exception(
            "[M9] gagal siapkan input MODIS dataset=%s tanggal=%s", dataset_id, target_date
        )
        return produced

    for output in modis_meta["outputs"]:
        output_date = date_type.fromisoformat(output["date"])
        date_key = output_date.strftime("%Y%m%d")
        aux_scene_id = _resolve_aux_scene(
            db, dataset_id, region_id, bbox_wkt, "MODIS", output_date
        )
        # product_type dibedakan per band supaya FLOOD/NDVI/NDWI tetap bisa
        # dipisahkan tanpa mengandalkan nama file.
        for band, entry in output["bands"].items():
            for tier, target in entry["targets"].items():
                path = target["path"]
                product_ids, _ = _register_aux_products(
                    db, dataset_id=dataset_id, region_id=region_id,
                    source="modis", nasa_source=MODIS_SOURCE, nasa_tile_id=MODIS_TILE_ID,
                    nasa_product_short_name=MODIS_PRODUCT_SHORT_NAME,
                    acquisition_date=output_date, aux_scene_id=aux_scene_id,
                    band_paths={band: path},
                    product_type=MODIS_PRODUCT_TYPES[band],
                    tier=tier, processing_level=target["processing_level"],
                )
                produced[tier].append(path)
                if tier != "SILVER":
                    continue
                gold_paths = _promote_aux_to_gold(
                    db, dataset_id=dataset_id, dataset_name=dataset_name, source="modis",
                    date_key=date_key, aux_scene_id=aux_scene_id,
                    silver_paths={band: path}, silver_product_ids=product_ids,
                )
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
    plan: SourcePlan | None = None,
) -> dict[str, list[str]]:
    """Siapkan input GPM untuk satu tanggal sesuai level yang dikonfigurasi.

    RAW      : curah hujan hari itu saja (window 24h) -> bronze/, ditandai
               processing_level='RAW', tanpa ekspor GOLD.
    PROCESSED: window 24h/72h/7d -> silver/ lalu COG GOLD.

    `plan` boleh None; kalau begitu konfigurasinya dibaca dari
    dataset_source_config. Lihat ensure_aux_inputs_for_date untuk kontraknya."""
    from etl.module8_gpm_download import (
        GPM_PRODUCT_TYPE,
        band_name as gpm_band_name,
        download_gpm_scene,
    )

    plan = plan or _aux_plan(db, dataset_id, GPM_PLAN_NAME)
    produced: dict[str, list[str]] = {"BRONZE": [], "SILVER": [], "GOLD": []}
    if plan is None:
        logger.info(
            "[M9] GPM tidak dikonfigurasi untuk dataset=%s, dilewati", dataset_id
        )
        return produced

    bbox_wkt = box(*aoi_bbox).wkt
    target_dt = datetime.combine(target_date, datetime.min.time(), tzinfo=timezone.utc)
    date_key = target_date.strftime("%Y%m%d")

    try:
        _, gpm_meta = download_gpm_scene(
            dataset_id, dataset_name, target_dt, aoi_bbox, plog=plog,
            processing_levels=plan.levels,
        )
    except Exception:
        logger.exception(
            "[M9] gagal siapkan input GPM dataset=%s tanggal=%s", dataset_id, target_date
        )
        return produced

    aux_scene_id = _resolve_aux_scene(db, dataset_id, region_id, bbox_wkt, "GPM", target_date)

    # Dikelompokkan per tier: satu job registrasi per tier, bukan per window.
    by_tier: dict[str, dict[str, dict[str, str]]] = {}
    for window_name, output in gpm_meta["windows"].items():
        for tier, target in output["targets"].items():
            by_tier.setdefault(tier, {})[gpm_band_name(window_name)] = target

    for tier, bands in by_tier.items():
        band_paths = {band: target["path"] for band, target in bands.items()}
        level = next(iter(bands.values()))["processing_level"]
        product_ids, _ = _register_aux_products(
            db, dataset_id=dataset_id, region_id=region_id,
            source="gpm", nasa_source=GPM_SOURCE, nasa_tile_id=GPM_TILE_ID,
            nasa_product_short_name=GPM_PRODUCT_SHORT_NAME,
            acquisition_date=target_date, aux_scene_id=aux_scene_id,
            band_paths=band_paths, product_type=GPM_PRODUCT_TYPE,
            tier=tier, processing_level=level,
        )
        produced[tier].extend(band_paths.values())
        if tier != "SILVER":
            continue
        gold_paths = _promote_aux_to_gold(
            db, dataset_id=dataset_id, dataset_name=dataset_name, source="gpm",
            date_key=date_key, aux_scene_id=aux_scene_id,
            silver_paths=band_paths, silver_product_ids=product_ids,
        )
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
    plan: "ProcessingPlan | None" = None,
) -> dict[str, list[str]]:
    """
    Siapkan input MODIS + GPM yang dibutuhkan untuk memfusikan scene S1
    tanggal `target_date`: download kalau belum ada di disk, daftarkan sebagai
    data_products, dan (untuk level PROCESSED) ekspor ke COG di tier GOLD.

    Sumber yang TIDAK ada di `plan` sama sekali tidak disentuh, dan sumber
    yang cuma diminta RAW berhenti di BRONZE — jadi dataset yang hanya
    mengkonfigurasi S1+GPM tidak lagi diam-diam mengunduh MODIS. `plan` boleh
    None; kalau begitu konfigurasinya dibaca dari dataset_source_config.

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
    plan = plan or load_processing_plan(db, dataset_id)
    produced: dict[str, list[str]] = {"BRONZE": [], "SILVER": [], "GOLD": []}

    # Sumber yang tidak ada di plan dilewati DI SINI, bukan diserahkan ke
    # fungsi per-sumber: fungsi itu punya fallback "baca dari database" untuk
    # pemanggil lain (live_scheduler), dan fallback itu akan menghidupkan lagi
    # sumber yang justru sengaja tidak dikonfigurasi job ini.
    for source_name, ensure_fn in (
        (MODIS_PLAN_NAME, ensure_modis_inputs_for_date),
        (GPM_PLAN_NAME, ensure_gpm_inputs_for_date),
    ):
        source_plan = plan.get(source_name)
        if source_plan is None:
            logger.info(
                "[M9] %s tidak dikonfigurasi dataset=%s, tidak diunduh",
                source_name, dataset_id,
            )
            continue
        part = ensure_fn(
            db, dataset_id, dataset_name, region_id, aoi_bbox, target_date,
            plog=plog, plan=source_plan,
        )
        for tier, paths in part.items():
            produced[tier].extend(paths)
    return produced


@dataclass(frozen=True)
class FusionRun:
    """Hasil satu stack fusion — satu berkas HDF5 dan barisnya di database."""

    fusion_id: int
    processing_level: str
    h5_path: Path
    json_path: Path
    layers: tuple[str, ...]
    source_levels: dict[str, str]
    product_id: int
    checksum_sha256: str


def fusion_h5_name(date_key: str, processing_level: str) -> str:
    """Nama berkas stack HDF5. Level SELALU ikut di nama, juga saat dataset
    cuma menghasilkan satu stack.

    Penamaan bersyarat ("fusion_{date}.h5" kalau satu, bersufiks kalau dua)
    akan memaksa setiap konsumen — API, notebook training, skrip pihak ketiga —
    menangani dua pola nama dan menebak mana yang berlaku dari konfigurasi
    dataset yang belum tentu dia punya. Satu pola untuk semua kasus lebih
    murah, dan level di nama berkas membuat isi folder fusion/ bisa dibaca
    tanpa membuka satu pun HDF5."""
    return f"fusion_{date_key}_{processing_level.lower()}.h5"


def fusion_metadata_name(processing_level: str) -> str:
    """Sidecar JSON pendamping `fusion_h5_name` untuk level yang sama."""
    return f"fusion_metadata_{processing_level.lower()}.json"


def _aux_layers_for_run(
    dataset_id: int,
    dataset_name: str,
    source: str,
    level: str,
    center_dt: datetime,
    filename_fn: Callable[[str, str], str],
) -> list[tuple[_AuxLayer, tuple[Path, date_type] | None]]:
    """Pasangkan tiap lapisan aux yang diminta level ini dengan berkasnya di
    disk (None kalau tidak ada dalam jendela toleransi)."""
    tier = "gold" if level == PROCESSED else "bronze"
    out = []
    for layer in _AUX_LAYERS_BY_SOURCE[source][level]:
        hit = _find_nearest_daily_file(
            dataset_id, dataset_name, GROUP_BY_SOURCE[source],
            lambda d, k=layer.file_key: filename_fn(k, d.strftime("%Y%m%d")),
            center_dt, tier=tier,
        )
        out.append((layer, hit))
    return out


def _build_fusion_stack_for_level(
    db: DatabaseClient,
    *,
    dataset_id: int,
    dataset_name: str,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    scene_id: int,
    plan: ProcessingPlan,
    run_level: str,
    fusion_strategy: str | None,
    progress_cb: Callable[[str, int, int], None] | None,
) -> FusionRun:
    """Bangun SATU stack HDF5 untuk `run_level`. Dipanggil sekali atau dua
    kali per scene oleh create_fusion_stack."""
    from etl.module7_modis_download import band_filename as modis_band_filename
    from etl.module8_gpm_download import band_filename as gpm_band_filename

    lineage = LineageTracker(db)
    meta = MetadataManager(db)

    source_levels = plan.source_levels_for_run(run_level)
    source_tiers = {
        name: plan.sources[name].tier_for_run(run_level) for name in source_levels
    }
    s1_tier = source_tiers[S1_PLAN_NAME]

    s1 = _find_s1_products(db, dataset_id, scene_id, tier=s1_tier)
    if s1 is None or (not s1["vv_path"] and not s1["vh_path"]):
        raise RuntimeError(
            f"No S1 {s1_tier} product found for scene_id={scene_id} "
            f"s1_date={s1_date.isoformat()} (run level {run_level})"
        )

    ref_path = s1["vv_path"] or s1["vh_path"]
    with rasterio.open(ref_path) as ref:
        ref_transform, ref_crs = ref.transform, ref.crs
        ref_shape = (ref.height, ref.width)

    expected_layers = fusion_layers_for(source_levels)
    total_layers = len(expected_layers)
    done = 0
    layers: dict[str, np.ndarray] = {}
    layer_sources: dict[str, dict] = {}
    found_dates: list[date_type] = []
    # Offset per sumber aux, untuk fusion_products.temporal_offset_*.
    offsets: dict[str, int | None] = {MODIS_PLAN_NAME: None, GPM_PLAN_NAME: None}

    def _tick(name: str) -> None:
        nonlocal done
        done += 1
        if progress_cb:
            progress_cb(name, done, total_layers)

    # --- Sentinel-1 ---------------------------------------------------------
    for band, path_key in (("VV", "vv_path"), ("VH", "vh_path")):
        name = f"sentinel1/{band}"
        layers[name] = _read_band_or_nan(s1[path_key], ref_shape)
        if s1[path_key] is None:
            logger.warning(
                "[M9] S1 %s missing at %s for scene=%s, filled with NaN",
                band, s1_tier, s1["scene_id"],
            )
        layer_sources[name] = {
            "path": s1[path_key],
            "date": s1_date.isoformat(),
            "tier": s1_tier,
            "processing_level": source_levels[S1_PLAN_NAME],
        }
        _tick(name)

    center_dt = s1["acquisition_datetime"]

    # --- MODIS / GPM --------------------------------------------------------
    # Sumber yang tidak ada di source_levels dilewati sama sekali: dia tidak
    # menghasilkan group HDF5, bukan group berisi NaN (lihat
    # fusion_layers_for untuk alasannya).
    for source, filename_fn in (
        (MODIS_PLAN_NAME, modis_band_filename),
        (GPM_PLAN_NAME, gpm_band_filename),
    ):
        if source not in source_levels:
            continue
        group = GROUP_BY_SOURCE[source]
        level = source_levels[source]
        tier = source_tiers[source]
        for layer, hit in _aux_layers_for_run(
            dataset_id, dataset_name, source, level, center_dt, filename_fn
        ):
            name = f"{group}/{layer.name}"
            if hit:
                values = _reproject_to_grid(
                    hit[0], ref_transform, ref_crs, ref_shape,
                    resampling=layer.resampling, fill_value=np.nan,
                )
                if layer.categorical:
                    # Kelas banjir itu kategorikal: NaN tidak muat di uint8,
                    # jadi pakai sentinel 255 yang sama dengan module7.
                    values = np.where(
                        np.isnan(values), MODIS_NODATA_U8, values
                    ).astype("uint8")
                layers[name] = values
                layer_sources[name] = {
                    "path": str(hit[0]), "date": hit[1].isoformat(),
                    "tier": tier, "processing_level": level,
                }
                found_dates.append(hit[1])
                offset = (hit[1] - s1_date).days
                if offsets[source] is None or abs(offset) < abs(offsets[source]):
                    offsets[source] = offset
            else:
                logger.warning(
                    "[M9] no %s %s %s product within %dh of %s",
                    source, layer.name, tier, ALIGNMENT_WINDOW_HOURS, center_dt,
                )
                layers[name] = (
                    np.full(ref_shape, MODIS_NODATA_U8, dtype="uint8")
                    if layer.categorical
                    else np.full(ref_shape, np.nan, dtype="float32")
                )
                layer_sources[name] = {
                    "path": None, "date": None,
                    "tier": tier, "processing_level": level,
                }
            _tick(name)

    date_key = s1_date.strftime("%Y%m%d")
    out_dir = fm.ensure_fusion_dir(dataset_id, dataset_name, date_key)
    h5_path = out_dir / fusion_h5_name(date_key, run_level)
    json_path = out_dir / fusion_metadata_name(run_level)
    processing_dt = datetime.now(tz=timezone.utc)

    _write_fusion_h5(
        h5_path, layers, ref_shape,
        acquisition_datetime=center_dt, processing_datetime=processing_dt,
        aoi_bbox=aoi_bbox, processing_level=run_level, source_levels=source_levels,
        fusion_strategy=fusion_strategy, crs=ref_crs, transform=ref_transform,
    )

    # nasa_scenes hanya didaftarkan untuk lapisan penanda tiap sumber (FLOOD
    # untuk MODIS, curah hujan harian untuk GPM) — satu baris scene per sumber
    # per tanggal, bukan per lapisan.
    def _register_nasa_scene(
        source: str, nasa_source: str, tile_id: str, short_name: str
    ) -> int | None:
        if source not in source_levels:
            return None
        anchors = _AUX_LAYERS_BY_SOURCE[source][source_levels[source]]
        if not anchors:
            return None
        hit = layer_sources.get(f"{GROUP_BY_SOURCE[source]}/{anchors[0].name}", {})
        if not hit.get("date"):
            return None
        return _get_or_create_nasa_scene(
            db, nasa_source, tile_id, short_name,
            date_type.fromisoformat(hit["date"]), s1["region_id"], Path(hit["path"]),
        )

    modis_scene_id = _register_nasa_scene(
        MODIS_PLAN_NAME, MODIS_SOURCE, MODIS_TILE_ID, MODIS_PRODUCT_SHORT_NAME
    )
    gpm_scene_id = _register_nasa_scene(
        GPM_PLAN_NAME, GPM_SOURCE, GPM_TILE_ID, GPM_PRODUCT_SHORT_NAME
    )

    days_since_s1 = max((abs((d - s1_date).days) for d in found_dates), default=0)

    with db.session() as sess:
        existing = sess.scalar(
            select(FusionProduct).where(
                FusionProduct.feature_date == s1_date,
                FusionProduct.region_id == s1["region_id"],
                FusionProduct.processing_level == run_level,
            )
        )
        if existing:
            existing.s1_scene_id = s1["scene_id"]
            existing.modis_scene_id = modis_scene_id
            existing.gpm_scene_id = gpm_scene_id
            existing.days_since_s1 = days_since_s1
            existing.feature_stack_path = str(h5_path)
            existing.fusion_strategy = fusion_strategy
            existing.temporal_offset_modis = offsets[MODIS_PLAN_NAME]
            existing.temporal_offset_gpm = offsets[GPM_PLAN_NAME]
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
                fusion_strategy=fusion_strategy,
                processing_level=run_level,
                temporal_offset_modis=offsets[MODIS_PLAN_NAME],
                temporal_offset_gpm=offsets[GPM_PLAN_NAME],
            )
            sess.add(fusion)
            sess.flush()
            fusion_id = fusion.fusion_id

    height, width = ref_shape
    checksum = lineage.compute_sha256(h5_path)
    _write_fusion_metadata_json(
        json_path, fusion_id, dataset_id, s1["region_id"], s1_date, s1,
        layer_sources, days_since_s1,
        center_dt, processing_dt, aoi_bbox, h5_path, height, width,
        processing_level=run_level,
        source_levels=source_levels,
        source_tiers=source_tiers,
        fusion_strategy=fusion_strategy,
        temporal_offsets={
            "modis": offsets[MODIS_PLAN_NAME], "gpm": offsets[GPM_PLAN_NAME],
        },
        checksum_sha256=checksum,
    )

    fusion_job_id = meta.insert_processing_job(
        s1["scene_id"], "FUSION",
        parameters={
            "dataset_id": dataset_id, "s1_date": s1_date.isoformat(),
            "processing_level": run_level, "source_levels": source_levels,
        },
    )
    meta.start_job(fusion_job_id)
    fusion_product_id = meta.insert_data_product(
        scene_id=s1["scene_id"], job_id=fusion_job_id, dataset_id=dataset_id,
        product_tier="FUSION", source=fm.FUSION_DB_SOURCE,
        product_type="FUSION_H5",
        # band_name membawa level-nya, bukan cuma "FUSION": dedup is_latest di
        # insert_data_product berjalan atas (scene_id, band_name, tier,
        # dataset_id), jadi dua stack tanggal yang sama dengan band_name yang
        # sama akan membuat stack RAW menandai dirinya sendiri usang begitu
        # stack PROCESSED didaftarkan — dan hilang dari semua listing API.
        band_name=f"FUSION_{run_level}",
        file_path=str(h5_path), file_name=h5_path.name,
        file_size_mb=round(h5_path.stat().st_size / (1024 ** 2), 3),
        data_hash_sha256=checksum,
        file_format="HDF5", rows=height, cols=width,
        processing_level=run_level,
    )
    for product_key in ("vv_product_id", "vh_product_id"):
        if s1[product_key]:
            lineage.record_transformation(
                s1[product_key], fusion_product_id, "FUSION", fusion_job_id,
                {"aoi_bbox": list(aoi_bbox), "processing_level": run_level},
            )
    meta.complete_job(fusion_job_id)

    logger.info(
        "[M9] fusion_id=%d dataset=%s date=%s level=%s sources=%s path=%s hash=%s",
        fusion_id, dataset_id, s1_date.isoformat(), run_level, source_levels,
        h5_path, checksum[:12],
    )

    return FusionRun(
        fusion_id=fusion_id,
        processing_level=run_level,
        h5_path=h5_path,
        json_path=json_path,
        layers=tuple(layer_sources),
        source_levels=source_levels,
        product_id=fusion_product_id,
        checksum_sha256=checksum,
    )


def create_fusion_stack(
    dataset_id: int,
    dataset_name: str,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    scene_id: int,
    db: DatabaseClient | None = None,
    progress_cb: Callable[[str, int, int], None] | None = None,
    plan: ProcessingPlan | None = None,
    fusion_strategy: str | None = None,
) -> list[FusionRun]:
    """
    Bangun stack fitur HDF5 untuk scene Sentinel-1 `scene_id` (akuisisi
    `s1_date`), mencocokkan produk MODIS/GPM dalam 24 jam dari waktu akuisisi
    S1. Ini deliverable tier FUSION.

    Isi tiap stack ditentukan `dataset_source_config`, bukan konstanta: sumber
    yang tidak dikonfigurasi tidak menghasilkan group HDF5 sama sekali, dan
    sumber yang cuma diminta RAW hanya menyumbang lapisan mentahnya
    (/modis/FLOOD, /gpm/rainfall_daily) yang dibaca dari BRONZE.

    Jumlah stack per tanggal = len(plan.output_levels()): satu untuk dataset
    biasa, DUA (satu RAW + satu PROCESSED) kalau ada sumber yang diminta di
    kedua level.

    Menulis, untuk tiap level:
        data/datasets/{id}_{slug}/{date}/fusion/fusion_{date}_{level}.h5
        data/datasets/{id}_{slug}/{date}/fusion/fusion_metadata_{level}.json

    Args:
        plan: ProcessingPlan dataset. Boleh None; kalau begitu dibaca dari
              dataset_source_config.
        fusion_strategy: strategi yang dipakai, dicatat apa adanya ke
              fusion_products.fusion_strategy dan ke sidecar JSON.

    Returns:
        list[FusionRun] — satu entri per stack yang ditulis, urut RAW lalu
        PROCESSED. (Sebelum model per-satelit fungsi ini mengembalikan satu
        `fusion_id` int; sekarang selalu list karena satu panggilan bisa
        menghasilkan dua stack.)

    Raises:
        RuntimeError: SENTINEL1 tidak dikonfigurasi, atau produk S1 di tier
        yang diminta tidak ada. Fusi di pipeline ini di-anchor ke scene S1 —
        lihat DOCS/IMPLEMENTATION_NOTES.md.
    """
    owns_db = db is None
    db = db or DatabaseClient.from_env()

    try:
        plan = plan or load_processing_plan(db, dataset_id)
        if not plan.is_configured(S1_PLAN_NAME):
            raise RuntimeError(
                f"FUSION dataset={dataset_id} diminta tanpa SENTINEL1 di "
                "dataset_source_config. Fusi di pipeline ini di-anchor ke grid "
                "dan tanggal akuisisi scene S1; tanpa S1 tidak ada grid "
                "referensi maupun tanggal untuk dipasangkan "
                "(DOCS/IMPLEMENTATION_NOTES.md)."
            )

        return [
            _build_fusion_stack_for_level(
                db,
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                s1_date=s1_date,
                aoi_bbox=aoi_bbox,
                scene_id=scene_id,
                plan=plan,
                run_level=run_level,
                fusion_strategy=fusion_strategy,
                progress_cb=progress_cb,
            )
            for run_level in plan.output_levels()
        ]

    finally:
        if owns_db:
            db.dispose()
