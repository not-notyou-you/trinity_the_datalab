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
    tanggal fitur (hari itu, atau sehari sebelumnya; tidak pernah sesudahnya
    -- lihat _find_aux_daily_file), direproject ke grid S1, dan
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
from typing import Callable, Sequence

import h5py
import numpy as np
import rasterio

from etl import WARP_THREADS
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.warp import reproject
from shapely.geometry import box
from sqlalchemy import select, update

from etl import folder_manager as fm
from etl import module4_gold_export as m4
from etl.database_client import (
    DatabaseClient,
    DataProduct,
    Dataset,
    FusionProduct,
    JobStatusEnum,
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
from etl.fusion_strategies import FILENAME_SUFFIX, SUBFOLDER
from etl.processing_plan import (
    PROCESSED,
    RAW,
    ProcessingPlan,
    SourcePlan,
    load_processing_plan,
)

from etl import tier_names as tn

logger = logging.getLogger(__name__)

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
    # Band di berkas sumber. Band 2 berkas MODIS adalah kualitas band 1
    # (FLOOD: asal piksel; NDVI/NDWI: umur observasi), lihat module7.
    band: int = 1


# Lapisan aux per level. Level RAW hanya memuat artefak mentah sumbernya;
# turunannya (NDVI/NDWI, akumulasi 72h/7d) tidak pernah dihitung di jalur RAW
# jadi tidak ada berkasnya untuk dimasukkan (DOCS/DESIGN.md, tabel RAW vs
# PROCESSED per satelit).
_FLOOD = _AuxLayer("FLOOD", "FLOOD", Resampling.nearest, categorical=True)
# Asal tiap piksel FLOOD (1 = komposit 2 hari, 2 = pengisi 1 hari CS). Ikut di
# kedua level: tanpa lapisan ini label dari dua produk dengan tingkat false
# positive berbeda tidak bisa dipisahkan lagi setelah digabung.
_FLOOD_SOURCE = _AuxLayer(
    "FLOOD_SOURCE", "FLOOD", Resampling.nearest, categorical=True, band=2
)

MODIS_FUSION_LAYERS: dict[str, tuple[_AuxLayer, ...]] = {
    RAW: (_FLOOD, _FLOOD_SOURCE),
    PROCESSED: (
        _FLOOD,
        _FLOOD_SOURCE,
        _AuxLayer("NDVI", "NDVI", Resampling.bilinear),
        # Umur observasi (hari) tiap piksel NDVI/NDWI komposit lookback.
        _AuxLayer("NDVI_AGE_DAYS", "NDVI", Resampling.nearest, band=2),
        _AuxLayer("NDWI", "NDWI", Resampling.bilinear),
        _AuxLayer("NDWI_AGE_DAYS", "NDWI", Resampling.nearest, band=2),
    ),
}

# GPM nearest, bukan bilinear: berkasnya sudah berupa blok sel IMERG 0.1
# derajat (module8), dan bilinear ke grid S1 akan menghaluskan lagi batas sel
# menjadi gradien yang tidak ada di data sumber.
GPM_FUSION_LAYERS: dict[str, tuple[_AuxLayer, ...]] = {
    RAW: (_AuxLayer("rainfall_daily", "24h", Resampling.nearest),),
    PROCESSED: (
        _AuxLayer("rainfall_24h", "24h", Resampling.nearest),
        _AuxLayer("rainfall_72h", "72h", Resampling.nearest),
        _AuxLayer("rainfall_7d", "7d", Resampling.nearest),
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
    db: DatabaseClient, dataset_id: int, scene_id: int, tier: str = tn.COG
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


def _s1_member_paths(
    db: DatabaseClient, dataset_id: int, scene_ids: Sequence[int], tier: str
) -> dict[str, list[str]]:
    """{band: [path raster tiap frame]} untuk frame-frame penyusun mosaik.

    Mosaiknya sendiri artefak antara di _work/ dan disapu di akhir job, jadi
    yang dicatat sebagai sumber adalah raster persisten per frame -- itulah
    yang bisa dibuka ulang oleh siapa pun yang menelusuri stack ini."""
    out: dict[str, list[str]] = {band: [] for band in S1_FUSION_BANDS}
    for member_id in scene_ids:
        found = _find_s1_products(db, dataset_id, member_id, tier=tier)
        if not found:
            continue
        for band in S1_FUSION_BANDS:
            path = found.get(f"{band.lower()}_path")
            if path:
                out[band].append(path)
    return out


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


# Urutan tanggal aux yang dicoba untuk satu tanggal fitur D: hari itu sendiri,
# lalu sehari sebelumnya. Tidak pernah D+1 -- lihat _find_aux_daily_file.
AUX_DAY_OFFSETS: tuple[int, ...] = (0, -1)


def _find_aux_daily_file(
    dataset_id: int,
    dataset_name: str,
    source: str,
    filename_fn,
    feature_date: date_type,
    tier: str = "cog",
) -> tuple[Path, date_type] | None:
    """Berkas MODIS/GPM harian untuk tanggal fitur `feature_date`, sebagai
    (path, tanggal berkas), atau None kalau tidak ada di disk.

    Yang dicoba hanya D lalu D-1 (AUX_DAY_OFFSETS), tidak pernah D+1.

    Dulu dipilih "tengah malam terdekat dari waktu akuisisi S1" dalam jendela
    24 jam. Untuk pass descending Jabodetabek (~22:25 UTC) itu hampir selalu
    jatuh ke HARI BERIKUTNYA: 24_try8 menulis fusion_20250114 yang isinya GPM
    dan MODIS 15 Jan. Granule IMERG harian mencakup 00:00-24:00 UTC, jadi
    hujan "24h" di stack itu seluruhnya turun 1,6-25,6 jam SETELAH citra
    diambil -- kebocoran informasi masa depan ke fitur prediktor, dan berkas
    yang namanya tidak cocok dengan isinya. Hasilnya juga tidak deterministik:
    D+1 hanya dipakai kalau kebetulan sudah diunduh (HYBRID/FULL_COVERAGE
    mengunduh harian, CO_OCCURRENCE tidak), jadi tanggal S1 yang sama bisa
    menghasilkan stack berbeda tergantung strategi dan rentang tanggal job.

    Referensinya tanggal FITUR, bukan waktu akuisisi scene: pada hari yang
    meminjam S1 dari tanggal lain (FULL_COVERAGE), MODIS/GPM tetap harus milik
    hari itu sendiri. Mencarinya dari waktu akuisisi scene pinjaman membuat
    lapisan aux hari D berisi data hari jangkarnya.

    D-1 hanya cadangan kalau berkas D gagal/belum terbit; offset yang terpakai
    dicatat di fusion_products.temporal_offset_* dan atribut lapisan HDF5.
    Berkas di laci {source}/{RAW|PROCESSED}/ (tier "cog" untuk PROCESSED,
    "aligned" untuk RAW) -- nama berkasnya sama di kedua laci."""
    for offset in AUX_DAY_OFFSETS:
        candidate = feature_date + timedelta(days=offset)
        tier_dir = fm.get_scene_dir(
            dataset_id, dataset_name, tier.lower(), source,
            candidate.strftime("%Y%m%d"),
        )
        path = tier_dir / filename_fn(candidate)
        if path.exists():
            return path, candidate
    return None


# Satuan per lapisan, ditulis sebagai atribut `units`. S1 sengaja disebut
# "linear": stack menyimpan sigma0 apa adanya (bukan dB), dan konsumen yang
# mengira dB akan menormalisasi nilai 0,002..7000 dengan cara yang keliru.
LAYER_UNITS: dict[str, str] = {
    "sentinel1/VV": "sigma0 linear (bukan dB)",
    "sentinel1/VH": "sigma0 linear (bukan dB)",
    "modis/FLOOD": "kelas MCDWD: 0 tanpa air, 1 air permanen, 2 banjir berulang, 3 banjir, 255 data tidak cukup",
    "modis/NDVI": "indeks tanpa satuan [-1, 1]",
    "modis/NDWI": "indeks tanpa satuan [-1, 1]",
    "modis/FLOOD_SOURCE": "asal piksel FLOOD: 1 komposit 2 hari, 2 komposit 1 hari CS (pengisi celah), 255 tanpa data",
    "modis/NDVI_AGE_DAYS": "hari (umur observasi NDVI terhadap tanggal fitur)",
    "modis/NDWI_AGE_DAYS": "hari (umur observasi NDWI terhadap tanggal fitur)",
    "gpm/rainfall_daily": "mm",
    "gpm/rainfall_24h": "mm",
    "gpm/rainfall_72h": "mm",
    "gpm/rainfall_7d": "mm",
}

# Tag GeoTIFF bawaan GDAL yang tidak membawa informasi asal-usul.
_IGNORED_SOURCE_TAGS = {"AREA_OR_POINT"}


def _source_tags(path: Path) -> dict[str, str]:
    """Tag asal-usul berkas sumber (ditulis module7/module8), dengan kunci
    huruf kecil supaya seragam dengan atribut HDF5 lainnya."""
    try:
        with rasterio.open(path) as src:
            tags = src.tags()
    except Exception:  # pragma: no cover - berkas sumber rusak sudah gagal lebih awal
        return {}
    return {
        key.lower(): value for key, value in tags.items()
        if key not in _IGNORED_SOURCE_TAGS
    }


def _hours_after_acquisition(window_end_utc: str | None, acquisition: datetime | None) -> float | None:
    """Berapa jam ujung jendela hujan melewati waktu akuisisi S1.

    Positif berarti sebagian hujan di lapisan itu turun SETELAH citra diambil.
    Granule IMERG harian tidak bisa dipotong di tengah hari, jadi nilai ini
    tidak bisa nol untuk semua scene -- tapi harus terlihat, supaya konsumen
    bisa memilih window yang aman atau menyaring sampelnya."""
    if not window_end_utc or acquisition is None:
        return None
    try:
        end = datetime.fromisoformat(window_end_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    return round((end - _as_utc(acquisition)).total_seconds() / 3600.0, 2)


class _MissingBand(RuntimeError):
    """Berkas sumber ada, tapi tidak punya band yang diminta lapisan ini."""


def _reproject_to_grid(
    src_path: Path,
    ref_transform,
    ref_crs,
    ref_shape: tuple[int, int],
    resampling: Resampling,
    fill_value: float,
    band: int = 1,
) -> np.ndarray:
    """Reproject band `band` of a raster onto the S1 reference grid, filling
    pixels outside the source extent with `fill_value`."""
    height, width = ref_shape
    dest = np.full((height, width), fill_value, dtype=np.float32)
    with rasterio.open(src_path) as src:
        if band > src.count:
            # Berkas dari versi sebelum band kualitas ditambahkan.
            raise _MissingBand(f"{src_path.name} cuma punya {src.count} band, butuh band {band}")
        reproject(
            source=rasterio.band(src, band),
            destination=dest,
            src_transform=src.transform,
            src_crs=src.crs,
            src_nodata=src.nodata,
            dst_transform=ref_transform,
            dst_crs=ref_crs,
            dst_nodata=fill_value,
            resampling=resampling,
            num_threads=WARP_THREADS,
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


# Di bawah ini sebuah lapisan dianggap nyaris kosong dan dicatat sebagai
# peringatan. 5% dipilih karena itu urutan besaran scene Sentinel-1 yang cuma
# menyerempet AOI (22_try6: tiga dari lima tanggal berisi 3,6-3,7% piksel
# valid) -- stack-nya tetap ditulis, tapi tidak boleh lolos tanpa jejak.
LOW_COVERAGE_FRACTION = 0.05


def _valid_fraction(array: "np.ndarray") -> float:
    """Fraksi piksel yang membawa nilai, bukan nodata.

    Dihitung di sini, bukan dibaca ulang dari HDF5 nanti: arraynya sudah ada
    di memori, dan membaca ulang 79 juta piksel cuma untuk menghitung ini
    berarti membayar dua kali."""
    if array.dtype == np.uint8:
        return float((array != MODIS_NODATA_U8).mean())
    return float(np.isfinite(array).mean())


class _FusionH5Layers:
    """Penulis stack HDF5: satu lapisan ditulis begitu selesai dihitung.

    Dulu seluruh lapisan dikumpulkan di satu dict lalu ditulis sekaligus. Itu
    aman selama grid referensinya sekecil jejak satu scene, tapi sejak stack
    dirakit di grid AOI penuh (lihat _aoi_reference_grid) satu lapisan bisa
    ratusan MB dan delapan lapisan sekaligus berarti gigabyte di memori -- untuk
    data yang sebagian besar NaN. Ditulis satu per satu, yang hidup di memori
    cuma lapisan yang sedang dihitung; gzip membuat daerah NaN hampir tidak
    memakan tempat di disk.

    `add()` menyimpan nama tiap lapisan karena atribut root dan sidecar JSON
    menyebut daftarnya, dan `finalize()` menulis atribut root lalu menutup
    berkas. Pemanggil yang gagal di tengah jalan meninggalkan berkas setengah
    jadi -- sama seperti sebelumnya, dan tetap ditimpa penuh di percobaan
    berikutnya karena berkasnya dibuka dengan mode "w".
    """

    def __init__(self, h5_path: Path, ref_shape: tuple[int, int]) -> None:
        self.path = h5_path
        self.shape = ref_shape
        height, width = ref_shape
        self._chunks = (min(HDF5_CHUNK_MAX, height), min(HDF5_CHUNK_MAX, width))
        h5_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = h5py.File(h5_path, "w")
        self.names: list[str] = []

    def add(self, name: str, array: np.ndarray, attrs: dict | None = None) -> None:
        # shuffle: menata ulang byte float sebelum gzip, jadi byte eksponen
        # yang hampir selalu sama berkumpul dan terkompresi jauh lebih rapat.
        # Terukur pada lapisan NDVI dataset 23_try7 (4000x4000, 64 MB mentah):
        # gzip saja 8,7 MB, gzip+shuffle 5,5 MB. Filter standar HDF5, jadi
        # pembaca mana pun tetap bisa membukanya tanpa perlakuan khusus.
        ds = self._file.create_dataset(
            name, data=array, dtype=array.dtype, chunks=self._chunks,
            compression="gzip", shuffle=True,
        )
        ds.attrs["nodata"] = MODIS_NODATA_U8 if array.dtype == np.uint8 else "NaN"
        # Provenance per lapisan (tanggal sumber, offset, cakupan, satuan,
        # jendela waktu). Tanpa ini HDF5-nya tidak bisa menjelaskan dirinya:
        # di 24_try8 satu-satunya jejak tanggal sumber ada di sidecar JSON,
        # dan sidecar itu tertimpa oleh tanggal berikutnya.
        for key, value in (attrs or {}).items():
            if value is None:
                continue
            ds.attrs[key] = value
        self.names.append(name)

    def finalize(
        self,
        *,
        acquisition_datetime: datetime,
        processing_datetime: datetime,
        aoi_bbox: tuple[float, float, float, float],
        processing_level: str,
        source_levels: dict[str, str],
        fusion_strategy: str | None = None,
        crs: object | None = None,
        transform: object | None = None,
        extra_attrs: dict | None = None,
    ) -> None:
        """Tulis atribut root lalu tutup berkas.

        `crs`/`transform` adalah grid referensi yang semua lapisan sudah
        direproject ke sana. Keduanya wajib ikut ditulis: `aoi_bbox` adalah
        kotak AOI yang diminta, bukan batas raster hasilnya, jadi tanpa affine
        transform yang sebenarnya konsumen tidak bisa memetakan piksel ke
        koordinat bumi selain dengan menebak.

        `processing_level` dan `source_levels` ditulis sebagai atribut root
        supaya berkasnya bisa menjelaskan dirinya sendiri: dua stack tanggal
        yang sama dari dataset RAW+PROCESSED punya lapisan yang bisa bernama
        sama, dan tanpa atribut ini konsumen harus menebak dari nama berkas
        mana yang mana."""
        height, width = self.shape
        f = self._file
        f.attrs["acquisition_datetime"] = acquisition_datetime.isoformat()
        f.attrs["processing_datetime"] = processing_datetime.isoformat()
        f.attrs["aoi_bbox"] = list(aoi_bbox)
        f.attrs["layers"] = list(self.names)
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
        for key, value in (extra_attrs or {}).items():
            if value is not None:
                f.attrs[key] = value
        if crs is not None:
            # WKT + string CRS: WKT supaya tidak bergantung ke lookup EPSG di
            # sisi pembaca, string pendek ("EPSG:4326") untuk keterbacaan.
            f.attrs["crs"] = str(crs)
            try:
                f.attrs["crs_wkt"] = crs.to_wkt()
            except AttributeError:
                pass
        if transform is not None:
            # Urutan GDAL-style 6 elemen (a, b, c, d, e, f) -- sama dengan
            # rasterio.Affine, jadi bisa langsung Affine(*attrs["transform"]).
            f.attrs["transform"] = [float(v) for v in tuple(transform)[:6]]
            # Batas raster yang SEBENARNYA, dihitung dari transform + ukuran.
            # `aoi_bbox` adalah AOI yang diminta user. Sejak stack dirakit di
            # grid AOI keduanya sama, tapi tetap ditulis terpisah supaya
            # konsumen tidak perlu percaya bahwa keduanya selalu identik --
            # berkas dari versi sebelumnya memang tidak begitu, dan di sana
            # aoi_bbox menjanjikan AOI penuh untuk raster yang cuma menutup
            # 17% AOI.
            west, north = transform * (0, 0)
            east, south = transform * (width, height)
            f.attrs["grid_bbox"] = [
                float(west), float(south), float(east), float(north)
            ]
            # Posisi raster ini di dalam kotak AOI, dalam piksel. Sejak semua
            # tanggal dirakit di grid AOI yang sama, nilainya selalu (0, 0) --
            # ditulis apa adanya supaya konsumen bisa MEMERIKSA itu, bukan
            # mengandaikannya, dan supaya berkas lama yang tidak punya atribut
            # ini bisa dibedakan dari berkas baru yang punya.
            aoi_west, _, _, aoi_north = aoi_bbox
            res_x, res_y = abs(float(transform.a)), abs(float(transform.e))
            f.attrs["grid_offset_row_col"] = [
                int(round((aoi_north - north) / res_y)),
                int(round((west - aoi_west) / res_x)),
            ]
        f.close()

        logger.info(
            "[M9] Saving fusion H5 to FUSION tier: %s level=%s shape=(%d, %d) "
            "layers=%d %s",
            self.path, processing_level, height, width, len(self.names),
            sorted(self.names),
        )

    def close(self) -> None:
        try:
            self._file.close()
        except Exception:  # pragma: no cover - berkas sudah tertutup
            pass


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
        # Ringkasan cakupan, supaya konsumen bisa menyaring tanggal yang
        # datanya nyaris kosong tanpa membuka HDF5-nya dulu.
        "layer_coverage": {
            name: round(source.get("valid_fraction", 0.0), 4)
            for name, source in layer_sources.items()
        },
        # `mosaic_of` hanya ada kalau tanggal ini dirakit dari beberapa frame
        # Sentinel-1; satu frame tetap menulis s1_scene_id saja.
        "source_scenes": {
            "s1_scene_id": s1["scene_id"],
            **({"s1_mosaic_scene_ids": s1["mosaic_of"]} if s1.get("mosaic_of") else {}),
        },
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


def _empty_produced() -> dict[str, list[str]]:
    """Wadah {tier: [path]} untuk artefak aux yang ditulis satu pemanggilan.

    Memuat KEDUA nama tier rank 2 (INDICES untuk MODIS, ACCUMULATED untuk GPM)
    walau satu pemanggilan biasanya cuma mengisi salah satunya: fungsi
    agregat ensure_aux_inputs_for_date menggabungkan hasil kedua sumber ke
    satu dict, jadi kuncinya harus sudah ada untuk keduanya.
    """
    return {tn.ALIGNED: [], tn.INDICES: [], tn.ACCUMULATED: [], tn.COG: []}


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
    tier: str = tn.INDICES,
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
    # Job ini dulu cuma dibuat, tidak pernah dijalankan/ditutup: setiap
    # registrasi aux meninggalkan satu baris processing_jobs QUEUED selamanya
    # (puluhan per dataset FULL_COVERAGE/HYBRID).
    meta.start_job(job_id)
    product_ids: dict[str, int] = {}
    try:
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
    except Exception as exc:
        meta.complete_job(
            job_id, status=JobStatusEnum.FAILED,
            error_code=type(exc).__name__, error_message=str(exc)[:2000],
        )
        raise
    meta.complete_job(job_id)

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
    try:
        for band, path in gold_paths.items():
            if _product_exists(db, dataset_id, path):
                continue
            gold_product_id = meta.insert_data_product(
                scene_id=aux_scene_id, job_id=gold_job_id, dataset_id=dataset_id,
                product_tier=tn.COG, source=fm.db_source(source),
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
    except Exception as exc:
        meta.complete_job(
            gold_job_id, status=JobStatusEnum.FAILED,
            error_code=type(exc).__name__, error_message=str(exc)[:2000],
        )
        raise
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
    produced: dict[str, list[str]] = _empty_produced()
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
                if tn.rank(tier) != 2:
                    continue
                gold_paths = _promote_aux_to_gold(
                    db, dataset_id=dataset_id, dataset_name=dataset_name, source="modis",
                    date_key=date_key, aux_scene_id=aux_scene_id,
                    silver_paths={band: path}, silver_product_ids=product_ids,
                )
                produced[tn.COG].extend(gold_paths.values())

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
    produced: dict[str, list[str]] = _empty_produced()
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
        if tn.rank(tier) != 2:
            continue
        gold_paths = _promote_aux_to_gold(
            db, dataset_id=dataset_id, dataset_name=dataset_name, source="gpm",
            date_key=date_key, aux_scene_id=aux_scene_id,
            silver_paths=band_paths, silver_product_ids=product_ids,
        )
        produced[tn.COG].extend(gold_paths.values())

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
    produced: dict[str, list[str]] = _empty_produced()

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


def fusion_h5_name(
    date_key: str, processing_level: str, fusion_strategy: str | None = None
) -> str:
    """Nama berkas stack HDF5. Level SELALU ikut di nama, juga saat dataset
    cuma menghasilkan satu stack.

    Penamaan bersyarat ("fusion_{date}.h5" kalau satu, bersufiks kalau dua)
    akan memaksa setiap konsumen — API, notebook training, skrip pihak ketiga —
    menangani dua pola nama dan menebak mana yang berlaku dari konfigurasi
    dataset yang belum tentu dia punya. Satu pola untuk semua kasus lebih
    murah, dan level di nama berkas membuat isi folder fusion/ bisa dibaca
    tanpa membuka satu pun HDF5.

    Strategi ikut ke nama kalau diberikan. Subfolder sudah memisahkan ketiga
    strategi, tapi berkas yang diunduh dan dipindahkan ke tempat lain kehilangan
    konteks foldernya -- dan dua HDF5 dari strategi berbeda untuk tanggal yang
    sama tidak bisa dibedakan dari isinya sekilas."""
    strategy = str(fusion_strategy).strip().upper() if fusion_strategy else None
    suffix = FILENAME_SUFFIX.get(strategy) if strategy else None
    if suffix:
        return f"fusion_{date_key}_{suffix}_{processing_level.lower()}.h5"
    return f"fusion_{date_key}_{processing_level.lower()}.h5"


def fusion_metadata_name(
    date_key: str, processing_level: str, fusion_strategy: str | None = None
) -> str:
    """Sidecar JSON pendamping `fusion_h5_name`: nama yang sama, akhiran
    `_metadata.json`.

    Tanggal WAJIB ada di nama. Sejak folder fusion tidak lagi dipecah per
    tanggal, nama lama (`fusion_metadata_{level}.json`) dipakai bersama oleh
    semua tanggal satu dataset, dan setiap stack menimpa sidecar stack
    sebelumnya -- 24_try8 menyisakan satu sidecar (tanggal terakhir yang
    difusikan) untuk tiga HDF5."""
    stem = fusion_h5_name(date_key, processing_level, fusion_strategy).removesuffix(".h5")
    return f"{stem}_metadata.json"


def _aux_layers_for_run(
    dataset_id: int,
    dataset_name: str,
    source: str,
    level: str,
    feature_date: date_type,
    filename_fn: Callable[[str, str], str],
) -> list[tuple[_AuxLayer, tuple[Path, date_type] | None]]:
    """Pasangkan tiap lapisan aux yang diminta level ini dengan berkasnya di
    disk (None kalau tidak ada untuk D maupun D-1)."""
    tier = "cog" if level == PROCESSED else "aligned"
    out = []
    for layer in _AUX_LAYERS_BY_SOURCE[source][level]:
        hit = _find_aux_daily_file(
            dataset_id, dataset_name, GROUP_BY_SOURCE[source],
            lambda d, k=layer.file_key: filename_fn(k, d.strftime("%Y%m%d")),
            feature_date, tier=tier,
        )
        out.append((layer, hit))
    return out


def _aoi_reference_grid(
    aoi_bbox: tuple[float, float, float, float]
) -> tuple[object, object, tuple[int, int]]:
    """Grid referensi dari AOI saja, untuk tanggal fusi yang tidak punya
    scene Sentinel-1.

    FULL_COVERAGE merakit satu HDF5 per hari, termasuk hari tanpa S1 — dan S1
    yang biasanya jadi grid referensi (CRS + transform + shape) tidak ada di
    hari itu. Grid dibangun dengan rumus yang sama persis dipakai
    module8_gpm_download saat memproyeksikan IMERG ke "grid Sentinel-1":
    bbox AOI pada S1_RESOLUTION_DEG, EPSG:4326. Menyamakan rumusnya disengaja
    — kalau hari ber-S1 dan hari tanpa-S1 mendarat di grid yang berbeda,
    deret waktu hasil FULL_COVERAGE tidak bisa ditumpuk jadi array tunggal,
    yang justru satu-satunya alasan strategi itu ada.
    """
    from rasterio.transform import from_origin

    # Impor lokal: module8 mengimpor module9 untuk registrasi produk, jadi
    # impor tingkat-modul di sini akan membuat siklus.
    from etl.module8_gpm_download import S1_RESOLUTION_DEG

    min_lon, min_lat, max_lon, max_lat = aoi_bbox
    width = max(1, int(np.ceil(round((max_lon - min_lon) / S1_RESOLUTION_DEG, 6))))
    height = max(1, int(np.ceil(round((max_lat - min_lat) / S1_RESOLUTION_DEG, 6))))
    transform = from_origin(min_lon, max_lat, S1_RESOLUTION_DEG, S1_RESOLUTION_DEG)
    return transform, CRS.from_epsg(4326), (height, width)


def _dataset_fusion_grid(
    db: DatabaseClient,
    dataset_id: int,
    tier: str,
    aoi_bbox: tuple[float, float, float, float],
) -> tuple[object, object, tuple[int, int]]:
    """Grid tempat SEMUA stack dataset ini dirakit: kotak AOI penuh, pada
    resolusi raster Sentinel-1 dataset itu.

    Dua hal digabung di sini, dan dua-duanya disengaja:

    1. EKSTENNYA selalu AOI penuh, bukan jejak scene hari itu. Dulu hari
       ber-S1 memakai grid rasternya sendiri, jadi satu dataset sebulan bisa
       menghasilkan lima bentuk berbeda (22_try6: 8789x1488, 6067x8752,
       8790x1483, 6068x8752, 8790x1492) yang tidak satu pun pikselnya
       berhimpit -- deret waktunya tidak bisa ditumpuk jadi satu array, yang
       justru satu-satunya alasan tier FUSION ada. Sebagian bahkan cuma
       menutup 17% AOI sementara atribut `aoi_bbox`-nya menjanjikan AOI penuh.

    2. RESOLUSINYA ikut raster S1 dataset (lewat _dataset_s1_reference_grid),
       bukan konstanta S1_RESOLUTION_DEG. module1b mereproyeksi scene dengan
       calculate_default_transform, jadi resolusi aslinya mengikuti geometri
       scene; memaksakan konstanta akan me-resample setiap piksel S1 tanpa
       alasan. Konstanta cuma dipakai kalau dataset belum punya satu pun
       raster S1 (hari tanpa-S1 di awal FULL_COVERAGE).

    Harga ekstennya: hari yang scene-nya cuma menyerempet AOI sekarang menulis
    raster seukuran AOI penuh yang sebagian besar NaN. Itu murah di disk --
    gzip memampatkan blok NaN hampir habis -- dan lapisan ditulis satu per satu
    (_FusionH5Layers) supaya memorinya tidak ikut membesar.
    """
    from rasterio.transform import from_origin

    # Impor lokal: module8 mengimpor module9 untuk registrasi produk, jadi
    # impor tingkat-modul di sini akan membuat siklus.
    from etl.module8_gpm_download import S1_RESOLUTION_DEG

    # 3. GRIDNYA DIPAKU. Sekali sebuah dataset memilih grid, pilihan itu
    #    disimpan dan dibaca ulang -- tidak pernah diturunkan lagi. Dulu grid
    #    dihitung ulang tiap jalan dari "raster S1 pertama yang filenya ada",
    #    dan jawaban itu berubah begitu berkas hilang atau is_latest bergeser;
    #    karena tiap scene punya ukuran piksel sendiri, berpindah acuan berarti
    #    berpindah grid. Dataset 26 (JAWA, 14 bentuk raster S1) sampai punya
    #    dua stack yang tidak berhimpit: 32040x103630 dan 31922x103248.
    pinned = _load_pinned_grid(db, dataset_id)
    if pinned is not None:
        return pinned

    reference = _dataset_s1_reference_grid(db, dataset_id, tier)
    if reference is not None:
        ref_transform, ref_crs, _ = reference
        res_x, res_y = abs(float(ref_transform.a)), abs(float(ref_transform.e))
        origin = "s1_raster"
    else:
        ref_crs = CRS.from_epsg(4326)
        res_x = res_y = S1_RESOLUTION_DEG
        origin = "aoi_constant"

    min_lon, min_lat, max_lon, max_lat = aoi_bbox
    width = max(1, int(np.ceil(round((max_lon - min_lon) / res_x, 6))))
    height = max(1, int(np.ceil(round((max_lat - min_lat) / res_y, 6))))
    transform = from_origin(min_lon, max_lat, res_x, res_y)

    # Dipaku juga ketika asalnya konstanta AOI, bukan hanya ketika ada raster
    # S1. Dataset FULL_COVERAGE yang harinya dimulai sebelum scene S1 pertama
    # akan terkunci pada grid konstanta, dan S1 yang datang belakangan
    # di-resample tipis ke grid itu. Itu disengaja: grid yang bercabang
    # membuat deret waktunya tidak bisa ditumpuk sama sekali, sedangkan
    # resampling tipis cuma menggeser nilai sepersekian piksel.
    _pin_grid(db, dataset_id, transform, ref_crs, (height, width), origin)
    return transform, ref_crs, (height, width)


def _load_pinned_grid(
    db: DatabaseClient, dataset_id: int
) -> tuple[object, object, tuple[int, int]] | None:
    """Grid yang sudah dipaku untuk dataset ini, atau None kalau belum.

    db boleh None: pemanggil yang menguji rumus gridnya saja (tests/
    test_fusion_grid.py) menyuntik acuan lewat _dataset_s1_reference_grid dan
    tidak punya database. Tanpa database tidak ada yang bisa dipaku, jadi
    perilakunya jatuh ke perhitungan seperti sebelum grid dipaku.
    """
    from rasterio.transform import Affine

    if db is None:
        return None
    with db.session() as sess:
        raw = sess.scalar(
            select(Dataset.fusion_grid).where(Dataset.dataset_id == dataset_id)
        )
    if not raw:
        return None
    try:
        t = [float(v) for v in raw["transform"]]
        shape = (int(raw["height"]), int(raw["width"]))
        crs = CRS.from_string(str(raw["crs"]))
    except (KeyError, TypeError, ValueError):
        # Baris rusak jangan sampai menghentikan fusion: perlakukan seperti
        # belum dipaku, dan jalan ini akan memakunya ulang dengan bentuk benar.
        logger.warning(
            "[M9] fusion_grid dataset %s tidak terbaca, dipaku ulang", dataset_id
        )
        return None
    return Affine(t[0], t[1], t[2], t[3], t[4], t[5]), crs, shape


def audit_dataset_grids(
    db: DatabaseClient, dataset_id: int, dataset_name: str
) -> dict:
    """Periksa semua stack fusion dataset ini terhadap grid yang dipaku.

    Memaku grid mencegah PERCABANGAN BARU, tapi tidak membuat percabangan yang
    sudah ada jadi kelihatan -- dan diam-diam itulah yang paling berbahaya.
    Dataset 26 (JAWA) berjalan berbulan-bulan dengan dua stack yang tidak
    berhimpit tanpa satu pun peringatan; yang menemukannya cuma kecurigaan
    manual atas ukuran berkas yang ganjil.

    Fungsi ini mengubahnya jadi terlihat: satu panggilan menjawab "apakah
    seluruh stack dataset ini benar-benar bisa ditumpuk?". Dipanggil setiap
    kali fusion menulis stack (murah: cuma baca atribut HDF5, bukan datanya),
    dan bisa dijalankan sendiri untuk mengaudit dataset lama.

    Mengembalikan {"pinned": ..., "matching": [...], "mismatched": [...]}.
    """
    pinned = _load_pinned_grid(db, dataset_id)
    result: dict = {"pinned": None, "matching": [], "mismatched": []}
    if pinned is None:
        return result
    _transform, _crs, shape = pinned
    result["pinned"] = {"height": shape[0], "width": shape[1]}

    root = fm.get_dataset_root(dataset_id, dataset_name)
    if not root.exists():
        return result

    for path in sorted(root.rglob("*.h5")):
        try:
            with h5py.File(path, "r") as h:
                got = (int(h.attrs["height"]), int(h.attrs["width"]))
        except (OSError, KeyError):
            # Berkas rusak atau bukan stack fusion: bukan urusan audit grid.
            continue
        entry = {"file": path.name, "height": got[0], "width": got[1]}
        if got == shape:
            result["matching"].append(entry)
        else:
            result["mismatched"].append(entry)
    return result


def _warn_on_grid_drift(
    db: DatabaseClient, dataset_id: int, dataset_name: str
) -> None:
    """Teriakkan ke log kalau ada stack yang tidak berhimpit dengan grid dataset.

    Sengaja hanya memperingatkan, tidak menggagalkan: stack yang sudah
    terlanjur lahir di grid lain adalah fakta sejarah, dan menjatuhkan job
    karenanya justru membuat dataset yang sedang berjalan tidak bisa
    dilanjutkan sama sekali. Yang dibutuhkan operator adalah TAHU, lalu
    memutuskan sendiri stack mana yang dirakit ulang.
    """
    try:
        audit = audit_dataset_grids(db, dataset_id, dataset_name)
    except Exception as exc:  # noqa: BLE001
        logger.debug("[M9] audit grid dilewati: %s", exc)
        return
    bad = audit.get("mismatched") or []
    if not bad:
        return
    pinned = audit["pinned"]
    logger.warning(
        "[M9] dataset %s punya %d stack di grid BERBEDA dari grid dataset "
        "(%dx%d) -- deret waktunya tidak bisa ditumpuk sampai dirakit ulang: %s",
        dataset_id, len(bad), pinned["height"], pinned["width"],
        ", ".join(f"{b['file']} ({b['height']}x{b['width']})" for b in bad[:5]),
    )


def _pin_grid(
    db: DatabaseClient,
    dataset_id: int,
    transform: object,
    crs: object,
    shape: tuple[int, int],
    origin: str,
) -> None:
    """Simpan grid dataset supaya jalan berikutnya memakai yang sama persis.

    Tanpa database (db None) tidak ada yang dipaku dan itu bukan galat: lihat
    alasannya di _load_pinned_grid.
    """
    if db is None:
        return

    payload = {
        "transform": [
            float(transform.a), float(transform.b), float(transform.c),
            float(transform.d), float(transform.e), float(transform.f),
        ],
        "height": int(shape[0]),
        "width": int(shape[1]),
        "crs": str(crs),
        "origin": origin,
        "pinned_at": datetime.now(timezone.utc).isoformat(),
    }
    with db.session() as sess:
        sess.execute(
            update(Dataset)
            .where(Dataset.dataset_id == dataset_id, Dataset.fusion_grid.is_(None))
            .values(fusion_grid=payload)
        )
        sess.commit()
    logger.info(
        "[M9] grid dataset %s dipaku: %dx%d dari %s",
        dataset_id, shape[0], shape[1], origin,
    )


def _dataset_s1_reference_grid(
    db: DatabaseClient, dataset_id: int, tier: str
) -> tuple[object, object, tuple[int, int]] | None:
    """Grid raster S1 mana pun milik dataset ini di `tier`, atau None.

    Dipakai hari tanpa-S1 sebelum jatuh ke _aoi_reference_grid. Rumus AOI itu
    TIDAK sama dengan grid S1 yang sebenarnya: module1b mereproyeksi scene
    dengan calculate_default_transform, jadi resolusinya mengikuti geometri
    scene, bukan S1_RESOLUTION_DEG. Terukur di dataset try2 (Tangerang):
    hari ber-S1 571x468, hari tanpa-S1 578x473 -- deret FULL_COVERAGE jadi
    tidak bisa ditumpuk. Selama dataset punya satu saja raster S1, semua
    harinya harus mendarat di grid raster itu."""
    tier_enum = ProductTierEnum[str(tier).upper()]
    with db.session() as sess:
        paths = sess.scalars(
            select(DataProduct.file_path).where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.product_tier == tier_enum,
                DataProduct.source == "SENTINEL1",
                DataProduct.is_latest == True,
                DataProduct.is_valid == True,
            ).order_by(DataProduct.product_id)
        ).all()
    for path in paths:
        if not path or not Path(path).exists():
            continue
        with rasterio.open(path) as ref:
            return ref.transform, ref.crs, (ref.height, ref.width)
    return None


def _build_fusion_stack_for_level(
    db: DatabaseClient,
    *,
    dataset_id: int,
    dataset_name: str,
    s1_date: date_type,
    aoi_bbox: tuple[float, float, float, float],
    scene_id: int | None,
    plan: ProcessingPlan,
    run_level: str,
    fusion_strategy: str | None,
    progress_cb: Callable[[str, int, int], None] | None,
    region_id: int | None = None,
    require_s1: bool = True,
    s1_offset_days: int | None = 0,
    s1_files: dict[str, str] | None = None,
    s1_member_scene_ids: tuple[int, ...] = (),
) -> FusionRun:
    """Bangun SATU stack HDF5 untuk `run_level`. Dipanggil sekali atau dua
    kali per tanggal fusi oleh create_fusion_stack.

    `require_s1=False` (dipakai FULL_COVERAGE) membuat tanggal tanpa scene S1
    tetap menghasilkan berkas: group sentinel1/ ada tapi berisi NaN, persis
    perlakuan yang sudah berlaku untuk MODIS/GPM yang hilang. Itu penting
    karena fusion_layers_for membedakan "sensor tidak diminta" (tidak ada
    group sama sekali) dari "diminta tapi datanya hilang hari itu" (group
    berisi NaN) — dan S1 di dataset FULL_COVERAGE jelas termasuk yang kedua."""
    from etl.module7_modis_download import band_filename as modis_band_filename
    from etl.module8_gpm_download import band_filename as gpm_band_filename

    lineage = LineageTracker(db)
    meta = MetadataManager(db)

    source_levels = plan.source_levels_for_run(run_level)
    source_tiers = {
        name: plan.sources[name].tier_for_run(run_level) for name in source_levels
    }
    s1_tier = source_tiers[S1_PLAN_NAME]

    s1 = (
        _find_s1_products(db, dataset_id, scene_id, tier=s1_tier)
        if scene_id is not None else None
    )
    if s1 is not None and s1_files:
        # Raster hasil mosaik menggantikan raster frame tunggal, tapi baris
        # DB-nya tidak: scene_id, region, dan waktu akuisisi tetap milik scene
        # utama, dan product_id frame-frame penyumbang dicatat lewat
        # s1_member_scene_ids di bagian lineage. Mosaik sendiri artefak antara
        # di _work/ (etl/s1_mosaic.py), jadi dia memang tidak punya baris
        # data_products untuk ditunjuk.
        s1 = {
            **s1,
            "vv_path": s1_files.get("VV") or s1["vv_path"],
            "vh_path": s1_files.get("VH") or s1["vh_path"],
            # Hanya diisi kalau tanggal ini BENAR-BENAR dirakit dari lebih
            # dari satu frame. Satu frame yang dilaporkan sebagai "mosaik dari
            # satu scene" cuma membuat pembaca sidecar mengira ada penggabungan
            # yang tidak pernah terjadi.
            **(
                {"mosaic_of": sorted(members)}
                if len(members := {*s1_member_scene_ids, s1["scene_id"]}) > 1
                else {}
            ),
        }
        if s1.get("mosaic_of"):
            s1["member_paths"] = _s1_member_paths(
                db, dataset_id, s1["mosaic_of"], s1_tier
            )
    if s1 is None or (not s1["vv_path"] and not s1["vh_path"]):
        if require_s1:
            raise RuntimeError(
                f"No S1 {s1_tier} product found for scene_id={scene_id} "
                f"s1_date={s1_date.isoformat()} (run level {run_level})"
            )
        # Rekaman S1 kosong, bukan cabang kode terpisah: perakitan lapisan di
        # bawah sudah menangani path None (diisi NaN seukuran grid referensi),
        # jadi membentuk rekaman null di sini jauh lebih sedikit permukaan
        # error daripada menduplikasi alur perakitan untuk kasus tanpa-S1.
        if region_id is None:
            raise RuntimeError(
                "require_s1=False butuh region_id untuk membuat scene "
                "placeholder tempat data_products fusi ditempelkan"
            )
        logger.info(
            "[M9] tanggal=%s tanpa scene S1 (strategi=%s): stack ditulis dengan "
            "group sentinel1/ berisi NaN di atas grid AOI",
            s1_date.isoformat(), fusion_strategy,
        )
        # Kunci rekaman ini HARUS sama dengan hasil _find_s1_products: kode di
        # bawah membaca vv_product_id/vh_product_id untuk lineage. Tanpa kedua
        # kunci itu setiap hari tanpa-S1 jatuh KeyError SETELAH HDF5 dan baris
        # fusion_products ditulis -- job FUSION-nya tertinggal RUNNING dan
        # data_products-nya tanpa lineage (dataset try2, 11 dari 16 hari).
        s1 = {
            "vv_path": None,
            "vh_path": None,
            "vv_product_id": None,
            "vh_product_id": None,
            "region_id": region_id,
            "scene_id": _resolve_aux_scene(
                db, dataset_id, region_id, box(*aoi_bbox).wkt, "FUSION", s1_date
            ),
            "acquisition_datetime": datetime.combine(
                s1_date, datetime.min.time(), tzinfo=timezone.utc
            ),
        }
        # Tanpa scene S1 sama sekali, "jarak hari" tidak terdefinisi. NULL,
        # bukan 0: nol berarti same-day, dan itu klaim yang tidak benar di sini.
        s1_offset_days = None

    # Grid referensi SELALU grid AOI, tidak lagi grid scene S1 hari itu.
    #
    # Alasannya justru yang sudah ditulis di _aoi_reference_grid: kalau hari
    # yang satu mendarat di grid berbeda dari hari lainnya, deret waktunya
    # tidak bisa ditumpuk jadi satu array -- dan itu inti tier FUSION. Dengan
    # grid scene, satu dataset sebulan bisa menghasilkan lima bentuk berbeda
    # (22_try6: 8789x1488, 6067x8752, 8790x1483, 6068x8752, 8790x1492),
    # sebagian cuma menutup 17% AOI, sementara atribut aoi_bbox di tiap berkas
    # menjanjikan AOI penuh untuk semuanya.
    #
    # Satu grid untuk seluruh dataset, hari ber-S1 maupun tidak: itulah yang
    # membuat deret waktunya bisa ditumpuk (lihat _dataset_fusion_grid).
    ref_transform, ref_crs, ref_shape = _dataset_fusion_grid(
        db, dataset_id, s1_tier, aoi_bbox
    )

    expected_layers = fusion_layers_for(source_levels)
    total_layers = len(expected_layers)
    done = 0
    # Berkas keluaran disiapkan SEBELUM lapisan dirakit: lapisan ditulis
    # langsung ke sana satu per satu, jadi path-nya harus sudah diketahui.
    date_key = s1_date.strftime("%Y%m%d")
    # Output dipecah per strategi: membandingkan CO_OCCURRENCE vs FULL_COVERAGE
    # vs HYBRID pada dataset yang sama adalah inti D1, dan itu cuma mungkin
    # kalau hasilnya tidak saling menimpa.
    subfolder = SUBFOLDER.get(str(fusion_strategy).strip().upper()) if fusion_strategy else None
    out_dir = fm.ensure_fusion_dir(dataset_id, dataset_name, date_key, subfolder)
    h5_path = out_dir / fusion_h5_name(date_key, run_level, fusion_strategy)
    json_path = out_dir / fusion_metadata_name(date_key, run_level, fusion_strategy)
    processing_dt = datetime.now(tz=timezone.utc)
    layers = _FusionH5Layers(h5_path, ref_shape)
    layer_sources: dict[str, dict] = {}
    # Fraksi piksel valid per lapisan. Dulu tidak pernah diukur: scene yang
    # cuma menyerempet AOI menghasilkan stack yang 96% NaN dan tetap dilaporkan
    # sukses tanpa satu pun angka yang menunjukkannya (22_try6).
    coverage: dict[str, float] = {}
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
        # Direproject seperti MODIS/GPM, bukan dibaca apa adanya: raster S1
        # hidup di grid jejak scene-nya sendiri, sementara stack ini dirakit di
        # grid AOI. Nearest supaya nilai backscatter tidak dirata-rata dengan
        # tetangganya cuma karena grid-nya bergeser sepersekian piksel.
        values = (
            _reproject_to_grid(
                Path(s1[path_key]), ref_transform, ref_crs, ref_shape,
                Resampling.nearest, float("nan"),
            )
            if s1[path_key]
            else np.full(ref_shape, np.nan, dtype="float32")
        )
        coverage[name] = _valid_fraction(values)
        source_paths = s1.get("member_paths", {}).get(band) or (
            [s1[path_key]] if s1[path_key] else []
        )
        layers.add(name, values, attrs={
            "units": LAYER_UNITS.get(name),
            "source_date": s1_date.isoformat() if s1[path_key] else None,
            "acquisition_datetime_utc": (
                _as_utc(s1["acquisition_datetime"]).isoformat()
                if s1[path_key] else None
            ),
            "s1_offset_days": s1_offset_days,
            "source_paths": [str(p) for p in source_paths] or None,
            "valid_fraction": round(coverage[name], 6),
        })
        if s1[path_key] is None:
            logger.warning(
                "[M9] S1 %s missing at %s for scene=%s, filled with NaN",
                band, s1_tier, s1["scene_id"],
            )
        layer_sources[name] = {
            # Raster persisten tiap frame, bukan mosaik antara di _work/ yang
            # disapu di akhir job (24_try8: path sidecar menunjuk berkas yang
            # sudah tidak ada).
            "path": (
                str(source_paths[0]) if len(source_paths) == 1
                else s1[path_key] if not source_paths else None
            ),
            "paths": [str(p) for p in source_paths],
            "date": s1_date.isoformat(),
            "tier": s1_tier,
            "processing_level": source_levels[S1_PLAN_NAME],
            "valid_fraction": coverage[name],
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
            dataset_id, dataset_name, source, level, s1_date, filename_fn
        ):
            name = f"{group}/{layer.name}"
            values = None
            if hit:
                try:
                    values = _reproject_to_grid(
                        hit[0], ref_transform, ref_crs, ref_shape,
                        resampling=layer.resampling, fill_value=np.nan,
                        band=layer.band,
                    )
                except _MissingBand as exc:
                    logger.warning("[M9] %s dilewati: %s", name, exc)
            if values is not None:
                if layer.categorical:
                    # Kelas banjir itu kategorikal: NaN tidak muat di uint8,
                    # jadi pakai sentinel 255 yang sama dengan module7.
                    values = np.where(
                        np.isnan(values), MODIS_NODATA_U8, values
                    ).astype("uint8")
                coverage[name] = _valid_fraction(values)
                tags = _source_tags(hit[0])
                day_offset = (hit[1] - s1_date).days
                hours_after = (
                    _hours_after_acquisition(
                        tags.get("window_end_utc"), s1["acquisition_datetime"]
                    )
                    if s1.get("vv_path") or s1.get("vh_path") else None
                )
                layers.add(name, values, attrs={
                    **tags,
                    "units": LAYER_UNITS.get(name),
                    "source_date": hit[1].isoformat(),
                    "day_offset": day_offset,
                    "source_path": str(hit[0]),
                    "valid_fraction": round(coverage[name], 6),
                    "hours_after_s1_acquisition": hours_after,
                })
                layer_sources[name] = {
                    "path": str(hit[0]), "date": hit[1].isoformat(),
                    "day_offset": day_offset,
                    "tier": tier, "processing_level": level,
                    "valid_fraction": coverage[name],
                    **({"source_tags": tags} if tags else {}),
                    **(
                        {"hours_after_s1_acquisition": hours_after}
                        if hours_after is not None else {}
                    ),
                }
                found_dates.append(hit[1])
                offset = (hit[1] - s1_date).days
                if offsets[source] is None or abs(offset) < abs(offsets[source]):
                    offsets[source] = offset
            else:
                logger.warning(
                    "[M9] no %s %s %s product for %s or the day before",
                    source, layer.name, tier, s1_date.isoformat(),
                )
                layers.add(name, (
                    np.full(ref_shape, MODIS_NODATA_U8, dtype="uint8")
                    if layer.categorical
                    else np.full(ref_shape, np.nan, dtype="float32")
                ), attrs={"units": LAYER_UNITS.get(name), "valid_fraction": 0.0})
                coverage[name] = 0.0
                layer_sources[name] = {
                    "path": None, "date": None,
                    "tier": tier, "processing_level": level,
                    "valid_fraction": 0.0,
                }
            _tick(name)

    # Satu baris ringkas per stack, bukan per lapisan: yang perlu terlihat di
    # log adalah "tanggal ini nyaris kosong", dan angkanya lengkap tetap
    # tersimpan di sidecar JSON untuk yang mau menelusuri.
    thin = {
        name: round(fraction, 4)
        for name, fraction in sorted(coverage.items())
        if fraction < LOW_COVERAGE_FRACTION
    }
    if thin:
        logger.warning(
            "[M9] %s level=%s: %d dari %d lapisan di bawah %.0f%% piksel valid %s",
            s1_date.isoformat(), run_level, len(thin), len(coverage),
            LOW_COVERAGE_FRACTION * 100, thin,
        )

    layers.finalize(
        acquisition_datetime=center_dt, processing_datetime=processing_dt,
        aoi_bbox=aoi_bbox, processing_level=run_level, source_levels=source_levels,
        fusion_strategy=fusion_strategy, crs=ref_crs, transform=ref_transform,
        extra_attrs={
            # Tanggal fitur = tanggal di nama berkas. acquisition_datetime di
            # atas bisa berzona waktu lain (mis. +07:00 jatuh di hari
            # berikutnya), jadi keduanya ditulis terpisah.
            "feature_date": s1_date.isoformat(),
            "acquisition_datetime_utc": (
                _as_utc(center_dt).isoformat()
                if s1.get("vv_path") or s1.get("vh_path") else None
            ),
            "s1_offset_days": s1_offset_days,
            "temporal_offset_modis": offsets[MODIS_PLAN_NAME],
            "temporal_offset_gpm": offsets[GPM_PLAN_NAME],
            "aux_day_rule": "tanggal fitur, atau sehari sebelumnya; tidak pernah sesudahnya",
        },
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

    # Hari yang MEMINJAM scene S1 (FULL_COVERAGE, offset >= 1) tidak boleh
    # menempelkan data_products-nya ke scene S1 itu. Dedup is_latest di
    # insert_data_product berjalan atas (scene_id, band_name, tier, dataset_id),
    # jadi lima hari yang meminjam satu scene saling menandai usang dan hanya
    # hari terakhir yang tersisa di listing API -- termasuk stack same-day-nya
    # sendiri (dataset try2: 4 dari 5 stack hilang). Placeholder per tanggal,
    # sama dengan hari tanpa-S1. s1_scene_id di fusion_products tetap scene
    # yang benar-benar dipakai.
    product_scene_id = (
        s1["scene_id"] if s1_offset_days in (0, None)
        else _resolve_aux_scene(
            db, dataset_id, s1["region_id"], box(*aoi_bbox).wkt, "FUSION", s1_date
        )
    )

    with db.session() as sess:
        # dataset_id ikut kunci (migrasi 021). Tanpa itu dua dataset atas AOI
        # dan tanggal yang sama berbagi SATU baris: try1/try2/try3 semuanya
        # menulis fusion_id=43 dan yang terakhir selesai menimpa path dua
        # lainnya.
        existing = sess.scalar(
            select(FusionProduct).where(
                FusionProduct.dataset_id == dataset_id,
                FusionProduct.feature_date == s1_date,
                FusionProduct.processing_level == run_level,
            )
        )
        if existing:
            existing.region_id = s1["region_id"]
            existing.s1_scene_id = s1["scene_id"]
            existing.modis_scene_id = modis_scene_id
            existing.gpm_scene_id = gpm_scene_id
            existing.days_since_s1 = days_since_s1
            existing.feature_stack_path = str(h5_path)
            existing.fusion_strategy = fusion_strategy
            existing.temporal_offset_modis = offsets[MODIS_PLAN_NAME]
            existing.temporal_offset_gpm = offsets[GPM_PLAN_NAME]
            existing.s1_offset_days = s1_offset_days
            sess.flush()
            fusion_id = existing.fusion_id
        else:
            fusion = FusionProduct(
                dataset_id=dataset_id,
                feature_date=s1_date,
                region_id=s1["region_id"],
                s1_scene_id=s1["scene_id"],
                modis_scene_id=modis_scene_id,
                gpm_scene_id=gpm_scene_id,
                days_since_s1=days_since_s1,
                feature_stack_path=str(h5_path),
                fusion_strategy=fusion_strategy,
                processing_level=run_level,
                s1_offset_days=s1_offset_days,
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
        product_scene_id, "FUSION",
        parameters={
            "dataset_id": dataset_id, "s1_date": s1_date.isoformat(),
            "processing_level": run_level, "source_levels": source_levels,
        },
    )
    meta.start_job(fusion_job_id)
    try:
        fusion_product_id = meta.insert_data_product(
            scene_id=product_scene_id, job_id=fusion_job_id, dataset_id=dataset_id,
            product_tier=tn.FUSED, source=fm.FUSION_DB_SOURCE,
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
            # Identitas stack fusion adalah BERKASNYA, bukan scene primary-nya.
            # Nama berkas cuma memuat tanggal, sementara produknya didaftarkan
            # atas nama anggota pertama tanggal itu — dan anggota pertama bisa
            # berganti antar jalan kalau job terputus lalu dilanjutkan dengan
            # himpunan scene yang berbeda. Tanpa ini, jalan kedua menimpa
            # berkasnya tapi baris jalan pertama tetap hidup mengklaim ukuran
            # yang sudah tidak ada (dataset 26, fusion_20251201).
            supersede_same_path=True,
        )
        # Induk lineage: produk S1 scene utama, DITAMBAH produk frame lain
        # yang ikut ke dalam mosaik. Tanpa yang kedua, stack yang separuh
        # datanya datang dari frame tetangga akan mengaku lahir dari satu
        # frame saja.
        parent_ids = [
            s1[key] for key in ("vv_product_id", "vh_product_id") if s1.get(key)
        ]
        for member_scene_id in s1_member_scene_ids:
            if member_scene_id == s1["scene_id"]:
                continue
            member = _find_s1_products(db, dataset_id, member_scene_id, tier=s1_tier)
            if not member:
                continue
            parent_ids.extend(
                member[key] for key in ("vv_product_id", "vh_product_id")
                if member.get(key)
            )
        for parent_id in dict.fromkeys(parent_ids):
            lineage.record_transformation(
                parent_id, fusion_product_id, "FUSION", fusion_job_id,
                {"aoi_bbox": list(aoi_bbox), "processing_level": run_level},
            )
    except Exception as exc:
        # Job yang sudah RUNNING wajib ditutup di jalur gagal juga; kalau tidak
        # ia tertinggal RUNNING selamanya dan terlihat seperti fusi yang masih
        # berjalan.
        meta.complete_job(
            fusion_job_id, status=JobStatusEnum.FAILED,
            error_code=type(exc).__name__, error_message=str(exc)[:2000],
        )
        raise
    meta.complete_job(fusion_job_id)

    logger.info(
        "[M9] fusion_id=%d dataset=%s date=%s level=%s sources=%s path=%s hash=%s",
        fusion_id, dataset_id, s1_date.isoformat(), run_level, source_levels,
        h5_path, checksum[:12],
    )

    # Audit setelah stack ditulis: memaku grid mencegah percabangan BARU, tapi
    # stack lama yang sudah lahir di grid lain tetap diam saja sampai ada yang
    # memeriksanya. Cuma membaca atribut HDF5, bukan datanya.
    _warn_on_grid_drift(db, dataset_id, dataset_name)

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
    region_id: int | None = None,
    require_s1: bool = True,
    s1_offset_days: int | None = 0,
    s1_files_by_level: dict[str, dict[str, str]] | None = None,
    s1_member_scene_ids: tuple[int, ...] | Sequence[int] | None = None,
) -> list[FusionRun]:
    """
    Bangun stack fitur HDF5 untuk scene Sentinel-1 `scene_id` (akuisisi
    `s1_date`), mencocokkan produk MODIS/GPM milik tanggal `s1_date` (atau
    sehari sebelumnya, tidak pernah sesudahnya). Ini deliverable tier FUSION.

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

    Args tambahan:
        region_id: wajib kalau require_s1=False — dipakai membuat scene
              placeholder tempat data_products fusi ditempelkan, karena
              data_products.scene_id NOT NULL sementara tanggal tanpa S1
              tidak punya baris satellite_scenes sendiri.
        require_s1: True (default) mempertahankan perilaku lama — tanggal
              tanpa produk S1 melempar. FULL_COVERAGE memakai False supaya
              tiap hari tetap jadi berkas walau S1-nya tidak ada.
        s1_files_by_level: {level: {band: path}} yang menggantikan raster S1
              milik `scene_id` pada level itu. Dipakai orchestrator untuk
              mengoper MOSAIK beberapa frame satu tanggal (etl/s1_mosaic.py):
              AOI yang lebih panjang dari satu frame Sentinel-1 tertutup dua
              scene yang saling melengkapi, dan tanpa ini stack tanggal itu
              cuma memuat salah satunya.
        s1_member_scene_ids: scene lain yang ikut menyumbang ke mosaik.
              Produk S1-nya ikut dicatat sebagai induk lineage, supaya stack
              hasil mosaik tidak mengaku lahir dari satu frame saja.

    Raises:
        RuntimeError: SENTINEL1 tidak dikonfigurasi; atau produk S1 di tier
        yang diminta tidak ada SEMENTARA require_s1=True.
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
                region_id=region_id,
                require_s1=require_s1,
                s1_offset_days=s1_offset_days,
                s1_files=(s1_files_by_level or {}).get(run_level),
                s1_member_scene_ids=tuple(s1_member_scene_ids or ()),
            )
            for run_level in plan.output_levels()
        ]

    finally:
        if owns_db:
            db.dispose()
