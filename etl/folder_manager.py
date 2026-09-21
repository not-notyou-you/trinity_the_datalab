# etl/folder_manager.py
"""
Path management utility untuk struktur penyimpanan data per dataset.

Layout on-disk — dikelompokkan per dataset, lalu per tanggal akuisisi, lalu
per tier:

    data/datasets/{dataset_id}_{slug(dataset_name)}/
        metadata.json
        {YYYYMMDD}/
            raw/
                sentinel1/{scene}/      # .SAFE.zip + TIFF hasil ekstrak per band
            bronze/
                sentinel1/{scene}/
                modis/                  # hanya kalau MODIS dikonfigurasi RAW
                gpm/                    # hanya kalau GPM dikonfigurasi RAW
            silver/
                sentinel1/{scene}/
                modis/
                gpm/
            gold/
                sentinel1/{scene}/
                modis/
                gpm/
            fusion/                     # lintas-source, jadi tidak punya level source
            preview/                    # lintas-source juga
                {PROCESSING_LEVEL}/     # RAW (dirender dari BRONZE) | PROCESSED (dari GOLD)
                    grayscale/          # PNG stretch persentil, 1 kanal + alpha
                    colored/            # PNG colormap RGBA
                    composite/          # PNG false-color RGB (VV/VH/VV-VH)
        _granule_cache/
            modis/                      # cache granule .hdf mentah (flat, lintas tanggal)
            gpm/                        # cache granule .nc4 mentah (flat, lintas tanggal)
        _work/{scene}/                  # scratch kalibrasi, dihapus setelah CROP

`{scene}` untuk Sentinel-1 adalah product_identifier scene tersebut (sudah
unik termasuk jam:menit:detik) — satu tanggal bisa punya lebih dari satu
scene S1, jadi folder scene tetap ada di bawah folder source. Untuk artefak
yang tidak terikat ke satu scene S1 (MODIS/GPM harian, fusion, preview),
kunci scene-nya adalah tanggal YYYYMMDD itu sendiri, jadi file-nya duduk
langsung di folder source/tier tanpa folder tanggal kedua.

Folder tanggal diturunkan dari kunci scene: tanggal YYYYMMDD apa adanya, atau
tanggal akuisisi pertama di dalam product_identifier S1
(`S1A_IW_GRDH_1SDV_20240115T...`). Kunci tanpa tanggal ditolak — lebih baik
gagal keras daripada diam-diam menulis ke folder yang salah.

Level `{source}` ada di setiap tier kecuali `fusion` dan `preview`: keduanya
justru *gabungan* dari semua source, jadi memberinya satu folder source akan
menyesatkan. Semua fungsi di sini menolak kombinasi tier/source yang tidak
valid alih-alih diam-diam menulis ke tempat yang salah.

Cache granule MODIS/GPM sengaja di luar folder tanggal: satu granule GPM
harian ikut dipakai window 72h/7d tanggal berikutnya, jadi tidak bisa
dimiliki satu tanggal saja. Dia tetap dihitung sebagai tier `raw`.

`preview` adalah tier turunan (PNG hasil render dari gold/) — dia ikut di
`TIERS` supaya terhitung di `storage_breakdown` dan bisa dilisting API, tapi
sengaja TIDAK ada di `dataset_manager.TIER_ORDER`: dia bukan mata rantai
lineage RAW→FUSION dan tidak pernah ikut dihapus `compute_tiers_to_delete`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Kosakata tier D14 dalam huruf kecil (bentuk yang dipakai kunci
# storage_breakdown dan parameter ?tier= di API). Nama lama ikut supaya
# dataset pra-migrasi tetap bisa dibaca; lihat etl/tier_names.py.
TIERS: tuple[str, ...] = (
    "raw", "aligned", "despeckled", "indices", "accumulated", "cog",
    "preview", "fused",
)
LEGACY_TIERS: tuple[str, ...] = ("bronze", "silver", "gold", "fusion")
ALL_TIERS: tuple[str, ...] = TIERS + LEGACY_TIERS

SOURCES: tuple[str, ...] = ("sentinel1", "modis", "gpm")

# Tier fusion dan preview sengaja dipetakan ke tuple kosong: keduanya
# lintas-source.
# bronze dipakai SEMUA source sejak model per-satelit (DOCS/ETL.md): dia tempat
# artefak level RAW tiap sumber berhenti — S1 hasil crop AOI, MODIS peta banjir
# tanpa indeks turunan, GPM curah hujan harian tanpa window akumulasi.
TIER_SOURCES: dict[str, tuple[str, ...]] = {
    "raw": SOURCES,
    "aligned": SOURCES,
    # Rank 2 bercabang per-source: satu nama, satu satelit (D14).
    "despeckled": ("sentinel1",),
    "indices": ("modis",),
    "accumulated": ("gpm",),
    "cog": SOURCES,
    "preview": (),
    "fused": (),
    # -- warisan pra-D14: dibaca, tidak pernah ditulis --
    "bronze": SOURCES,
    "silver": SOURCES,
    "gold": SOURCES,
    "fusion": (),
}

# Tier yang tidak punya level source: langsung {tanggal}/{tier}/.
SOURCELESS_TIERS: tuple[str, ...] = tuple(t for t, s in TIER_SOURCES.items() if not s)

# Subfolder di dalam satu scene preview. Urutannya ikut dipakai
# module10_generate_preview.py sebagai urutan tampil.
PREVIEW_KINDS: tuple[str, ...] = ("grayscale", "colored", "composite")

# Level pemrosesan yang bisa jadi nama folder di dalam {tanggal}/preview/.
# Sama dengan etl.processing_plan.LEVEL_ORDER, tapi ditulis ulang di sini
# supaya folder_manager tetap bisa diimpor tanpa menarik modul ETL lain
# (dia dipakai API dan skrip perawatan yang tidak butuh pipeline-nya).
PREVIEW_LEVELS: tuple[str, ...] = ("RAW", "PROCESSED")
DEFAULT_PREVIEW_LEVEL = "PROCESSED"

# Source yang file mentahnya berupa cache granule flat (bukan per-tanggal):
# satu granule GPM harian ikut dipakai window 72h/7d tanggal berikutnya, jadi
# tidak bisa dimiliki satu folder tanggal saja.
FLAT_RAW_SOURCES: frozenset[str] = frozenset({"modis", "gpm"})

DATA_ROOT = Path("data") / "datasets"

# Folder di root dataset yang bukan folder tanggal.
GRANULE_CACHE_DIRNAME = "_granule_cache"
SCRATCH_DIRNAME = "_work"

# Label scene semu untuk cache granule di listing berkas API.
GRANULE_CACHE_LABEL = "(granule cache)"

# Nama folder source <-> nilai kolom data_products.source.
SOURCE_DB_VALUES: dict[str, str] = {
    "sentinel1": "SENTINEL1",
    "modis": "MODIS",
    "gpm": "GPM",
}
FUSION_DB_SOURCE = "FUSION"

# ---------------------------------------------------------------------------
# Layout per-satelit (relayout Fase B)
# ---------------------------------------------------------------------------
# Layout lama menaruh tier di jalur: {YYYYMMDD}/{tier}/{source}/{scene}/.
# Bentuk itu menyulitkan hal yang paling sering diminta user -- "ambil
# Sentinel-1 PROCESSED saja" -- karena berkasnya tersebar di satu folder per
# tanggal per tier. Layout baru menaruh sumber di depan dan hanya punya dua
# laci per sumber, persis kosakata yang dipilih user saat membuat dataset:
#
#     {id}_{slug}/sentinel-1/{RAW|PROCESSED}/
#     {id}_{slug}/modis/{RAW|PROCESSED}/
#     {id}_{slug}/gpm-imerg/{RAW|PROCESSED}/
#     {id}_{slug}/fusion/{strategy}/
#     {id}_{slug}/preview/{LEVEL}/{kind}/
#
# Tanggal pindah ke NAMA BERKAS. Konsekuensinya list_date_dirs tidak bisa lagi
# menemukan tanggal dengan mencocokkan nama folder; tanggal dibaca dari nama
# berkas (_DATE_IN_KEY_RE) atau dari database.
#
# Tier tidak hilang -- dia tetap jadi nilai data_products.product_tier dan
# tetap dipakai lineage (DOCS/DECISIONS.md D14). Yang hilang adalah perannya
# sebagai segmen path. Pemetaannya:
#
#     ALIGNED (terkalibrasi + ter-crop, tanpa Lee) -> {source}/RAW/
#     COG     (analysis-ready)                     -> {source}/PROCESSED/
#     RAW (ZIP SAFE) dan tier rank 2 (pre-COG)     -> _work/, dibuang
#
# Dua yang terakhir sengaja tidak punya laci: keduanya artefak antara, dan
# menyimpannya akan menghidupkan lagi konsep tier di jalur yang justru ingin
# dihilangkan. Harganya nyata dan harus disadari: mengulang Lee filter dengan
# parameter berbeda berarti mengunduh ulang scene-nya.
LEVELS: tuple[str, ...] = ("RAW", "PROCESSED")

# Nama folder per sumber. Berbeda dari kunci internal (`sentinel1`, `gpm`)
# karena folder ini yang dilihat user saat membuka hasil unduhan -- nama yang
# dipakai literatur lebih berguna di sana daripada kunci yang dipakai kode.
SOURCE_DIR_NAMES: dict[str, str] = {
    "sentinel1": "sentinel-1",
    "modis": "modis",
    "gpm": "gpm-imerg",
}
DIR_NAME_TO_SOURCE: dict[str, str] = {v: k for k, v in SOURCE_DIR_NAMES.items()}

# Tier yang isinya mendarat di tiap laci. Dipakai modul ETL untuk menerjemahkan
# tier yang masih mereka pakai secara internal menjadi folder tujuan.
LEVEL_BY_TIER: dict[str, str] = {
    "ALIGNED": "RAW",
    "COG": "PROCESSED",
    # Warisan pra-D14, supaya dataset lama tetap teralamatkan.
    "BRONZE": "RAW",
    "GOLD": "PROCESSED",
}

FUSION_DIRNAME = "fusion"
PREVIEW_DIRNAME = "preview"

# Nama tier yang menunjuk artefak fusi, di kedua kosakata. Foldernya tetap
# bernama "fusion" -- yang berubah cuma nama TIER-nya (FUSION -> FUSED, D14).
_FUSION_TIER_NAMES: frozenset[str] = frozenset({"fusion", "fused"})


def normalize_level(level: str) -> str:
    """Level pemrosesan kanonik (huruf besar). Menolak yang tidak dikenal:
    level yang salah ketik akan membuat berkas mendarat di laci yang tidak
    pernah dilihat siapa pun."""
    lv = str(level).strip().upper()
    if lv not in LEVELS:
        raise ValueError(f"Level tidak valid: {level!r}. Valid: {LEVELS}")
    return lv


def source_dir_name(source: str) -> str:
    """Nama folder untuk sebuah source."""
    return SOURCE_DIR_NAMES[normalize_source(source)]


def get_source_level_dir(
    dataset_id: int, dataset_name: str, source: str, level: str
) -> Path:
    """Laci satu sumber pada satu level: {source}/{RAW|PROCESSED}/."""
    return (
        get_dataset_root(dataset_id, dataset_name)
        / source_dir_name(source)
        / normalize_level(level)
    )


def ensure_source_level_dir(
    dataset_id: int, dataset_name: str, source: str, level: str
) -> Path:
    p = get_source_level_dir(dataset_id, dataset_name, source, level)
    p.mkdir(parents=True, exist_ok=True)
    return p


def level_for_tier(tier: str) -> str | None:
    """Laci tujuan untuk artefak di `tier`, atau None kalau tier itu artefak
    antara yang tidak disimpan (RAW/SILVER)."""
    return LEVEL_BY_TIER.get(str(tier).strip().upper())


def dated_filename(date_key_str: str, stem: str) -> str:
    """Nama berkas berprefiks tanggal, mis. "20240920_modis_ndvi.tif".

    Tanggal WAJIB ada di nama berkas sejak tier tidak lagi jadi folder: tanpa
    folder tanggal, nama berkas adalah satu-satunya tempat tanggal bisa dibaca
    kembali -- oleh list_dates, oleh storage_breakdown, dan oleh user yang
    membuka foldernya. Berkas yang namanya sudah memuat tanggal (mis.
    product_identifier Sentinel-1) dibiarkan apa adanya supaya tidak berprefiks
    ganda.
    """
    if _DATE_IN_KEY_RE.search(stem):
        return stem
    return f"{date_key_str}_{stem}"


def date_from_filename(name: str) -> str | None:
    """Tanggal YYYYMMDD yang tertanam di nama berkas, atau None."""
    m = _DATE_IN_KEY_RE.search(str(name))
    return m.group(1) if m else None


_DATE_DIR_RE = re.compile(r"^\d{8}$")
# Tanggal pertama di product_identifier S1, mis. "..._1SDV_20240115T111407_...".
# Batas non-digit di kedua sisi supaya timestamp panjang tidak ikut terbaca.
_DATE_IN_KEY_RE = re.compile(r"(?<!\d)(\d{8})(?!\d)")


def slugify(name: str) -> str:
    """Nama dataset -> slug aman-filesystem, mis. "hakim d1" -> "hakim_d1".
    Dipakai untuk nama folder dataset dan nama file log run-nya
    (etl/pipeline_logger.py), jadi keduanya selalu konsisten."""
    slug = re.sub(r"[^A-Za-z0-9_.-]", "_", name).strip("_")
    return slug or "dataset"


def scene_slug(key: str) -> str:
    """Sanitasi kunci scene (product_identifier S1 atau tanggal YYYYMMDD)
    supaya aman jadi nama folder."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(key))


def date_key(d: date | datetime | str) -> str:
    """Normalisasi tanggal ke format kunci scene YYYYMMDD."""
    if isinstance(d, datetime):
        d = d.date()
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    s = str(d).replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError(
            f"tanggal tidak valid: {d!r}. Gunakan objek date/datetime atau "
            "string 'YYYYMMDD'/'YYYY-MM-DD'."
        )
    return s


def _valid_date(s: str) -> bool:
    try:
        datetime.strptime(s, "%Y%m%d")
    except ValueError:
        return False
    return True


def scene_date_key(scene_key: str) -> str:
    """Folder tanggal untuk sebuah kunci scene.

    scene_date_key("20240115")                               -> "20240115"
    scene_date_key("S1A_IW_GRDH_1SDV_20240115T111407_...")   -> "20240115"
    """
    key = str(scene_key)
    for candidate in _DATE_IN_KEY_RE.findall(key):
        if _valid_date(candidate):
            return candidate
    raise ValueError(
        f"Kunci scene {scene_key!r} tidak mengandung tanggal YYYYMMDD — "
        "folder tanggalnya tidak bisa ditentukan."
    )


def normalize_tier(tier: str) -> str:
    t = str(tier).lower()
    if t not in ALL_TIERS:
        raise ValueError(f"Tier tidak valid: {tier!r}. Valid: {ALL_TIERS}")
    return t


def normalize_source(source: str) -> str:
    s = str(source).lower()
    if s not in SOURCES:
        raise ValueError(f"Source tidak valid: {source!r}. Valid: {SOURCES}")
    return s


def sources_for_tier(tier: str) -> tuple[str, ...]:
    """Source yang absah untuk satu tier. Kosong untuk `fusion` (lintas-source)."""
    return TIER_SOURCES[normalize_tier(tier)]


def validate_tier_source(tier: str, source: str) -> tuple[str, str]:
    """Normalisasi + validasi pasangan tier/source. Menolak source untuk tier
    lintas-source dan source yang tidak dipakai tier itu."""
    tier = normalize_tier(tier)
    source = normalize_source(source)
    allowed = TIER_SOURCES[tier]
    if not allowed:
        raise ValueError(
            f"Tier {tier!r} tidak punya level source (dia gabungan semua "
            f"source). Pakai get_fusion_dir()/get_preview_dir()."
        )
    if source not in allowed:
        raise ValueError(
            f"Source {source!r} tidak dipakai di tier {tier!r}. Valid: {allowed}"
        )
    return tier, source


def db_source(source: str) -> str:
    """Nama folder source -> nilai kolom data_products.source."""
    return SOURCE_DB_VALUES[normalize_source(source)]


def dataset_dir_name(dataset_id: int, dataset_name: str) -> str:
    return f"{dataset_id}_{slugify(dataset_name)}"


def get_dataset_root(dataset_id: int, dataset_name: str) -> Path:
    """Folder root untuk sebuah dataset: data/datasets/{id}_{slug}/"""
    return DATA_ROOT / dataset_dir_name(dataset_id, dataset_name)


def get_dataset_metadata_path(dataset_id: int, dataset_name: str) -> Path:
    """Path ke metadata.json level-dataset."""
    return get_dataset_root(dataset_id, dataset_name) / "metadata.json"


def get_date_dir(dataset_id: int, dataset_name: str, date: date | datetime | str) -> Path:
    """Folder satu tanggal akuisisi: {id}_{slug}/{YYYYMMDD}/."""
    return get_dataset_root(dataset_id, dataset_name) / date_key(date)


def list_date_dirs(dataset_root: Path) -> list[Path]:
    """Folder tanggal warisan layout lama, urut kronologis.

    Setelah relayout tidak ada lagi folder tanggal, jadi untuk dataset baru
    fungsi ini selalu mengembalikan []. Dipertahankan justru sebagai DETEKTOR
    layout lama -- pemanggil yang menemukan isinya tidak kosong sedang melihat
    dataset pra-relayout (lihat is_legacy_layout)."""
    if not dataset_root.is_dir():
        return []
    return sorted(
        d for d in dataset_root.iterdir() if d.is_dir() and _DATE_DIR_RE.match(d.name)
    )


def is_legacy_layout(dataset_root: Path) -> bool:
    """True kalau dataset ini memakai struktur folder sebelum relayout.

    Dipakai API/UI untuk menolak merender pohon penyimpanan dataset lama alih
    -alih menampilkannya dengan kosakata yang sudah tidak berlaku."""
    return bool(list_date_dirs(dataset_root))


def list_dates(dataset_id: int, dataset_name: str) -> list[str]:
    """Tanggal (YYYYMMDD) yang benar-benar punya berkas di dataset ini.

    Dibaca dari NAMA BERKAS, bukan nama folder: sejak relayout tanggal tidak
    lagi jadi segmen path. Berkas tanpa tanggal yang bisa dibaca dilewati --
    satu-satunya yang begitu adalah cache granule, yang memang tidak dimiliki
    tanggal mana pun."""
    root = get_dataset_root(dataset_id, dataset_name)
    if not root.is_dir():
        return []
    if is_legacy_layout(root):
        return [d.name for d in list_date_dirs(root)]

    dates: set[str] = set()
    for source in SOURCES:
        src_root = root / SOURCE_DIR_NAMES[source]
        if not src_root.is_dir():
            continue
        for f in src_root.rglob("*"):
            if f.is_file():
                d = date_from_filename(f.name)
                if d:
                    dates.add(d)
    return sorted(dates)


def get_tier_dir(
    dataset_id: int, dataset_name: str, date: date | datetime | str, tier: str
) -> Path:
    """Folder satu tier pada satu tanggal: {YYYYMMDD}/{tier}/."""
    return get_date_dir(dataset_id, dataset_name, date) / normalize_tier(tier)


def get_source_dir(
    dataset_id: int, dataset_name: str, date: date | datetime | str, tier: str, source: str
) -> Path:
    """Folder satu source pada satu tier dan tanggal: {YYYYMMDD}/{tier}/{source}/."""
    tier, source = validate_tier_source(tier, source)
    return get_tier_dir(dataset_id, dataset_name, date, tier) / source


def get_scene_dir(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> Path:
    """
    Path folder satu scene. Tanggal diturunkan dari kunci scene-nya.

    Tier diterjemahkan jadi laci (LEVEL_BY_TIER): BRONZE -> RAW/,
    GOLD -> PROCESSED/. Tier antara (RAW, SILVER) tidak punya laci dan
    diarahkan ke _work/, yang dibuang setelah scene selesai.

    get_scene_dir(2, "Hakim D1", "gold", "sentinel1", "S1A_..._20240115T...")
        -> data/datasets/2_Hakim_D1/sentinel-1/PROCESSED
    get_scene_dir(2, "Hakim D1", "bronze", "modis", "20240115")
        -> data/datasets/2_Hakim_D1/modis/RAW
    get_scene_dir(2, "Hakim D1", "silver", "sentinel1", "S1A_...")
        -> data/datasets/2_Hakim_D1/_work/S1A_.../silver/sentinel1
    """
    tier_u = normalize_tier(tier).upper()
    level = level_for_tier(tier_u)
    if level is not None:
        # Laci final: {source}/{RAW|PROCESSED}/. Tidak ada folder scene di
        # dalamnya -- nama berkas sudah memuat tanggal (dan untuk Sentinel-1
        # seluruh product_identifier), jadi folder tambahan hanya akan
        # memecah satu deret waktu jadi puluhan folder berisi dua berkas.
        return get_source_level_dir(dataset_id, dataset_name, source, level)

    # Tier antara (RAW = ZIP SAFE, SILVER = Lee pre-COG): tidak punya laci,
    # hidup di _work/ dan dibuang setelah scene selesai. Source ikut ke path
    # supaya MODIS dan GPM -- yang kunci scene-nya sama-sama tanggal itu --
    # tidak berbagi satu folder scratch.
    return (
        get_scratch_dir(dataset_id, dataset_name, scene_key)
        / tier_u.lower()
        / normalize_source(source)
    )


def ensure_scene_dir(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> Path:
    """Buat (jika belum ada) dan kembalikan folder satu scene."""
    p = get_scene_dir(dataset_id, dataset_name, tier, source, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_fusion_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder output fusion: fusion/ di root dataset.

    Lepas dari folder tanggal sejak relayout: tanggal ada di nama berkas, dan
    satu folder berisi seluruh deret waktu jauh lebih berguna untuk konsumen
    yang memang ingin menumpuknya jadi satu array. `scene_key` dipertahankan
    di tanda tangan karena pemanggilnya masih meneruskannya, tapi tidak lagi
    memengaruhi path."""
    return get_dataset_root(dataset_id, dataset_name) / FUSION_DIRNAME


def ensure_fusion_dir(
    dataset_id: int, dataset_name: str, scene_key: str, strategy_subfolder: str | None = None
) -> Path:
    """Folder output fusion, opsional dipecah per strategi.

    `strategy_subfolder` (nilai `fusion_strategies.SUBFOLDER`, mis.
    "co-occurrence") memisahkan output tiap strategi supaya satu dataset yang
    dijalankan ulang dengan strategi berbeda tidak menimpa hasil sebelumnya --
    membandingkan strategi justru inti dari fitur ini (D1). None berarti
    langsung di fusion/, bentuk yang dipakai dataset sebelum strategi
    benar-benar bercabang.
    """
    p = get_fusion_dir(dataset_id, dataset_name, scene_key)
    if strategy_subfolder:
        p = p / strategy_subfolder
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_preview_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder preview: preview/ di root dataset.

    Sama seperti fusion, lintas-source dan lepas dari folder tanggal. Di
    dalamnya ada subfolder per level lalu per `PREVIEW_KINDS`."""
    return get_dataset_root(dataset_id, dataset_name) / PREVIEW_DIRNAME


def ensure_preview_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    p = get_preview_dir(dataset_id, dataset_name, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def normalize_preview_level(processing_level: str | None) -> str:
    """Validasi nama folder level preview. None -> PROCESSED."""
    if processing_level is None:
        return DEFAULT_PREVIEW_LEVEL
    level = str(processing_level).strip().upper()
    if level not in PREVIEW_LEVELS:
        raise ValueError(
            f"Level preview tidak valid: {processing_level!r}. Valid: {PREVIEW_LEVELS}"
        )
    return level


def get_preview_level_dir(
    dataset_id: int, dataset_name: str, scene_key: str,
    processing_level: str | None = None,
) -> Path:
    """Folder satu level pemrosesan: {YYYYMMDD}/preview/{RAW|PROCESSED}/.

    Level ikut ke path, bukan cuma ke nama berkas: dataset yang meminta sebuah
    sumber di kedua level me-render DUA set PNG untuk tanggal yang sama, dari
    tier yang berbeda (BRONZE vs GOLD), dengan nama berkas yang sama persis
    (`s1_vv.png`). Tanpa folder pemisah yang kedua menimpa yang pertama."""
    return (
        get_preview_dir(dataset_id, dataset_name, scene_key)
        / normalize_preview_level(processing_level)
    )


def get_preview_kind_dir(
    dataset_id: int, dataset_name: str, scene_key: str, kind: str,
    processing_level: str | None = None,
) -> Path:
    """Subfolder satu jenis render:
    {YYYYMMDD}/preview/{RAW|PROCESSED}/{grayscale|colored|composite}/."""
    if kind not in PREVIEW_KINDS:
        raise ValueError(f"Jenis preview tidak valid: {kind!r}. Valid: {PREVIEW_KINDS}")
    return get_preview_level_dir(
        dataset_id, dataset_name, scene_key, processing_level
    ) / kind


def ensure_preview_kind_dir(
    dataset_id: int, dataset_name: str, scene_key: str, kind: str,
    processing_level: str | None = None,
) -> Path:
    p = get_preview_kind_dir(dataset_id, dataset_name, scene_key, kind, processing_level)
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_preview_levels(dataset_id: int, dataset_name: str, scene_key: str) -> list[str]:
    """Level yang benar-benar punya folder di disk untuk tanggal ini, urut
    RAW lalu PROCESSED. Kosong berarti belum ada preview untuk tanggal itu."""
    root = get_preview_dir(dataset_id, dataset_name, scene_key)
    if not root.is_dir():
        return []
    return [level for level in PREVIEW_LEVELS if (root / level).is_dir()]


def get_scratch_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder kerja sementara (hasil kalibrasi radiometrik sebelum crop),
    dihapus otomatis setelah tahap CROP selesai — bukan bagian dari tier
    resmi, jadi diletakkan di luar folder tanggal."""
    return get_dataset_root(dataset_id, dataset_name) / SCRATCH_DIRNAME / scratch_slug(scene_key)


# Batas panjang nama folder scratch. product_identifier S1 (~70 karakter)
# muncul dua kali di path RAW (folder scratch + nama .zip di dalamnya), dan
# dengan root repo + nama dataset yang agak panjang totalnya melewati
# MAX_PATH Windows (260). Kasus nyata: dataset "jan_mar_2025_hybrid" -> 264
# karakter; M1 tetap bisa menulis lewat prefix extended-length tapi kalibrasi
# (zipfile) dan sapuan _work/ tidak melihat berkasnya -> semua scene S1 gagal.
SCRATCH_SLUG_MAX = 32


def scratch_slug(key: str) -> str:
    """Nama folder scratch yang pendek dan deterministik untuk `key`.

    Kunci pendek (tanggal YYYYMMDD) dipakai apa adanya; kunci panjang
    dipotong lalu diberi sufiks hash kunci utuh supaya tetap unik per scene
    (dua potongan S1 satu orbit hanya berbeda di akhir nama)."""
    slug = scene_slug(key)
    if len(slug) <= SCRATCH_SLUG_MAX:
        return slug
    digest = hashlib.sha1(str(key).encode("utf-8")).hexdigest()[:10]
    return f"{slug[:SCRATCH_SLUG_MAX - 11]}_{digest}"


def long_path(path: Path | str) -> Path:
    """Path aman dari batas MAX_PATH (260) Windows lewat prefix
    extended-length ``\\\\?\\``. LongPathsEnabled di registry sering dikunci
    kebijakan institusi, jadi jangan bergantung ke konfigurasi mesin.
    No-op di luar Windows. Hanya untuk API Python (open, zipfile, shutil);
    GDAL/rasterio tidak selalu menerima prefix ini."""
    path = Path(path)
    if os.name != "nt":
        return path
    s = str(path.resolve())
    prefix = "\\\\?\\"
    return Path(s) if s.startswith(prefix) else Path(prefix + s)


def get_granule_cache_root(dataset_root: Path) -> Path:
    """Folder induk cache granule di bawah satu root dataset."""
    return dataset_root / GRANULE_CACHE_DIRNAME


def get_granule_cache_dir(dataset_id: int, dataset_name: str, source: str) -> Path:
    """Folder cache granule mentah MODIS/GPM (.hdf/.nc4 sebelum
    di-mosaic/crop): _granule_cache/{source}/. Flat dan di luar folder
    tanggal: satu granule GPM harian ikut dipakai window 72h/7d
    tanggal-tanggal berikutnya, jadi tidak bisa dimiliki satu tanggal saja."""
    source = normalize_source(source)
    if source not in FLAT_RAW_SOURCES:
        raise ValueError(
            f"Source {source!r} tidak pakai cache granule flat. "
            f"Valid: {sorted(FLAT_RAW_SOURCES)}"
        )
    return get_granule_cache_root(get_dataset_root(dataset_id, dataset_name)) / source


def tier_dirs_under(dataset_root: Path, tier: str) -> list[Path]:
    """Semua folder yang menyimpan isi satu tier di bawah satu root dataset:
    {tanggal}/{tier}/ untuk tiap tanggal, ditambah _granule_cache/ untuk tier
    raw. Dipakai untuk menyapu satu tier lintas tanggal."""
    tier = normalize_tier(tier)

    # Layout lama: {tanggal}/{tier}/ untuk tiap tanggal.
    dirs = [d / tier for d in list_date_dirs(dataset_root) if (d / tier).is_dir()]
    if dirs:
        if tier == "raw":
            cache = get_granule_cache_root(dataset_root)
            if cache.is_dir():
                dirs.append(cache)
        return dirs

    # Layout baru: tier diterjemahkan jadi laci per source.
    if tier in _FUSION_TIER_NAMES:
        p = dataset_root / FUSION_DIRNAME
        return [p] if p.is_dir() else []
    if tier == "preview":
        p = dataset_root / PREVIEW_DIRNAME
        return [p] if p.is_dir() else []

    level = level_for_tier(tier)
    if level is not None:
        return [
            d for d in (
                dataset_root / SOURCE_DIR_NAMES[src] / level for src in SOURCES
            ) if d.is_dir()
        ]

    # Tier antara: cuma cache granule yang bertahan di disk.
    if tier == "raw":
        cache = get_granule_cache_root(dataset_root)
        if cache.is_dir():
            return [cache]
    return []


def _source_dirs(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[Path]:
    """Folder {tanggal}/{tier}/{source}/ yang ada di disk, lintas tanggal."""
    tier, source = validate_tier_source(tier, source)
    root = get_dataset_root(dataset_id, dataset_name)

    legacy = [
        d / tier / source for d in list_date_dirs(root) if (d / tier / source).is_dir()
    ]
    if legacy:
        return legacy

    level = level_for_tier(tier)
    if level is None:
        return []
    p = root / SOURCE_DIR_NAMES[source] / level
    return [p] if p.is_dir() else []


def list_sources(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """Source yang benar-benar punya folder on-disk di satu tier (di tanggal
    mana pun, termasuk cache granule untuk tier raw)."""
    tier = normalize_tier(tier)
    allowed = TIER_SOURCES[tier]
    if not allowed:
        return []
    root = get_dataset_root(dataset_id, dataset_name)
    found = set()
    for d in tier_dirs_under(root, tier):
        # Layout lama: d adalah {tanggal}/{tier}/, source ada di dalamnya.
        found.update(s for s in allowed if (d / s).is_dir())
        # Layout baru: d SUDAH {source}/{LEVEL}/, jadi source-nya nama kakeknya.
        src = DIR_NAME_TO_SOURCE.get(d.parent.name)
        if src in allowed and any(f.is_file() for f in d.iterdir()):
            found.add(src)
    return [s for s in allowed if s in found]


def list_scenes(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[str]:
    """Semua kunci scene satu source pada satu tier, lintas tanggal.

    Folder scene di bawah {tanggal}/{tier}/{source}/ (product_identifier S1)
    dikembalikan apa adanya; file yang duduk langsung di folder source
    (MODIS/GPM harian) berarti kunci scene-nya adalah tanggal itu sendiri.
    Folder berawalan "_" dilewati (bukan scene)."""
    scenes: set[str] = set()
    for src_dir in _source_dirs(dataset_id, dataset_name, tier, source):
        legacy_date = src_dir.parent.parent.name
        for entry in src_dir.iterdir():
            if entry.name.startswith(("_", ".")):
                continue
            if entry.is_dir():
                scenes.add(entry.name)
            elif entry.is_file():
                if _DATE_DIR_RE.match(legacy_date):
                    scenes.add(legacy_date)          # layout lama
                else:
                    # Layout baru: tidak ada folder scene, jadi kunci scene
                    # adalah tanggal yang tertanam di nama berkas. Dua scene
                    # Sentinel-1 pada hari yang sama karena itu berbagi satu
                    # kunci -- konsekuensi yang disengaja dari menghilangkan
                    # folder scene, dan sejalan dengan file browser yang
                    # memang mengelompokkan per tanggal.
                    d = date_from_filename(entry.name)
                    if d:
                        scenes.add(d)
    return sorted(scenes)


def list_loose_files(
    dataset_id: int, dataset_name: str, tier: str, source: str
) -> list[Path]:
    """Cache granule mentah `_granule_cache/modis/` dan `_granule_cache/gpm/`
    (lihat FLAT_RAW_SOURCES) — file tier raw yang tidak milik satu tanggal,
    jadi tidak pernah muncul lewat `list_scenes`. Kosong untuk kombinasi lain."""
    tier, source = validate_tier_source(tier, source)
    if tier != "raw" or source not in FLAT_RAW_SOURCES:
        return []
    p = get_granule_cache_dir(dataset_id, dataset_name, source)
    if not p.exists():
        return []
    return sorted(f for f in p.iterdir() if f.is_file() and not f.name.startswith("."))


def list_sourceless_scenes(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """Tanggal yang punya folder satu tier lintas-source (`fusion`,
    `preview`), yang isinya langsung {tanggal}/{tier}/ tanpa level {source}."""
    tier = normalize_tier(tier)
    if TIER_SOURCES[tier]:
        raise ValueError(
            f"Tier {tier!r} punya level source — pakai list_scenes(tier, source)."
        )
    root = get_dataset_root(dataset_id, dataset_name)

    legacy = [d.name for d in list_date_dirs(root) if (d / tier).is_dir()]
    if legacy:
        return legacy

    # Layout baru: satu folder lintas-tanggal, jadi "scene" dibaca dari
    # tanggal yang tertanam di nama berkas.
    base = root / (FUSION_DIRNAME if tier in _FUSION_TIER_NAMES else PREVIEW_DIRNAME)
    if not base.is_dir():
        return []
    return sorted({
        d for d in (date_from_filename(f.name) for f in base.rglob("*") if f.is_file())
        if d
    })


def list_fusion_scenes(dataset_id: int, dataset_name: str) -> list[str]:
    return list_sourceless_scenes(dataset_id, dataset_name, "fusion")


def list_preview_scenes(dataset_id: int, dataset_name: str) -> list[str]:
    return list_sourceless_scenes(dataset_id, dataset_name, "preview")


def _files_under(p: Path) -> list[Path]:
    if not p.exists():
        return []
    return sorted(f for f in p.rglob("*") if f.is_file())


def get_scene_files(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> list[Path]:
    """Semua file di dalam satu scene pada satu source/tier.

    Untuk kunci tanggal (MODIS/GPM) folder scene = folder source itu sendiri,
    jadi yang dihitung cuma file langsung di sana — folder scene S1 yang
    kebetulan satu tanggal tidak ikut terbawa."""
    p = get_scene_dir(dataset_id, dataset_name, tier, source, scene_key)
    if not p.exists():
        return []

    if level_for_tier(tier) is not None:
        # Laci final memuat semua tanggal sekaligus, jadi scene disaring lewat
        # nama berkas. Untuk kunci tanggal (MODIS/GPM) itu tanggalnya; untuk
        # Sentinel-1 product_identifier-nya ikut jadi prefiks nama berkas,
        # sehingga dua scene di hari yang sama tetap bisa dipisahkan.
        slug = scene_slug(scene_key)
        want_date = scene_date_key(scene_key)
        return sorted(
            f for f in p.iterdir()
            if f.is_file() and (
                slug in f.name
                or f.name.startswith(slug.removesuffix(".SAFE"))
                or (_DATE_DIR_RE.match(slug) and date_from_filename(f.name) == want_date)
            )
        )

    if _DATE_DIR_RE.match(scene_slug(scene_key)):
        return sorted(f for f in p.iterdir() if f.is_file())
    return _files_under(p)


def get_fusion_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    return _files_under(get_fusion_dir(dataset_id, dataset_name, scene_key))


def get_preview_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    """Semua berkas satu scene preview, termasuk yang ada di dalam subfolder
    {LEVEL}/{grayscale,colored,composite}/ (_files_under rglob rekursif)."""
    return _files_under(get_preview_dir(dataset_id, dataset_name, scene_key))


def get_preview_date_files(
    dataset_id: int, dataset_name: str, date_key_str: str
) -> list[Path]:
    """Berkas preview yang benar-benar milik SATU tanggal.

    Beda dari get_preview_scene_files, yang mengembalikan seluruh isi folder
    preview dataset: folder itu dipakai bersama semua tanggal, jadi memakainya
    untuk melaporkan ukuran satu tanggal akan melaporkan ukuran seluruh
    dataset di setiap tanggal. Berkas tanpa tanggal di namanya (sidecar
    bersama dari render lama) tidak ikut — dia bukan milik tanggal mana pun
    secara pasti."""
    want = scene_date_key(date_key_str)
    return [
        f for f in _files_under(get_preview_dir(dataset_id, dataset_name, date_key_str))
        if date_from_filename(f.name) == want
    ]


def get_sourceless_scene_files(
    dataset_id: int, dataset_name: str, tier: str, scene_key: str
) -> list[Path]:
    """Versi generik get_fusion_scene_files/get_preview_scene_files, untuk
    pemanggil yang tier-nya baru diketahui saat runtime (mis. API listing)."""
    tier = normalize_tier(tier)
    if TIER_SOURCES[tier]:
        raise ValueError(f"Tier {tier!r} punya level source — pakai get_scene_files().")
    root = get_dataset_root(dataset_id, dataset_name)
    legacy_dir = root / scene_date_key(scene_key) / tier
    if legacy_dir.is_dir():
        return _files_under(legacy_dir)

    # Layout baru: satu folder untuk semua tanggal, jadi disaring per berkas.
    base = root / (FUSION_DIRNAME if tier in _FUSION_TIER_NAMES else PREVIEW_DIRNAME)
    want = scene_date_key(scene_key)
    return sorted(
        f for f in _files_under(base)
        if date_from_filename(f.name) in (want, None)
    )


def get_source_files(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[Path]:
    """Semua file satu source di satu tier, lintas tanggal (semua scene +
    cache granule untuk raw)."""
    files: list[Path] = []
    for d in _source_dirs(dataset_id, dataset_name, tier, source):
        files.extend(_files_under(d))
    files.extend(list_loose_files(dataset_id, dataset_name, tier, source))
    return sorted(files)


def get_tier_files(dataset_id: int, dataset_name: str, tier: str) -> list[Path]:
    """Semua file di dalam satu tier, lintas tanggal dan source."""
    files: list[Path] = []
    for d in tier_dirs_under(get_dataset_root(dataset_id, dataset_name), tier):
        files.extend(_files_under(d))
    return sorted(files)


def _size_of(files: list[Path]) -> int:
    total = 0
    for f in files:
        try:
            total += f.stat().st_size
        except OSError:
            # File bisa hilang di antara rglob dan stat kalau cleanup jalan
            # bersamaan — hitung 0 daripada menjatuhkan seluruh ringkasan.
            continue
    return total


def storage_breakdown(dataset_id: int, dataset_name: str) -> dict:
    """Ringkasan pemakaian disk satu dataset, dipecah per tier lalu per source.

    Satu-satunya tempat angka ini dihitung: orchestrator memakainya untuk
    metadata.json dan API storage memakainya untuk respons-nya, jadi keduanya
    tidak bisa berbeda. Semua ukuran dalam byte; pemanggil yang mau MB
    membaginya sendiri supaya tidak ada pembulatan ganda.
    """
    tiers: dict[str, dict] = {}
    per_source: dict[str, dict] = {}

    for tier in TIERS:
        allowed = TIER_SOURCES[tier]
        sources: dict[str, dict] = {}

        if allowed:
            for source in allowed:
                files = get_source_files(dataset_id, dataset_name, tier, source)
                if not files:
                    continue
                size = _size_of(files)
                sources[source] = {
                    "size_bytes": size,
                    "file_count": len(files),
                    "scene_count": len(list_scenes(dataset_id, dataset_name, tier, source)),
                }
                agg = per_source.setdefault(source, {"size_bytes": 0, "file_count": 0})
                agg["size_bytes"] += size
                agg["file_count"] += len(files)
            scene_count = sum(v["scene_count"] for v in sources.values())
        else:
            # Tier fusion/preview: lintas-source, tidak punya pecahan per source.
            scene_count = len(list_sourceless_scenes(dataset_id, dataset_name, tier))

        files = get_tier_files(dataset_id, dataset_name, tier)
        size = _size_of(files)
        tiers[tier] = {
            "size_bytes": size,
            "file_count": len(files),
            "scene_count": scene_count,
            "sources": sources,
        }
        if not allowed and files:
            # Tier lintas-source dilaporkan sebagai "source" semu bernama sama
            # dengan tier-nya ("fusion", "preview") — dia memang tidak bisa
            # dipecah ke sentinel1/modis/gpm, tapi tanpa baris ini ukurannya
            # hilang dari ringkasan per-source dan totalnya tidak menjumlah.
            agg = per_source.setdefault(tier, {"size_bytes": 0, "file_count": 0})
            agg["size_bytes"] += size
            agg["file_count"] += len(files)

    return {
        "tiers": tiers,
        "sources": per_source,
        "total_size_bytes": sum(t["size_bytes"] for t in tiers.values()),
        "total_file_count": sum(t["file_count"] for t in tiers.values()),
    }


def write_dataset_metadata(dataset_id: int, dataset_name: str, metadata: dict) -> Path:
    """Tulis metadata.json level-dataset (ringkasan, bukan sumber kebenaran —
    DB tetap authoritative)."""
    path = get_dataset_metadata_path(dataset_id, dataset_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(metadata, f, indent=2, default=str)
    tmp.replace(path)
    return path


def read_dataset_metadata(dataset_id: int, dataset_name: str) -> dict | None:
    """Baca metadata.json level-dataset, None kalau belum pernah ditulis."""
    path = get_dataset_metadata_path(dataset_id, dataset_name)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("[FM] metadata.json dataset_id=%d tidak terbaca: %s", dataset_id, exc)
        return None
