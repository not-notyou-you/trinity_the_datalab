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

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

TIERS: tuple[str, ...] = ("raw", "bronze", "silver", "gold", "preview", "fusion")

SOURCES: tuple[str, ...] = ("sentinel1", "modis", "gpm")

# Tier fusion dan preview sengaja dipetakan ke tuple kosong: keduanya
# lintas-source.
# bronze dipakai SEMUA source sejak model per-satelit (DOCS/ETL.md): dia tempat
# artefak level RAW tiap sumber berhenti — S1 hasil crop AOI, MODIS peta banjir
# tanpa indeks turunan, GPM curah hujan harian tanpa window akumulasi.
TIER_SOURCES: dict[str, tuple[str, ...]] = {
    "raw": SOURCES,
    "bronze": SOURCES,
    "silver": SOURCES,
    "gold": SOURCES,
    "preview": (),
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
    if t not in TIERS:
        raise ValueError(f"Tier tidak valid: {tier!r}. Valid: {TIERS}")
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
    """Folder tanggal di bawah satu root dataset, urut kronologis. Menerima
    path langsung supaya bisa dipakai pemanggil yang menyapu data/datasets/
    tanpa tahu id/nama dataset (api/routes/storage.py)."""
    if not dataset_root.is_dir():
        return []
    return sorted(
        d for d in dataset_root.iterdir() if d.is_dir() and _DATE_DIR_RE.match(d.name)
    )


def list_dates(dataset_id: int, dataset_name: str) -> list[str]:
    """Tanggal (YYYYMMDD) yang punya folder di disk untuk dataset ini."""
    return [d.name for d in list_date_dirs(get_dataset_root(dataset_id, dataset_name))]


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

    get_scene_dir(2, "Hakim D1", "silver", "sentinel1", "S1A_IW_GRDH_1SDV_20240115T...")
        -> data/datasets/2_Hakim_D1/20240115/silver/sentinel1/S1A_IW_GRDH_1SDV_20240115T...
    get_scene_dir(2, "Hakim D1", "gold", "modis", "20240115")
        -> data/datasets/2_Hakim_D1/20240115/gold/modis
    """
    source_dir = get_source_dir(
        dataset_id, dataset_name, scene_date_key(scene_key), tier, source
    )
    slug = scene_slug(scene_key)
    # Kunci scene yang memang tanggal itu sendiri (MODIS/GPM harian) tidak
    # diberi folder kedua — {tanggal}/gold/modis/{tanggal}/ cuma redundan.
    return source_dir if _DATE_DIR_RE.match(slug) else source_dir / slug


def ensure_scene_dir(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> Path:
    """Buat (jika belum ada) dan kembalikan folder satu scene."""
    p = get_scene_dir(dataset_id, dataset_name, tier, source, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_fusion_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder output fusion untuk satu tanggal: {YYYYMMDD}/fusion/.
    Tier fusion tidak punya level source — isinya justru gabungan
    sentinel1 + modis + gpm."""
    return get_tier_dir(dataset_id, dataset_name, scene_date_key(scene_key), "fusion")


def ensure_fusion_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    p = get_fusion_dir(dataset_id, dataset_name, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_preview_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder preview untuk satu tanggal: {YYYYMMDD}/preview/.

    Sama seperti fusion, tier ini lintas-source: satu folder tanggal memuat
    render dari sentinel1 + modis + gpm sekaligus, jadi tidak punya level
    {source}. Di dalamnya ada subfolder per level lalu per `PREVIEW_KINDS`."""
    return get_tier_dir(dataset_id, dataset_name, scene_date_key(scene_key), "preview")


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
    return get_dataset_root(dataset_id, dataset_name) / SCRATCH_DIRNAME / scene_slug(scene_key)


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
    dirs = [d / tier for d in list_date_dirs(dataset_root) if (d / tier).is_dir()]
    if tier == "raw":
        cache = get_granule_cache_root(dataset_root)
        if cache.is_dir():
            dirs.append(cache)
    return dirs


def _source_dirs(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[Path]:
    """Folder {tanggal}/{tier}/{source}/ yang ada di disk, lintas tanggal."""
    tier, source = validate_tier_source(tier, source)
    root = get_dataset_root(dataset_id, dataset_name)
    return [
        d / tier / source for d in list_date_dirs(root) if (d / tier / source).is_dir()
    ]


def list_sources(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """Source yang benar-benar punya folder on-disk di satu tier (di tanggal
    mana pun, termasuk cache granule untuk tier raw)."""
    tier = normalize_tier(tier)
    allowed = TIER_SOURCES[tier]
    if not allowed:
        return []
    found = set()
    for d in tier_dirs_under(get_dataset_root(dataset_id, dataset_name), tier):
        found.update(s for s in allowed if (d / s).is_dir())
    return [s for s in allowed if s in found]


def list_scenes(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[str]:
    """Semua kunci scene satu source pada satu tier, lintas tanggal.

    Folder scene di bawah {tanggal}/{tier}/{source}/ (product_identifier S1)
    dikembalikan apa adanya; file yang duduk langsung di folder source
    (MODIS/GPM harian) berarti kunci scene-nya adalah tanggal itu sendiri.
    Folder berawalan "_" dilewati (bukan scene)."""
    scenes: set[str] = set()
    for src_dir in _source_dirs(dataset_id, dataset_name, tier, source):
        for entry in src_dir.iterdir():
            if entry.name.startswith(("_", ".")):
                continue
            if entry.is_dir():
                scenes.add(entry.name)
            elif entry.is_file():
                scenes.add(src_dir.parent.parent.name)
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
    return [d.name for d in list_date_dirs(root) if (d / tier).is_dir()]


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
    if _DATE_DIR_RE.match(scene_slug(scene_key)):
        if not p.exists():
            return []
        return sorted(f for f in p.iterdir() if f.is_file())
    return _files_under(p)


def get_fusion_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    return _files_under(get_fusion_dir(dataset_id, dataset_name, scene_key))


def get_preview_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    """Semua berkas satu scene preview, termasuk yang ada di dalam subfolder
    {LEVEL}/{grayscale,colored,composite}/ (_files_under rglob rekursif)."""
    return _files_under(get_preview_dir(dataset_id, dataset_name, scene_key))


def get_sourceless_scene_files(
    dataset_id: int, dataset_name: str, tier: str, scene_key: str
) -> list[Path]:
    """Versi generik get_fusion_scene_files/get_preview_scene_files, untuk
    pemanggil yang tier-nya baru diketahui saat runtime (mis. API listing)."""
    tier = normalize_tier(tier)
    if TIER_SOURCES[tier]:
        raise ValueError(f"Tier {tier!r} punya level source — pakai get_scene_files().")
    return _files_under(
        get_tier_dir(dataset_id, dataset_name, scene_date_key(scene_key), tier)
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
