# etl/folder_manager.py
"""
Path management utility untuk struktur penyimpanan data per dataset.

Layout on-disk:
    data/datasets/{dataset_id}_{slug(dataset_name)}/
        metadata.json
        raw/
            sentinel1/{scene}/          # .SAFE.zip + TIFF hasil ekstrak per band
            modis/                      # cache granule .hdf mentah (flat, lintas tanggal)
            gpm/                        # cache granule .nc4 mentah (flat, lintas tanggal)
        bronze/
            sentinel1/{scene}/
        silver/
            sentinel1/{scene}/
            modis/{scene}/
            gpm/{scene}/
        gold/
            sentinel1/{scene}/
            modis/{scene}/
            gpm/{scene}/
        fusion/
            {scene}/                    # lintas-source, jadi tidak punya level source
        preview/
            {scene}/                    # lintas-source juga
                grayscale/              # PNG stretch persentil, 1 kanal + alpha
                colored/                # PNG colormap/false-color RGBA

`{scene}` untuk file Sentinel-1 adalah product_identifier scene tersebut
(sudah unik termasuk jam:menit:detik). Untuk artefak yang tidak terikat ke
satu scene S1 tertentu (MODIS/GPM harian, dan output fusion yang di-dedup per
tanggal — lihat module9_fusion.py), `{scene}` adalah tanggal akuisisi dalam
format YYYYMMDD.

Level `{source}` ada di setiap tier kecuali `fusion` dan `preview`: keduanya
justru *gabungan* dari semua source, jadi memberinya satu folder source akan
menyesatkan. Semua fungsi di sini menolak kombinasi tier/source yang tidak
valid alih-alih diam-diam menulis ke tempat yang salah.

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
# bronze cuma dipakai Sentinel-1 (crop AOI) — MODIS/GPM langsung dari granule
# mentah di raw/ ke produk harian di silver/, tanpa tahap crop terpisah.
TIER_SOURCES: dict[str, tuple[str, ...]] = {
    "raw": SOURCES,
    "bronze": ("sentinel1",),
    "silver": SOURCES,
    "gold": SOURCES,
    "preview": (),
    "fusion": (),
}

# Tier yang tidak punya level source: langsung {tier}/{scene}/.
SOURCELESS_TIERS: tuple[str, ...] = tuple(t for t, s in TIER_SOURCES.items() if not s)

# Subfolder di dalam satu scene preview. Urutannya ikut dipakai
# module10_generate_preview.py sebagai urutan tampil.
PREVIEW_KINDS: tuple[str, ...] = ("grayscale", "colored")

# Source yang file mentahnya berupa cache granule flat (bukan per-scene):
# satu granule GPM harian ikut dipakai window 72h/7d tanggal berikutnya, jadi
# tidak bisa dimiliki satu folder tanggal saja.
FLAT_RAW_SOURCES: frozenset[str] = frozenset({"modis", "gpm"})

DATA_ROOT = Path("data") / "datasets"

# Nama folder source <-> nilai kolom data_products.source.
SOURCE_DB_VALUES: dict[str, str] = {
    "sentinel1": "SENTINEL1",
    "modis": "MODIS",
    "gpm": "GPM",
}
FUSION_DB_SOURCE = "FUSION"


def slugify(name: str) -> str:
    """Nama dataset -> slug aman-filesystem, mis. "hakim d1" -> "hakim_d1".
    Sama persis dengan etl/pipeline_logger.py:_slug_filename supaya nama
    folder dataset & nama file log tetap konsisten."""
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


def get_tier_dir(dataset_id: int, dataset_name: str, tier: str) -> Path:
    """Path folder satu tier. Untuk tier ber-source, isinya subfolder per
    source; untuk `fusion`, langsung subfolder per scene."""
    return get_dataset_root(dataset_id, dataset_name) / normalize_tier(tier)


def get_source_dir(dataset_id: int, dataset_name: str, tier: str, source: str) -> Path:
    """Path folder satu source di dalam satu tier, mis. silver/modis/."""
    tier = normalize_tier(tier)
    source = normalize_source(source)
    allowed = TIER_SOURCES[tier]
    if not allowed:
        raise ValueError(
            f"Tier {tier!r} tidak punya level source (dia gabungan semua "
            f"source). Pakai get_fusion_dir()."
        )
    if source not in allowed:
        raise ValueError(
            f"Source {source!r} tidak dipakai di tier {tier!r}. Valid: {allowed}"
        )
    return get_tier_dir(dataset_id, dataset_name, tier) / source


def get_scene_dir(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> Path:
    """
    Path folder satu scene, di dalam satu source, di dalam satu tier.

    get_scene_dir(2, "Hakim D1", "silver", "sentinel1", "S1A_IW_GRDH_...")
        -> data/datasets/2_hakim_d1/silver/sentinel1/S1A_IW_GRDH_...
    get_scene_dir(2, "Hakim D1", "gold", "modis", "20240115")
        -> data/datasets/2_hakim_d1/gold/modis/20240115
    """
    return get_source_dir(dataset_id, dataset_name, tier, source) / scene_slug(scene_key)


def ensure_scene_dir(
    dataset_id: int, dataset_name: str, tier: str, source: str, scene_key: str
) -> Path:
    """Buat (jika belum ada) dan kembalikan folder satu scene."""
    p = get_scene_dir(dataset_id, dataset_name, tier, source, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_fusion_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder output fusion untuk satu tanggal: fusion/{YYYYMMDD}/.
    Tier fusion tidak punya level source — isinya justru gabungan
    sentinel1 + modis + gpm."""
    return get_tier_dir(dataset_id, dataset_name, "fusion") / scene_slug(scene_key)


def ensure_fusion_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    p = get_fusion_dir(dataset_id, dataset_name, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_preview_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder preview untuk satu tanggal: preview/{YYYYMMDD}/.

    Sama seperti fusion, tier ini lintas-source: satu folder tanggal memuat
    render dari sentinel1 + modis + gpm sekaligus, jadi tidak punya level
    {source}. Di dalamnya ada subfolder per `PREVIEW_KINDS`."""
    return get_tier_dir(dataset_id, dataset_name, "preview") / scene_slug(scene_key)


def ensure_preview_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    p = get_preview_dir(dataset_id, dataset_name, scene_key)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_preview_kind_dir(
    dataset_id: int, dataset_name: str, scene_key: str, kind: str
) -> Path:
    """Subfolder satu jenis render: preview/{YYYYMMDD}/{grayscale|colored}/."""
    if kind not in PREVIEW_KINDS:
        raise ValueError(f"Jenis preview tidak valid: {kind!r}. Valid: {PREVIEW_KINDS}")
    return get_preview_dir(dataset_id, dataset_name, scene_key) / kind


def ensure_preview_kind_dir(
    dataset_id: int, dataset_name: str, scene_key: str, kind: str
) -> Path:
    p = get_preview_kind_dir(dataset_id, dataset_name, scene_key, kind)
    p.mkdir(parents=True, exist_ok=True)
    return p


def get_scratch_dir(dataset_id: int, dataset_name: str, scene_key: str) -> Path:
    """Folder kerja sementara (hasil kalibrasi radiometrik sebelum crop),
    dihapus otomatis setelah tahap CROP selesai — bukan bagian dari tier
    resmi, jadi diletakkan di luar semuanya."""
    return get_dataset_root(dataset_id, dataset_name) / "_work" / scene_slug(scene_key)


def get_granule_cache_dir(dataset_id: int, dataset_name: str, source: str) -> Path:
    """Folder cache granule mentah MODIS/GPM (.hdf/.nc4 sebelum
    di-mosaic/crop). Flat, bukan per-scene: satu granule GPM harian ikut
    dipakai window 72h/7d tanggal-tanggal berikutnya, jadi tidak bisa
    dimiliki satu folder tanggal saja."""
    source = normalize_source(source)
    if source not in FLAT_RAW_SOURCES:
        raise ValueError(
            f"Source {source!r} tidak pakai cache granule flat. "
            f"Valid: {sorted(FLAT_RAW_SOURCES)}"
        )
    return get_source_dir(dataset_id, dataset_name, "raw", source)


def list_sources(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """Source yang benar-benar punya folder on-disk di satu tier."""
    tier = normalize_tier(tier)
    if not TIER_SOURCES[tier]:
        return []
    root = get_tier_dir(dataset_id, dataset_name, tier)
    if not root.exists():
        return []
    return [s for s in TIER_SOURCES[tier] if (root / s).is_dir()]


def list_scenes(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[str]:
    """Semua nama folder scene di satu source pada satu tier. Folder
    berawalan "_" dilewati (bukan scene)."""
    p = get_source_dir(dataset_id, dataset_name, tier, source)
    if not p.exists():
        return []
    return sorted(d.name for d in p.iterdir() if d.is_dir() and not d.name.startswith("_"))


def list_loose_files(
    dataset_id: int, dataset_name: str, tier: str, source: str
) -> list[Path]:
    """File yang duduk langsung di folder source, tanpa folder scene di
    antaranya — yaitu cache granule mentah `raw/modis/` dan `raw/gpm/`
    (lihat FLAT_RAW_SOURCES). `list_scenes` cuma mengembalikan direktori,
    jadi tanpa fungsi ini granule-granule itu tak pernah muncul di listing
    berkas meski ikut terhitung di `storage_breakdown`."""
    p = get_source_dir(dataset_id, dataset_name, tier, source)
    if not p.exists():
        return []
    return sorted(f for f in p.iterdir() if f.is_file() and not f.name.startswith("."))


def list_sourceless_scenes(dataset_id: int, dataset_name: str, tier: str) -> list[str]:
    """Folder scene di satu tier lintas-source (`fusion`, `preview`), yang
    isinya langsung {tier}/{scene}/ tanpa level {source} di antaranya."""
    tier = normalize_tier(tier)
    if TIER_SOURCES[tier]:
        raise ValueError(
            f"Tier {tier!r} punya level source — pakai list_scenes(tier, source)."
        )
    p = get_tier_dir(dataset_id, dataset_name, tier)
    if not p.exists():
        return []
    return sorted(d.name for d in p.iterdir() if d.is_dir() and not d.name.startswith("_"))


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
    """Semua file di dalam satu scene pada satu source/tier."""
    return _files_under(get_scene_dir(dataset_id, dataset_name, tier, source, scene_key))


def get_fusion_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    return _files_under(get_fusion_dir(dataset_id, dataset_name, scene_key))


def get_preview_scene_files(dataset_id: int, dataset_name: str, scene_key: str) -> list[Path]:
    """Semua berkas satu scene preview, termasuk yang ada di dalam
    subfolder grayscale/ dan colored/ (_files_under rglob rekursif)."""
    return _files_under(get_preview_dir(dataset_id, dataset_name, scene_key))


def get_sourceless_scene_files(
    dataset_id: int, dataset_name: str, tier: str, scene_key: str
) -> list[Path]:
    """Versi generik get_fusion_scene_files/get_preview_scene_files, untuk
    pemanggil yang tier-nya baru diketahui saat runtime (mis. API listing)."""
    tier = normalize_tier(tier)
    if TIER_SOURCES[tier]:
        raise ValueError(f"Tier {tier!r} punya level source — pakai get_scene_files().")
    return _files_under(get_tier_dir(dataset_id, dataset_name, tier) / scene_slug(scene_key))


def get_source_files(dataset_id: int, dataset_name: str, tier: str, source: str) -> list[Path]:
    """Semua file satu source di satu tier (semua scene + cache granule)."""
    return _files_under(get_source_dir(dataset_id, dataset_name, tier, source))


def get_tier_files(dataset_id: int, dataset_name: str, tier: str) -> list[Path]:
    """Semua file di dalam satu tier, lintas source."""
    return _files_under(get_tier_dir(dataset_id, dataset_name, tier))


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
