# etl/processing_plan.py
"""
Rencana pemrosesan per-satelit — terjemahan `dataset_source_config` menjadi
keputusan konkret yang dipakai pipeline.

Model per-satelit (DOCS/DESIGN.md, DOCS/ETL.md) menyatakan setiap sumber punya
definisi RAW/PROCESSED-nya sendiri:

    SENTINEL1  RAW        calibrate + reproject + crop            -> BRONZE
               PROCESSED  + Lee filter + QA + COG                 -> SILVER, GOLD
    MODIS      RAW        flood map saja                          -> BRONZE
               PROCESSED  + NDVI + NDWI                           -> SILVER, GOLD
    GPM        RAW        curah hujan harian (1 hari)             -> BRONZE
               PROCESSED  + window akumulasi 24h/72h/7d (7 hari)  -> SILVER, GOLD

Modul ini adalah SATU-SATUNYA tempat aturan itu ditulis sebagai kode. Tanpa
itu, tiap modul (orchestrator, module7, module8, module9) akan menafsirkan
sendiri arti "RAW" untuk sumbernya, dan tafsiran itu pasti akan berbeda begitu
salah satunya diubah. Modul ini sengaja tidak mengimpor apa pun dari modul ETL
lain supaya bisa diimpor dari mana saja tanpa siklus impor.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

RAW = "RAW"
PROCESSED = "PROCESSED"
LEVEL_ORDER: tuple[str, ...] = (RAW, PROCESSED)

SENTINEL1 = "SENTINEL1"
MODIS = "MODIS"
GPM = "GPM"
SOURCE_ORDER: tuple[str, ...] = (SENTINEL1, MODIS, GPM)

# Tahap Sentinel-1 yang jalan di level apa pun. DOWNLOAD/CALIBRATE/CROP tidak
# bisa dilewati: nilai DN mentah tanpa LUT sigma-nought tidak punya arti fisik,
# jadi "RAW" untuk SAR tetap berarti terkalibrasi (DOCS/ETL.md).
S1_BASE_STAGES: tuple[str, ...] = ("DOWNLOAD", "CALIBRATE", "CROP")
# Tahap yang hanya jalan kalau PROCESSED diminta.
S1_PROCESSED_STAGES: tuple[str, ...] = ("LEE_FILTER", "QUALITY_ANALYTICS", "GOLD_EXPORT")

# Tier yang dihasilkan tiap level. Sama dengan _TIERS_BY_LEVEL di
# database_client.derive_required_tiers — keduanya harus sepakat.
_TIERS_BY_LEVEL: dict[str, frozenset[str]] = {
    RAW: frozenset({"RAW", "BRONZE"}),
    PROCESSED: frozenset({"RAW", "BRONZE", "SILVER", "GOLD"}),
}

# Band/window yang MERUPAKAN artefak RAW sumbernya. Sisanya turunan, jadi cuma
# ada di jalur PROCESSED.
MODIS_RAW_BANDS: tuple[str, ...] = ("FLOOD",)
MODIS_PROCESSED_BANDS: tuple[str, ...] = ("FLOOD", "NDVI", "NDWI")

GPM_RAW_WINDOWS: tuple[str, ...] = ("24h",)
GPM_PROCESSED_WINDOWS: tuple[str, ...] = ("24h", "72h", "7d")

# Jumlah hari granule yang perlu diunduh GPM. Window 7d menjumlahkan hari
# target + 6 hari sebelumnya; jalur RAW cuma butuh hari itu sendiri.
GPM_RAW_DAYS = 1
GPM_PROCESSED_DAYS = 7


def normalize_levels(levels) -> tuple[str, ...]:
    """Bersihkan daftar level menjadi tuple kanonik ("RAW" dulu, lalu
    "PROCESSED"). Level tak dikenal dibuang dengan warning — pemanggilnya
    adalah pipeline, dan menjatuhkan seluruh job karena satu nilai asing di
    kolom array lebih buruk daripada memproses sisanya."""
    if isinstance(levels, str):
        levels = [levels]
    seen: set[str] = set()
    for raw in levels or ():
        value = str(raw).strip().upper()
        if value in LEVEL_ORDER:
            seen.add(value)
        else:
            logger.warning("[PLAN] level tidak dikenal diabaikan: %r", raw)
    return tuple(level for level in LEVEL_ORDER if level in seen)


@dataclass(frozen=True)
class SourcePlan:
    """Apa yang harus dijalankan untuk SATU sumber pada sebuah dataset."""

    source_name: str
    levels: tuple[str, ...]

    # -- pertanyaan dasar --------------------------------------------------
    @property
    def has_raw(self) -> bool:
        return RAW in self.levels

    @property
    def has_processed(self) -> bool:
        return PROCESSED in self.levels

    @property
    def raw_only(self) -> bool:
        return self.has_raw and not self.has_processed

    @property
    def max_tier(self) -> str:
        return "GOLD" if self.has_processed else "BRONZE"

    def tiers(self) -> frozenset[str]:
        """Tier yang boleh diproduksi sumber ini."""
        out: set[str] = set()
        for level in self.levels:
            out |= _TIERS_BY_LEVEL[level]
        return frozenset(out)

    # -- partisipasi di run lintas-sumber (fusion, preview) -----------------
    def level_for_run(self, run_level: str) -> str:
        """Level yang DIPAKAI sumber ini pada run bertanda `run_level`.

        Fusion dan preview menghasilkan satu set artefak per level (lihat
        ProcessingPlan.output_levels). Sebuah run bertanda PROCESSED tidak
        berarti setiap sumber punya PROCESSED: dataset
        sentinel1[RAW] + modis[PROCESSED] cuma menghasilkan SATU stack, dan di
        dalamnya S1 mau tidak mau ikut sebagai RAW.

        Aturannya: pakai `run_level` kalau sumber ini memang memilikinya,
        kalau tidak pakai level tertinggi yang dia punya. Fallback ke level
        tertinggi, bukan ke NaN/melewatkan sumbernya, karena user memang
        meminta sumber itu — menghilangkannya dari stack akan membuat
        konfigurasi campuran diam-diam kehilangan satu sensor.
        """
        level = str(run_level).strip().upper()
        if level in self.levels:
            return level
        return self.levels[-1]  # LEVEL_ORDER terurut, jadi ini yang tertinggi

    def tier_for_run(self, run_level: str) -> str:
        """Tier yang dibaca fusion/preview untuk sumber ini pada run itu.

        PROCESSED berakhir di GOLD (COG analysis-ready), RAW berhenti di
        BRONZE (DOCS/ETL.md, "Which input tier does fusion use?").
        """
        return "GOLD" if self.level_for_run(run_level) == PROCESSED else "BRONZE"

    @property
    def has_both_levels(self) -> bool:
        return self.has_raw and self.has_processed

    # -- penandaan data_products.processing_level --------------------------
    def level_for_tier(self, tier: str) -> str:
        """Nilai `data_products.processing_level` untuk artefak di `tier`.

        RAW/BRONZE ditandai RAW hanya kalau user memang meminta level RAW;
        kalau sumber ini murni PROCESSED, keduanya cuma langkah antara jalur
        penuh dan ikut ditandai PROCESSED. SILVER/GOLD selalu PROCESSED —
        tier itu tidak pernah lahir dari jalur RAW.
        """
        upper = str(tier).upper()
        if upper in _TIERS_BY_LEVEL[RAW] and self.has_raw:
            return RAW
        return PROCESSED

    # -- Sentinel-1 --------------------------------------------------------
    def s1_skip_stages(self) -> set[str]:
        """Tahap S1 yang harus dilewati untuk level sumber ini."""
        if self.has_processed:
            return set()
        return set(S1_PROCESSED_STAGES)

    # -- MODIS / GPM -------------------------------------------------------
    def modis_bands(self) -> tuple[str, ...]:
        return modis_bands_for(self.levels)

    def gpm_windows(self) -> tuple[str, ...]:
        return gpm_windows_for(self.levels)

    def gpm_days(self) -> int:
        return GPM_PROCESSED_DAYS if self.has_processed else GPM_RAW_DAYS

    def targets(self) -> dict[str, tuple[tuple[str, str], ...]]:
        """{band: ((tier, processing_level), ...)} untuk MODIS/GPM.

        Sebuah band bisa punya DUA target ketika sumbernya dikonfigurasi
        RAW+PROCESSED: FLOOD (atau rainfall 24h) adalah deliverable RAW di
        BRONZE sekaligus lapisan pertama jalur PROCESSED di SILVER, dan
        DOCS/ETL.md menyatakan kedua artefak hidup berdampingan di disk.
        """
        if self.source_name == MODIS:
            base_bands = MODIS_RAW_BANDS
            all_bands = MODIS_PROCESSED_BANDS
        elif self.source_name == GPM:
            base_bands = GPM_RAW_WINDOWS
            all_bands = GPM_PROCESSED_WINDOWS
        else:
            raise ValueError(
                f"targets() hanya untuk MODIS/GPM, bukan {self.source_name}"
            )

        out: dict[str, list[tuple[str, str]]] = {}
        if self.has_raw:
            for band in base_bands:
                out.setdefault(band, []).append(("BRONZE", RAW))
        if self.has_processed:
            for band in all_bands:
                out.setdefault(band, []).append(("SILVER", PROCESSED))
        return {band: tuple(targets) for band, targets in out.items()}

    def to_dict(self) -> dict:
        return {"source": self.source_name, "processing": list(self.levels)}

    def __repr__(self) -> str:
        return f"<SourcePlan {self.source_name} levels={list(self.levels)}>"


def modis_bands_for(levels) -> tuple[str, ...]:
    """Band MODIS yang perlu dibangun untuk kumpulan level ini."""
    levels = normalize_levels(levels)
    bands: list[str] = []
    if PROCESSED in levels:
        bands.extend(MODIS_PROCESSED_BANDS)
    elif RAW in levels:
        bands.extend(MODIS_RAW_BANDS)
    return tuple(dict.fromkeys(bands))


def gpm_windows_for(levels) -> tuple[str, ...]:
    """Window akumulasi GPM yang perlu dibangun untuk kumpulan level ini."""
    levels = normalize_levels(levels)
    if PROCESSED in levels:
        return GPM_PROCESSED_WINDOWS
    if RAW in levels:
        return GPM_RAW_WINDOWS
    return ()


@dataclass(frozen=True)
class ProcessingPlan:
    """Rencana lengkap sebuah dataset: sumber mana, sampai level apa."""

    dataset_id: int
    sources: dict[str, SourcePlan]

    def get(self, source_name: str) -> SourcePlan | None:
        return self.sources.get(str(source_name).strip().upper())

    def is_configured(self, source_name: str) -> bool:
        return self.get(source_name) is not None

    @property
    def source_names(self) -> tuple[str, ...]:
        return tuple(
            name for name in SOURCE_ORDER if name in self.sources
        ) + tuple(sorted(set(self.sources) - set(SOURCE_ORDER)))

    @property
    def source_count(self) -> int:
        return len(self.sources)

    def aux_sources(self) -> tuple[str, ...]:
        """Sumber non-S1 yang dikonfigurasi, urut MODIS lalu GPM."""
        return tuple(name for name in (MODIS, GPM) if name in self.sources)

    def required_tiers(self) -> frozenset[str]:
        out: set[str] = set()
        for plan in self.sources.values():
            out |= plan.tiers()
        return frozenset(out)

    def output_levels(self) -> tuple[str, ...]:
        """Level run lintas-sumber yang harus dihasilkan dataset ini.

        Satu run = satu set artefak turunan lintas-sumber (satu HDF5 fusion,
        satu folder preview). Bukan sekadar union level semua sumber:

          * Ada sumber yang diminta RAW **dan** PROCESSED sekaligus -> dua
            run. Itu justru inti ablation study: stack yang isinya sama persis
            kecuali satu sensor difilter/diturunkan, jadi keduanya harus
            berdiri sebagai berkas terpisah yang bisa dibandingkan.
          * Selain itu -> satu run. Dataset sentinel1[RAW] + modis[PROCESSED]
            tidak punya perbandingan untuk dibuat: dua run-nya akan berisi
            byte yang identik (tiap sumber cuma punya satu level), jadi yang
            kedua hanya menggandakan disk tanpa menambah informasi. Run
            tunggal itu ditandai PROCESSED kalau ada sumber PROCESSED di
            dalamnya, karena itu level tertinggi yang ikut.

        Level tiap sumber di dalam sebuah run diselesaikan
        SourcePlan.level_for_run().
        """
        if not self.sources:
            return ()
        if any(plan.has_both_levels for plan in self.sources.values()):
            return LEVEL_ORDER  # (RAW, PROCESSED)
        levels = {level for plan in self.sources.values() for level in plan.levels}
        return (PROCESSED,) if PROCESSED in levels else (RAW,)

    def source_levels_for_run(self, run_level: str) -> dict[str, str]:
        """{SOURCE: level} yang berlaku pada run `run_level`."""
        return {
            name: self.sources[name].level_for_run(run_level)
            for name in self.source_names
        }

    def fusion_eligible(self, fusion_strategy: str | None) -> bool:
        """Fusi jalan hanya kalau >1 sumber dikonfigurasi DAN ada strategi
        (DOCS/ETL.md, "Fusion Stage")."""
        return self.source_count > 1 and bool(fusion_strategy)

    def summary(self) -> dict[str, list[str]]:
        return {name: list(self.sources[name].levels) for name in self.source_names}

    def __repr__(self) -> str:
        inner = ", ".join(f"{n}={list(p.levels)}" for n, p in self.sources.items())
        return f"<ProcessingPlan dataset={self.dataset_id} {inner}>"


# Perilaku prototipe sebelum migrasi 017: selalu tiga sumber, selalu jalur
# penuh. Dipakai sebagai fallback supaya dataset tanpa baris
# dataset_source_config (mis. database uji yang dibuat lewat create_all tanpa
# menjalankan backfill migrasi) tidak diam-diam menghasilkan nol produk.
_LEGACY_FALLBACK: dict[str, tuple[str, ...]] = {
    name: (PROCESSED,) for name in SOURCE_ORDER
}


def plan_from_configs(dataset_id: int, configs: dict) -> ProcessingPlan:
    """Bangun ProcessingPlan dari {SOURCE_NAME: [levels]}.

    Sumber dengan daftar level kosong DIBUANG, bukan dianggap RAW: array
    kosong berarti "dipilih tapi tidak diproses sama sekali", state yang tidak
    punya arti (CHECK chk_source_config_levels_not_empty mencegahnya di
    database, ini jaring pengaman untuk pemanggil in-memory).
    """
    sources: dict[str, SourcePlan] = {}
    for raw_name, raw_levels in (configs or {}).items():
        name = str(raw_name).strip().upper()
        levels = normalize_levels(raw_levels)
        if not levels:
            logger.warning(
                "[PLAN] dataset_id=%s sumber %s tanpa level, dilewati", dataset_id, name
            )
            continue
        sources[name] = SourcePlan(source_name=name, levels=levels)
    return ProcessingPlan(dataset_id=dataset_id, sources=sources)


def load_processing_plan(db, dataset_id: int) -> ProcessingPlan:
    """Baca dataset_source_config dan ubah jadi ProcessingPlan.

    Dataset tanpa satu pun baris konfigurasi jatuh ke perilaku prototipe
    (tiga sumber, PROCESSED) dengan warning — bukan plan kosong, karena plan
    kosong akan membuat job "berhasil" tanpa menghasilkan apa pun.
    """
    rows = db.list_dataset_source_configs(dataset_id)
    if not rows:
        logger.warning(
            "[PLAN] dataset_id=%s tidak punya dataset_source_config; "
            "memakai default legacy %s", dataset_id, _LEGACY_FALLBACK,
        )
        return plan_from_configs(dataset_id, _LEGACY_FALLBACK)
    return plan_from_configs(
        dataset_id, {row.source_name: row.processing_levels for row in rows}
    )
