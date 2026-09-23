# etl/fusion_strategies.py
"""
Strategi fusi — terjemahan `datasets.fusion_strategy` menjadi dua keputusan
konkret yang selama ini tercampur jadi satu.

Sebelum modul ini, `fusion_strategy` cuma dicatat: ia ditulis ke
`f.attrs["fusion_strategy"]` dan ke `fusion_products`, tapi tidak pernah jadi
percabangan. Seluruh pipeline berjangkar pada scene S1 (orchestrator memanggil
`ensure_aux_inputs_for_date(tanggal_S1)`), sehingga MODIS/GPM tidak pernah
diunduh untuk tanggal tanpa scene S1 — artinya ketiga strategi menghasilkan
berkas yang identik kecuali satu atribut teks.

Kuncinya: sebuah strategi sebenarnya menjawab DUA pertanyaan yang berbeda.

    Sumbu 1 (UNDUH)  tanggal MODIS/GPM mana yang diambil dari server NASA?
    Sumbu 2 (RAKIT)  tanggal mana yang jadi satu berkas HDF5?

    CO_OCCURRENCE   unduh: tanggal S1 saja   rakit: per tanggal S1
    FULL_COVERAGE   unduh: setiap hari       rakit: per hari
    HYBRID          unduh: setiap hari       rakit: per tanggal S1

HYBRID jadi strategi ketiga yang sah justru karena ia memilih sumbu yang
berbeda dari keduanya: mengunduh seperti FULL_COVERAGE, merakit seperti
CO_OCCURRENCE. Itu persis definisi DOCS/PIPELINE.md ("auxiliary harian, S1 jadi
jangkar") — ia BUKAN "hasilkan kedua strategi sekaligus".

Konsekuensi yang perlu disadari: CO_OCCURRENCE sudah menjadi perilaku sistem
hari ini di kedua sumbu. Yang benar-benar baru di sini adalah sumbu unduh
harian, yang dipakai bersama oleh FULL_COVERAGE dan HYBRID.

Modul ini sengaja tidak mengimpor apa pun dari modul ETL lain (alasan sama
dengan etl/processing_plan.py): ia dipanggil orchestrator, module9, dan API,
jadi harus bebas siklus impor. Ia juga murni — tidak menyentuh disk maupun DB,
sehingga seluruh aturan temporal bisa diuji tanpa fixture.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as date_type, timedelta

logger = logging.getLogger(__name__)

CO_OCCURRENCE = "CO_OCCURRENCE"
FULL_COVERAGE = "FULL_COVERAGE"
HYBRID = "HYBRID"

# Toleransi pasangan S1 default untuk FULL_COVERAGE.
#
# FULL_COVERAGE merakit satu HDF5 per hari, termasuk hari yang tidak punya
# scene S1 sama sekali — jadi ia harus memutuskan seberapa jauh boleh meminjam
# scene S1 dari hari lain. Sentinel-1A sendirian punya revisit ~12 hari di
# ekuator, jadi memaksa same-day akan membuat hampir semua hari kehilangan S1.
# +/-2 hari menangkap pasangan yang masih relevan untuk dinamika banjir tanpa
# mengklaim simultanitas yang tidak ada; offset sebenarnya selalu dicatat di
# `s1_offset_days` supaya konsumen bisa menyaring lebih ketat belakangan.
DEFAULT_S1_MATCH_TOLERANCE_DAYS = 2

# Sufiks nama berkas dan nama subfolder per strategi. Dipisah dari logika
# perencanaan supaya menambah strategi keempat cukup menyentuh tiga tempat:
# satu cabang di plan_fusion, satu entri di kedua dict ini, dan satu nilai di
# CHECK constraint `chk_datasets_fusion_strategy`.
SUBFOLDER: dict[str, str] = {
    CO_OCCURRENCE: "co-occurrence",
    FULL_COVERAGE: "full-coverage",
    HYBRID: "hybrid",
}

FILENAME_SUFFIX: dict[str, str] = {
    CO_OCCURRENCE: "cooccurrence",
    FULL_COVERAGE: "fullcoverage",
    HYBRID: "hybrid",
}

STRATEGIES: tuple[str, ...] = (CO_OCCURRENCE, FULL_COVERAGE, HYBRID)


@dataclass(frozen=True)
class FusionPlan:
    """Keputusan kedua sumbu untuk satu dataset, sudah jadi tanggal konkret.

    `fusion_dates` adalah sumbu RAKIT: satu tanggal = satu berkas HDF5.
    `aux_dates` adalah sumbu UNDUH: tanggal MODIS/GPM yang perlu ada di disk.
    Keduanya sengaja dipisah karena HYBRID membuat mereka berbeda — ia butuh
    aux harian (20 tanggal) untuk menghasilkan segelintir berkas (2 tanggal).

    `anchor_for` memetakan tiap tanggal fusi ke tanggal scene S1 yang dipakai,
    atau None kalau tidak ada S1 dalam toleransi. None hanya mungkin muncul di
    FULL_COVERAGE: dua strategi lain berjangkar pada S1, jadi tanggal fusinya
    selalu berasal dari scene S1 yang nyata.
    """

    strategy: str
    subfolder: str
    fusion_dates: tuple[date_type, ...]
    aux_dates: tuple[date_type, ...]
    anchor_for: dict[date_type, date_type | None]
    tolerance_days: int

    def anchor(self, fusion_date: date_type) -> date_type | None:
        return self.anchor_for.get(fusion_date)

    def offset_days(self, fusion_date: date_type) -> int | None:
        """Selisih hari antara tanggal fusi dan scene S1 yang dipakai.

        Nilai inilah yang masuk ke `fusion_products.s1_offset_days` dan ke
        atribut HDF5: tanpa itu, fusi same-day dan fusi bertoleransi tidak
        bisa dibedakan lagi setelah berkasnya ditulis.
        """
        anchor = self.anchor_for.get(fusion_date)
        if anchor is None:
            return None
        return abs((fusion_date - anchor).days)

    @property
    def requires_s1(self) -> bool:
        """True kalau S1 yang hilang membatalkan tanggal fusi.

        CO_OCCURRENCE dan HYBRID berjangkar pada S1 — tanpa S1 tidak ada yang
        perlu difusikan. FULL_COVERAGE tetap menulis berkas tanpa group
        `sentinel1/`, karena justru deret harian MODIS/GPM-nya yang dicari.
        """
        return self.strategy in (CO_OCCURRENCE, HYBRID)


def _daterange(date_from: date_type, date_to: date_type) -> tuple[date_type, ...]:
    if date_to < date_from:
        return ()
    span = (date_to - date_from).days
    return tuple(date_from + timedelta(days=i) for i in range(span + 1))


def _nearest_s1(
    target: date_type, s1_dates: tuple[date_type, ...], tolerance_days: int
) -> date_type | None:
    """Scene S1 terdekat dari `target` dalam toleransi, atau None.

    Seri diurutkan lebih dulu oleh pemanggil, tapi pencarian ini tidak
    mengandalkan urutan — jumlah scene S1 per dataset berorde puluhan, jadi
    pemindaian linier lebih murah daripada menjaga invarian urutan di setiap
    pemanggil.

    Kalau dua scene berjarak sama (target di tengah), yang LEBIH AWAL menang.
    Ini sengaja deterministik: pilihan sewenang-wenang akan membuat output
    dataset yang sama berubah antar-run dan merusak reproduktibilitas yang
    dijanjikan lineage.
    """
    best: date_type | None = None
    best_gap: int | None = None
    for candidate in s1_dates:
        gap = abs((target - candidate).days)
        if gap > tolerance_days:
            continue
        if best_gap is None or gap < best_gap or (gap == best_gap and candidate < best):
            best, best_gap = candidate, gap
    return best


def _normalize_s1_dates(s1_dates) -> tuple[date_type, ...]:
    """Buang duplikat lalu urutkan. Satu tanggal bisa punya lebih dari satu
    scene S1 (orbit menaik + menurun), tapi untuk perencanaan temporal yang
    dihitung adalah tanggalnya, bukan jumlah scene-nya."""
    return tuple(sorted({d for d in s1_dates if d is not None}))


def plan_fusion(
    strategy: str,
    s1_dates,
    date_from: date_type,
    date_to: date_type,
    tolerance_days: int = DEFAULT_S1_MATCH_TOLERANCE_DAYS,
) -> FusionPlan:
    """Susun FusionPlan untuk satu dataset.

    Fungsi murni: tidak menyentuh disk maupun DB. Pemanggil (orchestrator)
    yang menyediakan `s1_dates` dari hasil `discover_scenes`, lalu memakai
    `aux_dates` untuk menentukan berapa kali `ensure_aux_inputs_for_date`
    dipanggil dan `fusion_dates` untuk berapa berkas HDF5 yang ditulis.

    Raises:
        ValueError: strategi tidak dikenal. Sengaja tidak punya fallback —
            strategi yang salah ketik akan diam-diam menghasilkan dataset
            dengan cakupan temporal yang keliru, dan itu jauh lebih mahal
            daripada job yang gagal cepat.
    """
    s1 = _normalize_s1_dates(s1_dates)
    strategy = str(strategy).strip().upper()

    if strategy == CO_OCCURRENCE:
        # Kedua sumbu mengikuti S1. Ini perilaku sistem sebelum modul ini ada.
        fusion_dates = s1
        aux_dates = s1
        anchor_for = {d: d for d in s1}

    elif strategy == HYBRID:
        # Rakit seperti CO_OCCURRENCE, unduh seperti FULL_COVERAGE: berkasnya
        # tetap per tanggal S1, tapi aux harian sudah tersedia di disk sehingga
        # window akumulasi GPM dan komposit MODIS di sekitar jangkar terisi
        # penuh alih-alih bolong di hari tanpa S1.
        fusion_dates = s1
        aux_dates = _daterange(date_from, date_to)
        anchor_for = {d: d for d in s1}

    elif strategy == FULL_COVERAGE:
        # Kedua sumbu harian. S1 dipinjam dari tanggal terdekat dalam toleransi;
        # hari tanpa pasangan tetap jadi berkas, tanpa group sentinel1/.
        fusion_dates = _daterange(date_from, date_to)
        aux_dates = fusion_dates
        anchor_for = {d: _nearest_s1(d, s1, tolerance_days) for d in fusion_dates}

    else:
        raise ValueError(
            f"fusion_strategy tidak dikenal: {strategy!r} "
            f"(pilihan: {CO_OCCURRENCE}, {FULL_COVERAGE}, {HYBRID})"
        )

    plan = FusionPlan(
        strategy=strategy,
        subfolder=SUBFOLDER[strategy],
        fusion_dates=tuple(fusion_dates),
        aux_dates=tuple(aux_dates),
        anchor_for=anchor_for,
        tolerance_days=tolerance_days,
    )

    logger.info(
        "[FUSION] strategi=%s tanggal_fusi=%d tanggal_aux=%d tanpa_s1=%d toleransi=%d",
        strategy, len(plan.fusion_dates), len(plan.aux_dates),
        sum(1 for v in anchor_for.values() if v is None), tolerance_days,
    )
    return plan
