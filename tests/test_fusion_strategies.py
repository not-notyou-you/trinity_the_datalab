# tests/test_fusion_strategies.py
"""Aturan temporal tiap strategi fusi.

Modul yang diuji murni (tanpa disk/DB), jadi seluruh perilaku temporal bisa
dikunci di sini tanpa fixture. Ini penting: sebelum etl/fusion_strategies.py
ada, ketiga strategi menghasilkan output identik, dan berkas inilah yang
membuktikan mereka sekarang benar-benar berbeda.
"""

from datetime import date

import pytest

from etl.fusion_strategies import (
    CO_OCCURRENCE,
    DEFAULT_S1_MATCH_TOLERANCE_DAYS,
    FULL_COVERAGE,
    HYBRID,
    plan_fusion,
)

# Rentang 1-20 Januari dengan scene S1 di tanggal 5 dan 17.
DATE_FROM = date(2024, 1, 1)
DATE_TO = date(2024, 1, 20)
S1_DATES = [date(2024, 1, 5), date(2024, 1, 17)]


def _plan(strategy, **kw):
    return plan_fusion(strategy, S1_DATES, DATE_FROM, DATE_TO, **kw)


# --- sumbu unduh vs sumbu rakit -------------------------------------------

@pytest.mark.parametrize(
    "strategy, n_fusion, n_aux",
    [
        (CO_OCCURRENCE, 2, 2),    # keduanya mengikuti S1
        (FULL_COVERAGE, 20, 20),  # keduanya harian
        (HYBRID, 2, 20),          # rakit per S1, unduh harian
    ],
)
def test_two_axes_differ_per_strategy(strategy, n_fusion, n_aux):
    plan = _plan(strategy)
    assert len(plan.fusion_dates) == n_fusion
    assert len(plan.aux_dates) == n_aux


def test_hybrid_is_not_cooccurrence_nor_full_coverage():
    """Inti keputusan D1: HYBRID adalah strategi ketiga yang berdiri sendiri.

    Ia berbagi sumbu rakit dengan CO_OCCURRENCE dan sumbu unduh dengan
    FULL_COVERAGE, tapi tidak identik dengan keduanya.
    """
    co, full, hyb = _plan(CO_OCCURRENCE), _plan(FULL_COVERAGE), _plan(HYBRID)

    assert hyb.fusion_dates == co.fusion_dates
    assert hyb.aux_dates == full.aux_dates
    assert hyb.aux_dates != co.aux_dates
    assert hyb.fusion_dates != full.fusion_dates


# --- toleransi pasangan S1 -------------------------------------------------

def test_full_coverage_borrows_nearest_s1_within_tolerance():
    plan = _plan(FULL_COVERAGE, tolerance_days=2)

    # S1 ada di tanggal 5; tanggal 7 masih dalam toleransi 2 hari.
    assert plan.anchor(date(2024, 1, 7)) == date(2024, 1, 5)
    assert plan.offset_days(date(2024, 1, 7)) == 2
    # Same-day tetap offset 0.
    assert plan.offset_days(date(2024, 1, 5)) == 0


def test_full_coverage_keeps_dates_without_any_s1():
    """Hari di luar toleransi tetap jadi berkas, tanpa jangkar S1.

    Ini yang membedakan FULL_COVERAGE: deret harian MODIS/GPM-nya sendiri
    yang dicari, jadi absennya S1 bukan alasan membatalkan tanggal.
    """
    plan = _plan(FULL_COVERAGE, tolerance_days=2)

    assert date(2024, 1, 8) in plan.fusion_dates
    assert plan.anchor(date(2024, 1, 8)) is None
    assert plan.offset_days(date(2024, 1, 8)) is None
    assert plan.requires_s1 is False


def test_anchored_strategies_always_have_s1():
    for strategy in (CO_OCCURRENCE, HYBRID):
        plan = _plan(strategy)
        assert plan.requires_s1 is True
        assert all(plan.anchor(d) == d for d in plan.fusion_dates)
        assert all(plan.offset_days(d) == 0 for d in plan.fusion_dates)


def test_tie_breaks_to_earlier_scene_deterministically():
    """Target tepat di tengah dua scene harus selalu memilih yang lebih awal —
    pilihan sewenang-wenang akan membuat output berubah antar-run."""
    plan = plan_fusion(
        FULL_COVERAGE,
        [date(2024, 1, 4), date(2024, 1, 8)],
        date(2024, 1, 1), date(2024, 1, 10),
        tolerance_days=2,
    )
    assert plan.anchor(date(2024, 1, 6)) == date(2024, 1, 4)


def test_zero_tolerance_means_same_day_only():
    plan = _plan(FULL_COVERAGE, tolerance_days=0)
    assert plan.anchor(date(2024, 1, 5)) == date(2024, 1, 5)
    assert plan.anchor(date(2024, 1, 6)) is None


# --- kasus tepi ------------------------------------------------------------

def test_duplicate_s1_dates_collapse():
    """Satu tanggal bisa punya dua scene S1 (orbit menaik + menurun); yang
    dihitung untuk perencanaan temporal adalah tanggalnya."""
    plan = plan_fusion(
        CO_OCCURRENCE,
        [date(2024, 1, 5), date(2024, 1, 5), date(2024, 1, 17)],
        DATE_FROM, DATE_TO,
    )
    assert plan.fusion_dates == (date(2024, 1, 5), date(2024, 1, 17))


def test_no_s1_scenes():
    co = plan_fusion(CO_OCCURRENCE, [], DATE_FROM, DATE_TO)
    assert co.fusion_dates == ()

    # FULL_COVERAGE tetap menghasilkan 20 berkas walau tidak ada S1 sama sekali.
    full = plan_fusion(FULL_COVERAGE, [], DATE_FROM, DATE_TO)
    assert len(full.fusion_dates) == 20
    assert all(full.anchor(d) is None for d in full.fusion_dates)


def test_inverted_range_yields_nothing():
    plan = plan_fusion(FULL_COVERAGE, S1_DATES, DATE_TO, DATE_FROM)
    assert plan.fusion_dates == ()


def test_unknown_strategy_fails_fast():
    with pytest.raises(ValueError, match="tidak dikenal"):
        plan_fusion("BEST_EFFORT", S1_DATES, DATE_FROM, DATE_TO)


def test_strategy_is_case_insensitive():
    assert _plan("hybrid").strategy == HYBRID


def test_default_tolerance_applied():
    assert _plan(FULL_COVERAGE).tolerance_days == DEFAULT_S1_MATCH_TOLERANCE_DAYS


# --- penamaan output -------------------------------------------------------

def test_subfolder_per_strategy():
    """Subfolder memisahkan output ketiga strategi. Penamaan berkasnya sendiri
    dipegang module9.fusion_h5_name -- satu konvensi, satu tempat."""
    assert _plan(CO_OCCURRENCE).subfolder == "co-occurrence"
    assert _plan(FULL_COVERAGE).subfolder == "full-coverage"
    assert _plan(HYBRID).subfolder == "hybrid"
    assert len({_plan(s).subfolder for s in (CO_OCCURRENCE, FULL_COVERAGE, HYBRID)}) == 3
