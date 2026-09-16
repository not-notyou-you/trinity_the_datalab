"""
Kosakata tier D14: nama baru diutamakan, nama warisan hanya dibaca.

Tiga hal yang dijaga di sini:
  1. filter baca menjaring artefak yang sama di kedua kosakata;
  2. jalur tulis tidak pernah menyimpan nama warisan;
  3. whitelist `datasets.chk_required_tiers` di migrasi terakhir sama persis
     dengan etl/tier_names -- drift inilah yang membuat POST /api/datasets 500
     (CheckViolation) setelah UI mulai mengirim nama D14.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

from etl import tier_names as tn
from etl.database_client import ProductTierEnum

MIGRATIONS = Path(__file__).resolve().parent.parent / "database" / "migrations"


@pytest.mark.parametrize("tier, expected", [
    ("RAW", ("RAW",)),
    ("ALIGNED", ("ALIGNED", "BRONZE")),
    ("BRONZE", ("ALIGNED", "BRONZE")),
    ("COG", ("COG", "GOLD")),
    ("gold", ("COG", "GOLD")),
    ("FUSED", ("FUSED", "FUSION")),
    ("FUSION", ("FUSED", "FUSION")),
    ("INDICES", ("INDICES", "SILVER")),
    ("SILVER", ("DESPECKLED", "INDICES", "ACCUMULATED", "SILVER")),
    (ProductTierEnum.COG, ("COG", "GOLD")),
])
def test_equivalent_tiers(tier, expected):
    assert tn.equivalent_tiers(tier) == expected


def test_equivalent_tiers_rejects_unknown():
    with pytest.raises(ValueError):
        tn.equivalent_tiers("PLATINUM")


def test_tiers_up_to_rank_2_is_everything_before_cog():
    got = set(tn.tiers_up_to_rank(2))
    assert got == {"RAW", "ALIGNED", "BRONZE", "DESPECKLED", "INDICES", "ACCUMULATED", "SILVER"}
    assert not got & set(tn.tiers_at_rank(3)) and not got & set(tn.tiers_at_rank(4))


def test_new_names_come_first():
    for t in tn.TIERS:
        assert tn.equivalent_tiers(t)[0] in tn.TIERS


def _last_required_tiers_whitelist() -> set[str]:
    found: set[str] | None = None
    for f in sorted(MIGRATIONS.glob("*.sql")):
        sql = re.sub(r"--[^\n]*", "", f.read_text(encoding="utf-8"))
        for m in re.finditer(
            r"ADD\s+CONSTRAINT\s+chk_required_tiers\s+CHECK\s*\((.*?)\]", sql, re.S | re.I
        ):
            found = set(re.findall(r"'([A-Z]+)'", m.group(1)))
    assert found is not None, "chk_required_tiers tidak ditemukan di migrasi mana pun"
    return found


def test_required_tiers_constraint_matches_tier_names():
    assert _last_required_tiers_whitelist() == set(tn.TIERS) | set(tn.LEGACY_TIERS)


def test_product_tier_enum_matches_tier_names():
    assert {e.value for e in ProductTierEnum} == set(tn.TIERS) | set(tn.LEGACY_TIERS)


@pytest.mark.parametrize("legacy, source, expected", [
    ("BRONZE", "SENTINEL1", "ALIGNED"),
    ("SILVER", "MODIS", "INDICES"),
    ("GOLD", "GPM", "COG"),
])
def test_insert_never_stores_legacy_name(meta, sample_scene, legacy, source, expected):
    job_id = meta.insert_processing_job(sample_scene, "CROP")
    pid = meta.insert_data_product(
        scene_id=sample_scene, job_id=job_id,
        product_tier=legacy, source=source, product_type="T", band_name=f"B_{legacy}",
        file_path=f"/tmp/tn_{legacy}.tif", file_name=f"tn_{legacy}.tif", file_size_mb=1.0,
        data_hash_sha256=hashlib.sha256(legacy.encode()).hexdigest(),
    )
    new = [p for p in meta.get_products_by_scene(sample_scene, tier=expected) if p["product_id"] == pid]
    assert new and new[0]["product_tier"] == expected
    # Nama lama tetap bisa dipakai untuk mencari, dan menemukan baris yang sama.
    assert any(p["product_id"] == pid for p in meta.get_products_by_scene(sample_scene, tier=legacy))
