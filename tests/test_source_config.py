# tests/test_source_config.py
"""
Unit tests: konfigurasi pemrosesan per-satelit (dataset_source_config).

Coverage:
    - Model DatasetSourceConfig + relasi Dataset.source_configs
    - normalize_source_configs() / derive_required_tiers() / validator
    - DatabaseClient.get_dataset_source_config()
    - DatabaseClient.list_dataset_source_configs()
    - DatabaseClient.upsert_dataset_source_config()
    - DatabaseClient.create_dataset_with_sources() -- termasuk atomisitas
    - DatabaseClient.get_last_dataset_config()

Author : Julius Marselinus (BRONTO) - NIM 00000111989
Program: Sistem Informasi - Universitas Multimedia Nusantara

Run:
    pytest tests/test_source_config.py -v
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etl import database_client as dbc
from etl.database_client import (
    Dataset,
    DatasetSourceConfig,
    derive_required_tiers,
    normalize_source_configs,
    source_configs_to_api,
    _validate_fusion_strategy,
    _validate_preview_options,
)


BBOX_WKT = "POLYGON((106.4 -6.7, 107.2 -6.7, 107.2 -5.9, 106.4 -5.9, 106.4 -6.7))"


def dataset_payload(region_id: int, name: str | None = None, **overrides) -> dict:
    """dataset_dict minimal untuk create_dataset_with_sources()."""
    payload = {
        "name": name or f"TEST_SRCCFG_{datetime.now().timestamp()}",
        "location_label": "Test AOI",
        "region_id": region_id,
        "bbox": f"SRID=4326;{BBOX_WKT}",
        "bbox_wkt": BBOX_WKT,
        "date_start": date(2024, 1, 1),
        "date_end": date(2024, 1, 31),
        "dataset_kind": "STANDARD",
        "status": "DRAFT",
    }
    payload.update(overrides)
    return payload


@pytest.fixture(autouse=True)
def clean_datasets(db_client):
    """Kosongkan tabel datasets sebelum tiap tes.

    get_last_dataset_config() menjawab "dataset terakhir di seluruh tabel",
    jadi tes-tesnya hanya bermakna kalau tidak ada sisa baris dari tes lain
    (termasuk fixture sample_dataset milik modul tes lain, yang session-scoped
    database-nya sama). DELETE, bukan TRUNCATE: TRUNCATE mengunci tabel yang
    direferensikan FK dan bikin tes lain yang jalan paralel menunggu.
    """
    with db_client.session() as sess:
        sess.execute(text("DELETE FROM dataset_source_config"))
        sess.execute(text("DELETE FROM datasets"))
    yield


# ---------------------------------------------------------------------------
# 1. HELPER MURNI (tanpa database)
# ---------------------------------------------------------------------------

class TestNormalizeSourceConfigs:

    def test_api_shape_is_accepted(self):
        """Payload API {"sentinel1": {"processing": [...]}} dipetakan ke nama DB."""
        result = normalize_source_configs({
            "sentinel1": {"processing": ["RAW", "PROCESSED"]},
            "modis": {"processing": ["PROCESSED"]},
        })
        assert result == {"SENTINEL1": ["RAW", "PROCESSED"], "MODIS": ["PROCESSED"]}

    def test_short_shape_is_accepted(self):
        """Bentuk ringkas {"SENTINEL1": ["RAW"]} untuk pemanggil internal."""
        assert normalize_source_configs({"SENTINEL1": ["RAW"]}) == {"SENTINEL1": ["RAW"]}
        assert normalize_source_configs({"gpm": "raw"}) == {"GPM": ["RAW"]}

    def test_output_order_is_canonical(self):
        """Urutan output selalu S1 -> MODIS -> GPM, apa pun urutan input."""
        result = normalize_source_configs({
            "gpm": ["RAW"], "modis": ["RAW"], "sentinel1": ["RAW"],
        })
        assert list(result) == ["SENTINEL1", "MODIS", "GPM"]

    def test_levels_deduped_and_ordered(self):
        """Level duplikat dibuang dan diurutkan RAW lalu PROCESSED."""
        result = normalize_source_configs({"sentinel1": ["processed", "raw", "RAW"]})
        assert result["SENTINEL1"] == ["RAW", "PROCESSED"]

    def test_duplicate_source_in_different_case_rejected(self):
        """{"gpm": ..., "GPM": ...} adalah dua key dict yang berbeda tapi satu
        sumber yang sama -- menerimanya berarti satu dari dua permintaan user
        hilang tanpa suara."""
        with pytest.raises(ValueError, match="source ganda"):
            normalize_source_configs({"gpm": ["RAW"], "GPM": ["PROCESSED"]})

    @pytest.mark.parametrize("bad_payload", [
        {},
        None,
        {"landsat": ["RAW"]},
        {"sentinel1": {"processing": []}},
        {"sentinel1": {"processing": ["GOLD"]}},
        {"sentinel1": {}},
    ])
    def test_invalid_payload_raises(self, bad_payload):
        with pytest.raises(ValueError):
            normalize_source_configs(bad_payload)


class TestDeriveRequiredTiers:

    def test_raw_only_stops_at_bronze(self):
        assert derive_required_tiers({"SENTINEL1": ["RAW"]}) == ["RAW", "BRONZE"]

    def test_processed_reaches_gold(self):
        assert derive_required_tiers({"MODIS": ["PROCESSED"]}) == [
            "RAW", "BRONZE", "SILVER", "GOLD"
        ]

    def test_fusion_added_only_with_gold(self):
        """Fusi menyusun stack dari GOLD, jadi dataset RAW-only tidak dapat FUSION."""
        assert "FUSION" in derive_required_tiers(
            {"SENTINEL1": ["PROCESSED"], "MODIS": ["PROCESSED"]}, with_fusion=True
        )
        assert "FUSION" not in derive_required_tiers(
            {"SENTINEL1": ["RAW"], "MODIS": ["RAW"]}, with_fusion=True
        )


class TestValidators:

    def test_fusion_required_for_multi_source(self):
        with pytest.raises(ValueError, match="required when multiple sources"):
            _validate_fusion_strategy(None, source_count=2)

    def test_fusion_forbidden_for_single_source(self):
        with pytest.raises(ValueError, match="must be null when only 1 source"):
            _validate_fusion_strategy("HYBRID", source_count=1)

    def test_fusion_normalized_to_uppercase(self):
        assert _validate_fusion_strategy("hybrid", source_count=3) == "HYBRID"
        assert _validate_fusion_strategy("", source_count=1) is None

    def test_unknown_fusion_strategy_rejected(self):
        with pytest.raises(ValueError, match="tidak dikenal"):
            _validate_fusion_strategy("BEST_EFFORT", source_count=2)

    def test_preview_none_means_all_variants(self):
        assert _validate_preview_options(None) == ["GRAYSCALE", "COLORED", "COMPOSITE"]

    def test_preview_empty_list_means_none(self):
        """[] berbeda dari None: user sengaja mematikan semua varian."""
        assert _validate_preview_options([]) == []

    def test_preview_accepts_single_string(self):
        assert _validate_preview_options("colored") == ["COLORED"]

    def test_preview_deduped_and_ordered(self):
        assert _validate_preview_options(
            ["COMPOSITE", "GRAYSCALE", "COMPOSITE"]
        ) == ["GRAYSCALE", "COMPOSITE"]

    def test_unknown_preview_option_rejected(self):
        with pytest.raises(ValueError, match="tidak dikenal"):
            _validate_preview_options(["SEPIA"])


# ---------------------------------------------------------------------------
# 2. MODEL & RELASI
# ---------------------------------------------------------------------------

class TestModel:

    def test_table_and_columns_exist(self, db_client):
        from sqlalchemy import inspect
        cols = {c["name"] for c in inspect(db_client._engine).get_columns("dataset_source_config")}
        assert {"config_id", "dataset_id", "source_name", "processing_levels"} <= cols

    def test_datasets_has_new_columns_and_not_the_old_ones(self, db_client):
        """Model baru: fusion_strategy/preview_options ada, kolom lama tidak."""
        from sqlalchemy import inspect
        cols = {c["name"] for c in inspect(db_client._engine).get_columns("datasets")}
        assert {"fusion_strategy", "preview_options"} <= cols
        assert "selected_satellites" not in cols
        assert "processing_level" not in cols

    def test_relationship_loads_both_ways(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region),
            {"sentinel1": {"processing": ["RAW"]}},
        )
        assert [c.source_name for c in ds.source_configs] == ["SENTINEL1"]
        cfg = db_client.get_dataset_source_config(ds.dataset_id, "SENTINEL1")
        with db_client.session() as sess:
            reloaded = sess.get(DatasetSourceConfig, cfg.config_id)
            assert reloaded.dataset.dataset_id == ds.dataset_id

    def test_delete_dataset_cascades_to_configs(self, db_client, sample_region):
        """cascade delete-orphan: config tidak boleh hidup tanpa datasetnya."""
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, fusion_strategy="FULL_COVERAGE"),
            {"sentinel1": ["RAW"], "gpm": ["RAW"]},
        )
        with db_client.session() as sess:
            sess.delete(sess.get(Dataset, ds.dataset_id))
        assert db_client.list_dataset_source_configs(ds.dataset_id) == []

    def test_unique_constraint_per_dataset_source(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"sentinel1": ["RAW"]}
        )
        with pytest.raises(IntegrityError):
            with db_client.session() as sess:
                sess.add(DatasetSourceConfig(
                    dataset_id=ds.dataset_id,
                    source_name="SENTINEL1",
                    processing_levels=["PROCESSED"],
                ))

    @pytest.mark.parametrize("source_name,levels", [
        ("LANDSAT", ["RAW"]),        # chk_source_config_source_name
        ("SENTINEL1", []),           # chk_source_config_levels_not_empty
        ("SENTINEL1", ["GOLD"]),     # chk_source_config_levels_valid
    ])
    def test_check_constraints_reject_bad_rows(self, db_client, sample_region,
                                                source_name, levels):
        """CHECK di database adalah jaring pengaman untuk penulis yang melewati
        normalize_source_configs() (SQL mentah, ORM langsung)."""
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"modis": ["RAW"]}
        )
        with pytest.raises(IntegrityError):
            with db_client.session() as sess:
                sess.add(DatasetSourceConfig(
                    dataset_id=ds.dataset_id,
                    source_name=source_name,
                    processing_levels=levels,
                ))

    def test_helper_methods(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"gpm": {"processing": ["RAW"]}}
        )
        cfg = ds.source_configs[0]
        assert cfg.api_key == "gpm"
        assert cfg.has_level("raw") is True
        assert cfg.has_level("PROCESSED") is False
        assert cfg.to_dict()["processing"] == ["RAW"]
        assert source_configs_to_api([cfg]) == {"gpm": {"processing": ["RAW"]}}


# ---------------------------------------------------------------------------
# 3. create_dataset_with_sources()
# ---------------------------------------------------------------------------

class TestCreateDatasetWithSources:

    def test_two_sources_with_different_levels(self, db_client, sample_region):
        """Skenario inti: dua sumber, masing-masing level berbeda."""
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="DUA_SUMBER",
                             fusion_strategy="CO_OCCURRENCE"),
            {
                "sentinel1": {"processing": ["RAW", "PROCESSED"]},
                "gpm": {"processing": ["RAW"]},
            },
        )
        assert ds.dataset_id is not None
        assert ds.fusion_strategy == "CO_OCCURRENCE"

        stored = {c.source_name: list(c.processing_levels)
                  for c in db_client.list_dataset_source_configs(ds.dataset_id)}
        assert stored == {"SENTINEL1": ["RAW", "PROCESSED"], "GPM": ["RAW"]}

    def test_required_tiers_derived_from_sources(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, fusion_strategy="FULL_COVERAGE"),
            {"sentinel1": ["PROCESSED"], "modis": ["RAW"]},
        )
        with db_client.session() as sess:
            tiers = list(sess.get(Dataset, ds.dataset_id).required_tiers)
        assert tiers == ["RAW", "BRONZE", "SILVER", "GOLD", "FUSION"]

    def test_explicit_required_tiers_wins(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, required_tiers=["RAW"]),
            {"sentinel1": ["PROCESSED"]},
        )
        with db_client.session() as sess:
            assert list(sess.get(Dataset, ds.dataset_id).required_tiers) == ["RAW"]

    def test_single_source_has_no_fusion_strategy(self, db_client, sample_region):
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"modis": ["PROCESSED"]}
        )
        assert ds.fusion_strategy is None

    def test_preview_options_defaults_and_explicit_empty(self, db_client, sample_region):
        default_ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"modis": ["RAW"]}
        )
        assert list(default_ds.preview_options) == ["GRAYSCALE", "COLORED", "COMPOSITE"]

        no_preview = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, preview_options=[]), {"modis": ["RAW"]}
        )
        assert list(no_preview.preview_options) == []

    @pytest.mark.parametrize("payload_kwargs,sources,match", [
        ({}, {}, "minimal 1 sumber"),
        ({}, {"landsat": ["RAW"]}, "source tidak dikenal"),
        ({}, {"sentinel1": ["RAW"], "modis": ["RAW"]}, "fusion_strategy required"),
        ({"fusion_strategy": "HYBRID"}, {"modis": ["RAW"]}, "must be null"),
        ({"selected_satellites": ["S1"]}, {"modis": ["RAW"]}, "tidak dikenal"),
    ])
    def test_invalid_input_raises_before_any_insert(self, db_client, sample_region,
                                                     payload_kwargs, sources, match):
        with pytest.raises(ValueError, match=match):
            db_client.create_dataset_with_sources(
                dataset_payload(sample_region, **payload_kwargs), sources
            )
        assert self._dataset_count(db_client) == 0
        assert self._config_count(db_client) == 0

    def test_atomic_rollback_on_database_error(self, db_client, sample_region, monkeypatch):
        """Kalau INSERT config ditolak database, baris `datasets` ikut batal.

        Validasi Python di-bypass lewat monkeypatch supaya nama sumber yang
        tidak valid benar-benar sampai ke database dan CHECK constraint yang
        menggagalkannya -- ini yang menguji transaksinya, bukan validatornya.
        """
        monkeypatch.setitem(dbc.API_KEY_TO_SOURCE_NAME, "landsat", "LANDSAT")

        with pytest.raises(IntegrityError):
            db_client.create_dataset_with_sources(
                dataset_payload(sample_region, name="HARUS_ROLLBACK",
                                 fusion_strategy="HYBRID"),
                {"sentinel1": ["PROCESSED"], "landsat": ["RAW"]},
            )

        # Tidak ada partial insert: dataset maupun config sumber yang valid
        # (SENTINEL1) sama-sama hilang.
        assert self._dataset_count(db_client) == 0
        assert self._config_count(db_client) == 0

    @staticmethod
    def _dataset_count(db_client) -> int:
        with db_client.session() as sess:
            return sess.scalar(text("SELECT COUNT(*) FROM datasets"))

    @staticmethod
    def _config_count(db_client) -> int:
        with db_client.session() as sess:
            return sess.scalar(text("SELECT COUNT(*) FROM dataset_source_config"))


# ---------------------------------------------------------------------------
# 4. get_dataset_source_config() / list / upsert
# ---------------------------------------------------------------------------

class TestGetDatasetSourceConfig:

    @pytest.fixture
    def dataset_id(self, db_client, sample_region) -> int:
        ds = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, fusion_strategy="HYBRID"),
            {
                "sentinel1": {"processing": ["RAW", "PROCESSED"]},
                "gpm": {"processing": ["RAW"]},
            },
        )
        return ds.dataset_id

    def test_returns_config_object(self, db_client, dataset_id):
        cfg = db_client.get_dataset_source_config(dataset_id, "SENTINEL1")
        assert isinstance(cfg, DatasetSourceConfig)
        assert cfg.dataset_id == dataset_id
        assert cfg.source_name == "SENTINEL1"
        assert list(cfg.processing_levels) == ["RAW", "PROCESSED"]
        assert cfg.config_id is not None

    def test_accepts_api_spelling(self, db_client, dataset_id):
        assert db_client.get_dataset_source_config(dataset_id, "sentinel1").source_name == "SENTINEL1"
        assert db_client.get_dataset_source_config(dataset_id, " Gpm ").source_name == "GPM"

    def test_returns_none_for_unconfigured_source(self, db_client, dataset_id):
        assert db_client.get_dataset_source_config(dataset_id, "modis") is None

    def test_returns_none_for_unknown_source_name(self, db_client, dataset_id):
        assert db_client.get_dataset_source_config(dataset_id, "landsat") is None

    def test_returns_none_for_unknown_dataset(self, db_client):
        assert db_client.get_dataset_source_config(999_999, "sentinel1") is None

    def test_list_is_ordered_canonically(self, db_client, dataset_id):
        names = [c.source_name for c in db_client.list_dataset_source_configs(dataset_id)]
        assert names == ["SENTINEL1", "GPM"]

    def test_upsert_creates_then_updates(self, db_client, dataset_id):
        created = db_client.upsert_dataset_source_config(dataset_id, "modis", ["RAW"])
        assert created.source_name == "MODIS"
        assert len(db_client.list_dataset_source_configs(dataset_id)) == 3

        updated = db_client.upsert_dataset_source_config(
            dataset_id, "modis", ["RAW", "PROCESSED"]
        )
        assert updated.config_id == created.config_id      # baris yang sama
        assert list(
            db_client.get_dataset_source_config(dataset_id, "MODIS").processing_levels
        ) == ["RAW", "PROCESSED"]

    def test_upsert_rejects_invalid_input(self, db_client, dataset_id):
        with pytest.raises(ValueError):
            db_client.upsert_dataset_source_config(dataset_id, "landsat", ["RAW"])
        with pytest.raises(ValueError):
            db_client.upsert_dataset_source_config(dataset_id, "modis", [])


# ---------------------------------------------------------------------------
# 5. get_last_dataset_config()
# ---------------------------------------------------------------------------

class TestGetLastDatasetConfig:

    def test_empty_dict_when_no_dataset(self, db_client):
        assert db_client.get_last_dataset_config() == {}

    def test_returns_most_recent_config(self, db_client, sample_region):
        older = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="LAMA"), {"modis": ["RAW"]}
        )
        newer = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="BARU",
                             fusion_strategy="HYBRID",
                             preview_options=["COLORED", "COMPOSITE"]),
            {
                "sentinel1": {"processing": ["RAW", "PROCESSED"]},
                "modis": {"processing": ["PROCESSED"]},
            },
        )
        # created_at server-side bisa identik untuk dua INSERT beruntun; beda
        # urutan dipastikan lewat pemecah seri dataset_id (desc).
        cfg = db_client.get_last_dataset_config()

        assert cfg["created_from_dataset_id"] == newer.dataset_id
        assert cfg["created_from_dataset_id"] != older.dataset_id
        assert cfg["region_id"] == sample_region
        assert cfg["region_name"] == "Jabodetabek"
        assert cfg["sources"] == {
            "sentinel1": {"processing": ["RAW", "PROCESSED"]},
            "modis": {"processing": ["PROCESSED"]},
        }
        assert cfg["fusion_strategy"] == "HYBRID"
        assert cfg["preview_options"] == ["COLORED", "COMPOSITE"]
        assert cfg["created_at"] is not None

    def test_structure_has_no_identity_fields(self, db_client, sample_region):
        """D13: endpoint hanya mengembalikan field konfigurasi -- nama dan
        rentang tanggal sengaja tidak ikut supaya user mengisinya ulang."""
        db_client.create_dataset_with_sources(
            dataset_payload(sample_region), {"gpm": ["RAW"]}
        )
        cfg = db_client.get_last_dataset_config()
        assert set(cfg) == {
            "region_id", "region_name", "sources", "fusion_strategy",
            "preview_options", "created_from_dataset_id", "created_at",
        }
        assert "name" not in cfg
        assert "date_start" not in cfg

    def test_soft_deleted_dataset_is_skipped(self, db_client, sample_region):
        keeper = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="MASIH_ADA"), {"modis": ["RAW"]}
        )
        removed = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="TERHAPUS"), {"gpm": ["RAW"]}
        )
        with db_client.session() as sess:
            sess.get(Dataset, removed.dataset_id).deleted_at = datetime.now(timezone.utc)

        cfg = db_client.get_last_dataset_config()
        assert cfg["created_from_dataset_id"] == keeper.dataset_id

    def test_orders_by_created_at_not_insertion_order(self, db_client, sample_region):
        """Dataset yang di-backdate tidak boleh menang meski di-insert terakhir."""
        recent = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="TERBARU"), {"modis": ["RAW"]}
        )
        backdated = db_client.create_dataset_with_sources(
            dataset_payload(sample_region, name="MUNDUR"), {"gpm": ["RAW"]}
        )
        with db_client.session() as sess:
            sess.get(Dataset, backdated.dataset_id).created_at = (
                datetime.now(timezone.utc) - timedelta(days=7)
            )

        assert db_client.get_last_dataset_config()["created_from_dataset_id"] == recent.dataset_id
