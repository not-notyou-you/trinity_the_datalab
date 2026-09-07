# etl/migrate_data_structure.py
"""
Migrasi struktur folder data/datasets/... ke layout tier-source-scene.

Layout tujuan (lihat etl/folder_manager.py):

    data/datasets/{dataset_id}_{slug_nama}/
        {raw,bronze,silver,gold}/{source}/{scene}/...
        fusion/{scene}/...

Script ini menerima dua layout asal sekaligus, jadi bisa dijalankan dari
mana pun instalasi berada:

  L1 (paling lama)  data/datasets/{dataset_id}/{YYYYMMDD}/{tier}/*
  L2 (sebelumnya)   data/datasets/{dataset_id}_{slug}/{tier}/{scene}/*

Perbedaan L2 -> layout sekarang: sisipan folder `{source}` di bawah tiap
tier, dan stack fusion pindah dari `gold/{tanggal}/` ke tier `fusion/`
sendiri (arti GOLD berubah -- lihat database/migrations/013).

Kunci scene:
  - Produk Sentinel-1 memakai `product_identifier` scene tsb, diambil dari
    satellite_scenes lewat data_products.scene_id -- bukan diparse dari
    nama folder.
  - Artefak yang bukan milik satu scene S1 tertentu (MODIS/GPM harian dan
    output fusion yang di-dedup per tanggal) memakai tanggal YYYYMMDD,
    diambil dari posisi folder tanggal kalau layout asalnya punya, kalau
    tidak dari tanggal di nama file.

Kolom `source` di data_products ikut diisi kalau masih kosong -- migrasi SQL
013 sudah membackfill-nya, ini cuma jaring pengaman untuk baris yang masuk
setelah itu lewat kode lama.

Setiap dataset di-backup penuh ke backup/data_structure_migration/ sebelum
satu file pun dipindah. Setelah `shutil.move`, checksum SHA-256 file di
lokasi baru dibandingkan dengan data_products.data_hash_sha256 (atau dihitung
ulang dari file lama kalau kolom itu kosong) -- kalau tidak cocok, file
dikembalikan ke lokasi lama dan product itu dilaporkan gagal, tidak pernah
didiamkan begitu saja. Script idempotent: file yang sudah di lokasi baru
dilewati.

JALANKAN SETELAH database/migrations/013_add_fusion_tier_and_source.sql.

Usage:
    python -m etl.migrate_data_structure                  # migrasi semua dataset
    python -m etl.migrate_data_structure --dataset-id 2    # migrasi 1 dataset
    python -m etl.migrate_data_structure --dry-run         # tampilkan rencana saja
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import select

from etl import folder_manager as fm
from etl.database_client import DataProduct, Dataset, DatabaseClient, SatelliteScene

logger = logging.getLogger("migrate_data_structure")

BACKUP_ROOT = Path("backup") / "data_structure_migration"
LOG_DIR = Path("logs_pipeline")

_DATE_DIR_RE = re.compile(r"^\d{8}$")
_DATE_IN_NAME_RE = re.compile(r"(\d{8})")

# product_type -> source folder. Produk yang tidak terdaftar di sini
# dianggap Sentinel-1: semua product_type S1 historis (ORIGINAL_TIFF,
# RAW_EXTRACTED_TIFF, CROPPED_TIFF, LEE_FILTERED, COG, S1_COG) berasal dari
# satu-satunya sensor yang punya pipeline per-scene.
_SOURCE_BY_PRODUCT_TYPE = {
    "MODIS_FLOOD": "modis",
    "MODIS_NDVI": "modis",
    "MODIS_NDWI": "modis",
    "MODIS_COG": "modis",
    "GPM_RAINFALL": "gpm",
    "GPM_COG": "gpm",
}

# product_type yang tidak terikat ke satu scene Sentinel-1 -- kunci scene-nya
# tanggal, bukan product_identifier.
_AUX_PRODUCT_TYPES = set(_SOURCE_BY_PRODUCT_TYPE) | {"FUSION_H5"}


def _configure_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_DIR / "migrate_data_structure.log"),
        ],
    )


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _legacy_roots(dataset_id: int, dataset_name: str) -> list[Path]:
    """Folder asal yang mungkin masih menyimpan file dataset ini: L1 (tanpa
    slug nama) dan L2 (dengan slug, tapi belum punya level source). L2 punya
    path yang sama dengan root tujuan -- yang berubah cuma isinya."""
    roots = [fm.DATA_ROOT / str(dataset_id)]
    new_root = fm.get_dataset_root(dataset_id, dataset_name)
    if new_root not in roots:
        roots.append(new_root)
    return [r for r in roots if r.exists()]


def _backup_dataset(dataset_id: int, dataset_name: str) -> Path | None:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    made: Path | None = None
    for i, src in enumerate(_legacy_roots(dataset_id, dataset_name)):
        if not any(src.rglob("*")):
            continue
        dest = BACKUP_ROOT / f"dataset_{dataset_id}_{stamp}" / src.name
        logger.info("[BACKUP] dataset_id=%d %s -> %s", dataset_id, src, dest)
        shutil.copytree(src, dest)
        made = dest
    return made


def _products_with_scene_info(db: DatabaseClient, dataset_id: int) -> list[dict]:
    with db.session() as sess:
        rows = sess.execute(
            select(DataProduct, SatelliteScene.product_identifier)
            .join(SatelliteScene, SatelliteScene.scene_id == DataProduct.scene_id)
            .where(DataProduct.dataset_id == dataset_id)
        ).all()
        return [
            {
                "product_id": p.product_id,
                "product_tier": (
                    p.product_tier.value if hasattr(p.product_tier, "value") else str(p.product_tier)
                ),
                "source": p.source,
                "product_type": p.product_type,
                "file_path": p.file_path,
                "file_name": p.file_name,
                "data_hash_sha256": p.data_hash_sha256,
                "product_identifier": pid,
            }
            for p, pid in rows
        ]


def _source_for(row: dict) -> str | None:
    """Folder source untuk satu produk, atau None kalau produk itu tinggal
    di tier fusion (yang memang tidak punya level source)."""
    if row["product_type"] == "FUSION_H5":
        return None
    explicit = (row.get("source") or "").lower()
    if explicit in fm.SOURCES:
        return explicit
    if explicit == fm.FUSION_DB_SOURCE.lower():
        return None
    return _SOURCE_BY_PRODUCT_TYPE.get(row["product_type"], "sentinel1")


def _tier_for(row: dict) -> str:
    """Tier tujuan. FUSION_H5 selalu ke tier fusion walau baris DB-nya masih
    tercatat GOLD (instalasi yang belum menjalankan migrasi SQL 013)."""
    if row["product_type"] == "FUSION_H5":
        return "fusion"
    return row["product_tier"].lower()


def _date_key_from(old_path: Path, file_name: str) -> str | None:
    """Tanggal YYYYMMDD untuk artefak aux. Dicari dari folder tanggal di
    layout asal dulu (L2: parent, L1: parent.parent), baru dari nama file."""
    for candidate in (old_path.parent.name, old_path.parent.parent.name):
        if _DATE_DIR_RE.match(candidate):
            return candidate
    m = _DATE_IN_NAME_RE.search(file_name)
    return m.group(1) if m else None


def _scene_key_for(row: dict, old_path: Path) -> str | None:
    if row["product_type"] in _AUX_PRODUCT_TYPES:
        return _date_key_from(old_path, row["file_name"])
    return row["product_identifier"]


def _target_path(dataset_id: int, dataset_name: str, row: dict, old_path: Path) -> Path | None:
    tier = _tier_for(row)
    scene_key = _scene_key_for(row, old_path)
    if not scene_key:
        logger.error(
            "[MIGRATE] tidak bisa menentukan kunci scene (product_id=%d type=%s): %s",
            row["product_id"], row["product_type"], old_path,
        )
        return None
    if tier == "fusion":
        return fm.get_fusion_dir(dataset_id, dataset_name, scene_key) / row["file_name"]
    source = _source_for(row)
    if source is None:
        logger.error(
            "[MIGRATE] source tidak diketahui (product_id=%d type=%s)",
            row["product_id"], row["product_type"],
        )
        return None
    return fm.get_scene_dir(dataset_id, dataset_name, tier, source, scene_key) / row["file_name"]


def _move_untracked(src: Path, dst: Path, dry_run: bool) -> str:
    """Pindahkan satu file yang tidak tercatat di data_products. Return
    'moved', 'skipped', atau 'failed'."""
    if src.as_posix() == dst.as_posix() or not src.exists():
        return "skipped"
    if dry_run:
        logger.info("[DRY-RUN] (untracked) %s -> %s", src, dst)
        return "moved"
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dst.exists():
            dst.unlink()
        shutil.move(str(src), str(dst))
        logger.info("[MIGRATE] (untracked) %s -> %s", src, dst)
        return "moved"
    except Exception:
        logger.exception("[MIGRATE] gagal pindah file untracked %s", src)
        return "failed"


def _migrate_untracked_files(
    dataset_id: int, dataset_name: str, roots: list[Path], dry_run: bool
) -> dict:
    """File yang tidak tercatat sebagai data_products tapi tetap perlu
    dipindah: arsip .SAFE.zip mentah, sidecar metadata_qa.json (berisi
    product_identifier pemiliknya), dan sidecar fusion_metadata.json (milik
    scene tanggal itu sendiri)."""
    counts = {"moved": 0, "skipped": 0, "failed": 0}
    new_root = fm.get_dataset_root(dataset_id, dataset_name)

    for root in roots:
        for zip_path in root.rglob("*.SAFE.zip"):
            # "{product_identifier}.SAFE.zip" -> "{product_identifier}.SAFE"
            scene_key = zip_path.name[: -len(".zip")]
            dst = fm.get_scene_dir(
                dataset_id, dataset_name, "raw", "sentinel1", scene_key
            ) / zip_path.name
            counts[_move_untracked(zip_path, dst, dry_run)] += 1

        for qa_path in root.rglob("metadata_qa.json"):
            try:
                scene_key = json.loads(qa_path.read_text())["product_identifier"]
            except Exception:
                logger.exception("[MIGRATE] gagal baca metadata_qa.json: %s", qa_path)
                counts["failed"] += 1
                continue
            dst = fm.get_scene_dir(
                dataset_id, dataset_name, "silver", "sentinel1", scene_key
            ) / qa_path.name
            counts[_move_untracked(qa_path, dst, dry_run)] += 1

        for meta_path in root.rglob("fusion_metadata.json"):
            scene_key = _date_key_from(meta_path, meta_path.name)
            if not scene_key:
                try:
                    scene_key = fm.date_key(json.loads(meta_path.read_text())["feature_date"])
                except Exception:
                    logger.exception(
                        "[MIGRATE] tidak bisa menentukan tanggal fusion_metadata.json: %s", meta_path
                    )
                    counts["failed"] += 1
                    continue
            dst = fm.get_fusion_dir(dataset_id, dataset_name, scene_key) / meta_path.name
            counts[_move_untracked(meta_path, dst, dry_run)] += 1

        # Cache granule mentah MODIS/GPM: L1 memakai raw/{source}/, L2 memakai
        # raw/_aux_{source}/. Keduanya jadi raw/{source}/ di layout sekarang.
        for source in ("modis", "gpm"):
            new_cache = fm.get_granule_cache_dir(dataset_id, dataset_name, source)
            for legacy in (root / "raw" / f"_aux_{source}", root / "raw" / source):
                if not legacy.exists() or legacy.resolve() == new_cache.resolve():
                    continue
                for f in sorted(p for p in legacy.rglob("*") if p.is_file()):
                    counts[_move_untracked(f, new_cache / f.name, dry_run)] += 1

        if root != new_root:
            old_meta = root / "metadata.json"
            if old_meta.exists():
                counts[
                    _move_untracked(
                        old_meta, fm.get_dataset_metadata_path(dataset_id, dataset_name), dry_run
                    )
                ] += 1

    return counts


def _backfill_source_column(db: DatabaseClient, dataset_id: int) -> int:
    """Isi data_products.source yang masih kosong. Migrasi SQL 013 sudah
    melakukannya; ini jaring pengaman untuk baris yang ditulis kode lama
    setelah migrasi itu jalan."""
    updated = 0
    with db.session() as sess:
        rows = sess.scalars(
            select(DataProduct).where(
                DataProduct.dataset_id == dataset_id,
                DataProduct.source.is_(None),
            )
        ).all()
        for p in rows:
            if p.product_type == "FUSION_H5":
                p.source = fm.FUSION_DB_SOURCE
            else:
                p.source = fm.SOURCE_DB_VALUES[
                    _SOURCE_BY_PRODUCT_TYPE.get(p.product_type, "sentinel1")
                ]
            updated += 1
    if updated:
        logger.info("[MIGRATE] dataset_id=%d: %d baris source di-backfill", dataset_id, updated)
    return updated


def migrate_dataset(db: DatabaseClient, dataset_id: int, dry_run: bool = False) -> dict:
    with db.session() as sess:
        dataset = sess.get(Dataset, dataset_id)
        if dataset is None:
            logger.warning(
                "[MIGRATE] dataset_id=%d tidak ditemukan di database, dilewati", dataset_id
            )
            return {"dataset_id": dataset_id, "moved": 0, "skipped": 0, "failed": 0}
        dataset_name = dataset.name

    roots = _legacy_roots(dataset_id, dataset_name)
    products = _products_with_scene_info(db, dataset_id)
    if not products and not roots:
        logger.info("[MIGRATE] dataset_id=%d tidak punya data di disk, dilewati", dataset_id)
        return {"dataset_id": dataset_id, "moved": 0, "skipped": 0, "failed": 0}

    if not dry_run:
        _backfill_source_column(db, dataset_id)
        products = _products_with_scene_info(db, dataset_id)
        _backup_dataset(dataset_id, dataset_name)

    moved = skipped = failed = 0
    with db.session() as sess:
        for row in products:
            old_path = Path(row["file_path"])
            new_path = _target_path(dataset_id, dataset_name, row, old_path)
            if new_path is None:
                failed += 1
                continue

            if old_path.as_posix() == new_path.as_posix():
                skipped += 1
                continue
            if not old_path.exists():
                if new_path.exists():
                    # Sudah pindah di run sebelumnya tapi baris DB belum
                    # ikut di-update (mis. proses terhenti di tengah).
                    if not dry_run:
                        product = sess.get(DataProduct, row["product_id"])
                        if product is not None:
                            product.file_path = str(new_path)
                    skipped += 1
                    continue
                logger.warning(
                    "[MIGRATE] file hilang di disk, dilewati (product_id=%d): %s",
                    row["product_id"], old_path,
                )
                skipped += 1
                continue

            try:
                expected_hash = row["data_hash_sha256"] or _sha256(old_path)

                if dry_run:
                    logger.info(
                        "[DRY-RUN] product_id=%d %s -> %s", row["product_id"], old_path, new_path
                    )
                    moved += 1
                    continue

                new_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(old_path), str(new_path))

                actual_hash = _sha256(new_path)
                if expected_hash and actual_hash != expected_hash:
                    logger.error(
                        "[MIGRATE] checksum tidak cocok setelah pindah (product_id=%d): "
                        "%s != %s -- mengembalikan file ke lokasi lama",
                        row["product_id"], actual_hash, expected_hash,
                    )
                    shutil.move(str(new_path), str(old_path))
                    failed += 1
                    continue

                product = sess.get(DataProduct, row["product_id"])
                if product is not None:
                    product.file_path = str(new_path)
                moved += 1
                logger.info(
                    "[MIGRATE] product_id=%d %s -> %s", row["product_id"], old_path, new_path
                )
            except Exception:
                logger.exception(
                    "[MIGRATE] gagal migrasi product_id=%d %s", row["product_id"], old_path
                )
                failed += 1

    untracked_counts = _migrate_untracked_files(dataset_id, dataset_name, roots, dry_run)
    moved += untracked_counts["moved"]
    skipped += untracked_counts["skipped"]
    failed += untracked_counts["failed"]

    if not dry_run:
        new_root = fm.get_dataset_root(dataset_id, dataset_name)
        for root in roots:
            _remove_empty_dirs(root)
            if root != new_root and root.exists() and not any(root.rglob("*")):
                root.rmdir()

    logger.info(
        "[MIGRATE] dataset_id=%d selesai: moved=%d skipped=%d failed=%d",
        dataset_id, moved, skipped, failed,
    )
    return {"dataset_id": dataset_id, "moved": moved, "skipped": skipped, "failed": failed}


def _remove_empty_dirs(root: Path) -> None:
    if not root.exists():
        return
    for d in sorted(root.rglob("*"), reverse=True):
        if d.is_dir():
            try:
                d.rmdir()
            except OSError:
                pass  # tidak kosong, biarkan


def _all_dataset_ids(db: DatabaseClient) -> list[int]:
    with db.session() as sess:
        return list(sess.scalars(select(Dataset.dataset_id).order_by(Dataset.dataset_id)).all())


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Migrasi data/datasets/... ke layout "
            "{id}_{slug}/{tier}/{source}/{scene}/ + fusion/{scene}/"
        )
    )
    parser.add_argument("--dataset-id", type=int, default=None, help="Migrasi satu dataset saja")
    parser.add_argument(
        "--dry-run", action="store_true", help="Tampilkan rencana tanpa memindahkan file"
    )
    args = parser.parse_args()

    _configure_logging()
    db = DatabaseClient.from_env()

    dataset_ids = [args.dataset_id] if args.dataset_id is not None else _all_dataset_ids(db)
    logger.info("[MIGRATE] mulai migrasi dataset_ids=%s dry_run=%s", dataset_ids, args.dry_run)

    totals = {"moved": 0, "skipped": 0, "failed": 0}
    for dataset_id in dataset_ids:
        result = migrate_dataset(db, dataset_id, dry_run=args.dry_run)
        for k in totals:
            totals[k] += result[k]

    logger.info("[MIGRATE] selesai semua dataset: %s", totals)
    if totals["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
