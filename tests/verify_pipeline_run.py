# tests/verify_pipeline_run.py
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from etl.database_client import DatabaseClient
from etl.lineage_tracker import LineageTracker
from etl.metadata_manager import MetadataManager


def verify_scene(scene_id: int) -> bool:
    db = DatabaseClient.from_env()
    meta = MetadataManager(db)
    lineage = LineageTracker(db)
    ok = True

    status = meta.get_pipeline_status(scene_id)
    for stage in status:
        state = stage["status"]
        print(f"[STAGE] {stage['stage_name']:<20} {state}")
        if state != "SUCCESS":
            ok = False
        if state == "FAILED" and stage.get("error_message"):
            print(f"        error: {stage['error_message']}")

    products = meta.get_products_by_scene(scene_id, tier="GOLD")
    if len(products) < 1:
        print(f"[FAIL] expected 1 GOLD fusion product, found {len(products)}")
        ok = False

    for p in products:
        path = p["file_path"]
        if not Path(path).exists():
            print(f"[FAIL] file missing on disk: {path}")
            ok = False
            continue
        result = lineage.verify_integrity(p["product_id"], path)
        if not result["integrity_ok"]:
            print(f"[FAIL] hash mismatch: {path}")
            ok = False
        else:
            print(f"[OK] {p['band_name']} hash verified, {result['file_size_mb']} MB")

    metrics = meta.get_quality_by_scene(scene_id)
    if len(metrics) < 2:
        print(f"[FAIL] expected quality metrics for 2 bands, found {len(metrics)}")
        ok = False
    for m in metrics:
        print(f"[QUALITY] {m['band_name']} score={m['quality_score']} flag={m['quality_flag']}")

    db.dispose()
    return ok


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m tests.verify_pipeline_run <scene_id>")
        sys.exit(1)
    result = verify_scene(int(sys.argv[1]))
    sys.exit(0 if result else 1)